package protocol

import (
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// rabbitmqHandler speaks just enough AMQP 0-9-1
// (https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf), by hand
// over a raw net.Conn, to open a connection/channel, optionally
// declare a durable queue, and publish one message to it via the
// default exchange. No AMQP client library is used.
//
// Deliberately out of scope: consuming, TLS, SASL mechanisms other
// than PLAIN, publisher confirms, and a graceful close handshake (the
// socket is just closed after publishing -- brokers tolerate that for
// a one-shot publish, they'll just log a connection reset).
//
//	opts.URL   amqp://user:pass@host:port/vhost  (defaults: guest/guest, port 5672, vhost "/")
//	opts.Topic queue name (via -queue/-topic)
//	opts.Data  message body
type rabbitmqHandler struct{}

func init() {
	Register(&rabbitmqHandler{})
}

func (r *rabbitmqHandler) Name() string { return "rabbitmq" }

func (r *rabbitmqHandler) Execute(opts *RequestOptions) (*Response, error) {
	if opts.URL == "" {
		return nil, fmt.Errorf("rabbitmq: no broker URL given (use -u amqp://user:pass@host:port/)")
	}
	if opts.Topic == "" {
		return nil, fmt.Errorf("rabbitmq: no queue given (use -queue or -topic)")
	}

	addr, user, pass, vhost, err := parseAMQPURL(opts.URL)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: %w", err)
	}

	conn, err := net.DialTimeout("tcp", addr, timeoutOrDefault(opts.Timeout))
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: dial %s: %w", addr, err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("AMQP\x00\x00\x09\x01")); err != nil {
		return nil, fmt.Errorf("rabbitmq: protocol header: %w", err)
	}

	// Connection.Start (server -> client): just drain it, we assume
	// PLAIN auth is offered, which every stock RabbitMQ does.
	if _, _, _, err := readAMQPFrame(conn); err != nil {
		return nil, fmt.Errorf("rabbitmq: waiting for Connection.Start: %w", err)
	}

	// Connection.StartOk
	startOk := amqpEmptyFieldTable()
	startOk = append(startOk, amqpShortStr("PLAIN")...)
	startOk = append(startOk, amqpLongStr("\x00"+user+"\x00"+pass)...)
	startOk = append(startOk, amqpShortStr("en_US")...)
	if err := writeAMQPMethod(conn, 0, 10, 11, startOk); err != nil {
		return nil, fmt.Errorf("rabbitmq: Connection.StartOk: %w", err)
	}

	// Connection.Tune (server -> client)
	_, _, tunePayload, err := readAMQPFrame(conn)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: waiting for Connection.Tune: %w", err)
	}
	if len(tunePayload) < 12 {
		return nil, fmt.Errorf("rabbitmq: malformed Connection.Tune from server")
	}
	channelMax := tunePayload[4:6]
	frameMax := tunePayload[6:10]

	// Connection.TuneOk: echo channel-max/frame-max, disable heartbeats.
	tuneOk := append([]byte{}, channelMax...)
	tuneOk = append(tuneOk, frameMax...)
	tuneOk = append(tuneOk, 0, 0) // heartbeat = 0
	if err := writeAMQPMethod(conn, 0, 10, 31, tuneOk); err != nil {
		return nil, fmt.Errorf("rabbitmq: Connection.TuneOk: %w", err)
	}

	// Connection.Open
	openArgs := amqpShortStr(vhost)
	openArgs = append(openArgs, amqpShortStr("")...) // reserved capabilities
	openArgs = append(openArgs, 0)                   // reserved insist bit
	if err := writeAMQPMethod(conn, 0, 10, 40, openArgs); err != nil {
		return nil, fmt.Errorf("rabbitmq: Connection.Open: %w", err)
	}
	if _, _, _, err := readAMQPFrame(conn); err != nil { // Connection.OpenOk
		return nil, fmt.Errorf("rabbitmq: waiting for Connection.OpenOk (check credentials/vhost): %w", err)
	}

	// Channel.Open on channel 1
	if err := writeAMQPMethod(conn, 1, 20, 10, amqpShortStr("")); err != nil {
		return nil, fmt.Errorf("rabbitmq: Channel.Open: %w", err)
	}
	if _, _, _, err := readAMQPFrame(conn); err != nil { // Channel.OpenOk
		return nil, fmt.Errorf("rabbitmq: waiting for Channel.OpenOk: %w", err)
	}

	// Queue.Declare (durable), channel 1
	declArgs := []byte{0, 0} // reserved "ticket"
	declArgs = append(declArgs, amqpShortStr(opts.Topic)...)
	declArgs = append(declArgs, 0x02) // bits: passive=0 durable=1 exclusive=0 autoDelete=0 noWait=0
	declArgs = append(declArgs, amqpEmptyFieldTable()...)
	if err := writeAMQPMethod(conn, 1, 50, 10, declArgs); err != nil {
		return nil, fmt.Errorf("rabbitmq: Queue.Declare: %w", err)
	}
	if _, _, _, err := readAMQPFrame(conn); err != nil { // Queue.DeclareOk
		return nil, fmt.Errorf("rabbitmq: waiting for Queue.DeclareOk: %w", err)
	}

	// Basic.Publish (default exchange, routing key = queue name)
	pubArgs := []byte{0, 0} // reserved "ticket"
	pubArgs = append(pubArgs, amqpShortStr("")...)
	pubArgs = append(pubArgs, amqpShortStr(opts.Topic)...)
	pubArgs = append(pubArgs, 0) // bits: mandatory=0 immediate=0
	if err := writeAMQPMethod(conn, 1, 60, 40, pubArgs); err != nil {
		return nil, fmt.Errorf("rabbitmq: Basic.Publish: %w", err)
	}

	body := []byte(opts.Data)
	header := make([]byte, 14)
	binary.BigEndian.PutUint16(header[0:2], 60) // class id: basic
	binary.BigEndian.PutUint16(header[2:4], 0)  // weight
	binary.BigEndian.PutUint64(header[4:12], uint64(len(body)))
	binary.BigEndian.PutUint16(header[12:14], 0) // property flags: none set
	if err := writeAMQPFrame(conn, 2, 1, header); err != nil {
		return nil, fmt.Errorf("rabbitmq: content header: %w", err)
	}

	const chunk = 4096 // conservative; below AMQP's required minimum frame size
	for i := 0; i < len(body); i += chunk {
		end := i + chunk
		if end > len(body) {
			end = len(body)
		}
		if err := writeAMQPFrame(conn, 3, 1, body[i:end]); err != nil {
			return nil, fmt.Errorf("rabbitmq: content body: %w", err)
		}
	}

	return &Response{
		StatusLine: fmt.Sprintf("published to queue %q", opts.Topic),
		Headers:    map[string][]string{},
		Body:       []byte(fmt.Sprintf("ok: %d bytes published to %s\n", len(body), opts.Topic)),
	}, nil
}

func parseAMQPURL(raw string) (addr, user, pass, vhost string, err error) {
	u, perr := url.Parse(raw)
	if perr != nil {
		return "", "", "", "", fmt.Errorf("invalid AMQP URL: %w", perr)
	}
	host := u.Hostname()
	if host == "" {
		host = raw // allow a bare host:port with no amqp:// scheme
		port := "5672"
		if strings.Contains(host, ":") {
			return host, "guest", "guest", "/", nil
		}
		return host + ":" + port, "guest", "guest", "/", nil
	}
	port := u.Port()
	if port == "" {
		port = "5672"
	}
	user, pass = "guest", "guest"
	if u.User != nil {
		user = u.User.Username()
		if p, ok := u.User.Password(); ok {
			pass = p
		}
	}
	vhost = "/"
	if p := strings.TrimPrefix(u.Path, "/"); p != "" {
		vhost = p
	}
	return host + ":" + port, user, pass, vhost, nil
}

// ---- AMQP framing helpers -------------------------------------------------

func writeAMQPFrame(conn net.Conn, frameType byte, channel uint16, payload []byte) error {
	buf := make([]byte, 7+len(payload)+1)
	buf[0] = frameType
	binary.BigEndian.PutUint16(buf[1:3], channel)
	binary.BigEndian.PutUint32(buf[3:7], uint32(len(payload)))
	copy(buf[7:], payload)
	buf[len(buf)-1] = 0xCE
	_, err := conn.Write(buf)
	return err
}

func writeAMQPMethod(conn net.Conn, channel uint16, classID, methodID uint16, args []byte) error {
	payload := make([]byte, 4+len(args))
	binary.BigEndian.PutUint16(payload[0:2], classID)
	binary.BigEndian.PutUint16(payload[2:4], methodID)
	copy(payload[4:], args)
	return writeAMQPFrame(conn, 1, channel, payload)
}

func readAMQPFrame(conn net.Conn) (frameType byte, channel uint16, payload []byte, err error) {
	head := make([]byte, 7)
	if _, err = readFull(conn, head); err != nil {
		return 0, 0, nil, err
	}
	frameType = head[0]
	channel = binary.BigEndian.Uint16(head[1:3])
	size := binary.BigEndian.Uint32(head[3:7])
	payload = make([]byte, size)
	if _, err = readFull(conn, payload); err != nil {
		return 0, 0, nil, err
	}
	end := make([]byte, 1)
	if _, err = readFull(conn, end); err != nil {
		return 0, 0, nil, err
	}
	if end[0] != 0xCE {
		return 0, 0, nil, fmt.Errorf("malformed AMQP frame (missing end marker)")
	}
	return frameType, channel, payload, nil
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func amqpShortStr(s string) []byte {
	return append([]byte{byte(len(s))}, []byte(s)...)
}

func amqpLongStr(s string) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(len(s)))
	return append(b, []byte(s)...)
}

func amqpEmptyFieldTable() []byte {
	return []byte{0, 0, 0, 0} // 4-byte length prefix, zero entries
}
