package protocol

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
)

// kafkaHandler speaks the legacy Kafka Produce API, version 0
// (https://kafka.apache.org/protocol#The_Messages_Produce), by hand
// over a raw net.Conn. Version 0 is deliberately the target: it uses
// the old fixed-width message format with a plain CRC32 checksum
// (via the standard library's hash/crc32), instead of the newer
// RecordBatch format, which needs zigzag varints and CRC32C. Lower-risk
// to hand-roll correctly, and brokers still accept it for compatibility.
//
// Deliberately out of scope: consuming, compression, TLS/SASL, and
// cluster metadata discovery -- opts.URL is assumed to be the address
// of the partition-0 leader directly (true for a typical single-broker
// dev/test setup; a real multi-broker cluster would need a Metadata
// request first to find the leader).
//
//	opts.URL   broker address, e.g. localhost:9092
//	opts.Topic topic name (via -topic)
//	opts.Data  message value (no key)
type kafkaHandler struct{}

func init() {
	Register(&kafkaHandler{})
}

func (k *kafkaHandler) Name() string { return "kafka" }

func (k *kafkaHandler) Execute(opts *RequestOptions) (*Response, error) {
	if opts.URL == "" {
		return nil, fmt.Errorf("kafka: no broker given (use -u host:port)")
	}
	if opts.Topic == "" {
		return nil, fmt.Errorf("kafka: no topic given (use -topic)")
	}

	conn, err := net.DialTimeout("tcp", opts.URL, timeoutOrDefault(opts.Timeout))
	if err != nil {
		return nil, fmt.Errorf("kafka: dial %s: %w", opts.URL, err)
	}
	defer conn.Close()

	req := buildProduceRequestV0(opts.Topic, []byte(opts.Data))
	if err := writeKafkaFrame(conn, req); err != nil {
		return nil, fmt.Errorf("kafka: sending produce request: %w", err)
	}

	respBody, err := readKafkaFrame(conn)
	if err != nil {
		return nil, fmt.Errorf("kafka: reading produce response: %w", err)
	}

	partition, errCode, offset, err := parseProduceResponseV0(respBody)
	if err != nil {
		return nil, fmt.Errorf("kafka: %w", err)
	}
	if errCode != 0 {
		return nil, fmt.Errorf("kafka: broker returned error code %d for topic %q partition %d", errCode, opts.Topic, partition)
	}

	return &Response{
		StatusLine: fmt.Sprintf("produced to topic %q partition %d offset %d", opts.Topic, partition, offset),
		Headers:    map[string][]string{},
		Body:       []byte(fmt.Sprintf("ok: %d bytes produced to %s (partition %d, offset %d)\n", len(opts.Data), opts.Topic, partition, offset)),
	}, nil
}

// buildProduceRequestV0 builds a full Kafka request: 4-byte size prefix
// is added by writeKafkaFrame, this returns everything after it.
func buildProduceRequestV0(topic string, value []byte) []byte {
	message := buildMessageV0(value)

	messageSet := make([]byte, 0, 12+len(message))
	messageSet = appendInt64(messageSet, 0) // offset (ignored by broker on produce)
	messageSet = appendInt32(messageSet, int32(len(message)))
	messageSet = append(messageSet, message...)

	body := make([]byte, 0, 32+len(topic)+len(messageSet))
	body = appendInt16(body, 1)  // RequiredAcks: 1 = leader ack only
	body = appendInt32(body, 5000) // Timeout (ms)
	body = appendInt32(body, 1)  // one topic
	body = appendString(body, topic)
	body = appendInt32(body, 1) // one partition
	body = appendInt32(body, 0) // partition 0
	body = appendInt32(body, int32(len(messageSet)))
	body = append(body, messageSet...)

	header := make([]byte, 0, 16)
	header = appendInt16(header, 0)  // ApiKey: 0 = Produce
	header = appendInt16(header, 0)  // ApiVersion: 0
	header = appendInt32(header, 1)  // CorrelationId
	header = appendString(header, "eurl")

	return append(header, body...)
}

func buildMessageV0(value []byte) []byte {
	// Everything from magicByte onward is covered by the CRC.
	rest := make([]byte, 0, 6+len(value))
	rest = append(rest, 0) // magic byte: message format v0
	rest = append(rest, 0) // attributes: no compression
	rest = appendBytes(rest, nil) // key: null
	rest = appendBytes(rest, value)

	crc := crc32.ChecksumIEEE(rest)
	out := make([]byte, 0, 4+len(rest))
	out = appendInt32(out, int32(crc))
	out = append(out, rest...)
	return out
}

// parseProduceResponseV0 reads the first (and, for our single-topic,
// single-partition request, only) partition result.
func parseProduceResponseV0(b []byte) (partition int32, errCode int16, offset int64, err error) {
	pos := 0
	readInt32 := func() int32 {
		v := int32(binary.BigEndian.Uint32(b[pos:]))
		pos += 4
		return v
	}
	readInt16 := func() int16 {
		v := int16(binary.BigEndian.Uint16(b[pos:]))
		pos += 2
		return v
	}
	readInt64 := func() int64 {
		v := int64(binary.BigEndian.Uint64(b[pos:]))
		pos += 8
		return v
	}

	if len(b) < 8 {
		return 0, 0, 0, fmt.Errorf("malformed produce response (too short)")
	}
	pos += 4 // correlation id, ignored
	topicCount := readInt32()
	if topicCount < 1 {
		return 0, 0, 0, fmt.Errorf("broker reported no topics in produce response")
	}
	topicNameLen := readInt16()
	pos += int(topicNameLen) // skip topic name
	partitionCount := readInt32()
	if partitionCount < 1 {
		return 0, 0, 0, fmt.Errorf("broker reported no partitions in produce response")
	}
	partition = readInt32()
	errCode = readInt16()
	offset = readInt64()
	return partition, errCode, offset, nil
}

// ---- Kafka wire helpers ----------------------------------------------------

func writeKafkaFrame(conn net.Conn, payload []byte) error {
	buf := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	copy(buf[4:], payload)
	_, err := conn.Write(buf)
	return err
}

func readKafkaFrame(conn net.Conn) ([]byte, error) {
	sizeBuf := make([]byte, 4)
	if _, err := readFull(conn, sizeBuf); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(sizeBuf)
	payload := make([]byte, size)
	if _, err := readFull(conn, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func appendInt16(b []byte, v int16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(v))
	return append(b, buf...)
}

func appendInt32(b []byte, v int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(v))
	return append(b, buf...)
}

func appendInt64(b []byte, v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return append(b, buf...)
}

// appendString writes a Kafka "string": int16 length + bytes.
func appendString(b []byte, s string) []byte {
	b = appendInt16(b, int16(len(s)))
	return append(b, []byte(s)...)
}

// appendBytes writes a Kafka "bytes" field: int32 length (-1 for null) + bytes.
func appendBytes(b []byte, v []byte) []byte {
	if v == nil {
		return appendInt32(b, -1)
	}
	b = appendInt32(b, int32(len(v)))
	return append(b, v...)
}
