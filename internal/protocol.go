package internal

import "time"

const (
	ADD int = iota
	GET
	GETALL
	DEL
	SCALE
)

type Request struct {
	OP      int
	Kv      KeyValue
	ReplyTo string
}

func NewKeyValue(key string, value string) KeyValue {
	return KeyValue{
		Key: key,
		Value: Value{
			Value: value,
		},
	}
}

func NewKey(key string) KeyValue {
	return KeyValue{
		Key: key,
	}
}

type KeyValue struct {
	Key   string
	Value Value
}

type Value struct {
	Value     string
	Timestamp int64
}

type ByTimestamp []Value

func (t ByTimestamp) Len() int { return len(t) }
func (t ByTimestamp) Less(i, j int) bool {
	return time.UnixMicro(t[i].Timestamp).Before(time.UnixMicro(t[j].Timestamp))
}
func (t ByTimestamp) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

type Metadata struct {
	Scale int64
}
