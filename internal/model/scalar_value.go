package model

import "bytes"

type ScalarKind uint8

const (
	ScalarNull ScalarKind = iota
	ScalarBytes
	ScalarString
	ScalarInt
	ScalarUint
	ScalarF64
	ScalarCounter
	ScalarTimestamp
	ScalarBoolean
	ScalarUnknown
)

type ScalarValue struct {
	Kind ScalarKind

	Bytes    []byte
	String   string
	Int      int64
	Uint     uint64
	F64      float64
	Counter  int64
	Time     int64
	Boolean  bool
	TypeCode uint8
}

func Null() ScalarValue { return ScalarValue{Kind: ScalarNull} }

func BytesValue(v []byte) ScalarValue {
	cp := make([]byte, len(v))
	copy(cp, v)
	return ScalarValue{Kind: ScalarBytes, Bytes: cp}
}

func StringValue(v string) ScalarValue { return ScalarValue{Kind: ScalarString, String: v} }
func IntValue(v int64) ScalarValue     { return ScalarValue{Kind: ScalarInt, Int: v} }
func UintValue(v uint64) ScalarValue   { return ScalarValue{Kind: ScalarUint, Uint: v} }
func F64Value(v float64) ScalarValue   { return ScalarValue{Kind: ScalarF64, F64: v} }
func CounterValue(v int64) ScalarValue { return ScalarValue{Kind: ScalarCounter, Counter: v} }
func TimestampValue(v int64) ScalarValue {
	return ScalarValue{Kind: ScalarTimestamp, Time: v}
}
func BoolValue(v bool) ScalarValue { return ScalarValue{Kind: ScalarBoolean, Boolean: v} }

func UnknownValue(typeCode uint8, v []byte) ScalarValue {
	cp := make([]byte, len(v))
	copy(cp, v)
	return ScalarValue{Kind: ScalarUnknown, TypeCode: typeCode, Bytes: cp}
}

func (v ScalarValue) Equal(other ScalarValue) bool {
	if v.Kind != other.Kind {
		return false
	}
	switch v.Kind {
	case ScalarNull:
		return true
	case ScalarBytes:
		return bytes.Equal(v.Bytes, other.Bytes)
	case ScalarString:
		return v.String == other.String
	case ScalarInt:
		return v.Int == other.Int
	case ScalarUint:
		return v.Uint == other.Uint
	case ScalarF64:
		return v.F64 == other.F64
	case ScalarCounter:
		return v.Counter == other.Counter
	case ScalarTimestamp:
		return v.Time == other.Time
	case ScalarBoolean:
		return v.Boolean == other.Boolean
	case ScalarUnknown:
		return v.TypeCode == other.TypeCode && bytes.Equal(v.Bytes, other.Bytes)
	default:
		return false
	}
}
