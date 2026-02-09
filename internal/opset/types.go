package opset

import (
	"fmt"

	"github.com/cjanietz/automerge-native-go/internal/model"
)

type ObjType uint8

const (
	ObjMap ObjType = iota
	ObjList
	ObjText
)

func (t ObjType) String() string {
	switch t {
	case ObjMap:
		return "map"
	case ObjList:
		return "list"
	case ObjText:
		return "text"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(t))
	}
}

type ValueKind uint8

const (
	ValueScalar ValueKind = iota
	ValueObject
)

type ObjectValue struct {
	ID   model.ObjID
	Type ObjType
}

type Value struct {
	Kind   ValueKind
	Scalar model.ScalarValue
	Object ObjectValue
}

func NewScalarValue(v model.ScalarValue) Value {
	return Value{Kind: ValueScalar, Scalar: v}
}

func NewObjectValue(id model.ObjID, typ ObjType) Value {
	return Value{Kind: ValueObject, Object: ObjectValue{ID: id, Type: typ}}
}

func (v Value) Equal(other Value) bool {
	if v.Kind != other.Kind {
		return false
	}
	if v.Kind == ValueScalar {
		return v.Scalar.Equal(other.Scalar)
	}
	return v.Object.ID == other.Object.ID && v.Object.Type == other.Object.Type
}

func (v Value) String() string {
	if v.Kind == ValueObject {
		return fmt.Sprintf("object(%s,%s)", v.Object.ID, v.Object.Type)
	}
	switch v.Scalar.Kind {
	case model.ScalarString:
		return v.Scalar.String
	case model.ScalarInt:
		return fmt.Sprintf("%d", v.Scalar.Int)
	case model.ScalarUint:
		return fmt.Sprintf("%d", v.Scalar.Uint)
	case model.ScalarCounter:
		return fmt.Sprintf("counter(%d)", v.Scalar.Counter)
	case model.ScalarBoolean:
		if v.Scalar.Boolean {
			return "true"
		}
		return "false"
	case model.ScalarNull:
		return "null"
	default:
		return fmt.Sprintf("scalar(kind=%d)", v.Scalar.Kind)
	}
}

type VersionedValue struct {
	OpID  model.OpID
	Actor uint32
	Seq   uint64
	Value Value
}

type Mark struct {
	Start int
	End   int
	Name  string
	Value model.ScalarValue
	OpID  model.OpID
	Actor uint32
	Seq   uint64
}
