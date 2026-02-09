package automerge

import (
	"github.com/cjanietz/automerge-native-go/internal/model"
)

type ChangeOperationKind uint8

const (
	OpPut ChangeOperationKind = iota
	OpPutObject
	OpInsert
	OpInsertObject
	OpDeleteMap
	OpDeleteList
	OpIncrement
	OpSpliceText
	OpMark
)

type ChangeOperation struct {
	Kind  ChangeOperationKind
	ObjID model.ObjID
	// ChildObjID is set for object-creation operations.
	ChildObjID model.ObjID
	Key        string
	Index      int

	Value   model.ScalarValue
	ObjType ObjType
	By      int64

	DeleteCount int
	InsertText  string

	Start    int
	End      int
	MarkName string

	OpID model.OpID
}

type Change struct {
	Hash    model.ChangeHash
	Actor   uint32
	Seq     uint64
	StartOp uint64
	MaxOp   uint64
	Deps    []model.ChangeHash

	Message *string
	Time    *int64

	Operations []ChangeOperation
}
