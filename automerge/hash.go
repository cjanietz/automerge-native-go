package automerge

import (
	"math"
	"sync"

	"github.com/cjanietz/automerge-native-go/internal/model"
)

var changeHasherPool = sync.Pool{
	New: func() any {
		return model.NewChangeHasher()
	},
}

func computeChangeHash(
	actor uint32,
	seq uint64,
	startOp uint64,
	maxOp uint64,
	deps []model.ChangeHash,
	opts CommitOptions,
	ops []ChangeOperation,
) model.ChangeHash {
	h := changeHasherPool.Get().(*model.ChangeHasher)
	h.Reset()
	defer changeHasherPool.Put(h)
	h.WriteString("am-change-v1")
	h.WriteUint64(uint64(actor))
	h.WriteUint64(seq)
	h.WriteUint64(startOp)
	h.WriteUint64(maxOp)
	h.WriteUint64(uint64(len(deps)))
	for _, d := range deps {
		h.WriteChangeHash(d)
	}
	if opts.Message != nil {
		h.WriteBool(true)
		h.WriteString(*opts.Message)
	} else {
		h.WriteBool(false)
	}
	if opts.Time != nil {
		h.WriteBool(true)
		h.WriteInt64(*opts.Time)
	} else {
		h.WriteBool(false)
	}
	h.WriteUint64(uint64(len(ops)))
	for _, op := range ops {
		h.WriteUint64(uint64(op.Kind))
		h.WriteObjID(op.ObjID)
		h.WriteObjID(op.ChildObjID)
		h.WriteString(op.Key)
		h.WriteInt64(int64(op.Index))
		h.WriteInt64(op.By)
		h.WriteInt64(int64(op.DeleteCount))
		h.WriteString(op.InsertText)
		h.WriteInt64(int64(op.Start))
		h.WriteInt64(int64(op.End))
		h.WriteString(op.MarkName)
		h.WriteUint64(uint64(op.ObjType))
		h.WriteOpID(op.OpID)
		h.WriteUint64(uint64(op.Value.Kind))
		switch op.Value.Kind {
		case model.ScalarString:
			h.WriteString(op.Value.String)
		case model.ScalarInt:
			h.WriteInt64(op.Value.Int)
		case model.ScalarUint:
			h.WriteUint64(op.Value.Uint)
		case model.ScalarCounter:
			h.WriteInt64(op.Value.Counter)
		case model.ScalarTimestamp:
			h.WriteInt64(op.Value.Time)
		case model.ScalarBoolean:
			h.WriteBool(op.Value.Boolean)
		case model.ScalarBytes, model.ScalarUnknown:
			h.WriteBytes(op.Value.Bytes)
			h.WriteUint64(uint64(op.Value.TypeCode))
		case model.ScalarF64:
			h.WriteUint64(math.Float64bits(op.Value.F64))
		case model.ScalarNull:
			h.WriteString("null")
		}
	}
	return h.Sum()
}
