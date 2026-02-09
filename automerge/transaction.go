package automerge

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/cjanietz/automerge-native-go/internal/changegraph"
	"github.com/cjanietz/automerge-native-go/internal/model"
	"github.com/cjanietz/automerge-native-go/internal/opset"
)

var (
	ErrTransactionClosed = errors.New("transaction is closed")
	ErrMarkNotOpen       = errors.New("mark not open")
	ErrMarkNotClosed     = errors.New("mark not closed")
	ErrInvalidMarkRange  = errors.New("invalid mark range")
)

type CommitOptions struct {
	Message *string
	Time    *int64
}

type txCheckpoint struct {
	actor   uint32
	seq     uint64
	startOp uint64
	deps    []model.ChangeHash
}

type txMutation interface {
	toChangeOp(opid model.OpID) ChangeOperation
	apply(ops *opset.OpSet, opid model.OpID, actor uint32, seq uint64) error
	opCount() uint64
}

type Transaction struct {
	doc *Document

	closed bool
	cp     txCheckpoint
	ops    []txMutation
	openMk []openMark
}

type openMark struct {
	obj   model.ObjID
	start int
	name  string
	value model.ScalarValue
}

func newTransaction(doc *Document) *Transaction {
	actor := doc.actor
	seq := doc.graph.SeqForActor(actor) + 1
	startOp := doc.graph.MaxOp() + 1
	deps := doc.dependenciesForActorSeq(actor, seq)
	return &Transaction{
		doc: doc,
		cp: txCheckpoint{
			actor:   actor,
			seq:     seq,
			startOp: startOp,
			deps:    deps,
		},
	}
}

func (tx *Transaction) ensureOpen() error {
	if tx.closed {
		return ErrTransactionClosed
	}
	return nil
}

func (tx *Transaction) Put(obj model.ObjID, key string, value model.ScalarValue) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	tx.ops = append(tx.ops, putMutation{obj: obj, key: key, value: value})
	return nil
}

func (tx *Transaction) PutObject(obj model.ObjID, key string, typ ObjType) (model.ObjID, error) {
	if err := tx.ensureOpen(); err != nil {
		return model.ObjID{}, err
	}
	objID := model.ObjID{Op: tx.nextOpIDForNextMutation()}
	tx.ops = append(tx.ops, putObjectMutation{obj: obj, key: key, typ: typ, child: objID})
	return objID, nil
}

func (tx *Transaction) Insert(obj model.ObjID, index int, value model.ScalarValue) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	tx.ops = append(tx.ops, insertMutation{obj: obj, index: index, value: value})
	return nil
}

func (tx *Transaction) InsertObject(obj model.ObjID, index int, typ ObjType) (model.ObjID, error) {
	if err := tx.ensureOpen(); err != nil {
		return model.ObjID{}, err
	}
	objID := model.ObjID{Op: tx.nextOpIDForNextMutation()}
	tx.ops = append(tx.ops, insertObjectMutation{obj: obj, index: index, typ: typ, child: objID})
	return objID, nil
}

func (tx *Transaction) DeleteMap(obj model.ObjID, key string) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	tx.ops = append(tx.ops, deleteMapMutation{obj: obj, key: key})
	return nil
}

func (tx *Transaction) DeleteList(obj model.ObjID, index int) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	tx.ops = append(tx.ops, deleteListMutation{obj: obj, index: index})
	return nil
}

func (tx *Transaction) Increment(obj model.ObjID, key string, by int64) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	tx.ops = append(tx.ops, incrementMutation{obj: obj, key: key, by: by})
	return nil
}

func (tx *Transaction) SpliceText(obj model.ObjID, index int, deleteCount int, insert string) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if deleteCount == 0 && insert == "" {
		return nil
	}
	tx.ops = append(tx.ops, spliceTextMutation{obj: obj, index: index, deleteCount: deleteCount, insert: insert})
	return nil
}

func (tx *Transaction) Mark(obj model.ObjID, start int, end int, name string, value model.ScalarValue) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if start < 0 || end < start {
		return ErrInvalidMarkRange
	}
	tx.ops = append(tx.ops, markMutation{obj: obj, start: start, end: end, name: name, value: value})
	return nil
}

func (tx *Transaction) MarkBegin(obj model.ObjID, index int, name string, value model.ScalarValue) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if index < 0 {
		return ErrInvalidMarkRange
	}
	tx.openMk = append(tx.openMk, openMark{obj: obj, start: index, name: name, value: value})
	return nil
}

func (tx *Transaction) MarkEnd(obj model.ObjID, index int, name string) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if index < 0 {
		return ErrInvalidMarkRange
	}
	for i := len(tx.openMk) - 1; i >= 0; i-- {
		m := tx.openMk[i]
		if m.obj != obj || m.name != name {
			continue
		}
		if index < m.start {
			return ErrInvalidMarkRange
		}
		tx.openMk = append(tx.openMk[:i], tx.openMk[i+1:]...)
		tx.ops = append(tx.ops, markMutation{obj: obj, start: m.start, end: index, name: name, value: m.value})
		return nil
	}
	return ErrMarkNotOpen
}

func (tx *Transaction) Commit() (*Change, error) {
	return tx.CommitWith(CommitOptions{})
}

func (tx *Transaction) CommitWith(opts CommitOptions) (*Change, error) {
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	if len(tx.openMk) > 0 {
		return nil, ErrMarkNotClosed
	}
	if len(tx.ops) == 0 {
		tx.closed = true
		tx.doc.transactionClosed(tx)
		return nil, nil
	}

	changeOps := make([]ChangeOperation, 0, len(tx.ops))
	offset := uint64(0)
	for _, m := range tx.ops {
		opid := model.OpID{Counter: tx.cp.startOp + offset, Actor: tx.cp.actor}
		seq := opid.Counter
		if err := m.apply(tx.doc.ops, opid, tx.cp.actor, seq); err != nil {
			return nil, err
		}
		changeOps = append(changeOps, m.toChangeOp(opid))
		offset += m.opCount()
	}
	if offset == 0 {
		tx.closed = true
		tx.doc.transactionClosed(tx)
		return nil, nil
	}

	maxOp := tx.cp.startOp + offset - 1
	hash := computeChangeHash(tx.cp.actor, tx.cp.seq, tx.cp.startOp, maxOp, tx.cp.deps, opts, changeOps)

	if err := tx.doc.graph.AddChange(changegraph.ChangeMeta{
		Hash:  hash,
		Deps:  tx.cp.deps,
		Actor: tx.cp.actor,
		Seq:   tx.cp.seq,
		MaxOp: maxOp,
	}); err != nil {
		return nil, fmt.Errorf("add change to graph: %w", err)
	}

	change := &Change{
		Hash:       hash,
		Actor:      tx.cp.actor,
		Seq:        tx.cp.seq,
		StartOp:    tx.cp.startOp,
		MaxOp:      maxOp,
		Deps:       append([]model.ChangeHash(nil), tx.cp.deps...),
		Message:    opts.Message,
		Time:       opts.Time,
		Operations: changeOps,
	}
	dcop := *change
	dcop.Deps = append([]model.ChangeHash(nil), change.Deps...)
	dcop.Operations = append([]ChangeOperation(nil), change.Operations...)
	tx.doc.changes[change.Hash] = dcop
	tx.doc.clearLegacyRaw()
	tx.doc.last = change

	tx.closed = true
	tx.doc.transactionClosed(tx)
	return change, nil
}

func (tx *Transaction) Rollback() error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	tx.ops = nil
	tx.openMk = nil
	tx.closed = true
	tx.doc.transactionClosed(tx)
	return nil
}

func (tx *Transaction) nextOpIDForNextMutation() model.OpID {
	return model.OpID{Counter: tx.cp.startOp + tx.totalMutationOpCount(), Actor: tx.cp.actor}
}

func (tx *Transaction) totalMutationOpCount() uint64 {
	var total uint64
	for _, m := range tx.ops {
		total += m.opCount()
	}
	return total
}

type putMutation struct {
	obj   model.ObjID
	key   string
	value model.ScalarValue
}

func (m putMutation) toChangeOp(opid model.OpID) ChangeOperation {
	return ChangeOperation{Kind: OpPut, ObjID: m.obj, Key: m.key, Value: m.value, OpID: opid}
}

func (m putMutation) apply(ops *opset.OpSet, opid model.OpID, actor uint32, seq uint64) error {
	return ops.PutMap(m.obj, m.key, opset.NewScalarValue(m.value), opid, actor, seq)
}
func (m putMutation) opCount() uint64 { return 1 }

type putObjectMutation struct {
	obj   model.ObjID
	key   string
	typ   ObjType
	child model.ObjID
}

func (m putObjectMutation) toChangeOp(opid model.OpID) ChangeOperation {
	return ChangeOperation{
		Kind:       OpPutObject,
		ObjID:      m.obj,
		ChildObjID: m.child,
		Key:        m.key,
		ObjType:    m.typ,
		OpID:       opid,
	}
}

func (m putObjectMutation) apply(ops *opset.OpSet, opid model.OpID, actor uint32, seq uint64) error {
	ops.CreateObject(m.child, opset.ObjType(m.typ))
	return ops.PutMap(m.obj, m.key, opset.NewObjectValue(m.child, opset.ObjType(m.typ)), opid, actor, seq)
}
func (m putObjectMutation) opCount() uint64 { return 1 }

type insertMutation struct {
	obj   model.ObjID
	index int
	value model.ScalarValue
}

func (m insertMutation) toChangeOp(opid model.OpID) ChangeOperation {
	return ChangeOperation{Kind: OpInsert, ObjID: m.obj, Index: m.index, Value: m.value, OpID: opid}
}

func (m insertMutation) apply(ops *opset.OpSet, opid model.OpID, actor uint32, seq uint64) error {
	return ops.InsertList(m.obj, m.index, opset.NewScalarValue(m.value), opid, actor, seq)
}
func (m insertMutation) opCount() uint64 { return 1 }

type insertObjectMutation struct {
	obj   model.ObjID
	index int
	typ   ObjType
	child model.ObjID
}

func (m insertObjectMutation) toChangeOp(opid model.OpID) ChangeOperation {
	return ChangeOperation{
		Kind:       OpInsertObject,
		ObjID:      m.obj,
		ChildObjID: m.child,
		Index:      m.index,
		ObjType:    m.typ,
		OpID:       opid,
	}
}

func (m insertObjectMutation) apply(ops *opset.OpSet, opid model.OpID, actor uint32, seq uint64) error {
	ops.CreateObject(m.child, opset.ObjType(m.typ))
	return ops.InsertList(m.obj, m.index, opset.NewObjectValue(m.child, opset.ObjType(m.typ)), opid, actor, seq)
}
func (m insertObjectMutation) opCount() uint64 { return 1 }

type deleteMapMutation struct {
	obj model.ObjID
	key string
}

func (m deleteMapMutation) toChangeOp(opid model.OpID) ChangeOperation {
	return ChangeOperation{Kind: OpDeleteMap, ObjID: m.obj, Key: m.key, OpID: opid}
}

func (m deleteMapMutation) apply(ops *opset.OpSet, opid model.OpID, actor uint32, seq uint64) error {
	return ops.DeleteMap(m.obj, m.key, opid, actor, seq)
}
func (m deleteMapMutation) opCount() uint64 { return 1 }

type deleteListMutation struct {
	obj   model.ObjID
	index int
}

func (m deleteListMutation) toChangeOp(opid model.OpID) ChangeOperation {
	return ChangeOperation{Kind: OpDeleteList, ObjID: m.obj, Index: m.index, OpID: opid}
}

func (m deleteListMutation) apply(ops *opset.OpSet, opid model.OpID, actor uint32, seq uint64) error {
	return ops.DeleteList(m.obj, m.index, opid, actor, seq)
}
func (m deleteListMutation) opCount() uint64 { return 1 }

type incrementMutation struct {
	obj model.ObjID
	key string
	by  int64
}

func (m incrementMutation) toChangeOp(opid model.OpID) ChangeOperation {
	return ChangeOperation{Kind: OpIncrement, ObjID: m.obj, Key: m.key, By: m.by, OpID: opid}
}

func (m incrementMutation) apply(ops *opset.OpSet, opid model.OpID, actor uint32, seq uint64) error {
	return ops.IncrementMapCounter(m.obj, m.key, m.by, opid, actor, seq)
}
func (m incrementMutation) opCount() uint64 { return 1 }

type spliceTextMutation struct {
	obj         model.ObjID
	index       int
	deleteCount int
	insert      string
}

func (m spliceTextMutation) toChangeOp(opid model.OpID) ChangeOperation {
	return ChangeOperation{
		Kind:        OpSpliceText,
		ObjID:       m.obj,
		Index:       m.index,
		DeleteCount: m.deleteCount,
		InsertText:  m.insert,
		OpID:        opid,
	}
}

func (m spliceTextMutation) apply(ops *opset.OpSet, opid model.OpID, actor uint32, seq uint64) error {
	start := seq
	if start > 0 {
		start--
	}
	_, err := ops.SpliceText(m.obj, m.index, m.deleteCount, m.insert, actor, start)
	return err
}
func (m spliceTextMutation) opCount() uint64 {
	return uint64(m.deleteCount + utf8.RuneCountInString(m.insert))
}

type markMutation struct {
	obj   model.ObjID
	start int
	end   int
	name  string
	value model.ScalarValue
}

func (m markMutation) toChangeOp(opid model.OpID) ChangeOperation {
	return ChangeOperation{
		Kind:     OpMark,
		ObjID:    m.obj,
		Start:    m.start,
		End:      m.end,
		MarkName: m.name,
		Value:    m.value,
		OpID:     opid,
	}
}

func (m markMutation) apply(ops *opset.OpSet, opid model.OpID, actor uint32, seq uint64) error {
	return ops.AddMark(m.obj, m.start, m.end, m.name, m.value, opid, actor, seq)
}

func (m markMutation) opCount() uint64 { return 1 }
