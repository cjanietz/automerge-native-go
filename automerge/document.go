package automerge

import (
	"errors"
	"slices"

	"github.com/cjanietz/automerge-native-go/internal/changegraph"
	"github.com/cjanietz/automerge-native-go/internal/model"
	"github.com/cjanietz/automerge-native-go/internal/opset"
)

var (
	ErrTransactionOpen       = errors.New("transaction already open")
	ErrNoOpenTransaction     = errors.New("no open transaction")
	ErrInvalidCurrentActor   = errors.New("invalid current actor")
	ErrNoLastCommittedChange = errors.New("no last committed change")
)

type Document struct {
	graph *changegraph.Graph
	ops   *opset.OpSet

	changes   map[model.ChangeHash]Change
	queue     []Change
	legacyRaw []byte
	saveCache map[saveCacheKey][]byte

	actor uint32
	open  *Transaction
	last  *Change
}

type saveCacheKey struct {
	deflate       bool
	retainOrphans bool
}

func NewDocument() *Document {
	return &Document{
		graph:     changegraph.New(),
		ops:       opset.New(),
		changes:   make(map[model.ChangeHash]Change),
		queue:     nil,
		legacyRaw: nil,
		saveCache: make(map[saveCacheKey][]byte),
		actor:     1,
	}
}

func (d *Document) SetActor(actor uint32) error {
	if actor == 0 {
		return ErrInvalidCurrentActor
	}
	d.actor = actor
	return nil
}

func (d *Document) Actor() uint32 {
	return d.actor
}

func (d *Document) Heads() []model.ChangeHash {
	return d.graph.Heads()
}

func (d *Document) LastChange() (*Change, error) {
	if d.last == nil {
		return nil, ErrNoLastCommittedChange
	}
	cp := *d.last
	cp.Deps = append([]model.ChangeHash(nil), d.last.Deps...)
	cp.Operations = append([]ChangeOperation(nil), d.last.Operations...)
	return &cp, nil
}

func (d *Document) AllChanges() []Change {
	hashes, err := d.graph.GetHashesFromHeads(d.Heads())
	if err != nil {
		return nil
	}
	out := make([]Change, 0, len(hashes))
	for _, h := range hashes {
		c, ok := d.changes[h]
		if !ok {
			continue
		}
		out = append(out, deepCopyChange(c))
	}
	return out
}

func (d *Document) Begin() (*Transaction, error) {
	if d.open != nil {
		return nil, ErrTransactionOpen
	}
	tx := newTransaction(d)
	d.open = tx
	return tx, nil
}

func (d *Document) transactionClosed(tx *Transaction) {
	if d.open == tx {
		d.open = nil
	}
}

func (d *Document) GetMap(obj model.ObjID, key string, at *changegraph.Clock) (opset.Value, bool) {
	return d.ops.GetMap(obj, key, at)
}

func (d *Document) GetMapAt(obj model.ObjID, key string, heads []model.ChangeHash) (opset.Value, bool, error) {
	clk, err := d.clockFromHeads(heads)
	if err != nil {
		return opset.Value{}, false, err
	}
	v, ok := d.ops.GetMap(obj, key, clk)
	return v, ok, nil
}

func (d *Document) GetAllMap(obj model.ObjID, key string, at *changegraph.Clock) []opset.Value {
	return d.ops.GetAllMap(obj, key, at)
}

func (d *Document) GetAllMapAt(obj model.ObjID, key string, heads []model.ChangeHash) ([]opset.Value, error) {
	clk, err := d.clockFromHeads(heads)
	if err != nil {
		return nil, err
	}
	return d.ops.GetAllMap(obj, key, clk), nil
}

func (d *Document) Text(obj model.ObjID, at *changegraph.Clock) string {
	return d.ops.Text(obj, at)
}

func (d *Document) TextAt(obj model.ObjID, heads []model.ChangeHash) (string, error) {
	clk, err := d.clockFromHeads(heads)
	if err != nil {
		return "", err
	}
	return d.ops.Text(obj, clk), nil
}

func (d *Document) Marks(obj model.ObjID, at *changegraph.Clock) []opset.Mark {
	return d.ops.Marks(obj, at)
}

func (d *Document) MarksAt(obj model.ObjID, heads []model.ChangeHash) ([]opset.Mark, error) {
	clk, err := d.clockFromHeads(heads)
	if err != nil {
		return nil, err
	}
	return d.ops.Marks(obj, clk), nil
}

func (d *Document) MarksAtIndex(obj model.ObjID, index int, at *changegraph.Clock) []opset.Mark {
	return d.ops.MarksAtIndex(obj, index, at)
}

func (d *Document) MarksAtIndexAt(obj model.ObjID, index int, heads []model.ChangeHash) ([]opset.Mark, error) {
	clk, err := d.clockFromHeads(heads)
	if err != nil {
		return nil, err
	}
	return d.ops.MarksAtIndex(obj, index, clk), nil
}

func (d *Document) ListRange(obj model.ObjID, start, end int, at *changegraph.Clock) []opset.Value {
	return d.ops.ListRange(obj, start, end, at)
}

func (d *Document) ListRangeAt(obj model.ObjID, start, end int, heads []model.ChangeHash) ([]opset.Value, error) {
	clk, err := d.clockFromHeads(heads)
	if err != nil {
		return nil, err
	}
	return d.ops.ListRange(obj, start, end, clk), nil
}

func (d *Document) ClockForHeads(heads []model.ChangeHash) (changegraph.Clock, error) {
	if len(heads) == 0 {
		heads = d.graph.Heads()
	}
	return d.graph.ClockForHeads(heads)
}

func (d *Document) KeysMapAt(obj model.ObjID, heads []model.ChangeHash) ([]string, error) {
	clk, err := d.clockFromHeads(heads)
	if err != nil {
		return nil, err
	}
	return d.ops.KeysMap(obj, clk), nil
}

func (d *Document) ValuesMapAt(obj model.ObjID, heads []model.ChangeHash) ([]opset.Value, error) {
	clk, err := d.clockFromHeads(heads)
	if err != nil {
		return nil, err
	}
	return d.ops.ValuesMap(obj, clk), nil
}

func (d *Document) IterMapAt(obj model.ObjID, heads []model.ChangeHash) ([]struct {
	Key   string
	Value opset.Value
}, error) {
	clk, err := d.clockFromHeads(heads)
	if err != nil {
		return nil, err
	}
	return d.ops.IterMap(obj, clk), nil
}

func (d *Document) dependenciesForActorSeq(actor uint32, seq uint64) []model.ChangeHash {
	deps := d.graph.Heads()
	if seq > 1 {
		if prev, ok := d.graph.HashForActorSeq(actor, seq-1); ok && !slices.Contains(deps, prev) {
			deps = append(deps, prev)
			model.SortChangeHashes(deps)
		}
	}
	return deps
}

func (d *Document) hasChange(hash model.ChangeHash) bool {
	return d.graph.HasChange(hash)
}

func (d *Document) clearLegacyRaw() {
	d.legacyRaw = nil
	clear(d.saveCache)
}
