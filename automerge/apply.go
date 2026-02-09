package automerge

import (
	"errors"
	"fmt"
	"slices"

	intapply "github.com/cjanietz/automerge-native-go/internal/apply"
	"github.com/cjanietz/automerge-native-go/internal/changegraph"
	"github.com/cjanietz/automerge-native-go/internal/model"
	"github.com/cjanietz/automerge-native-go/internal/opset"
)

var (
	ErrDuplicateSeqNumber = errors.New("duplicate sequence number")
)

func (d *Document) ApplyChanges(changes []Change) error {
	return d.ApplyChangesWithActorMap(changes, nil)
}

func (d *Document) ApplyChangesWithActorMap(changes []Change, actorMap map[uint32]uint32) error {
	ready := make(map[model.ChangeHash]struct{})
	batch := make([]Change, 0, len(changes))

	for _, c := range changes {
		c = remapChangeActors(c, actorMap)
		if d.hasChange(c.Hash) {
			continue
		}
		if existing, ok := d.graph.HashForActorSeq(c.Actor, c.Seq); ok {
			if existing != c.Hash {
				return fmt.Errorf("%w: actor=%d seq=%d", ErrDuplicateSeqNumber, c.Actor, c.Seq)
			}
			continue
		}
		if d.isCausallyReady(c, ready) {
			batch = append(batch, c)
			ready[c.Hash] = struct{}{}
		} else {
			d.queue = append(d.queue, deepCopyChange(c))
		}
	}

	for {
		next, ok := d.popNextCausallyReady(ready)
		if !ok {
			break
		}
		batch = append(batch, next)
		ready[next.Hash] = struct{}{}
	}

	for _, idx := range orderChangeIndicesTopologically(batch) {
		c := batch[idx]
		if d.hasChange(c.Hash) {
			continue
		}
		if err := d.applyOneChange(c); err != nil {
			return err
		}
	}
	return nil
}

func (d *Document) Merge(other *Document) error {
	hashes := d.getChangesAdded(other)
	changes := make([]Change, 0, len(hashes))
	for _, h := range hashes {
		c, ok := other.changes[h]
		if !ok {
			return fmt.Errorf("missing change %s in merge source", h)
		}
		changes = append(changes, c)
	}
	return d.ApplyChangesWithActorMap(changes, nil)
}

func (d *Document) getChangesAdded(other *Document) []model.ChangeHash {
	stack := other.Heads()
	seen := make(map[model.ChangeHash]struct{})
	out := make([]model.ChangeHash, 0)

	for len(stack) > 0 {
		h := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		if d.hasChange(h) {
			continue
		}
		out = append(out, h)
		deps, ok := other.graph.DepsForHash(h)
		if !ok {
			continue
		}
		stack = append(stack, deps...)
	}

	slices.Reverse(out)
	return out
}

func (d *Document) isCausallyReady(c Change, ready map[model.ChangeHash]struct{}) bool {
	for _, dep := range c.Deps {
		if d.hasChange(dep) {
			continue
		}
		if _, ok := ready[dep]; ok {
			continue
		}
		return false
	}
	return true
}

func (d *Document) popNextCausallyReady(ready map[model.ChangeHash]struct{}) (Change, bool) {
	for i := 0; i < len(d.queue); i++ {
		c := d.queue[i]
		if !d.isCausallyReady(c, ready) {
			continue
		}
		d.queue[i] = d.queue[len(d.queue)-1]
		d.queue = d.queue[:len(d.queue)-1]
		return c, true
	}
	return Change{}, false
}

func (d *Document) applyOneChange(c Change) error {
	for _, op := range c.Operations {
		if err := applyChangeOperation(d.ops, c.Actor, op); err != nil {
			return err
		}
	}
	if err := d.graph.AddChange(changegraph.ChangeMeta{
		Hash:  c.Hash,
		Deps:  c.Deps,
		Actor: c.Actor,
		Seq:   c.Seq,
		MaxOp: c.MaxOp,
	}); err != nil {
		return fmt.Errorf("graph add change: %w", err)
	}
	cp := deepCopyChange(c)
	d.changes[c.Hash] = cp
	d.clearLegacyRaw()
	d.last = &cp
	return nil
}

func applyChangeOperation(ops *opset.OpSet, actor uint32, op ChangeOperation) error {
	seq := op.OpID.Counter
	switch op.Kind {
	case OpPut:
		return ops.PutMap(op.ObjID, op.Key, opset.NewScalarValue(op.Value), op.OpID, actor, seq)
	case OpPutObject:
		ops.CreateObject(op.ChildObjID, opset.ObjType(op.ObjType))
		return ops.PutMap(op.ObjID, op.Key, opset.NewObjectValue(op.ChildObjID, opset.ObjType(op.ObjType)), op.OpID, actor, seq)
	case OpInsert:
		return ops.InsertList(op.ObjID, op.Index, opset.NewScalarValue(op.Value), op.OpID, actor, seq)
	case OpInsertObject:
		ops.CreateObject(op.ChildObjID, opset.ObjType(op.ObjType))
		return ops.InsertList(op.ObjID, op.Index, opset.NewObjectValue(op.ChildObjID, opset.ObjType(op.ObjType)), op.OpID, actor, seq)
	case OpDeleteMap:
		return ops.DeleteMap(op.ObjID, op.Key, op.OpID, actor, seq)
	case OpDeleteList:
		return ops.DeleteList(op.ObjID, op.Index, op.OpID, actor, seq)
	case OpIncrement:
		return ops.IncrementMapCounter(op.ObjID, op.Key, op.By, op.OpID, actor, seq)
	case OpSpliceText:
		start := seq
		if start > 0 {
			start--
		}
		_, err := ops.SpliceText(op.ObjID, op.Index, op.DeleteCount, op.InsertText, actor, start)
		return err
	case OpMark:
		return ops.AddMark(op.ObjID, op.Start, op.End, op.MarkName, op.Value, op.OpID, actor, seq)
	default:
		return fmt.Errorf("unknown change operation kind %d", op.Kind)
	}
}

func orderChangeIndicesTopologically(in []Change) []int {
	if len(in) <= 1 {
		out := make([]int, len(in))
		for i := range in {
			out[i] = i
		}
		return out
	}
	indexByHash := make(map[model.ChangeHash]int, len(in))
	for i, c := range in {
		indexByHash[c.Hash] = i
	}
	depsCount := make([]int, len(in))
	dependents := make(map[int][]int)
	for i, c := range in {
		for _, dep := range c.Deps {
			if j, ok := indexByHash[dep]; ok {
				depsCount[i]++
				dependents[j] = append(dependents[j], i)
			}
		}
	}
	ready := changeIndexHeap{changes: in, items: make([]int, 0, len(in))}
	for i := range in {
		if depsCount[i] == 0 {
			ready.Push(i)
		}
	}
	out := make([]int, 0, len(in))
	for ready.Len() > 0 {
		i := ready.Pop()
		out = append(out, i)
		for _, dep := range dependents[i] {
			depsCount[dep]--
			if depsCount[dep] == 0 {
				ready.Push(dep)
			}
		}
	}

	if len(out) != len(in) {
		fallback := make([]int, len(in))
		for i := range in {
			fallback[i] = i
		}
		slices.SortFunc(fallback, func(a, b int) int { return in[a].Hash.Compare(in[b].Hash) })
		return fallback
	}
	return out
}

type changeIndexHeap struct {
	changes []Change
	items   []int
}

func (h changeIndexHeap) Len() int { return len(h.items) }
func (h changeIndexHeap) lessPos(i, j int) bool {
	return h.changes[h.items[i]].Hash.Compare(h.changes[h.items[j]].Hash) < 0
}

func (h *changeIndexHeap) Push(v int) {
	h.items = append(h.items, v)
	i := len(h.items) - 1
	for i > 0 {
		p := (i - 1) / 2
		if !h.lessPos(i, p) {
			break
		}
		h.items[i], h.items[p] = h.items[p], h.items[i]
		i = p
	}
}

func (h *changeIndexHeap) Pop() int {
	n := len(h.items)
	x := h.items[0]
	last := h.items[n-1]
	h.items = h.items[:n-1]
	if len(h.items) == 0 {
		return x
	}
	h.items[0] = last
	i := 0
	for {
		left := 2*i + 1
		right := left + 1
		smallest := i
		if left < len(h.items) && h.lessPos(left, smallest) {
			smallest = left
		}
		if right < len(h.items) && h.lessPos(right, smallest) {
			smallest = right
		}
		if smallest == i {
			break
		}
		h.items[i], h.items[smallest] = h.items[smallest], h.items[i]
		i = smallest
	}
	return x
}

func deepCopyChange(c Change) Change {
	cp := c
	cp.Deps = append([]model.ChangeHash(nil), c.Deps...)
	cp.Operations = append([]ChangeOperation(nil), c.Operations...)
	return cp
}

func remapChangeActors(c Change, actorMap map[uint32]uint32) Change {
	if actorMap == nil {
		return c
	}
	cp := deepCopyChange(c)
	if mapped, ok := actorMap[cp.Actor]; ok {
		cp.Actor = mapped
	}
	for i := range cp.Operations {
		cp.Operations[i].OpID = intapply.RemapActor(cp.Operations[i].OpID, actorMap)
		cp.Operations[i].ObjID = intapply.RemapObjID(cp.Operations[i].ObjID, actorMap)
		cp.Operations[i].ChildObjID = intapply.RemapObjID(cp.Operations[i].ChildObjID, actorMap)
	}
	return cp
}
