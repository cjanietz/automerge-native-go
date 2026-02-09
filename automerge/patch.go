package automerge

import (
	"errors"
	"sort"

	"github.com/cjanietz/automerge-native-go/internal/changegraph"
	"github.com/cjanietz/automerge-native-go/internal/model"
	"github.com/cjanietz/automerge-native-go/internal/opset"
)

var ErrDiffUnknownObject = errors.New("diff unknown object")

type PatchKind uint8

const (
	PatchMapPut PatchKind = iota
	PatchMapDelete
	PatchTextSplice
	PatchListReplace
)

type Patch struct {
	Kind       PatchKind
	ObjID      model.ObjID
	Key        string
	OldValue   *opset.Value
	NewValue   *opset.Value
	BeforeText string
	AfterText  string
	BeforeList []opset.Value
	AfterList  []opset.Value
}

type PatchLog struct {
	active  bool
	patches []Patch
	heads   []model.ChangeHash
}

func ActivePatchLog() *PatchLog    { return &PatchLog{active: true} }
func InactivePatchLog() *PatchLog  { return &PatchLog{active: false} }
func (p *PatchLog) IsActive() bool { return p != nil && p.active }
func (p *PatchLog) Reset() {
	if p != nil {
		p.patches = nil
		p.heads = nil
	}
}
func (p *PatchLog) Add(patch Patch) {
	if p != nil && p.active {
		p.patches = append(p.patches, patch)
	}
}
func (p *PatchLog) setHeads(heads []model.ChangeHash) {
	if p != nil {
		p.heads = append([]model.ChangeHash(nil), heads...)
	}
}
func (p *PatchLog) MakePatches() []Patch {
	if p == nil {
		return nil
	}
	out := make([]Patch, len(p.patches))
	copy(out, p.patches)
	return out
}
func (p *PatchLog) Heads() []model.ChangeHash {
	if p == nil {
		return nil
	}
	out := make([]model.ChangeHash, len(p.heads))
	copy(out, p.heads)
	return out
}

func (d *Document) Diff(beforeHeads, afterHeads []model.ChangeHash) []Patch {
	patches, _ := d.DiffObj(model.RootObjID(), beforeHeads, afterHeads, true)
	return patches
}

func (d *Document) DiffToPatchLog(beforeHeads, afterHeads []model.ChangeHash, log *PatchLog) {
	if log == nil || !log.IsActive() {
		return
	}
	patches := d.Diff(beforeHeads, afterHeads)
	for _, p := range patches {
		log.Add(p)
	}
	log.setHeads(afterHeads)
}

func (d *Document) DiffObj(obj model.ObjID, beforeHeads, afterHeads []model.ChangeHash, recursive bool) ([]Patch, error) {
	beforeClock, err := d.clockFromHeads(beforeHeads)
	if err != nil {
		return nil, err
	}
	afterClock, err := d.clockFromHeads(afterHeads)
	if err != nil {
		return nil, err
	}
	seen := map[model.ObjID]struct{}{}
	return d.diffObjAt(obj, beforeClock, afterClock, recursive, seen)
}

func (d *Document) clockFromHeads(heads []model.ChangeHash) (*changegraph.Clock, error) {
	if len(heads) == 0 {
		return &changegraph.Clock{}, nil
	}
	clk, err := d.ClockForHeads(heads)
	if err != nil {
		return nil, err
	}
	return &clk, nil
}

func (d *Document) diffObjAt(obj model.ObjID, before, after *changegraph.Clock, recursive bool, seen map[model.ObjID]struct{}) ([]Patch, error) {
	if _, ok := seen[obj]; ok {
		return nil, nil
	}
	seen[obj] = struct{}{}
	typ, ok := d.ops.ObjectType(obj)
	if !ok {
		return nil, ErrDiffUnknownObject
	}
	switch typ {
	case opset.ObjMap:
		return d.diffMap(obj, before, after, recursive, seen)
	case opset.ObjText:
		beforeText := d.ops.Text(obj, before)
		afterText := d.ops.Text(obj, after)
		if beforeText == afterText {
			return nil, nil
		}
		return []Patch{{Kind: PatchTextSplice, ObjID: obj, BeforeText: beforeText, AfterText: afterText}}, nil
	case opset.ObjList:
		beforeList := d.ops.ListRange(obj, 0, -1, before)
		afterList := d.ops.ListRange(obj, 0, -1, after)
		if valuesEqual(beforeList, afterList) {
			return nil, nil
		}
		return []Patch{{Kind: PatchListReplace, ObjID: obj, BeforeList: beforeList, AfterList: afterList}}, nil
	default:
		return nil, ErrDiffUnknownObject
	}
}

func (d *Document) diffMap(obj model.ObjID, before, after *changegraph.Clock, recursive bool, seen map[model.ObjID]struct{}) ([]Patch, error) {
	beforeMap := d.mapSnapshot(obj, before)
	afterMap := d.mapSnapshot(obj, after)
	keysSet := map[string]struct{}{}
	for k := range beforeMap {
		keysSet[k] = struct{}{}
	}
	for k := range afterMap {
		keysSet[k] = struct{}{}
	}
	keys := make([]string, 0, len(keysSet))
	for k := range keysSet {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	patches := make([]Patch, 0)
	for _, k := range keys {
		bv, bok := beforeMap[k]
		av, aok := afterMap[k]
		switch {
		case bok && !aok:
			bcp := bv
			patches = append(patches, Patch{Kind: PatchMapDelete, ObjID: obj, Key: k, OldValue: &bcp})
		case !bok && aok:
			acp := av
			patches = append(patches, Patch{Kind: PatchMapPut, ObjID: obj, Key: k, NewValue: &acp})
		case bok && aok && !bv.Equal(av):
			bcp := bv
			acp := av
			patches = append(patches, Patch{Kind: PatchMapPut, ObjID: obj, Key: k, OldValue: &bcp, NewValue: &acp})
		}
		if recursive && bok && aok && bv.Kind == opset.ValueObject && av.Kind == opset.ValueObject {
			if bv.Object.ID == av.Object.ID {
				sub, err := d.diffObjAt(bv.Object.ID, before, after, true, seen)
				if err != nil {
					return nil, err
				}
				patches = append(patches, sub...)
			}
		}
	}
	return patches, nil
}

func (d *Document) mapSnapshot(obj model.ObjID, at *changegraph.Clock) map[string]opset.Value {
	items := d.ops.IterMap(obj, at)
	out := make(map[string]opset.Value, len(items))
	for _, item := range items {
		out[item.Key] = item.Value
	}
	return out
}

func valuesEqual(a, b []opset.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}
