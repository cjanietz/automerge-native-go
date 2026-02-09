package opset

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/cjanietz/automerge-native-go/internal/changegraph"
	"github.com/cjanietz/automerge-native-go/internal/model"
)

var (
	ErrUnknownObject   = errors.New("unknown object")
	ErrWrongObjectType = errors.New("wrong object type")
	ErrInvalidIndex    = errors.New("invalid index")
)

type listEntry struct {
	versions []VersionedValue
}

type objectState struct {
	typ ObjType
	m   map[string]*listEntry
	l   []*listEntry
	mk  []Mark
}

type opKind uint8

const (
	opMapPut opKind = iota
	opMapDelete
	opListInsert
	opListSet
	opListDelete
	opMark
)

type opRecord struct {
	kind  opKind
	id    model.OpID
	actor uint32
	seq   uint64
	obj   model.ObjID
	key   string
	index int
	value Value
	pred  []model.OpID
	start int
	end   int
	name  string
}

type OpSet struct {
	objects map[model.ObjID]*objectState
	current map[model.ObjID]*objectState
	ops     []opRecord
}

func New() *OpSet {
	o := &OpSet{
		objects: make(map[model.ObjID]*objectState),
		current: make(map[model.ObjID]*objectState),
	}
	root := &objectState{typ: ObjMap, m: make(map[string]*listEntry)}
	o.objects[model.RootObjID()] = &objectState{typ: ObjMap, m: make(map[string]*listEntry)}
	o.current[model.RootObjID()] = root
	return o
}

func (o *OpSet) ObjectType(id model.ObjID) (ObjType, bool) {
	obj, ok := o.objects[id]
	if !ok {
		return 0, false
	}
	return obj.typ, true
}

func (o *OpSet) CreateObject(id model.ObjID, typ ObjType) {
	if _, ok := o.objects[id]; ok {
		return
	}
	st := &objectState{typ: typ}
	if typ == ObjMap {
		st.m = make(map[string]*listEntry)
	}
	o.objects[id] = st
	cur := &objectState{typ: typ}
	if typ == ObjMap {
		cur.m = make(map[string]*listEntry)
	}
	o.current[id] = cur
}

func (o *OpSet) PutMap(obj model.ObjID, key string, value Value, id model.OpID, actor uint32, seq uint64) error {
	if err := o.ensureType(obj, ObjMap); err != nil {
		return err
	}
	pred := o.visibleMapVersionIDs(obj, key, nil)
	rec := opRecord{kind: opMapPut, obj: obj, key: key, value: value, id: id, actor: actor, seq: seq, pred: pred}
	o.ops = append(o.ops, rec)
	o.applyRecordToCurrent(rec)
	return nil
}

func (o *OpSet) PutMapRaw(obj model.ObjID, key string, value Value, id model.OpID, actor uint32, seq uint64, pred []model.OpID) error {
	if err := o.ensureType(obj, ObjMap); err != nil {
		return err
	}
	cp := append([]model.OpID(nil), pred...)
	rec := opRecord{kind: opMapPut, obj: obj, key: key, value: value, id: id, actor: actor, seq: seq, pred: cp}
	o.ops = append(o.ops, rec)
	o.applyRecordToCurrent(rec)
	return nil
}

func (o *OpSet) DeleteMap(obj model.ObjID, key string, id model.OpID, actor uint32, seq uint64) error {
	if err := o.ensureType(obj, ObjMap); err != nil {
		return err
	}
	pred := o.visibleMapVersionIDs(obj, key, nil)
	rec := opRecord{kind: opMapDelete, obj: obj, key: key, id: id, actor: actor, seq: seq, pred: pred}
	o.ops = append(o.ops, rec)
	o.applyRecordToCurrent(rec)
	return nil
}

func (o *OpSet) InsertList(obj model.ObjID, index int, value Value, id model.OpID, actor uint32, seq uint64) error {
	if err := o.ensureType(obj, ObjList, ObjText); err != nil {
		return err
	}
	if index < 0 {
		return ErrInvalidIndex
	}
	rec := opRecord{kind: opListInsert, obj: obj, index: index, value: value, id: id, actor: actor, seq: seq}
	o.ops = append(o.ops, rec)
	o.applyRecordToCurrent(rec)
	return nil
}

func (o *OpSet) SetList(obj model.ObjID, index int, value Value, id model.OpID, actor uint32, seq uint64) error {
	if err := o.ensureType(obj, ObjList, ObjText); err != nil {
		return err
	}
	if index < 0 {
		return ErrInvalidIndex
	}
	pred := o.visibleListVersionIDs(obj, index, nil)
	rec := opRecord{kind: opListSet, obj: obj, index: index, value: value, id: id, actor: actor, seq: seq, pred: pred}
	o.ops = append(o.ops, rec)
	o.applyRecordToCurrent(rec)
	return nil
}

func (o *OpSet) SetListRaw(obj model.ObjID, index int, value Value, id model.OpID, actor uint32, seq uint64, pred []model.OpID) error {
	if err := o.ensureType(obj, ObjList, ObjText); err != nil {
		return err
	}
	if index < 0 {
		return ErrInvalidIndex
	}
	cp := append([]model.OpID(nil), pred...)
	rec := opRecord{kind: opListSet, obj: obj, index: index, value: value, id: id, actor: actor, seq: seq, pred: cp}
	o.ops = append(o.ops, rec)
	o.applyRecordToCurrent(rec)
	return nil
}

func (o *OpSet) DeleteList(obj model.ObjID, index int, id model.OpID, actor uint32, seq uint64) error {
	if err := o.ensureType(obj, ObjList, ObjText); err != nil {
		return err
	}
	if index < 0 {
		return ErrInvalidIndex
	}
	rec := opRecord{kind: opListDelete, obj: obj, index: index, id: id, actor: actor, seq: seq}
	o.ops = append(o.ops, rec)
	o.applyRecordToCurrent(rec)
	return nil
}

func (o *OpSet) IncrementMapCounter(obj model.ObjID, key string, by int64, id model.OpID, actor uint32, seq uint64) error {
	current, ok := o.GetMap(obj, key, nil)
	if !ok || current.Kind != ValueScalar || current.Scalar.Kind != model.ScalarCounter {
		return fmt.Errorf("counter not found at key=%s", key)
	}
	next := model.CounterValue(current.Scalar.Counter + by)
	return o.PutMap(obj, key, NewScalarValue(next), id, actor, seq)
}

func (o *OpSet) SpliceText(obj model.ObjID, index int, deleteCount int, insert string, actor uint32, startSeq uint64) (uint64, error) {
	if err := o.ensureType(obj, ObjText); err != nil {
		return startSeq, err
	}
	if index < 0 || deleteCount < 0 {
		return startSeq, ErrInvalidIndex
	}
	seq := startSeq
	for i := 0; i < deleteCount; i++ {
		seq++
		if err := o.DeleteList(obj, index, model.OpID{Counter: seq, Actor: actor}, actor, seq); err != nil {
			return seq, err
		}
	}
	for _, r := range insert {
		seq++
		v := NewScalarValue(model.StringValue(string(r)))
		if err := o.InsertList(obj, index, v, model.OpID{Counter: seq, Actor: actor}, actor, seq); err != nil {
			return seq, err
		}
		index++
	}
	return seq, nil
}

func (o *OpSet) AddMark(obj model.ObjID, start int, end int, name string, value model.ScalarValue, id model.OpID, actor uint32, seq uint64) error {
	if err := o.ensureType(obj, ObjText); err != nil {
		return err
	}
	if start < 0 || end < start {
		return ErrInvalidIndex
	}
	rec := opRecord{
		kind:  opMark,
		id:    id,
		actor: actor,
		seq:   seq,
		obj:   obj,
		start: start,
		end:   end,
		name:  name,
		value: NewScalarValue(value),
	}
	o.ops = append(o.ops, rec)
	o.applyRecordToCurrent(rec)
	return nil
}

func (o *OpSet) GetMap(obj model.ObjID, key string, at *changegraph.Clock) (Value, bool) {
	state, err := o.materialize(at)
	if err != nil {
		return Value{}, false
	}
	st := state[obj]
	if st == nil {
		return Value{}, false
	}
	entry := st.m[key]
	versions := sortedVersions(entry)
	if len(versions) == 0 {
		return Value{}, false
	}
	return versions[len(versions)-1].Value, true
}

func (o *OpSet) GetAllMap(obj model.ObjID, key string, at *changegraph.Clock) []Value {
	state, err := o.materialize(at)
	if err != nil {
		return nil
	}
	st := state[obj]
	if st == nil {
		return nil
	}
	entry := st.m[key]
	versions := sortedVersions(entry)
	out := make([]Value, 0, len(versions))
	for _, v := range versions {
		out = append(out, v.Value)
	}
	return out
}

func (o *OpSet) KeysMap(obj model.ObjID, at *changegraph.Clock) []string {
	state, err := o.materialize(at)
	if err != nil {
		return nil
	}
	st := state[obj]
	if st == nil {
		return nil
	}
	keys := make([]string, 0, len(st.m))
	for k, entry := range st.m {
		if len(entry.versions) > 0 {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

func (o *OpSet) ValuesMap(obj model.ObjID, at *changegraph.Clock) []Value {
	keys := o.KeysMap(obj, at)
	out := make([]Value, 0, len(keys))
	for _, k := range keys {
		v, ok := o.GetMap(obj, k, at)
		if ok {
			out = append(out, v)
		}
	}
	return out
}

func (o *OpSet) IterMap(obj model.ObjID, at *changegraph.Clock) []struct {
	Key   string
	Value Value
} {
	keys := o.KeysMap(obj, at)
	out := make([]struct {
		Key   string
		Value Value
	}, 0, len(keys))
	for _, k := range keys {
		v, ok := o.GetMap(obj, k, at)
		if ok {
			out = append(out, struct {
				Key   string
				Value Value
			}{Key: k, Value: v})
		}
	}
	return out
}

func (o *OpSet) ListLength(obj model.ObjID, at *changegraph.Clock) int {
	state, err := o.materialize(at)
	if err != nil {
		return 0
	}
	st := state[obj]
	if st == nil {
		return 0
	}
	return len(st.l)
}

func (o *OpSet) ListRange(obj model.ObjID, start, end int, at *changegraph.Clock) []Value {
	state, err := o.materialize(at)
	if err != nil {
		return nil
	}
	st := state[obj]
	if st == nil {
		return nil
	}
	list := st.l
	if start < 0 {
		start = 0
	}
	if end > len(list) || end < 0 {
		end = len(list)
	}
	if start > end {
		start = end
	}
	out := make([]Value, 0, end-start)
	for i := start; i < end; i++ {
		versions := sortedVersions(list[i])
		if len(versions) == 0 {
			continue
		}
		out = append(out, versions[len(versions)-1].Value)
	}
	return out
}

func (o *OpSet) Text(obj model.ObjID, at *changegraph.Clock) string {
	vals := o.ListRange(obj, 0, -1, at)
	var b strings.Builder
	for _, v := range vals {
		if v.Kind == ValueScalar && v.Scalar.Kind == model.ScalarString {
			b.WriteString(v.Scalar.String)
		}
	}
	return b.String()
}

func (o *OpSet) SequenceElementIDs(obj model.ObjID, at *changegraph.Clock) []model.OpID {
	state, err := o.materialize(at)
	if err != nil {
		return nil
	}
	st := state[obj]
	if st == nil || (st.typ != ObjList && st.typ != ObjText) {
		return nil
	}
	out := make([]model.OpID, 0, len(st.l))
	for _, entry := range st.l {
		versions := sortedVersions(entry)
		if len(versions) == 0 {
			continue
		}
		out = append(out, versions[len(versions)-1].OpID)
	}
	return out
}

func (o *OpSet) Marks(obj model.ObjID, at *changegraph.Clock) []Mark {
	state, err := o.materialize(at)
	if err != nil {
		return nil
	}
	st := state[obj]
	if st == nil {
		return nil
	}
	marks := append([]Mark(nil), st.mk...)
	sort.Slice(marks, func(i, j int) bool {
		if marks[i].Start != marks[j].Start {
			return marks[i].Start < marks[j].Start
		}
		if marks[i].End != marks[j].End {
			return marks[i].End < marks[j].End
		}
		return marks[i].OpID.Compare(marks[j].OpID) < 0
	})
	return marks
}

func (o *OpSet) MarksAtIndex(obj model.ObjID, index int, at *changegraph.Clock) []Mark {
	if index < 0 {
		return nil
	}
	all := o.Marks(obj, at)
	filtered := make([]Mark, 0, len(all))
	for _, m := range all {
		if m.Start <= index && index < m.End {
			filtered = append(filtered, m)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	byName := make(map[string]Mark, len(filtered))
	for _, m := range filtered {
		curr, ok := byName[m.Name]
		if !ok || curr.OpID.Compare(m.OpID) < 0 {
			byName[m.Name] = m
		}
	}
	out := make([]Mark, 0, len(byName))
	for _, m := range byName {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].OpID.Compare(out[j].OpID) < 0
	})
	return out
}

func (o *OpSet) materialize(at *changegraph.Clock) (map[model.ObjID]*objectState, error) {
	state := make(map[model.ObjID]*objectState, len(o.objects))
	for id, obj := range o.objects {
		copyObj := &objectState{typ: obj.typ}
		if obj.typ == ObjMap {
			copyObj.m = make(map[string]*listEntry)
		} else {
			copyObj.l = make([]*listEntry, 0)
		}
		state[id] = copyObj
	}

	for _, op := range o.ops {
		if at != nil && !at.Covers(op.actor, op.seq) {
			continue
		}
		applyRecord(state, op)
	}
	return state, nil
}

func (o *OpSet) applyRecordToCurrent(op opRecord) {
	obj := o.current[op.obj]
	if obj == nil || obj.m == nil {
		return
	}
	switch op.kind {
	case opMapPut:
		entry := obj.m[op.key]
		if entry == nil {
			entry = &listEntry{}
			obj.m[op.key] = entry
		}
		entry.versions = removePreds(entry.versions, op.pred)
		entry.versions = append(entry.versions, VersionedValue{OpID: op.id, Actor: op.actor, Seq: op.seq, Value: op.value})
	case opMapDelete:
		entry := obj.m[op.key]
		if entry == nil {
			return
		}
		entry.versions = removePreds(entry.versions, op.pred)
	}
}

func applyRecord(state map[model.ObjID]*objectState, op opRecord) {
	obj := state[op.obj]
	if obj == nil {
		return
	}
	switch op.kind {
	case opMapPut:
		entry := obj.m[op.key]
		if entry == nil {
			entry = &listEntry{}
			obj.m[op.key] = entry
		}
		entry.versions = removePreds(entry.versions, op.pred)
		entry.versions = append(entry.versions, VersionedValue{OpID: op.id, Actor: op.actor, Seq: op.seq, Value: op.value})
	case opMapDelete:
		entry := obj.m[op.key]
		if entry == nil {
			return
		}
		entry.versions = removePreds(entry.versions, op.pred)
	case opListInsert:
		if op.index < 0 || op.index > len(obj.l) {
			return
		}
		entry := &listEntry{versions: []VersionedValue{{OpID: op.id, Actor: op.actor, Seq: op.seq, Value: op.value}}}
		obj.l = append(obj.l, nil)
		copy(obj.l[op.index+1:], obj.l[op.index:])
		obj.l[op.index] = entry
	case opListSet:
		if op.index < 0 || op.index >= len(obj.l) {
			return
		}
		entry := obj.l[op.index]
		entry.versions = removePreds(entry.versions, op.pred)
		entry.versions = append(entry.versions, VersionedValue{OpID: op.id, Actor: op.actor, Seq: op.seq, Value: op.value})
	case opListDelete:
		if op.index < 0 || op.index >= len(obj.l) {
			return
		}
		obj.l = append(obj.l[:op.index], obj.l[op.index+1:]...)
	case opMark:
		obj.mk = append(obj.mk, Mark{
			Start: op.start,
			End:   op.end,
			Name:  op.name,
			Value: op.value.Scalar,
			OpID:  op.id,
			Actor: op.actor,
			Seq:   op.seq,
		})
	}
}

func removePreds(in []VersionedValue, pred []model.OpID) []VersionedValue {
	if len(pred) == 0 {
		return in
	}
	dead := make(map[model.OpID]struct{}, len(pred))
	for _, p := range pred {
		dead[p] = struct{}{}
	}
	out := in[:0]
	for _, v := range in {
		if _, ok := dead[v.OpID]; ok {
			continue
		}
		out = append(out, v)
	}
	return out
}

func sortedVersions(entry *listEntry) []VersionedValue {
	if entry == nil || len(entry.versions) == 0 {
		return nil
	}
	out := make([]VersionedValue, len(entry.versions))
	copy(out, entry.versions)
	sort.Slice(out, func(i, j int) bool {
		return out[i].OpID.Compare(out[j].OpID) < 0
	})
	return out
}

func (o *OpSet) visibleMapVersionIDs(obj model.ObjID, key string, at *changegraph.Clock) []model.OpID {
	if at == nil {
		entry := o.currentMapEntry(obj, key)
		versions := sortedVersions(entry)
		out := make([]model.OpID, 0, len(versions))
		for _, v := range versions {
			out = append(out, v.OpID)
		}
		return out
	}
	state, err := o.materialize(at)
	if err != nil {
		return nil
	}
	st := state[obj]
	if st == nil {
		return nil
	}
	entry := st.m[key]
	versions := sortedVersions(entry)
	out := make([]model.OpID, 0, len(versions))
	for _, v := range versions {
		out = append(out, v.OpID)
	}
	return out
}

func (o *OpSet) currentMapEntry(obj model.ObjID, key string) *listEntry {
	st := o.current[obj]
	if st == nil || st.m == nil {
		return nil
	}
	return st.m[key]
}

func (o *OpSet) visibleListVersionIDs(obj model.ObjID, index int, at *changegraph.Clock) []model.OpID {
	state, err := o.materialize(at)
	if err != nil {
		return nil
	}
	st := state[obj]
	if st == nil {
		return nil
	}
	list := st.l
	if index < 0 || index >= len(list) {
		return nil
	}
	versions := sortedVersions(list[index])
	out := make([]model.OpID, 0, len(versions))
	for _, v := range versions {
		out = append(out, v.OpID)
	}
	return out
}

func (o *OpSet) ensureType(obj model.ObjID, want ...ObjType) error {
	st, ok := o.objects[obj]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownObject, obj)
	}
	for _, t := range want {
		if st.typ == t {
			return nil
		}
	}
	return fmt.Errorf("%w: have=%s", ErrWrongObjectType, st.typ)
}
