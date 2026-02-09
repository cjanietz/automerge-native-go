package opset

import (
	"testing"

	"github.com/cjanietz/automerge-native-go/internal/changegraph"
	"github.com/cjanietz/automerge-native-go/internal/model"
)

func TestMapReadAndHistorical(t *testing.T) {
	op := New()
	root := model.RootObjID()

	if err := op.PutMap(root, "title", NewScalarValue(model.StringValue("v1")), model.OpID{Counter: 1, Actor: 1}, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := op.PutMap(root, "title", NewScalarValue(model.StringValue("v2")), model.OpID{Counter: 2, Actor: 1}, 1, 2); err != nil {
		t.Fatal(err)
	}

	v, ok := op.GetMap(root, "title", nil)
	if !ok || v.Scalar.String != "v2" {
		t.Fatalf("unexpected latest map value: %#v ok=%v", v, ok)
	}

	clk := changegraph.NewClock()
	clk.Observe(1, 1)
	vh, ok := op.GetMap(root, "title", &clk)
	if !ok || vh.Scalar.String != "v1" {
		t.Fatalf("unexpected historical map value: %#v ok=%v", vh, ok)
	}
}

func TestMapConflictsGetAll(t *testing.T) {
	op := New()
	root := model.RootObjID()

	if err := op.PutMapRaw(root, "name", NewScalarValue(model.StringValue("alice")), model.OpID{Counter: 1, Actor: 1}, 1, 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := op.PutMapRaw(root, "name", NewScalarValue(model.StringValue("ally")), model.OpID{Counter: 1, Actor: 2}, 2, 1, nil); err != nil {
		t.Fatal(err)
	}

	all := op.GetAllMap(root, "name", nil)
	if len(all) != 2 {
		t.Fatalf("expected conflict set of 2 values, got %d", len(all))
	}

	winner, ok := op.GetMap(root, "name", nil)
	if !ok || winner.Scalar.String != "ally" {
		t.Fatalf("unexpected deterministic winner: %#v", winner)
	}
}

func TestListRangeAndDelete(t *testing.T) {
	op := New()
	listID := model.ObjID{Op: model.OpID{Counter: 10, Actor: 1}}
	op.CreateObject(listID, ObjList)

	_ = op.InsertList(listID, 0, NewScalarValue(model.StringValue("a")), model.OpID{Counter: 1, Actor: 1}, 1, 1)
	_ = op.InsertList(listID, 1, NewScalarValue(model.StringValue("b")), model.OpID{Counter: 2, Actor: 1}, 1, 2)
	_ = op.InsertList(listID, 2, NewScalarValue(model.StringValue("c")), model.OpID{Counter: 3, Actor: 1}, 1, 3)
	_ = op.DeleteList(listID, 1, model.OpID{Counter: 4, Actor: 1}, 1, 4)

	vals := op.ListRange(listID, 0, -1, nil)
	if len(vals) != 2 || vals[0].Scalar.String != "a" || vals[1].Scalar.String != "c" {
		t.Fatalf("unexpected list range: %#v", vals)
	}
	if got := op.ListLength(listID, nil); got != 2 {
		t.Fatalf("unexpected list length: %d", got)
	}
}

func TestTextSpliceAndMaterialize(t *testing.T) {
	op := New()
	textID := model.ObjID{Op: model.OpID{Counter: 11, Actor: 1}}
	op.CreateObject(textID, ObjText)

	seq, err := op.SpliceText(textID, 0, 0, "hello", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if seq == 0 {
		t.Fatal("expected sequence to advance")
	}
	if got := op.Text(textID, nil); got != "hello" {
		t.Fatalf("unexpected text after initial splice: %q", got)
	}
	_, err = op.SpliceText(textID, 1, 2, "a", 1, seq)
	if err != nil {
		t.Fatal(err)
	}
	if got := op.Text(textID, nil); got != "halo" {
		t.Fatalf("unexpected text after replace splice: %q", got)
	}
}

func TestCounterIncrement(t *testing.T) {
	op := New()
	root := model.RootObjID()
	_ = op.PutMap(root, "count", NewScalarValue(model.CounterValue(10)), model.OpID{Counter: 1, Actor: 1}, 1, 1)
	if err := op.IncrementMapCounter(root, "count", 5, model.OpID{Counter: 2, Actor: 1}, 1, 2); err != nil {
		t.Fatal(err)
	}
	v, ok := op.GetMap(root, "count", nil)
	if !ok || v.Scalar.Kind != model.ScalarCounter || v.Scalar.Counter != 15 {
		t.Fatalf("unexpected counter value: %#v", v)
	}
}

func TestMapKeysValuesIterDeterministic(t *testing.T) {
	op := New()
	root := model.RootObjID()
	_ = op.PutMap(root, "z", NewScalarValue(model.StringValue("3")), model.OpID{Counter: 1, Actor: 1}, 1, 1)
	_ = op.PutMap(root, "a", NewScalarValue(model.StringValue("1")), model.OpID{Counter: 2, Actor: 1}, 1, 2)
	_ = op.PutMap(root, "m", NewScalarValue(model.StringValue("2")), model.OpID{Counter: 3, Actor: 1}, 1, 3)

	keys := op.KeysMap(root, nil)
	if len(keys) != 3 || keys[0] != "a" || keys[1] != "m" || keys[2] != "z" {
		t.Fatalf("unexpected key order: %#v", keys)
	}

	vals := op.ValuesMap(root, nil)
	if len(vals) != 3 || vals[0].Scalar.String != "1" || vals[1].Scalar.String != "2" || vals[2].Scalar.String != "3" {
		t.Fatalf("unexpected values order: %#v", vals)
	}

	iter := op.IterMap(root, nil)
	if len(iter) != 3 || iter[0].Key != "a" || iter[2].Key != "z" {
		t.Fatalf("unexpected iter order: %#v", iter)
	}
}

func TestTextMarksAndHistorical(t *testing.T) {
	op := New()
	textID := model.ObjID{Op: model.OpID{Counter: 21, Actor: 1}}
	op.CreateObject(textID, ObjText)

	seq, err := op.SpliceText(textID, 0, 0, "abcd", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := op.AddMark(textID, 1, 3, "bold", model.BoolValue(true), model.OpID{Counter: seq + 1, Actor: 1}, 1, seq+1); err != nil {
		t.Fatal(err)
	}

	marks := op.Marks(textID, nil)
	if len(marks) != 1 {
		t.Fatalf("expected 1 mark, got %d", len(marks))
	}
	if marks[0].Start != 1 || marks[0].End != 3 || marks[0].Name != "bold" {
		t.Fatalf("unexpected mark: %#v", marks[0])
	}

	clk := changegraph.NewClock()
	clk.Observe(1, seq) // before mark op
	if got := len(op.Marks(textID, &clk)); got != 0 {
		t.Fatalf("expected no marks at historical clock, got %d", got)
	}

	if at := op.MarksAtIndex(textID, 2, nil); len(at) != 1 || at[0].Name != "bold" {
		t.Fatalf("unexpected marks at index: %#v", at)
	}
}

func TestMarksAtIndexUsesLatestByName(t *testing.T) {
	op := New()
	textID := model.ObjID{Op: model.OpID{Counter: 31, Actor: 1}}
	op.CreateObject(textID, ObjText)

	seq, err := op.SpliceText(textID, 0, 0, "abcd", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := op.AddMark(textID, 1, 3, "bold", model.BoolValue(true), model.OpID{Counter: seq + 1, Actor: 1}, 1, seq+1); err != nil {
		t.Fatal(err)
	}
	if err := op.AddMark(textID, 0, 4, "bold", model.BoolValue(false), model.OpID{Counter: seq + 2, Actor: 1}, 1, seq+2); err != nil {
		t.Fatal(err)
	}

	got := op.MarksAtIndex(textID, 2, nil)
	if len(got) != 1 {
		t.Fatalf("expected 1 mark winner, got %d: %#v", len(got), got)
	}
	if got[0].Value.Kind != model.ScalarBoolean || got[0].Value.Boolean {
		t.Fatalf("expected latest mark value false, got %#v", got[0])
	}
}

func TestReadsWithUnknownObjectReturnEmptyResults(t *testing.T) {
	op := New()
	unknown := model.ObjID{Op: model.OpID{Counter: 999, Actor: 7}}

	if v, ok := op.GetMap(unknown, "missing", nil); ok {
		t.Fatalf("expected GetMap to return ok=false for unknown object, got value=%#v", v)
	}
	if all := op.GetAllMap(unknown, "missing", nil); len(all) != 0 {
		t.Fatalf("expected no map conflicts for unknown object, got %#v", all)
	}
	if keys := op.KeysMap(unknown, nil); len(keys) != 0 {
		t.Fatalf("expected no keys for unknown object, got %#v", keys)
	}
	if got := op.ListLength(unknown, nil); got != 0 {
		t.Fatalf("expected list length 0 for unknown object, got %d", got)
	}
	if vals := op.ListRange(unknown, 0, -1, nil); len(vals) != 0 {
		t.Fatalf("expected empty list range for unknown object, got %#v", vals)
	}
	if marks := op.Marks(unknown, nil); len(marks) != 0 {
		t.Fatalf("expected no marks for unknown object, got %#v", marks)
	}
}
