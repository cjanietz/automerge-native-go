package automerge

import (
	"testing"

	"github.com/cjanietz/automerge-native-go/internal/model"
)

func TestTransactionCommitAndMetadata(t *testing.T) {
	doc := NewDocument()
	tx, err := doc.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(model.RootObjID(), "name", model.StringValue("alice")); err != nil {
		t.Fatal(err)
	}
	msg := "initial"
	tm := int64(1730000000)
	change, err := tx.CommitWith(CommitOptions{Message: &msg, Time: &tm})
	if err != nil {
		t.Fatal(err)
	}
	if change == nil {
		t.Fatal("expected committed change")
	}
	if change.Actor != 1 || change.Seq != 1 {
		t.Fatalf("unexpected actor/seq: actor=%d seq=%d", change.Actor, change.Seq)
	}
	if change.StartOp == 0 || change.MaxOp < change.StartOp {
		t.Fatalf("unexpected op bounds: start=%d max=%d", change.StartOp, change.MaxOp)
	}
	if change.Message == nil || *change.Message != msg {
		t.Fatalf("missing commit message: %#v", change.Message)
	}
	if change.Time == nil || *change.Time != tm {
		t.Fatalf("missing commit time: %#v", change.Time)
	}
	if len(change.Operations) != 1 || change.Operations[0].Kind != OpPut {
		t.Fatalf("unexpected operations: %#v", change.Operations)
	}
	v, ok := doc.GetMap(model.RootObjID(), "name", nil)
	if !ok || v.Scalar.String != "alice" {
		t.Fatalf("unexpected document value after commit: %#v ok=%v", v, ok)
	}
}

func TestTransactionRollback(t *testing.T) {
	doc := NewDocument()
	tx, err := doc.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(model.RootObjID(), "name", model.StringValue("alice")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, ok := doc.GetMap(model.RootObjID(), "name", nil); ok {
		t.Fatal("value should not exist after rollback")
	}
	if len(doc.Heads()) != 0 {
		t.Fatal("rollback should not add heads")
	}
}

func TestTransactionObjectAndListMutations(t *testing.T) {
	doc := NewDocument()
	tx, _ := doc.Begin()
	listID, err := tx.PutObject(model.RootObjID(), "items", ObjList)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Insert(listID, 0, model.StringValue("a")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Insert(listID, 1, model.StringValue("b")); err != nil {
		t.Fatal(err)
	}
	if err := tx.DeleteList(listID, 0); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	vals := doc.ListRange(listID, 0, -1, nil)
	if len(vals) != 1 || vals[0].Scalar.String != "b" {
		t.Fatalf("unexpected list state: %#v", vals)
	}
}

func TestTransactionIncrementAndSplice(t *testing.T) {
	doc := NewDocument()
	tx, _ := doc.Begin()
	if err := tx.Put(model.RootObjID(), "count", model.CounterValue(10)); err != nil {
		t.Fatal(err)
	}
	textID, err := tx.PutObject(model.RootObjID(), "text", ObjText)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.SpliceText(textID, 0, 0, "hello"); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	tx2, _ := doc.Begin()
	if err := tx2.Increment(model.RootObjID(), "count", 5); err != nil {
		t.Fatal(err)
	}
	if err := tx2.SpliceText(textID, 1, 2, "a"); err != nil {
		t.Fatal(err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Fatal(err)
	}

	count, ok := doc.GetMap(model.RootObjID(), "count", nil)
	if !ok || count.Scalar.Counter != 15 {
		t.Fatalf("unexpected count: %#v", count)
	}
	if got := doc.Text(textID, nil); got != "halo" {
		t.Fatalf("unexpected text after splice: %q", got)
	}
}

func TestTransactionDepsAndSeqAdvance(t *testing.T) {
	doc := NewDocument()
	tx1, _ := doc.Begin()
	_ = tx1.Put(model.RootObjID(), "v", model.StringValue("1"))
	c1, err := tx1.Commit()
	if err != nil {
		t.Fatal(err)
	}
	tx2, _ := doc.Begin()
	_ = tx2.Put(model.RootObjID(), "v", model.StringValue("2"))
	c2, err := tx2.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if c2.Seq != c1.Seq+1 {
		t.Fatalf("expected actor seq increment, got %d then %d", c1.Seq, c2.Seq)
	}
	if len(c2.Deps) == 0 {
		t.Fatal("expected deps on second change")
	}
}
