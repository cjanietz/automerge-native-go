package automerge

import (
	"math/rand"
	"testing"

	"github.com/cjanietz/automerge-native-go/internal/model"
)

func makeSinglePutChange(t *testing.T, actor uint32, key, value string) Change {
	t.Helper()
	d := NewDocument()
	if err := d.SetActor(actor); err != nil {
		t.Fatal(err)
	}
	tx, _ := d.Begin()
	if err := tx.Put(model.RootObjID(), key, model.StringValue(value)); err != nil {
		t.Fatal(err)
	}
	c, err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	return *c
}

func TestApplyChangesQueueDrainOutOfOrder(t *testing.T) {
	source := NewDocument()
	tx1, _ := source.Begin()
	_ = tx1.Put(model.RootObjID(), "k", model.StringValue("v1"))
	c1, _ := tx1.Commit()
	tx2, _ := source.Begin()
	_ = tx2.Put(model.RootObjID(), "k", model.StringValue("v2"))
	c2, _ := tx2.Commit()

	target := NewDocument()
	if err := target.ApplyChanges([]Change{*c2, *c1}); err != nil {
		t.Fatal(err)
	}
	if gotQueue := len(target.queue); gotQueue != 0 {
		t.Fatalf("expected empty queue after drain, got %d", gotQueue)
	}
	v, ok := target.GetMap(model.RootObjID(), "k", nil)
	if !ok || v.Scalar.String != "v2" {
		t.Fatalf("unexpected merged value: %#v ok=%v", v, ok)
	}
}

func TestApplyChangesDuplicateHashDedup(t *testing.T) {
	c := makeSinglePutChange(t, 1, "k", "v")
	d := NewDocument()
	if err := d.ApplyChanges([]Change{c, c}); err != nil {
		t.Fatal(err)
	}
	if len(d.Heads()) != 1 {
		t.Fatalf("expected 1 head after dedupe, got %d", len(d.Heads()))
	}
}

func TestApplyChangesDuplicateSeqError(t *testing.T) {
	c1 := makeSinglePutChange(t, 1, "k", "v1")
	c2 := makeSinglePutChange(t, 1, "k", "v2") // same actor/seq with different hash
	d := NewDocument()
	if err := d.ApplyChanges([]Change{c1}); err != nil {
		t.Fatal(err)
	}
	err := d.ApplyChanges([]Change{c2})
	if err == nil {
		t.Fatal("expected duplicate sequence error")
	}
}

func TestMergeChangesAddedTraversal(t *testing.T) {
	base := NewDocument()
	tx0, _ := base.Begin()
	_ = tx0.Put(model.RootObjID(), "root", model.StringValue("base"))
	c0, _ := tx0.Commit()

	doc1 := NewDocument()
	_ = doc1.ApplyChanges([]Change{*c0})
	tx1, _ := doc1.Begin()
	_ = tx1.Put(model.RootObjID(), "a", model.StringValue("1"))
	_, _ = tx1.Commit()

	doc2 := NewDocument()
	_ = doc2.SetActor(2)
	_ = doc2.ApplyChanges([]Change{*c0})
	tx2, _ := doc2.Begin()
	_ = tx2.Put(model.RootObjID(), "b", model.StringValue("2"))
	_, _ = tx2.Commit()

	if err := doc1.Merge(doc2); err != nil {
		t.Fatal(err)
	}
	if _, ok := doc1.GetMap(model.RootObjID(), "a", nil); !ok {
		t.Fatal("missing local key after merge")
	}
	if v, ok := doc1.GetMap(model.RootObjID(), "b", nil); !ok || v.Scalar.String != "2" {
		t.Fatalf("missing merged key b: %#v ok=%v", v, ok)
	}
}

func TestRandomizedPermutedApplyConverges(t *testing.T) {
	source := NewDocument()
	for i := 0; i < 25; i++ {
		tx, _ := source.Begin()
		_ = tx.Put(model.RootObjID(), "k", model.StringValue(string(rune('a'+(i%26)))))
		if _, err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	changes := source.AllChanges()
	if len(changes) == 0 {
		t.Fatal("expected non-empty change list")
	}

	rng := rand.New(rand.NewSource(77))
	for i := len(changes) - 1; i > 0; i-- {
		j := rng.Intn(i + 1)
		changes[i], changes[j] = changes[j], changes[i]
	}

	target := NewDocument()
	if err := target.ApplyChanges(changes); err != nil {
		t.Fatal(err)
	}
	if got, want := len(target.Heads()), len(source.Heads()); got != want {
		t.Fatalf("head count mismatch: got %d want %d", got, want)
	}
	srcV, _ := source.GetMap(model.RootObjID(), "k", nil)
	tgtV, _ := target.GetMap(model.RootObjID(), "k", nil)
	if !srcV.Equal(tgtV) {
		t.Fatalf("state diverged: source=%#v target=%#v", srcV, tgtV)
	}
}

func TestApplyChangesWithActorMap(t *testing.T) {
	c := makeSinglePutChange(t, 1, "mapped", "yes")
	target := NewDocument()
	if err := target.ApplyChangesWithActorMap([]Change{c}, map[uint32]uint32{1: 9}); err != nil {
		t.Fatal(err)
	}
	clk, err := target.ClockForHeads(target.Heads())
	if err != nil {
		t.Fatal(err)
	}
	if got := clk.MaxSeq(9); got != 1 {
		t.Fatalf("expected remapped actor seq, got %d", got)
	}
}
