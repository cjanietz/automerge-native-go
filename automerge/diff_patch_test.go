package automerge

import (
	"testing"

	"github.com/cjanietz/automerge-native-go/internal/model"
)

func TestHistoricalReadAPIsAtHeads(t *testing.T) {
	d := NewDocument()
	tx1, _ := d.Begin()
	_ = tx1.Put(model.RootObjID(), "k", model.StringValue("v1"))
	_, _ = tx1.Commit()
	h1 := d.Heads()

	tx2, _ := d.Begin()
	_ = tx2.Put(model.RootObjID(), "k", model.StringValue("v2"))
	_, _ = tx2.Commit()

	v, ok, err := d.GetMapAt(model.RootObjID(), "k", h1)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || v.Scalar.String != "v1" {
		t.Fatalf("unexpected historical value: %#v ok=%v", v, ok)
	}
	keys, err := d.KeysMapAt(model.RootObjID(), h1)
	if err != nil || len(keys) != 1 || keys[0] != "k" {
		t.Fatalf("unexpected keys at heads: keys=%#v err=%v", keys, err)
	}
}

func TestDiffForwardAndBackward(t *testing.T) {
	d := NewDocument()
	tx1, _ := d.Begin()
	_ = tx1.Put(model.RootObjID(), "k", model.StringValue("v1"))
	_, _ = tx1.Commit()
	h1 := d.Heads()

	tx2, _ := d.Begin()
	_ = tx2.Put(model.RootObjID(), "k", model.StringValue("v2"))
	_, _ = tx2.Commit()
	h2 := d.Heads()

	forward := d.Diff(h1, h2)
	if len(forward) == 0 {
		t.Fatal("expected forward patches")
	}
	if forward[0].Kind != PatchMapPut || forward[0].NewValue == nil || forward[0].NewValue.Scalar.String != "v2" {
		t.Fatalf("unexpected forward patch: %#v", forward[0])
	}

	backward := d.Diff(h2, h1)
	if len(backward) == 0 {
		t.Fatal("expected backward patches")
	}
	if backward[0].Kind != PatchMapPut || backward[0].NewValue == nil || backward[0].NewValue.Scalar.String != "v1" {
		t.Fatalf("unexpected backward patch: %#v", backward[0])
	}
}

func TestPatchLogMaterialization(t *testing.T) {
	d := NewDocument()
	tx1, _ := d.Begin()
	_ = tx1.Put(model.RootObjID(), "k", model.StringValue("v1"))
	_, _ = tx1.Commit()
	h1 := d.Heads()

	tx2, _ := d.Begin()
	_ = tx2.Put(model.RootObjID(), "k", model.StringValue("v2"))
	_, _ = tx2.Commit()
	h2 := d.Heads()

	log := ActivePatchLog()
	d.DiffToPatchLog(h1, h2, log)
	patches := log.MakePatches()
	if len(patches) == 0 {
		t.Fatal("expected patches in patch log")
	}
	loggedHeads := log.Heads()
	if len(loggedHeads) != len(h2) || loggedHeads[0] != h2[0] {
		t.Fatalf("unexpected logged heads: %#v vs %#v", loggedHeads, h2)
	}
}

func TestDiffTextPatch(t *testing.T) {
	d := NewDocument()
	tx1, _ := d.Begin()
	textID, _ := tx1.PutObject(model.RootObjID(), "text", ObjText)
	_ = tx1.SpliceText(textID, 0, 0, "hello")
	_, _ = tx1.Commit()
	h1 := d.Heads()

	tx2, _ := d.Begin()
	_ = tx2.SpliceText(textID, 1, 2, "a")
	_, _ = tx2.Commit()
	h2 := d.Heads()

	patches, err := d.DiffObj(textID, h1, h2, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(patches) != 1 || patches[0].Kind != PatchTextSplice || patches[0].BeforeText != "hello" || patches[0].AfterText != "halo" {
		t.Fatalf("unexpected text diff patches: %#v", patches)
	}
}
