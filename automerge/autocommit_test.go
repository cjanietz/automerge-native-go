package automerge

import (
	"testing"

	"github.com/cjanietz/automerge-native-go/internal/model"
)

func TestAutoCommitMutations(t *testing.T) {
	ac := NewAutoCommit()
	if _, err := ac.Put(model.RootObjID(), "title", model.StringValue("hello")); err != nil {
		t.Fatal(err)
	}
	listID, _, err := ac.PutObject(model.RootObjID(), "items", ObjList)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ac.Insert(listID, 0, model.StringValue("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := ac.Insert(listID, 1, model.StringValue("y")); err != nil {
		t.Fatal(err)
	}
	if _, err := ac.DeleteList(listID, 0); err != nil {
		t.Fatal(err)
	}

	vals := ac.Document().ListRange(listID, 0, -1, nil)
	if len(vals) != 1 || vals[0].Scalar.String != "y" {
		t.Fatalf("unexpected list values: %#v", vals)
	}

	v, ok := ac.Document().GetMap(model.RootObjID(), "title", nil)
	if !ok || v.Scalar.String != "hello" {
		t.Fatalf("unexpected title value: %#v ok=%v", v, ok)
	}
}

func TestAutoCommitDiffCursorIncremental(t *testing.T) {
	ac := NewAutoCommit()
	ac.UpdateDiffCursor() // start cursor at empty heads

	_, err := ac.Put(model.RootObjID(), "k", model.StringValue("v1"))
	if err != nil {
		t.Fatal(err)
	}
	p1 := ac.DiffIncremental()
	if len(p1) == 0 {
		t.Fatal("expected incremental patches after first put")
	}

	_, err = ac.Put(model.RootObjID(), "k", model.StringValue("v2"))
	if err != nil {
		t.Fatal(err)
	}
	p2 := ac.DiffIncremental()
	if len(p2) == 0 {
		t.Fatal("expected incremental patches after second put")
	}
	if len(ac.DiffCursor()) == 0 {
		t.Fatal("expected non-empty diff cursor")
	}
	ac.ResetDiffCursor()
	if len(ac.DiffCursor()) != 0 {
		t.Fatal("expected empty diff cursor after reset")
	}
}
