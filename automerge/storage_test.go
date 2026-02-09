package automerge

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cjanietz/automerge-native-go/internal/model"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	d := NewDocument()
	tx, _ := d.Begin()
	_ = tx.Put(model.RootObjID(), "name", model.StringValue("alice"))
	textID, _ := tx.PutObject(model.RootObjID(), "text", ObjText)
	_ = tx.SpliceText(textID, 0, 0, "hello")
	_, err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	buf, err := d.Save()
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(buf)
	if err != nil {
		t.Fatal(err)
	}
	v1, _ := d.GetMap(model.RootObjID(), "name", nil)
	v2, ok := loaded.GetMap(model.RootObjID(), "name", nil)
	if !ok || !v1.Equal(v2) {
		t.Fatalf("map mismatch after load: %#v %#v", v1, v2)
	}
	if got := loaded.Text(textID, nil); got != "hello" {
		t.Fatalf("text mismatch after load: %q", got)
	}
}

func TestSaveAfterAndLoadIncremental(t *testing.T) {
	d := NewDocument()
	tx1, _ := d.Begin()
	_ = tx1.Put(model.RootObjID(), "k", model.StringValue("v1"))
	_, _ = tx1.Commit()
	heads := d.Heads()
	tx2, _ := d.Begin()
	_ = tx2.Put(model.RootObjID(), "k", model.StringValue("v2"))
	_, _ = tx2.Commit()

	inc, err := d.SaveAfter(heads)
	if err != nil {
		t.Fatal(err)
	}

	d2 := NewDocument()
	full, _ := d.Save()
	loaded, err := Load(full)
	if err != nil {
		t.Fatal(err)
	}
	base, _ := loaded.SaveAfter(loaded.Heads())
	if len(base) != 0 {
		t.Fatalf("expected empty save-after-current-heads")
	}
	_, _ = d2.LoadIncremental(full)
	if _, err := d2.LoadIncremental(inc); err != nil {
		t.Fatal(err)
	}
	v, ok := d2.GetMap(model.RootObjID(), "k", nil)
	if !ok || v.Scalar.String != "v2" {
		t.Fatalf("unexpected incremental value: %#v", v)
	}
}

func TestCorruptDataHandling(t *testing.T) {
	d := NewDocument()
	tx, _ := d.Begin()
	_ = tx.Put(model.RootObjID(), "k", model.StringValue("v"))
	_, _ = tx.Commit()
	buf, _ := d.Save()
	buf[len(buf)-1] ^= 0x42
	if _, err := LoadWithOptions(buf, DefaultLoadOptions()); err == nil {
		t.Fatal("expected error for corrupt data")
	}
	if _, err := LoadWithOptions(buf, LoadOptions{OnPartialLoad: OnPartialIgnore, Verification: VerificationCheck, StringMigration: StringMigrationNone}); err != nil {
		t.Fatalf("ignore partial load should not fail, got %v", err)
	}
}

func TestLoadRustFixtureCompatibilityPath(t *testing.T) {
	fixture := filepath.Join("..", "..", "rust", "automerge", "tests", "fixtures", "two_change_chunks.automerge")
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Skipf("fixture not available: %v", err)
	}
	loaded, err := LoadWithOptions(data, DefaultLoadOptions())
	if err != nil {
		t.Fatalf("expected rust compatibility load path to succeed, got %v", err)
	}
	out, err := loaded.Save()
	if err != nil {
		t.Fatalf("save loaded rust bytes: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("expected non-empty re-saved bytes")
	}
}

func TestSaveCacheInvalidatedOnMutation(t *testing.T) {
	d := NewDocument()
	tx1, _ := d.Begin()
	_ = tx1.Put(model.RootObjID(), "k", model.StringValue("v1"))
	_, _ = tx1.Commit()

	b1, err := d.Save()
	if err != nil {
		t.Fatal(err)
	}
	b2, err := d.Save()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatal("expected repeated save output to be stable")
	}

	tx2, _ := d.Begin()
	_ = tx2.Put(model.RootObjID(), "k", model.StringValue("v2"))
	_, _ = tx2.Commit()

	b3, err := d.Save()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(b1, b3) {
		t.Fatal("expected save output to change after mutation")
	}
	loaded, err := Load(b3)
	if err != nil {
		t.Fatal(err)
	}
	v, ok := loaded.GetMap(model.RootObjID(), "k", nil)
	if !ok || v.Scalar.String != "v2" {
		t.Fatalf("unexpected loaded value after cache invalidation: %#v ok=%v", v, ok)
	}
}
