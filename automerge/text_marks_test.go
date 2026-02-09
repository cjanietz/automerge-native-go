package automerge

import (
	"errors"
	"testing"

	"github.com/cjanietz/automerge-native-go/internal/model"
	inttext "github.com/cjanietz/automerge-native-go/internal/text"
)

func TestTransactionMarkBeginEndAndRetrieval(t *testing.T) {
	doc := NewDocument()
	tx, err := doc.Begin()
	if err != nil {
		t.Fatal(err)
	}
	textID, err := tx.PutObject(model.RootObjID(), "text", ObjText)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.SpliceText(textID, 0, 0, "hello"); err != nil {
		t.Fatal(err)
	}
	if err := tx.MarkBegin(textID, 1, "bold", model.BoolValue(true)); err != nil {
		t.Fatal(err)
	}
	if err := tx.MarkEnd(textID, 4, "bold"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	marks := doc.Marks(textID, nil)
	if len(marks) != 1 {
		t.Fatalf("expected 1 mark, got %d", len(marks))
	}
	if marks[0].Start != 1 || marks[0].End != 4 || marks[0].Name != "bold" {
		t.Fatalf("unexpected mark span: %#v", marks[0])
	}
	if at := doc.MarksAtIndex(textID, 2, nil); len(at) != 1 || at[0].Name != "bold" {
		t.Fatalf("unexpected active marks at index: %#v", at)
	}
	if at := doc.MarksAtIndex(textID, 0, nil); len(at) != 0 {
		t.Fatalf("expected no active marks at index 0, got %#v", at)
	}
}

func TestTransactionUnclosedMarkFailsCommit(t *testing.T) {
	doc := NewDocument()
	tx, err := doc.Begin()
	if err != nil {
		t.Fatal(err)
	}
	textID, err := tx.PutObject(model.RootObjID(), "text", ObjText)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.MarkBegin(textID, 0, "bold", model.BoolValue(true)); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Commit()
	if !errors.Is(err, ErrMarkNotClosed) {
		t.Fatalf("expected ErrMarkNotClosed, got %v", err)
	}
}

func TestTextCursorTracksSequencePositions(t *testing.T) {
	doc := NewDocument()
	tx, _ := doc.Begin()
	textID, _ := tx.PutObject(model.RootObjID(), "text", ObjText)
	_ = tx.SpliceText(textID, 0, 0, "ab")
	_, _ = tx.Commit()

	cur, err := doc.CursorForText(textID, 1, inttext.EncodingUTF8)
	if err != nil {
		t.Fatal(err)
	}

	tx2, _ := doc.Begin()
	_ = tx2.SpliceText(textID, 0, 0, "X")
	_, _ = tx2.Commit()

	idx, err := doc.ResolveTextCursor(cur, inttext.EncodingUTF8)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 2 {
		t.Fatalf("expected cursor to track after 'a' at index 2, got %d", idx)
	}
}

func TestTextCursorEncodingRoundTrip(t *testing.T) {
	doc := NewDocument()
	tx, _ := doc.Begin()
	textID, _ := tx.PutObject(model.RootObjID(), "text", ObjText)
	_ = tx.SpliceText(textID, 0, 0, "A😀B")
	_, _ = tx.Commit()

	cur, err := doc.CursorForText(textID, 3, inttext.EncodingUTF16)
	if err != nil {
		t.Fatal(err)
	}
	runeIdx, err := doc.ResolveTextCursor(cur, inttext.EncodingUTF8)
	if err != nil {
		t.Fatal(err)
	}
	if runeIdx != 2 {
		t.Fatalf("expected rune index 2 (after emoji), got %d", runeIdx)
	}
}
