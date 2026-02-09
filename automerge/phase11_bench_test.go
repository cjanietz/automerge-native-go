package automerge

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cjanietz/automerge-native-go/internal/model"
	intsync "github.com/cjanietz/automerge-native-go/internal/sync"
)

func benchmarkSeedDoc(changeCount int) *Document {
	d := NewDocument()
	for i := 0; i < changeCount; i++ {
		tx, _ := d.Begin()
		_ = tx.Put(model.RootObjID(), "k", model.StringValue(fmt.Sprintf("v-%04d", i)))
		_, _ = tx.Commit()
	}
	return d
}

func benchmarkSeedTextDoc(initialLen int) (*Document, model.ObjID) {
	d := NewDocument()
	tx, _ := d.Begin()
	textID, _ := tx.PutObject(model.RootObjID(), "text", ObjText)
	_ = tx.SpliceText(textID, 0, 0, strings.Repeat("a", initialLen))
	_, _ = tx.Commit()
	return d, textID
}

func BenchmarkLoadSave(b *testing.B) {
	source := benchmarkSeedDoc(500)
	blob, err := source.Save()
	if err != nil {
		b.Fatalf("seed save: %v", err)
	}

	b.Run("Save", func(b *testing.B) {
		b.ReportAllocs()
		doc := benchmarkSeedDoc(500)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := doc.Save(); err != nil {
				b.Fatalf("save: %v", err)
			}
		}
	})

	b.Run("Load", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := Load(blob); err != nil {
				b.Fatalf("load: %v", err)
			}
		}
	})
}

func BenchmarkApplyMerge(b *testing.B) {
	source := benchmarkSeedDoc(400)
	changes := source.AllChanges()

	b.Run("ApplyChangesBatch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			d := NewDocument()
			if err := d.ApplyChanges(changes); err != nil {
				b.Fatalf("apply: %v", err)
			}
		}
	})

	b.Run("MergeDivergedDocs", func(b *testing.B) {
		b.ReportAllocs()
		left := NewDocument()
		right := NewDocument()
		base := benchmarkSeedDoc(200)
		baseChanges := base.AllChanges()
		_ = left.ApplyChanges(baseChanges)
		_ = right.ApplyChanges(baseChanges)
		_ = right.SetActor(2)
		for i := 0; i < 100; i++ {
			txl, _ := left.Begin()
			_ = txl.Put(model.RootObjID(), fmt.Sprintf("left-%03d", i), model.StringValue("x"))
			_, _ = txl.Commit()
			txr, _ := right.Begin()
			_ = txr.Put(model.RootObjID(), fmt.Sprintf("right-%03d", i), model.StringValue("y"))
			_, _ = txr.Commit()
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			a := NewDocument()
			bb := NewDocument()
			_ = a.ApplyChanges(left.AllChanges())
			_ = bb.ApplyChanges(right.AllChanges())
			if err := a.Merge(bb); err != nil {
				b.Fatalf("merge: %v", err)
			}
		}
	})
}

func BenchmarkTextSplice(b *testing.B) {
	doc, textID := benchmarkSeedTextDoc(1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := doc.Begin()
		index := (i % 800) + 100
		if err := tx.SpliceText(textID, index, 1, "bc"); err != nil {
			b.Fatalf("splice: %v", err)
		}
		if _, err := tx.Commit(); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}
}

func BenchmarkSyncMessageGeneration(b *testing.B) {
	doc := benchmarkSeedDoc(500)

	mkState := func() *intsync.State {
		s := intsync.NewState()
		caps := []intsync.Capability{intsync.CapabilityMessageV1, intsync.CapabilityMessageV2}
		s.TheirCapabilities = &caps
		need := []model.ChangeHash{}
		have := []intsync.Have{}
		theirHeads := []model.ChangeHash{}
		s.TheirNeed = &need
		s.TheirHave = &have
		s.TheirHeads = &theirHeads
		return s
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg, err := doc.Sync().GenerateSyncMessage(mkState())
		if err != nil {
			b.Fatalf("generate sync message: %v", err)
		}
		if msg == nil {
			b.Fatal("expected non-nil sync message")
		}
	}
}
