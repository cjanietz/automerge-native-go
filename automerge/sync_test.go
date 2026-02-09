package automerge

import (
	"testing"

	"github.com/cjanietz/automerge-native-go/internal/model"
	intsync "github.com/cjanietz/automerge-native-go/internal/sync"
)

func syncRoundTrip(from *Document, fromState *intsync.State, to *Document, toState *intsync.State) (bool, error) {
	msg, err := from.Sync().GenerateSyncMessage(fromState)
	if err != nil {
		return false, err
	}
	if msg == nil {
		return false, nil
	}
	enc, err := msg.Encode()
	if err != nil {
		return false, err
	}
	dec, err := intsync.DecodeMessage(enc)
	if err != nil {
		return false, err
	}
	if err := to.Sync().ReceiveSyncMessage(toState, dec); err != nil {
		return false, err
	}
	return true, nil
}

func TestTwoPeerSyncConverges(t *testing.T) {
	p1 := NewDocument()
	p2 := NewDocument()
	_ = p2.SetActor(2)

	tx1, _ := p1.Begin()
	_ = tx1.Put(model.RootObjID(), "a", model.StringValue("one"))
	_, _ = tx1.Commit()

	tx2, _ := p2.Begin()
	_ = tx2.Put(model.RootObjID(), "b", model.StringValue("two"))
	_, _ = tx2.Commit()

	s12 := intsync.NewState()
	s21 := intsync.NewState()
	caps := []intsync.Capability{intsync.CapabilityMessageV1, intsync.CapabilityMessageV2}
	s12.TheirCapabilities = &caps
	s21.TheirCapabilities = &caps

	for i := 0; i < 20; i++ {
		moved1, err := syncRoundTrip(p1, s12, p2, s21)
		if err != nil {
			t.Fatal(err)
		}
		moved2, err := syncRoundTrip(p2, s21, p1, s12)
		if err != nil {
			t.Fatal(err)
		}
		if !moved1 && !moved2 {
			break
		}
	}

	if v, ok := p1.GetMap(model.RootObjID(), "b", nil); !ok || v.Scalar.String != "two" {
		t.Fatalf("p1 missing b after sync: %#v ok=%v", v, ok)
	}
	if v, ok := p2.GetMap(model.RootObjID(), "a", nil); !ok || v.Scalar.String != "one" {
		t.Fatalf("p2 missing a after sync: %#v ok=%v", v, ok)
	}
}

func TestSyncInFlightGating(t *testing.T) {
	d := NewDocument()
	tx, _ := d.Begin()
	_ = tx.Put(model.RootObjID(), "k", model.StringValue("v"))
	_, _ = tx.Commit()

	s := intsync.NewState()
	caps := []intsync.Capability{intsync.CapabilityMessageV1}
	s.TheirCapabilities = &caps
	need := []model.ChangeHash{}
	have := []intsync.Have{}
	s.TheirNeed = &need
	s.TheirHave = &have

	m1, err := d.Sync().GenerateSyncMessage(s)
	if err != nil || m1 == nil {
		t.Fatalf("expected first message, err=%v", err)
	}
	m2, err := d.Sync().GenerateSyncMessage(s)
	if err != nil {
		t.Fatal(err)
	}
	if m2 != nil {
		t.Fatal("expected nil second message while in-flight")
	}
}

func TestSyncMessageDeterministicEncoding(t *testing.T) {
	d := NewDocument()
	for i := 0; i < 3; i++ {
		tx, _ := d.Begin()
		_ = tx.Put(model.RootObjID(), "k", model.StringValue(string(rune('a'+i))))
		_, _ = tx.Commit()
	}
	mkState := func() *intsync.State {
		s := intsync.NewState()
		caps := []intsync.Capability{intsync.CapabilityMessageV1}
		s.TheirCapabilities = &caps
		need := []model.ChangeHash{}
		have := []intsync.Have{}
		s.TheirNeed = &need
		s.TheirHave = &have
		return s
	}
	m1, err := d.Sync().GenerateSyncMessage(mkState())
	if err != nil || m1 == nil {
		t.Fatalf("message1 generate failed: %v", err)
	}
	m2, err := d.Sync().GenerateSyncMessage(mkState())
	if err != nil || m2 == nil {
		t.Fatalf("message2 generate failed: %v", err)
	}
	b1, _ := m1.Encode()
	b2, _ := m2.Encode()
	if string(b1) != string(b2) {
		t.Fatalf("message encoding not deterministic\n1=%x\n2=%x", b1, b2)
	}
}
