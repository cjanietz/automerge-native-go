package sync

import (
	"testing"

	"github.com/cjanietz/automerge-native-go/internal/model"
)

func TestStateEncodeDecode(t *testing.T) {
	h1 := model.MustChangeHashFromHex("0000000000000000000000000000000000000000000000000000000000000001")
	h2 := model.MustChangeHashFromHex("0000000000000000000000000000000000000000000000000000000000000002")
	s := NewState()
	s.SharedHeads = []model.ChangeHash{h1, h2}
	enc := s.Encode()
	dec, err := DecodeState(enc)
	if err != nil {
		t.Fatal(err)
	}
	if len(dec.SharedHeads) != 2 || dec.SharedHeads[0] != h1 || dec.SharedHeads[1] != h2 {
		t.Fatalf("shared heads mismatch: %#v", dec.SharedHeads)
	}
}

func TestMessageEncodeDecode(t *testing.T) {
	h := model.MustChangeHashFromHex("000000000000000000000000000000000000000000000000000000000000000a")
	m := Message{
		Version:               MessageV2,
		Heads:                 []model.ChangeHash{h},
		Need:                  []model.ChangeHash{h},
		SupportedCapabilities: []Capability{CapabilityMessageV1, CapabilityMessageV2},
		ChangePayload:         []byte("abc"),
		DocumentPayload:       []byte("doc"),
		Have:                  []Have{{LastSync: []model.ChangeHash{h}, Bloom: BloomFromHashes([]model.ChangeHash{h})}},
	}
	enc, err := m.Encode()
	if err != nil {
		t.Fatal(err)
	}
	dec, err := DecodeMessage(enc)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Version != MessageV2 || len(dec.Heads) != 1 || dec.Heads[0] != h {
		t.Fatalf("decoded message mismatch: %#v", dec)
	}
}

func TestBloomContains(t *testing.T) {
	h1 := model.MustChangeHashFromHex("00000000000000000000000000000000000000000000000000000000000000aa")
	h2 := model.MustChangeHashFromHex("00000000000000000000000000000000000000000000000000000000000000bb")
	b := BloomFromHashes([]model.ChangeHash{h1})
	if !b.ContainsHash(h1) {
		t.Fatal("expected bloom to contain h1")
	}
	_ = h2 // false positives possible; do not assert false
}
