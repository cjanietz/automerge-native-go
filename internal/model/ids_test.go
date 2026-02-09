package model

import "testing"

func TestActorIDHexRoundTrip(t *testing.T) {
	id, err := ActorIDFromHex("a1b2c3")
	if err != nil {
		t.Fatalf("decode actor id: %v", err)
	}
	if got, want := id.String(), "a1b2c3"; got != want {
		t.Fatalf("actor roundtrip mismatch: got %s want %s", got, want)
	}
}

func TestChangeHashLengthValidation(t *testing.T) {
	_, err := ChangeHashFromBytes([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for invalid hash length")
	}
}

func TestObjIDOrderingRootFirst(t *testing.T) {
	root := RootObjID()
	nonRoot := ObjID{Op: OpID{Counter: 1, Actor: 1}}
	if root.Compare(nonRoot) >= 0 {
		t.Fatal("expected root obj id to sort before non-root")
	}
}
