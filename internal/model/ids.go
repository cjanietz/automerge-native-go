package model

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
)

var ErrInvalidChangeHashLength = errors.New("invalid change hash length")

// ActorID identifies the author of operations/changes.
type ActorID []byte

func NewActorID(raw []byte) ActorID {
	out := make([]byte, len(raw))
	copy(out, raw)
	return ActorID(out)
}

func ActorIDFromHex(s string) (ActorID, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return NewActorID(b), nil
}

func (a ActorID) Bytes() []byte {
	out := make([]byte, len(a))
	copy(out, a)
	return out
}

func (a ActorID) String() string {
	return hex.EncodeToString(a)
}

func (a ActorID) Compare(other ActorID) int {
	return bytes.Compare(a, other)
}

func (a ActorID) Equal(other ActorID) bool {
	return bytes.Equal(a, other)
}

// ChangeHash is a 32-byte hash that identifies a change.
type ChangeHash [32]byte

func ChangeHashFromBytes(raw []byte) (ChangeHash, error) {
	var h ChangeHash
	if len(raw) != len(h) {
		return h, fmt.Errorf("%w: got %d", ErrInvalidChangeHashLength, len(raw))
	}
	copy(h[:], raw)
	return h, nil
}

func MustChangeHashFromHex(s string) ChangeHash {
	h, err := ChangeHashFromHex(s)
	if err != nil {
		panic(err)
	}
	return h
}

func ChangeHashFromHex(s string) (ChangeHash, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return ChangeHash{}, err
	}
	return ChangeHashFromBytes(raw)
}

func (h ChangeHash) Bytes() []byte {
	out := make([]byte, len(h))
	copy(out, h[:])
	return out
}

func (h ChangeHash) String() string {
	return hex.EncodeToString(h[:])
}

func (h ChangeHash) Compare(other ChangeHash) int {
	return bytes.Compare(h[:], other[:])
}

func (h ChangeHash) Equal(other ChangeHash) bool {
	return h == other
}

// OpID identifies one operation (counter + actor index).
type OpID struct {
	Counter uint64
	Actor   uint32
}

func (id OpID) Compare(other OpID) int {
	if id.Counter < other.Counter {
		return -1
	}
	if id.Counter > other.Counter {
		return 1
	}
	if id.Actor < other.Actor {
		return -1
	}
	if id.Actor > other.Actor {
		return 1
	}
	return 0
}

func (id OpID) String() string {
	return fmt.Sprintf("%d@%d", id.Counter, id.Actor)
}

// ObjID identifies an object in the CRDT graph.
type ObjID struct {
	Root bool
	Op   OpID
}

func RootObjID() ObjID {
	return ObjID{Root: true}
}

func (id ObjID) Compare(other ObjID) int {
	if id.Root && !other.Root {
		return -1
	}
	if !id.Root && other.Root {
		return 1
	}
	return id.Op.Compare(other.Op)
}

func (id ObjID) String() string {
	if id.Root {
		return "_root"
	}
	return id.Op.String()
}
