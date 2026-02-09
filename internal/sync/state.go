package sync

import (
	"bytes"
	"errors"

	"github.com/cjanietz/automerge-native-go/internal/model"
)

const syncStateType byte = 0x43

var (
	ErrStateDecode = errors.New("sync state decode")
)

type Capability uint8

const (
	CapabilityMessageV1 Capability = iota
	CapabilityMessageV2
)

type Have struct {
	LastSync []model.ChangeHash
	Bloom    BloomFilter
}

type State struct {
	SharedHeads       []model.ChangeHash
	LastSentHeads     []model.ChangeHash
	TheirHeads        *[]model.ChangeHash
	TheirNeed         *[]model.ChangeHash
	TheirHave         *[]Have
	SentHashes        map[model.ChangeHash]struct{}
	InFlight          bool
	HaveResponded     bool
	TheirCapabilities *[]Capability
}

func NewState() *State {
	return &State{SentHashes: make(map[model.ChangeHash]struct{})}
}

func (s *State) Encode() []byte {
	out := []byte{syncStateType}
	out = appendULEB(out, uint64(len(s.SharedHeads)))
	for _, h := range s.SharedHeads {
		out = append(out, h[:]...)
	}
	return out
}

func DecodeState(in []byte) (*State, error) {
	if len(in) == 0 || in[0] != syncStateType {
		return nil, ErrStateDecode
	}
	offset := 1
	count, n, err := readULEB(in[offset:])
	if err != nil {
		return nil, ErrStateDecode
	}
	offset += n
	head := make([]model.ChangeHash, 0, count)
	for i := uint64(0); i < count; i++ {
		if len(in)-offset < 32 {
			return nil, ErrStateDecode
		}
		var h model.ChangeHash
		copy(h[:], in[offset:offset+32])
		offset += 32
		head = append(head, h)
	}
	if len(in) != offset {
		return nil, ErrStateDecode
	}
	s := NewState()
	s.SharedHeads = head
	emptyHave := []Have{}
	s.TheirHave = &emptyHave
	return s, nil
}

func (s *State) SendDoc() bool {
	if s.TheirHeads == nil || s.TheirCapabilities == nil {
		return false
	}
	if len(*s.TheirHeads) != 0 {
		return false
	}
	return s.SupportsV2()
}

func (s *State) SupportsV2() bool {
	if s.TheirCapabilities == nil {
		return false
	}
	for _, c := range *s.TheirCapabilities {
		if c == CapabilityMessageV2 {
			return true
		}
	}
	return false
}

func (s *State) EqualSharedHeads(other []model.ChangeHash) bool {
	if len(s.SharedHeads) != len(other) {
		return false
	}
	for i := range other {
		if !bytes.Equal(s.SharedHeads[i][:], other[i][:]) {
			return false
		}
	}
	return true
}
