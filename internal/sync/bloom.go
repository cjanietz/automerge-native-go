package sync

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/cjanietz/automerge-native-go/internal/model"
)

const (
	bloomBits  = 1024
	bloomWords = bloomBits / 64
)

type BloomFilter struct {
	Words [bloomWords]uint64
}

func NewBloomFilter() BloomFilter {
	return BloomFilter{}
}

func BloomFromHashes(hashes []model.ChangeHash) BloomFilter {
	b := NewBloomFilter()
	for _, h := range hashes {
		b.AddHash(h)
	}
	return b
}

func (b *BloomFilter) AddHash(hash model.ChangeHash) {
	h := fnv.New64a()
	_, _ = h.Write(hash[:])
	sum := h.Sum64()
	idx := sum % bloomBits
	word := idx / 64
	bit := idx % 64
	b.Words[word] |= (uint64(1) << bit)

	// second probe
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], sum^0x9e3779b97f4a7c15)
	h2 := fnv.New64a()
	_, _ = h2.Write(tmp[:])
	s2 := h2.Sum64()
	idx2 := s2 % bloomBits
	w2 := idx2 / 64
	b2 := idx2 % 64
	b.Words[w2] |= (uint64(1) << b2)
}

func (b BloomFilter) ContainsHash(hash model.ChangeHash) bool {
	h := fnv.New64a()
	_, _ = h.Write(hash[:])
	sum := h.Sum64()
	idx := sum % bloomBits
	word := idx / 64
	bit := idx % 64
	if (b.Words[word] & (uint64(1) << bit)) == 0 {
		return false
	}

	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], sum^0x9e3779b97f4a7c15)
	h2 := fnv.New64a()
	_, _ = h2.Write(tmp[:])
	s2 := h2.Sum64()
	idx2 := s2 % bloomBits
	w2 := idx2 / 64
	b2 := idx2 % 64
	return (b.Words[w2] & (uint64(1) << b2)) != 0
}
