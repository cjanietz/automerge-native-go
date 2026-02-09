package model

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
)

const (
	tagActorID    byte = 0x01
	tagChangeHash byte = 0x02
	tagOpID       byte = 0x03
	tagObjID      byte = 0x04
	tagString     byte = 0x05
	tagBytes      byte = 0x06
	tagBool       byte = 0x07
	tagUint64     byte = 0x08
	tagInt64      byte = 0x09
)

type ChangeHasher struct {
	h  hash.Hash
	b1 [1]byte
	b8 [8]byte
}

func NewChangeHasher() *ChangeHasher {
	return &ChangeHasher{h: sha256.New()}
}

func (c *ChangeHasher) Reset() {
	c.h.Reset()
}

func (c *ChangeHasher) writeTag(tag byte) {
	c.b1[0] = tag
	_, _ = c.h.Write(c.b1[:])
}

func (c *ChangeHasher) writeLen(n int) {
	binary.BigEndian.PutUint64(c.b8[:], uint64(n))
	_, _ = c.h.Write(c.b8[:])
}

func (c *ChangeHasher) WriteActorID(id ActorID) {
	c.writeTag(tagActorID)
	c.writeLen(len(id))
	_, _ = c.h.Write(id)
}

func (c *ChangeHasher) WriteChangeHash(h ChangeHash) {
	c.writeTag(tagChangeHash)
	_, _ = c.h.Write(h[:])
}

func (c *ChangeHasher) WriteOpID(id OpID) {
	c.writeTag(tagOpID)
	c.WriteUint64(id.Counter)
	c.WriteUint64(uint64(id.Actor))
}

func (c *ChangeHasher) WriteObjID(id ObjID) {
	c.writeTag(tagObjID)
	c.WriteBool(id.Root)
	if !id.Root {
		c.WriteOpID(id.Op)
	}
}

func (c *ChangeHasher) WriteString(s string) {
	c.writeTag(tagString)
	c.writeLen(len(s))
	_, _ = c.h.Write([]byte(s))
}

func (c *ChangeHasher) WriteBytes(v []byte) {
	c.writeTag(tagBytes)
	c.writeLen(len(v))
	_, _ = c.h.Write(v)
}

func (c *ChangeHasher) WriteBool(v bool) {
	c.writeTag(tagBool)
	if v {
		c.b1[0] = 1
		_, _ = c.h.Write(c.b1[:])
		return
	}
	c.b1[0] = 0
	_, _ = c.h.Write(c.b1[:])
}

func (c *ChangeHasher) WriteUint64(v uint64) {
	c.writeTag(tagUint64)
	binary.BigEndian.PutUint64(c.b8[:], v)
	_, _ = c.h.Write(c.b8[:])
}

func (c *ChangeHasher) WriteInt64(v int64) {
	c.writeTag(tagInt64)
	binary.BigEndian.PutUint64(c.b8[:], uint64(v))
	_, _ = c.h.Write(c.b8[:])
}

func (c *ChangeHasher) Sum() ChangeHash {
	var h ChangeHash
	_ = c.h.Sum(h[:0])
	return h
}

// HashDeterministicStringMap produces a stable hash independent of map iteration order.
func HashDeterministicStringMap(m map[string]string) ChangeHash {
	h := NewChangeHasher()
	keys := SortedMapKeys(m)
	h.WriteUint64(uint64(len(keys)))
	for _, k := range keys {
		h.WriteString(k)
		h.WriteString(m[k])
	}
	return h.Sum()
}
