package storage

import (
	"bytes"
	"compress/flate"
	"crypto/sha256"
	"errors"
	"io"
)

var (
	RustMagic = [4]byte{0x85, 0x6f, 0x4a, 0x83}

	ErrRustBadMagic  = errors.New("rust bad magic")
	ErrRustShort     = errors.New("rust short input")
	ErrRustChecksum  = errors.New("rust checksum mismatch")
	ErrRustChunkType = errors.New("rust unknown chunk type")
)

type RustChunkType uint8

const (
	RustChunkDocument   RustChunkType = 0
	RustChunkChange     RustChunkType = 1
	RustChunkCompressed RustChunkType = 2
	RustChunkBundle     RustChunkType = 3
)

type RustChunk struct {
	Type     RustChunkType
	Checksum [4]byte
	Payload  []byte
}

func ParseRustChunks(data []byte) ([]RustChunk, error) {
	offset := 0
	out := make([]RustChunk, 0)
	for offset < len(data) {
		if len(data)-offset < 9 {
			return out, ErrRustShort
		}
		if !bytes.Equal(data[offset:offset+4], RustMagic[:]) {
			return out, ErrRustBadMagic
		}
		offset += 4
		var chk [4]byte
		copy(chk[:], data[offset:offset+4])
		offset += 4
		typ := RustChunkType(data[offset])
		offset++
		if typ > RustChunkBundle {
			return out, ErrRustChunkType
		}
		l, n, err := readULEB(data[offset:])
		if err != nil {
			return out, err
		}
		offset += n
		if len(data)-offset < int(l) {
			return out, ErrRustShort
		}
		payload := append([]byte(nil), data[offset:offset+int(l)]...)
		offset += int(l)

		if typ == RustChunkCompressed {
			uncompressed, err := inflateRust(payload)
			if err != nil {
				return out, err
			}
			if !rustChecksumMatches(chk, RustChunkChange, uncompressed) {
				return out, ErrRustChecksum
			}
		} else {
			if !rustChecksumMatches(chk, typ, payload) {
				return out, ErrRustChecksum
			}
		}

		out = append(out, RustChunk{Type: typ, Checksum: chk, Payload: payload})
	}
	return out, nil
}

func rustChecksumMatches(checksum [4]byte, typ RustChunkType, payload []byte) bool {
	h := sha256.New()
	h.Write([]byte{byte(typ)})
	lenEnc := appendULEB(nil, uint64(len(payload)))
	h.Write(lenEnc)
	h.Write(payload)
	sum := h.Sum(nil)
	return bytes.Equal(checksum[:], sum[:4])
}

func inflateRust(in []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(in))
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, ErrInflatePayload
	}
	return out, nil
}
