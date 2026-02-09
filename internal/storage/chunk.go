package storage

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync"
)

var (
	Magic = [4]byte{'A', 'M', 'G', '6'}

	ErrBadMagic       = errors.New("bad magic")
	ErrShortHeader    = errors.New("short header")
	ErrShortPayload   = errors.New("short payload")
	ErrBadChecksum    = errors.New("bad checksum")
	ErrUnknownChunk   = errors.New("unknown chunk type")
	ErrInflatePayload = errors.New("inflate payload")
	ErrDeflatePayload = errors.New("deflate payload")
)

var (
	deflateBufferPool = sync.Pool{
		New: func() any {
			return &bytes.Buffer{}
		},
	}
	deflateWriterPool = sync.Pool{
		New: func() any {
			w, _ := flate.NewWriter(io.Discard, flate.BestSpeed)
			return w
		},
	}
	inflateBufferPool = sync.Pool{
		New: func() any {
			return &bytes.Buffer{}
		},
	}
	inflateReaderPool = sync.Pool{
		New: func() any {
			return flate.NewReader(bytes.NewReader(nil))
		},
	}
	inflateScratchPool = sync.Pool{
		New: func() any {
			return make([]byte, 32*1024)
		},
	}
)

type ChunkType uint8

const (
	ChunkDocument         ChunkType = 1
	ChunkChange           ChunkType = 2
	ChunkCompressedChange ChunkType = 3
	ChunkBundle           ChunkType = 4
)

type Header struct {
	Version    uint8
	Type       ChunkType
	Flags      uint8
	Reserved   uint8
	PayloadLen uint32
	Checksum   uint32
}

const (
	headerLen         = 12
	flagDeflate uint8 = 1 << 0
	Version1    uint8 = 1
)

type DecodedChunk struct {
	Header  Header
	Payload []byte
}

func EncodeChunk(typ ChunkType, payload []byte, deflate bool) ([]byte, error) {
	body := payload
	flags := uint8(0)
	if deflate {
		compressed, err := deflateBytes(payload)
		if err != nil {
			return nil, err
		}
		body = compressed
		flags |= flagDeflate
	}
	chk := crc32.ChecksumIEEE(payload)
	h := Header{Version: Version1, Type: typ, Flags: flags, Reserved: 0, PayloadLen: uint32(len(body)), Checksum: chk}
	out := make([]byte, 0, len(Magic)+headerLen+len(body))
	out = append(out, Magic[:]...)
	out = append(out, h.Version, uint8(h.Type), h.Flags, h.Reserved)
	var tmp [8]byte
	binary.BigEndian.PutUint32(tmp[:4], h.PayloadLen)
	binary.BigEndian.PutUint32(tmp[4:8], h.Checksum)
	out = append(out, tmp[:8]...)
	out = append(out, body...)
	return out, nil
}

func ParseChunks(data []byte) ([]DecodedChunk, error) {
	chunks := make([]DecodedChunk, 0)
	offset := 0
	for offset < len(data) {
		if len(data)-offset < len(Magic)+headerLen {
			return chunks, ErrShortHeader
		}
		if !bytes.Equal(data[offset:offset+len(Magic)], Magic[:]) {
			return chunks, ErrBadMagic
		}
		offset += len(Magic)
		hdrBytes := data[offset : offset+headerLen]
		offset += headerLen
		h := Header{
			Version:    hdrBytes[0],
			Type:       ChunkType(hdrBytes[1]),
			Flags:      hdrBytes[2],
			Reserved:   hdrBytes[3],
			PayloadLen: binary.BigEndian.Uint32(hdrBytes[4:8]),
			Checksum:   binary.BigEndian.Uint32(hdrBytes[8:12]),
		}
		if !isKnownType(h.Type) {
			return chunks, ErrUnknownChunk
		}
		if len(data)-offset < int(h.PayloadLen) {
			return chunks, ErrShortPayload
		}
		raw := data[offset : offset+int(h.PayloadLen)]
		offset += int(h.PayloadLen)
		payload := raw
		if h.Flags&flagDeflate != 0 {
			inflated, err := inflateBytes(raw)
			if err != nil {
				return chunks, err
			}
			payload = inflated
		}
		if crc32.ChecksumIEEE(payload) != h.Checksum {
			return chunks, ErrBadChecksum
		}
		chunks = append(chunks, DecodedChunk{Header: h, Payload: payload})
	}
	return chunks, nil
}

func isKnownType(t ChunkType) bool {
	switch t {
	case ChunkDocument, ChunkChange, ChunkCompressedChange, ChunkBundle:
		return true
	default:
		return false
	}
}

func deflateBytes(in []byte) ([]byte, error) {
	buf := deflateBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	w := deflateWriterPool.Get().(*flate.Writer)
	w.Reset(buf)
	if _, err := w.Write(in); err != nil {
		w.Reset(io.Discard)
		deflateWriterPool.Put(w)
		releasePooledBuffer(buf, &deflateBufferPool)
		return nil, ErrDeflatePayload
	}
	if err := w.Close(); err != nil {
		w.Reset(io.Discard)
		deflateWriterPool.Put(w)
		releasePooledBuffer(buf, &deflateBufferPool)
		return nil, ErrDeflatePayload
	}
	out := append([]byte(nil), buf.Bytes()...)
	w.Reset(io.Discard)
	deflateWriterPool.Put(w)
	releasePooledBuffer(buf, &deflateBufferPool)
	return out, nil
}

func inflateBytes(in []byte) ([]byte, error) {
	r := inflateReaderPool.Get().(io.ReadCloser)
	if resetter, ok := r.(flate.Resetter); ok {
		resetter.Reset(bytes.NewReader(in), nil)
	} else {
		_ = r.Close()
		r = flate.NewReader(bytes.NewReader(in))
	}
	buf := inflateBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	est := len(in) * 3
	if est < 1024 {
		est = 1024
	}
	buf.Grow(est)
	scratch := inflateScratchPool.Get().([]byte)
	_, err := io.CopyBuffer(buf, r, scratch)
	inflateScratchPool.Put(scratch)
	if err != nil {
		_ = r.Close()
		inflateReaderPool.Put(r)
		releasePooledBuffer(buf, &inflateBufferPool)
		return nil, ErrInflatePayload
	}
	out := append([]byte(nil), buf.Bytes()...)
	_ = r.Close()
	inflateReaderPool.Put(r)
	releasePooledBuffer(buf, &inflateBufferPool)
	return out, nil
}

func releasePooledBuffer(buf *bytes.Buffer, pool *sync.Pool) {
	if buf.Cap() > 4*1024*1024 {
		return
	}
	buf.Reset()
	pool.Put(buf)
}
