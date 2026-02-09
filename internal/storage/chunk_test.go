package storage

import (
	"bytes"
	"testing"
)

func TestChunkRoundTrip(t *testing.T) {
	in := []byte("hello-storage")
	buf, err := EncodeChunk(ChunkDocument, in, true)
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := ParseChunks(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if chunks[0].Header.Type != ChunkDocument {
		t.Fatalf("wrong chunk type: %d", chunks[0].Header.Type)
	}
	if !bytes.Equal(chunks[0].Payload, in) {
		t.Fatalf("payload mismatch: got %q want %q", string(chunks[0].Payload), string(in))
	}
}

func TestBadChecksum(t *testing.T) {
	buf, err := EncodeChunk(ChunkChange, []byte("abc"), false)
	if err != nil {
		t.Fatal(err)
	}
	buf[len(buf)-1] ^= 0xff
	_, err = ParseChunks(buf)
	if err == nil {
		t.Fatal("expected checksum error")
	}
}
