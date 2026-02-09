package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseRustFixtureChunks(t *testing.T) {
	fixture := filepath.Join("..", "..", "..", "rust", "automerge", "tests", "fixtures", "two_change_chunks.automerge")
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Skipf("fixture not available: %v", err)
	}
	chunks, err := ParseRustChunks(data)
	if err != nil {
		t.Fatalf("parse rust chunks: %v", err)
	}
	if len(chunks) == 0 {
		t.Fatal("expected non-empty rust chunk list")
	}
}
