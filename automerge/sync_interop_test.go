package automerge

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cjanietz/automerge-native-go/internal/model"
	intsync "github.com/cjanietz/automerge-native-go/internal/sync"
)

func TestSyncWithRustFixturePeer(t *testing.T) {
	fixture := filepath.Join("..", "..", "rust", "automerge", "tests", "fixtures", "two_change_chunks.automerge")
	fixtureBytes, err := os.ReadFile(fixture)
	if err != nil {
		t.Skipf("fixture not available: %v", err)
	}

	rustPeer, err := LoadWithOptions(fixtureBytes, DefaultLoadOptions())
	if err != nil {
		t.Fatalf("load rust fixture peer: %v", err)
	}
	goPeer := NewDocument()

	sRustToGo := intsync.NewState()
	sGoToRust := intsync.NewState()
	caps := []intsync.Capability{intsync.CapabilityMessageV1, intsync.CapabilityMessageV2}
	sRustToGo.TheirCapabilities = &caps
	sGoToRust.TheirCapabilities = &caps

	emptyHeads := []model.ChangeHash{}
	emptyNeed := []model.ChangeHash{}
	emptyHave := []intsync.Have{}
	// Force full-doc send optimization path for fixture interop.
	sRustToGo.TheirHeads = &emptyHeads
	sRustToGo.TheirNeed = &emptyNeed
	sRustToGo.TheirHave = &emptyHave

	msg, err := rustPeer.Sync().GenerateSyncMessage(sRustToGo)
	if err != nil {
		t.Fatalf("generate sync message from rust peer: %v", err)
	}
	if msg == nil {
		t.Fatal("expected rust fixture peer to produce sync message")
	}
	if len(msg.DocumentPayload) == 0 {
		t.Fatal("expected V2 sync message with document payload")
	}

	enc, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode sync message: %v", err)
	}
	dec, err := intsync.DecodeMessage(enc)
	if err != nil {
		t.Fatalf("decode sync message: %v", err)
	}
	if err := goPeer.Sync().ReceiveSyncMessage(sGoToRust, dec); err != nil {
		t.Fatalf("receive sync message on go peer: %v", err)
	}

	syncedBytes, err := goPeer.Save()
	if err != nil {
		t.Fatalf("save synced go peer: %v", err)
	}
	if !bytes.Equal(syncedBytes, fixtureBytes) {
		t.Fatalf("synced payload mismatch with rust fixture: got %d bytes want %d", len(syncedBytes), len(fixtureBytes))
	}
}
