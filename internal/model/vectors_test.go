package model

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type phase1Vectors struct {
	ActorIDsUnsorted   []string `json:"actor_ids_unsorted"`
	ActorIDsSorted     []string `json:"actor_ids_sorted"`
	ChangeUnsorted     []string `json:"change_hashes_unsorted"`
	ChangeSorted       []string `json:"change_hashes_sorted"`
	StreamHashVector   streamHV `json:"stream_hash_vector"`
	DeterministicMapHV mapHV    `json:"map_hash_vector"`
}

type streamHV struct {
	ActorID string `json:"actor_id"`
	OpID    struct {
		Counter uint64 `json:"counter"`
		Actor   uint32 `json:"actor"`
	} `json:"op_id"`
	ObjID struct {
		Counter uint64 `json:"counter"`
		Actor   uint32 `json:"actor"`
		Root    bool   `json:"root"`
	} `json:"obj_id"`
	Text     string `json:"text"`
	Bool     bool   `json:"bool"`
	Int      int64  `json:"int"`
	Expected string `json:"expected"`
}

type mapHV struct {
	Pairs    map[string]string `json:"pairs"`
	Expected string            `json:"expected"`
}

func loadPhase1Vectors(t *testing.T) phase1Vectors {
	t.Helper()
	path := filepath.Join("testdata", "phase1_vectors.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var v phase1Vectors
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("decode vectors: %v", err)
	}
	return v
}

func mustActorIDs(t *testing.T, hexValues []string) []ActorID {
	t.Helper()
	out := make([]ActorID, 0, len(hexValues))
	for _, h := range hexValues {
		id, err := ActorIDFromHex(h)
		if err != nil {
			t.Fatalf("decode actor id %s: %v", h, err)
		}
		out = append(out, id)
	}
	return out
}

func mustHashes(t *testing.T, hexValues []string) []ChangeHash {
	t.Helper()
	out := make([]ChangeHash, 0, len(hexValues))
	for _, h := range hexValues {
		ch, err := ChangeHashFromHex(h)
		if err != nil {
			t.Fatalf("decode hash %s: %v", h, err)
		}
		out = append(out, ch)
	}
	return out
}

func TestSortVectorsFromFixture(t *testing.T) {
	v := loadPhase1Vectors(t)

	actors := mustActorIDs(t, v.ActorIDsUnsorted)
	SortActorIDs(actors)
	wantActors := mustActorIDs(t, v.ActorIDsSorted)
	for i := range actors {
		if !actors[i].Equal(wantActors[i]) {
			t.Fatalf("actor ordering mismatch at %d: got %s want %s", i, actors[i], wantActors[i])
		}
	}

	hashes := mustHashes(t, v.ChangeUnsorted)
	SortChangeHashes(hashes)
	wantHashes := mustHashes(t, v.ChangeSorted)
	for i := range hashes {
		if !hashes[i].Equal(wantHashes[i]) {
			t.Fatalf("hash ordering mismatch at %d: got %s want %s", i, hashes[i], wantHashes[i])
		}
	}
}

func TestChangeHashVectorFromFixture(t *testing.T) {
	v := loadPhase1Vectors(t)
	s := v.StreamHashVector

	actor, err := ActorIDFromHex(s.ActorID)
	if err != nil {
		t.Fatalf("decode actor: %v", err)
	}

	h := NewChangeHasher()
	h.WriteActorID(actor)
	h.WriteOpID(OpID{Counter: s.OpID.Counter, Actor: s.OpID.Actor})
	h.WriteObjID(ObjID{Root: s.ObjID.Root, Op: OpID{Counter: s.ObjID.Counter, Actor: s.ObjID.Actor}})
	h.WriteString(s.Text)
	h.WriteBool(s.Bool)
	h.WriteInt64(s.Int)
	if got := h.Sum().String(); got != s.Expected {
		t.Fatalf("stream hash mismatch: got %s want %s", got, s.Expected)
	}
}

func TestMapHashVectorFromFixture(t *testing.T) {
	v := loadPhase1Vectors(t)
	if got := HashDeterministicStringMap(v.DeterministicMapHV.Pairs).String(); got != v.DeterministicMapHV.Expected {
		t.Fatalf("map hash mismatch: got %s want %s", got, v.DeterministicMapHV.Expected)
	}

	mapA := map[string]string{"z": "last", "a": "first", "m": "middle"}
	mapB := map[string]string{"m": "middle", "z": "last", "a": "first"}
	hashA := HashDeterministicStringMap(mapA)
	hashB := HashDeterministicStringMap(mapB)
	if !hashA.Equal(hashB) {
		t.Fatalf("deterministic map hash changed with insertion order: %s vs %s", hashA, hashB)
	}
}
