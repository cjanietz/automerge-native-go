package changegraph

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/cjanietz/automerge-native-go/internal/model"
)

type fixtureFile struct {
	Changes  []fixtureChange `json:"changes"`
	Expected fixtureExpect   `json:"expected"`
}

type fixtureChange struct {
	Hash  string   `json:"hash"`
	Deps  []string `json:"deps"`
	Actor uint32   `json:"actor"`
	Seq   uint64   `json:"seq"`
	MaxOp uint64   `json:"max_op"`
}

type fixtureExpect struct {
	Heads           []string          `json:"heads"`
	SeqByActor      map[string]uint64 `json:"seq_by_actor"`
	MaxOp           uint64            `json:"max_op"`
	DepsForA3       []string          `json:"deps_for_a3"`
	HashesForHeadA3 []string          `json:"hashes_for_head_a3"`
	ClockForHeads   map[string]uint64 `json:"clock_for_heads"`
}

func mustHash(t *testing.T, s string) model.ChangeHash {
	t.Helper()
	h, err := model.ChangeHashFromHex(s)
	if err != nil {
		t.Fatalf("decode hash %s: %v", s, err)
	}
	return h
}

func mustHashes(t *testing.T, values []string) []model.ChangeHash {
	t.Helper()
	out := make([]model.ChangeHash, 0, len(values))
	for _, v := range values {
		out = append(out, mustHash(t, v))
	}
	return out
}

func loadFixture(t *testing.T) fixtureFile {
	t.Helper()
	path := filepath.Join("testdata", "phase2_fixture.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var f fixtureFile
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	return f
}

func TestGraphFixtureExpectations(t *testing.T) {
	f := loadFixture(t)
	g := New()

	for _, c := range f.Changes {
		err := g.AddChange(ChangeMeta{
			Hash:  mustHash(t, c.Hash),
			Deps:  mustHashes(t, c.Deps),
			Actor: c.Actor,
			Seq:   c.Seq,
			MaxOp: c.MaxOp,
		})
		if err != nil {
			t.Fatalf("add change %s: %v", c.Hash, err)
		}
	}

	if err := g.Validate(); err != nil {
		t.Fatalf("validate graph: %v", err)
	}

	headWant := mustHashes(t, f.Expected.Heads)
	headGot := g.Heads()
	if len(headGot) != len(headWant) {
		t.Fatalf("head length mismatch: got %d want %d", len(headGot), len(headWant))
	}
	for i := range headGot {
		if !headGot[i].Equal(headWant[i]) {
			t.Fatalf("head mismatch[%d]: got %s want %s", i, headGot[i], headWant[i])
		}
	}

	if got := g.MaxOp(); got != f.Expected.MaxOp {
		t.Fatalf("max op mismatch: got %d want %d", got, f.Expected.MaxOp)
	}

	for actorS, wantSeq := range f.Expected.SeqByActor {
		var actor uint32
		_, err := fmt.Sscanf(actorS, "%d", &actor)
		if err != nil {
			t.Fatalf("parse actor %q: %v", actorS, err)
		}
		if got := g.SeqForActor(actor); got != wantSeq {
			t.Fatalf("seq mismatch actor=%d got=%d want=%d", actor, got, wantSeq)
		}
	}

	a3 := mustHash(t, "00000000000000000000000000000000000000000000000000000000000000a3")
	depsGot, ok := g.DepsForHash(a3)
	if !ok {
		t.Fatal("expected deps for a3")
	}
	depsWant := mustHashes(t, f.Expected.DepsForA3)
	if len(depsGot) != len(depsWant) {
		t.Fatalf("deps length mismatch: got %d want %d", len(depsGot), len(depsWant))
	}
	for i := range depsGot {
		if !depsGot[i].Equal(depsWant[i]) {
			t.Fatalf("deps mismatch[%d]: got %s want %s", i, depsGot[i], depsWant[i])
		}
	}

	hashesGot, err := g.GetHashesFromHeads([]model.ChangeHash{a3})
	if err != nil {
		t.Fatalf("hashes from head a3: %v", err)
	}
	hashesWant := mustHashes(t, f.Expected.HashesForHeadA3)
	if len(hashesGot) != len(hashesWant) {
		t.Fatalf("hashes length mismatch: got %d want %d", len(hashesGot), len(hashesWant))
	}
	for i := range hashesGot {
		if !hashesGot[i].Equal(hashesWant[i]) {
			t.Fatalf("hashes mismatch[%d]: got %s want %s", i, hashesGot[i], hashesWant[i])
		}
	}

	clk, err := g.ClockForHeads(g.Heads())
	if err != nil {
		t.Fatalf("clock for heads: %v", err)
	}
	for actorS, wantSeq := range f.Expected.ClockForHeads {
		var actor uint32
		_, err := fmt.Sscanf(actorS, "%d", &actor)
		if err != nil {
			t.Fatalf("parse actor %q: %v", actorS, err)
		}
		if got := clk.MaxSeq(actor); got != wantSeq {
			t.Fatalf("clock mismatch actor=%d got=%d want=%d", actor, got, wantSeq)
		}
	}
}

func TestGraphRandomizedInvariantInsertion(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	type planned struct {
		meta ChangeMeta
	}

	const (
		numActors  = 5
		numChanges = 200
	)

	plannedChanges := make([]planned, 0, numChanges)
	actorSeq := make([]uint64, numActors)
	lastHashByActor := make(map[uint32]model.ChangeHash, numActors)
	allHashes := make([]model.ChangeHash, 0, numChanges)
	maxOp := uint64(0)

	for i := 0; i < numChanges; i++ {
		hasher := model.NewChangeHasher()
		hasher.WriteString("phase2-rand")
		hasher.WriteUint64(uint64(i))
		h := hasher.Sum()

		actor := uint32(rng.Intn(numActors) + 1)
		actorSeq[actor-1]++
		seq := actorSeq[actor-1]

		depCount := 0
		if len(allHashes) > 0 {
			depCount = rng.Intn(4)
			if depCount > len(allHashes) {
				depCount = len(allHashes)
			}
		}
		depSet := map[model.ChangeHash]struct{}{}
		deps := make([]model.ChangeHash, 0, depCount)
		if seq > 1 {
			prev := lastHashByActor[actor]
			depSet[prev] = struct{}{}
			deps = append(deps, prev)
		}
		for len(deps) < depCount {
			dep := allHashes[rng.Intn(len(allHashes))]
			if _, exists := depSet[dep]; exists {
				continue
			}
			depSet[dep] = struct{}{}
			deps = append(deps, dep)
		}
		model.SortChangeHashes(deps)

		maxOp += uint64(rng.Intn(3) + 1)

		plannedChanges = append(plannedChanges, planned{meta: ChangeMeta{
			Hash:  h,
			Deps:  deps,
			Actor: actor,
			Seq:   seq,
			MaxOp: maxOp,
		}})
		allHashes = append(allHashes, h)
		lastHashByActor[actor] = h
	}

	g := New()
	pending := append([]planned(nil), plannedChanges...)
	added := map[model.ChangeHash]struct{}{}

	for len(pending) > 0 {
		readyIdx := make([]int, 0)
		for i, p := range pending {
			ok := true
			for _, d := range p.meta.Deps {
				if _, seen := added[d]; !seen {
					ok = false
					break
				}
			}
			if ok {
				readyIdx = append(readyIdx, i)
			}
		}
		if len(readyIdx) == 0 {
			t.Fatal("no causally ready change available")
		}
		pickPos := readyIdx[rng.Intn(len(readyIdx))]
		pick := pending[pickPos]

		if err := g.AddChange(pick.meta); err != nil {
			t.Fatalf("add change %s failed: %v", pick.meta.Hash, err)
		}
		added[pick.meta.Hash] = struct{}{}

		pending[pickPos] = pending[len(pending)-1]
		pending = pending[:len(pending)-1]

		if err := g.Validate(); err != nil {
			t.Fatalf("validate failed mid-build: %v", err)
		}
	}

	if g.Len() != numChanges {
		t.Fatalf("len mismatch: got %d want %d", g.Len(), numChanges)
	}
	if g.MaxOp() == 0 {
		t.Fatal("max op should be non-zero")
	}
	for actor := uint32(1); actor <= numActors; actor++ {
		if g.SeqForActor(actor) != actorSeq[actor-1] {
			t.Fatalf("actor seq mismatch actor=%d got=%d want=%d", actor, g.SeqForActor(actor), actorSeq[actor-1])
		}
	}

	clk, err := g.ClockForHeads(g.Heads())
	if err != nil {
		t.Fatalf("clock for heads failed: %v", err)
	}
	for actor := uint32(1); actor <= numActors; actor++ {
		if got, want := clk.MaxSeq(actor), g.MaxOpForActor(actor); got != want {
			t.Fatalf("clock actor max-op mismatch actor=%d got=%d want=%d", actor, got, want)
		}
	}
}
