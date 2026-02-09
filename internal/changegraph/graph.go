package changegraph

import (
	"errors"
	"fmt"

	"github.com/cjanietz/automerge-native-go/internal/model"
)

var (
	ErrChangeExists       = errors.New("change already exists")
	ErrUnknownDependency  = errors.New("unknown dependency")
	ErrInvalidActorSeq    = errors.New("invalid actor sequence")
	ErrUnknownHead        = errors.New("unknown head")
	ErrInvalidNodeIndex   = errors.New("invalid node index")
	ErrValidationMismatch = errors.New("change graph validation mismatch")
)

type ChangeMeta struct {
	Hash  model.ChangeHash
	Deps  []model.ChangeHash
	Actor uint32
	Seq   uint64
	MaxOp uint64
}

type node struct {
	hash    model.ChangeHash
	depIdx  []int
	depHash []model.ChangeHash
	actor   uint32
	seq     uint64
	maxOp   uint64
}

type Graph struct {
	nodes   []node
	byHash  map[model.ChangeHash]int
	headSet map[model.ChangeHash]struct{}
	byActor map[uint32][]int
	maxOp   uint64
}

func New() *Graph {
	return &Graph{
		byHash:  make(map[model.ChangeHash]int),
		headSet: make(map[model.ChangeHash]struct{}),
		byActor: make(map[uint32][]int),
	}
}

func (g *Graph) Len() int {
	return len(g.nodes)
}

func (g *Graph) IsEmpty() bool {
	return len(g.nodes) == 0
}

func (g *Graph) MaxOp() uint64 {
	return g.maxOp
}

func (g *Graph) MaxOpForActor(actor uint32) uint64 {
	indexes := g.byActor[actor]
	if len(indexes) == 0 {
		return 0
	}
	return g.nodes[indexes[len(indexes)-1]].maxOp
}

func (g *Graph) SeqForActor(actor uint32) uint64 {
	return uint64(len(g.byActor[actor]))
}

func (g *Graph) HashForActorSeq(actor uint32, seq uint64) (model.ChangeHash, bool) {
	if seq == 0 {
		return model.ChangeHash{}, false
	}
	indexes := g.byActor[actor]
	if int(seq) > len(indexes) {
		return model.ChangeHash{}, false
	}
	return g.nodes[indexes[seq-1]].hash, true
}

func (g *Graph) ActorIDs() []uint32 {
	ids := make([]uint32, 0, len(g.byActor))
	for id := range g.byActor {
		if len(g.byActor[id]) > 0 {
			ids = append(ids, id)
		}
	}
	// deterministic order
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			if ids[j] < ids[i] {
				ids[i], ids[j] = ids[j], ids[i]
			}
		}
	}
	return ids
}

func (g *Graph) HasChange(hash model.ChangeHash) bool {
	_, ok := g.byHash[hash]
	return ok
}

func (g *Graph) HashToIndex(hash model.ChangeHash) (int, bool) {
	idx, ok := g.byHash[hash]
	return idx, ok
}

func (g *Graph) IndexToHash(index int) (model.ChangeHash, bool) {
	if index < 0 || index >= len(g.nodes) {
		return model.ChangeHash{}, false
	}
	return g.nodes[index].hash, true
}

func (g *Graph) Heads() []model.ChangeHash {
	heads := make([]model.ChangeHash, 0, len(g.headSet))
	for h := range g.headSet {
		heads = append(heads, h)
	}
	model.SortChangeHashes(heads)
	return heads
}

func (g *Graph) DepsForHash(hash model.ChangeHash) ([]model.ChangeHash, bool) {
	idx, ok := g.byHash[hash]
	if !ok {
		return nil, false
	}
	deps := make([]model.ChangeHash, len(g.nodes[idx].depHash))
	copy(deps, g.nodes[idx].depHash)
	return deps, true
}

func (g *Graph) AddChange(meta ChangeMeta) error {
	if _, exists := g.byHash[meta.Hash]; exists {
		return ErrChangeExists
	}
	wantSeq := uint64(len(g.byActor[meta.Actor])) + 1
	if meta.Seq != wantSeq {
		return fmt.Errorf("%w: actor=%d got=%d want=%d", ErrInvalidActorSeq, meta.Actor, meta.Seq, wantSeq)
	}

	depHash := append([]model.ChangeHash(nil), meta.Deps...)
	model.SortChangeHashes(depHash)
	depIdx := make([]int, 0, len(depHash))
	for _, dep := range depHash {
		idx, ok := g.byHash[dep]
		if !ok {
			return fmt.Errorf("%w: %s", ErrUnknownDependency, dep)
		}
		depIdx = append(depIdx, idx)
	}

	n := node{
		hash:    meta.Hash,
		depIdx:  depIdx,
		depHash: depHash,
		actor:   meta.Actor,
		seq:     meta.Seq,
		maxOp:   meta.MaxOp,
	}
	index := len(g.nodes)
	g.nodes = append(g.nodes, n)
	g.byHash[meta.Hash] = index
	g.byActor[meta.Actor] = append(g.byActor[meta.Actor], index)
	if meta.MaxOp > g.maxOp {
		g.maxOp = meta.MaxOp
	}

	g.headSet[meta.Hash] = struct{}{}
	for _, dep := range depHash {
		delete(g.headSet, dep)
	}

	return nil
}

// GetHashesFromHeads returns the transitive closure of changes reachable from heads,
// ordered deterministically with dependencies appearing before dependents.
func (g *Graph) GetHashesFromHeads(heads []model.ChangeHash) ([]model.ChangeHash, error) {
	if len(heads) == 0 {
		all := make([]model.ChangeHash, len(g.nodes))
		for i := range g.nodes {
			all[i] = g.nodes[i].hash
		}
		return all, nil
	}

	orderedHeads := append([]model.ChangeHash(nil), heads...)
	model.SortChangeHashes(orderedHeads)
	for _, h := range orderedHeads {
		if !g.HasChange(h) {
			return nil, fmt.Errorf("%w: %s", ErrUnknownHead, h)
		}
	}

	visited := make(map[int]bool, len(g.nodes))
	out := make([]model.ChangeHash, 0, len(g.nodes))

	var dfs func(int)
	dfs = func(idx int) {
		if visited[idx] {
			return
		}
		visited[idx] = true
		for _, depIdx := range g.nodes[idx].depIdx {
			dfs(depIdx)
		}
		out = append(out, g.nodes[idx].hash)
	}

	for _, h := range orderedHeads {
		idx := g.byHash[h]
		dfs(idx)
	}

	return out, nil
}

func (g *Graph) ClockForHeads(heads []model.ChangeHash) (Clock, error) {
	hashes, err := g.GetHashesFromHeads(heads)
	if err != nil {
		return Clock{}, err
	}
	clk := NewClock()
	for _, h := range hashes {
		idx := g.byHash[h]
		n := g.nodes[idx]
		clk.Observe(n.actor, n.maxOp)
	}
	return clk, nil
}

func (g *Graph) Validate() error {
	if len(g.nodes) != len(g.byHash) {
		return fmt.Errorf("%w: node/hash count mismatch", ErrValidationMismatch)
	}

	computedHeads := make(map[model.ChangeHash]struct{}, len(g.nodes))
	for _, n := range g.nodes {
		computedHeads[n.hash] = struct{}{}
	}

	localMax := uint64(0)
	for idx, n := range g.nodes {
		if n.maxOp > localMax {
			localMax = n.maxOp
		}
		mapped, ok := g.byHash[n.hash]
		if !ok || mapped != idx {
			return fmt.Errorf("%w: hash index mismatch for %s", ErrValidationMismatch, n.hash)
		}
		if len(n.depIdx) != len(n.depHash) {
			return fmt.Errorf("%w: dep index/hash length mismatch", ErrValidationMismatch)
		}
		for i, depIdx := range n.depIdx {
			if depIdx < 0 || depIdx >= len(g.nodes) {
				return fmt.Errorf("%w: %w", ErrValidationMismatch, ErrInvalidNodeIndex)
			}
			if g.nodes[depIdx].hash != n.depHash[i] {
				return fmt.Errorf("%w: dependency hash mismatch", ErrValidationMismatch)
			}
			delete(computedHeads, n.depHash[i])
		}
	}

	if localMax != g.maxOp {
		return fmt.Errorf("%w: max op mismatch", ErrValidationMismatch)
	}

	if len(computedHeads) != len(g.headSet) {
		return fmt.Errorf("%w: head count mismatch", ErrValidationMismatch)
	}
	for h := range computedHeads {
		if _, ok := g.headSet[h]; !ok {
			return fmt.Errorf("%w: missing expected head %s", ErrValidationMismatch, h)
		}
	}

	for actor, idxs := range g.byActor {
		for i, idx := range idxs {
			n := g.nodes[idx]
			if n.actor != actor {
				return fmt.Errorf("%w: actor mismatch", ErrValidationMismatch)
			}
			wantSeq := uint64(i + 1)
			if n.seq != wantSeq {
				return fmt.Errorf("%w: actor seq mismatch actor=%d got=%d want=%d", ErrValidationMismatch, actor, n.seq, wantSeq)
			}
		}
	}

	return nil
}
