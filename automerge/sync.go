package automerge

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cjanietz/automerge-native-go/internal/model"
	intsync "github.com/cjanietz/automerge-native-go/internal/sync"
)

var ErrSyncDecodeChanges = errors.New("sync decode changes")

type SyncEngine struct {
	doc *Document
}

func (d *Document) Sync() *SyncEngine {
	return &SyncEngine{doc: d}
}

func (s *SyncEngine) GenerateSyncMessage(state *intsync.State) (*intsync.Message, error) {
	if state == nil {
		return nil, nil
	}
	if state.SentHashes == nil {
		state.SentHashes = make(map[model.ChangeHash]struct{})
	}

	ourHeads := s.doc.Heads()
	theirHeads := []model.ChangeHash{}
	if state.TheirHeads != nil {
		theirHeads = append(theirHeads, (*state.TheirHeads)...)
	}

	ourNeed := s.doc.getMissingDeps(theirHeads)
	ourHave := []intsync.Have{s.makeHave(state.SharedHeads)}

	msg := &intsync.Message{
		Version:               intsync.MessageV1,
		Heads:                 ourHeads,
		Have:                  ourHave,
		Need:                  ourNeed,
		SupportedCapabilities: []intsync.Capability{intsync.CapabilityMessageV1, intsync.CapabilityMessageV2},
	}

	var hashesToSend []model.ChangeHash
	if state.SendDoc() {
		all, err := s.doc.graph.GetHashesFromHeads(ourHeads)
		if err != nil {
			return nil, err
		}
		hashesToSend = all
		docBytes, err := s.doc.Save()
		if err != nil {
			return nil, err
		}
		msg.Version = intsync.MessageV2
		msg.DocumentPayload = docBytes
	} else if state.TheirHave != nil && state.TheirNeed != nil {
		hashesToSend = s.getHashesToSend(*state.TheirHave, *state.TheirNeed)
		if len(hashesToSend) > 0 {
			all, _ := s.doc.graph.GetHashesFromHeads(ourHeads)
			if len(all) > 0 && len(hashesToSend) > len(all)/3 && state.SupportsV2() {
				docBytes, err := s.doc.Save()
				if err != nil {
					return nil, err
				}
				msg.Version = intsync.MessageV2
				msg.DocumentPayload = docBytes
			} else {
				payload, err := s.serializeChangesByHashes(hashesToSend)
				if err != nil {
					return nil, err
				}
				msg.ChangePayload = payload
			}
		}
	} else {
		// initial handshake path: send all changes to ensure convergence
		all, err := s.doc.graph.GetHashesFromHeads(ourHeads)
		if err != nil {
			return nil, err
		}
		hashesToSend = all
		payload, err := s.serializeChangesByHashes(hashesToSend)
		if err != nil {
			return nil, err
		}
		msg.ChangePayload = payload
	}

	headsUnchanged := hashesEqual(state.LastSentHeads, ourHeads)
	headsEqual := state.TheirHeads != nil && hashesEqual(*state.TheirHeads, ourHeads)
	msgEmpty := len(msg.ChangePayload) == 0 && len(msg.DocumentPayload) == 0
	if headsUnchanged && state.HaveResponded {
		if headsEqual && msgEmpty {
			return nil, nil
		}
		if state.InFlight {
			return nil, nil
		}
	}

	state.HaveResponded = true
	state.LastSentHeads = append([]model.ChangeHash(nil), ourHeads...)
	for _, h := range hashesToSend {
		state.SentHashes[h] = struct{}{}
	}
	state.InFlight = true
	return msg, nil
}

func (s *SyncEngine) ReceiveSyncMessage(state *intsync.State, msg intsync.Message) error {
	if state == nil {
		return nil
	}
	state.InFlight = false
	state.TheirHeads = ptrHashes(msg.Heads)
	state.TheirNeed = ptrHashes(msg.Need)
	if msg.Have != nil {
		h := make([]intsync.Have, len(msg.Have))
		copy(h, msg.Have)
		state.TheirHave = &h
	}
	if msg.SupportedCapabilities != nil {
		caps := append([]intsync.Capability(nil), msg.SupportedCapabilities...)
		state.TheirCapabilities = &caps
	}

	if len(msg.DocumentPayload) > 0 {
		doc, err := LoadWithOptions(msg.DocumentPayload, LoadOptions{OnPartialLoad: OnPartialIgnore, Verification: VerificationCheck, StringMigration: StringMigrationNone})
		if err != nil {
			return err
		}
		if len(doc.legacyRaw) > 0 && len(doc.changes) == 0 {
			// Interop path: preserve Rust/JS binary payloads we currently cannot fully materialize.
			s.doc.legacyRaw = append([]byte(nil), doc.legacyRaw...)
		} else {
			if err := s.doc.ApplyChanges(doc.AllChanges()); err != nil {
				return err
			}
		}
	}

	if len(msg.ChangePayload) > 0 {
		var dtos []changeDTO
		if err := json.Unmarshal(msg.ChangePayload, &dtos); err != nil {
			return fmt.Errorf("%w: %v", ErrSyncDecodeChanges, err)
		}
		changes, err := decodeChanges(dtos)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrSyncDecodeChanges, err)
		}
		if err := s.doc.ApplyChanges(changes); err != nil {
			return err
		}
	}

	shared := intersectHashes(s.doc.Heads(), msg.Heads)
	state.SharedHeads = shared
	state.SentHashes = make(map[model.ChangeHash]struct{})
	return nil
}

func (s *SyncEngine) makeHave(lastSync []model.ChangeHash) intsync.Have {
	hashes, err := s.doc.graph.GetHashesFromHeads(lastSync)
	if err != nil || len(lastSync) == 0 {
		hashes, _ = s.doc.graph.GetHashesFromHeads(s.doc.Heads())
	}
	return intsync.Have{LastSync: append([]model.ChangeHash(nil), lastSync...), Bloom: intsync.BloomFromHashes(hashes)}
}

func (s *SyncEngine) getHashesToSend(have []intsync.Have, need []model.ChangeHash) []model.ChangeHash {
	all, err := s.doc.graph.GetHashesFromHeads(s.doc.Heads())
	if err != nil {
		return nil
	}
	needSet := make(map[model.ChangeHash]struct{}, len(need))
	for _, n := range need {
		needSet[n] = struct{}{}
	}
	out := make([]model.ChangeHash, 0, len(all))
	for _, h := range all {
		if _, ok := s.doc.changes[h]; !ok {
			continue
		}
		if _, needed := needSet[h]; needed {
			out = append(out, h)
			continue
		}
		known := false
		for _, hv := range have {
			if hv.Bloom.ContainsHash(h) {
				known = true
				break
			}
		}
		if !known {
			out = append(out, h)
		}
	}
	model.SortChangeHashes(out)
	return out
}

func (s *SyncEngine) serializeChangesByHashes(hashes []model.ChangeHash) ([]byte, error) {
	dtos := make([]changeDTO, 0, len(hashes))
	for _, h := range hashes {
		c, ok := s.doc.changes[h]
		if !ok {
			continue
		}
		dtos = append(dtos, encodeChange(c))
	}
	return json.Marshal(dtos)
}

func (d *Document) getMissingDeps(heads []model.ChangeHash) []model.ChangeHash {
	inQueue := make(map[model.ChangeHash]struct{}, len(d.queue))
	for _, c := range d.queue {
		inQueue[c.Hash] = struct{}{}
	}
	missingSet := make(map[model.ChangeHash]struct{})
	for _, c := range d.queue {
		for _, dep := range c.Deps {
			if !d.hasChange(dep) {
				missingSet[dep] = struct{}{}
			}
		}
	}
	for _, h := range heads {
		if !d.hasChange(h) {
			missingSet[h] = struct{}{}
		}
	}
	missing := make([]model.ChangeHash, 0, len(missingSet))
	for h := range missingSet {
		if _, queued := inQueue[h]; queued {
			continue
		}
		missing = append(missing, h)
	}
	model.SortChangeHashes(missing)
	return missing
}

func ptrHashes(h []model.ChangeHash) *[]model.ChangeHash {
	cp := append([]model.ChangeHash(nil), h...)
	return &cp
}

func hashesEqual(a, b []model.ChangeHash) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func intersectHashes(a, b []model.ChangeHash) []model.ChangeHash {
	set := make(map[model.ChangeHash]struct{}, len(a))
	for _, x := range a {
		set[x] = struct{}{}
	}
	out := make([]model.ChangeHash, 0)
	for _, y := range b {
		if _, ok := set[y]; ok {
			out = append(out, y)
		}
	}
	model.SortChangeHashes(out)
	return out
}
