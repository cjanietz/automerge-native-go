package automerge

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cjanietz/automerge-native-go/internal/changegraph"
	"github.com/cjanietz/automerge-native-go/internal/model"
	"github.com/cjanietz/automerge-native-go/internal/storage"
)

var (
	ErrPartialLoad = errors.New("partial load")
)

type OnPartialLoad uint8

const (
	OnPartialError OnPartialLoad = iota
	OnPartialIgnore
)

type VerificationMode uint8

const (
	VerificationCheck VerificationMode = iota
	VerificationDontCheck
)

type StringMigration uint8

const (
	StringMigrationNone StringMigration = iota
	StringMigrationConvertToText
)

type LoadOptions struct {
	OnPartialLoad   OnPartialLoad
	Verification    VerificationMode
	StringMigration StringMigration
}

func DefaultLoadOptions() LoadOptions {
	return LoadOptions{OnPartialLoad: OnPartialError, Verification: VerificationCheck, StringMigration: StringMigrationNone}
}

type SaveOptions struct {
	Deflate       bool
	RetainOrphans bool
}

func DefaultSaveOptions() SaveOptions {
	return SaveOptions{Deflate: true, RetainOrphans: true}
}

type changeDTO struct {
	Hash       string               `json:"hash"`
	Actor      uint32               `json:"actor"`
	Seq        uint64               `json:"seq"`
	StartOp    uint64               `json:"start_op"`
	MaxOp      uint64               `json:"max_op"`
	Deps       []string             `json:"deps"`
	Message    *string              `json:"message,omitempty"`
	Time       *int64               `json:"time,omitempty"`
	Operations []changeOperationDTO `json:"operations"`
}

type changeOperationDTO struct {
	Kind        uint8     `json:"kind"`
	ObjID       objIDDTO  `json:"obj_id"`
	ChildObjID  objIDDTO  `json:"child_obj_id"`
	Key         string    `json:"key"`
	Index       int       `json:"index"`
	Start       int       `json:"start"`
	End         int       `json:"end"`
	MarkName    string    `json:"mark_name"`
	Value       scalarDTO `json:"value"`
	ObjType     uint8     `json:"obj_type"`
	By          int64     `json:"by"`
	DeleteCount int       `json:"delete_count"`
	InsertText  string    `json:"insert_text"`
	OpID        opIDDTO   `json:"op_id"`
}

type objIDDTO struct {
	Root    bool   `json:"root"`
	Counter uint64 `json:"counter"`
	Actor   uint32 `json:"actor"`
}
type opIDDTO struct {
	Counter uint64 `json:"counter"`
	Actor   uint32 `json:"actor"`
}
type scalarDTO struct {
	Kind     uint8   `json:"kind"`
	Bytes    []byte  `json:"bytes,omitempty"`
	String   string  `json:"string,omitempty"`
	Int      int64   `json:"int,omitempty"`
	Uint     uint64  `json:"uint,omitempty"`
	F64      float64 `json:"f64,omitempty"`
	Counter  int64   `json:"counter,omitempty"`
	Time     int64   `json:"time,omitempty"`
	Boolean  bool    `json:"bool,omitempty"`
	TypeCode uint8   `json:"type_code,omitempty"`
}

type documentDTO struct {
	Changes []changeDTO `json:"changes"`
}

func (d *Document) Save() ([]byte, error) { return d.SaveWithOptions(DefaultSaveOptions()) }

func (d *Document) SaveWithOptions(opts SaveOptions) ([]byte, error) {
	if len(d.legacyRaw) > 0 && len(d.changes) == 0 {
		return append([]byte(nil), d.legacyRaw...), nil
	}
	key := saveCacheKey{deflate: opts.Deflate, retainOrphans: opts.RetainOrphans}
	if len(d.queue) == 0 {
		if cached, ok := d.saveCache[key]; ok {
			return append([]byte(nil), cached...), nil
		}
	}
	all := d.AllChanges()
	dto := documentDTO{Changes: make([]changeDTO, 0, len(all))}
	for _, c := range all {
		dto.Changes = append(dto.Changes, encodeChange(c))
	}
	payload, err := json.Marshal(dto)
	if err != nil {
		return nil, err
	}
	buf, err := storage.EncodeChunk(storage.ChunkDocument, payload, opts.Deflate)
	if err != nil {
		return nil, err
	}
	if opts.RetainOrphans {
		for _, q := range d.queue {
			p, err := json.Marshal(encodeChange(q))
			if err != nil {
				return nil, err
			}
			cbuf, err := storage.EncodeChunk(storage.ChunkChange, p, opts.Deflate)
			if err != nil {
				return nil, err
			}
			buf = append(buf, cbuf...)
		}
	}
	if len(d.queue) == 0 {
		d.saveCache[key] = append([]byte(nil), buf...)
	}
	return buf, nil
}

func (d *Document) SaveNoCompress() ([]byte, error) {
	o := DefaultSaveOptions()
	o.Deflate = false
	return d.SaveWithOptions(o)
}

func (d *Document) SaveAfter(heads []model.ChangeHash) ([]byte, error) {
	baseSet := map[model.ChangeHash]struct{}{}
	if len(heads) > 0 {
		hashes, err := d.graph.GetHashesFromHeads(heads)
		if err != nil {
			return nil, err
		}
		for _, h := range hashes {
			baseSet[h] = struct{}{}
		}
	}
	current, err := d.graph.GetHashesFromHeads(d.Heads())
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0)
	for _, h := range current {
		if _, ok := baseSet[h]; ok {
			continue
		}
		c := d.changes[h]
		p, err := json.Marshal(encodeChange(c))
		if err != nil {
			return nil, err
		}
		chunk, err := storage.EncodeChunk(storage.ChunkChange, p, false)
		if err != nil {
			return nil, err
		}
		out = append(out, chunk...)
	}
	return out, nil
}

func Load(data []byte) (*Document, error) { return LoadWithOptions(data, DefaultLoadOptions()) }

func LoadWithOptions(data []byte, opts LoadOptions) (*Document, error) {
	d := NewDocument()
	if len(data) == 0 {
		return d, nil
	}
	chunks, err := storage.ParseChunks(data)
	if err != nil {
		if len(data) >= 4 && string(data[:4]) == string(storage.RustMagic[:]) {
			if _, rerr := storage.ParseRustChunks(data); rerr != nil {
				if opts.OnPartialLoad == OnPartialIgnore {
					return d, nil
				}
				return nil, rerr
			}
			d.legacyRaw = append([]byte(nil), data...)
			return d, nil
		}
		if opts.OnPartialLoad == OnPartialIgnore {
			return d, nil
		}
		return nil, err
	}
	for _, ch := range chunks {
		switch ch.Header.Type {
		case storage.ChunkDocument:
			var doc documentDTO
			if err := json.Unmarshal(ch.Payload, &doc); err != nil {
				if opts.OnPartialLoad == OnPartialIgnore {
					continue
				}
				return nil, err
			}
			changes, err := decodeChanges(doc.Changes)
			if err != nil {
				return nil, err
			}
			if err := d.ApplyChanges(changes); err != nil {
				if opts.OnPartialLoad == OnPartialIgnore {
					continue
				}
				return nil, fmt.Errorf("%w: %v", ErrPartialLoad, err)
			}
		case storage.ChunkChange, storage.ChunkCompressedChange:
			var one changeDTO
			if err := json.Unmarshal(ch.Payload, &one); err != nil {
				if opts.OnPartialLoad == OnPartialIgnore {
					continue
				}
				return nil, err
			}
			changes, err := decodeChanges([]changeDTO{one})
			if err != nil {
				return nil, err
			}
			if err := d.ApplyChanges(changes); err != nil {
				if opts.OnPartialLoad == OnPartialIgnore {
					continue
				}
				return nil, fmt.Errorf("%w: %v", ErrPartialLoad, err)
			}
		case storage.ChunkBundle:
			var doc documentDTO
			if err := json.Unmarshal(ch.Payload, &doc); err != nil {
				if opts.OnPartialLoad == OnPartialIgnore {
					continue
				}
				return nil, err
			}
			changes, err := decodeChanges(doc.Changes)
			if err != nil {
				return nil, err
			}
			if err := d.ApplyChanges(changes); err != nil {
				if opts.OnPartialLoad == OnPartialIgnore {
					continue
				}
				return nil, fmt.Errorf("%w: %v", ErrPartialLoad, err)
			}
		}
	}
	if opts.Verification == VerificationCheck {
		if err := d.graph.Validate(); err != nil {
			return nil, err
		}
	}
	_ = opts.StringMigration
	return d, nil
}

func (d *Document) LoadIncremental(data []byte) (int, error) {
	before := len(d.Heads())
	loaded, err := LoadWithOptions(data, LoadOptions{OnPartialLoad: OnPartialIgnore, Verification: VerificationCheck, StringMigration: StringMigrationNone})
	if err != nil {
		return 0, err
	}
	if err := d.ApplyChanges(loaded.AllChanges()); err != nil {
		return 0, err
	}
	after := len(d.Heads())
	if after < before {
		return 0, nil
	}
	return after - before, nil
}

func encodeChange(c Change) changeDTO {
	deps := make([]string, len(c.Deps))
	for i, d := range c.Deps {
		deps[i] = d.String()
	}
	ops := make([]changeOperationDTO, len(c.Operations))
	for i, op := range c.Operations {
		ops[i] = changeOperationDTO{
			Kind:        uint8(op.Kind),
			ObjID:       encodeObjID(op.ObjID),
			ChildObjID:  encodeObjID(op.ChildObjID),
			Key:         op.Key,
			Index:       op.Index,
			Start:       op.Start,
			End:         op.End,
			MarkName:    op.MarkName,
			Value:       encodeScalar(op.Value),
			ObjType:     uint8(op.ObjType),
			By:          op.By,
			DeleteCount: op.DeleteCount,
			InsertText:  op.InsertText,
			OpID:        encodeOpID(op.OpID),
		}
	}
	return changeDTO{Hash: c.Hash.String(), Actor: c.Actor, Seq: c.Seq, StartOp: c.StartOp, MaxOp: c.MaxOp, Deps: deps, Message: c.Message, Time: c.Time, Operations: ops}
}

func decodeChanges(in []changeDTO) ([]Change, error) {
	out := make([]Change, 0, len(in))
	for _, c := range in {
		h, err := model.ChangeHashFromHex(c.Hash)
		if err != nil {
			return nil, err
		}
		deps := make([]model.ChangeHash, len(c.Deps))
		for i, d := range c.Deps {
			deps[i], err = model.ChangeHashFromHex(d)
			if err != nil {
				return nil, err
			}
		}
		ops := make([]ChangeOperation, len(c.Operations))
		for i, op := range c.Operations {
			ops[i] = ChangeOperation{
				Kind:        ChangeOperationKind(op.Kind),
				ObjID:       decodeObjID(op.ObjID),
				ChildObjID:  decodeObjID(op.ChildObjID),
				Key:         op.Key,
				Index:       op.Index,
				Start:       op.Start,
				End:         op.End,
				MarkName:    op.MarkName,
				Value:       decodeScalar(op.Value),
				ObjType:     ObjType(op.ObjType),
				By:          op.By,
				DeleteCount: op.DeleteCount,
				InsertText:  op.InsertText,
				OpID:        decodeOpID(op.OpID),
			}
		}
		out = append(out, Change{Hash: h, Actor: c.Actor, Seq: c.Seq, StartOp: c.StartOp, MaxOp: c.MaxOp, Deps: deps, Message: c.Message, Time: c.Time, Operations: ops})
	}
	return out, nil
}

func encodeObjID(v model.ObjID) objIDDTO {
	return objIDDTO{Root: v.Root, Counter: v.Op.Counter, Actor: v.Op.Actor}
}
func decodeObjID(v objIDDTO) model.ObjID {
	if v.Root {
		return model.RootObjID()
	}
	return model.ObjID{Op: model.OpID{Counter: v.Counter, Actor: v.Actor}}
}
func encodeOpID(v model.OpID) opIDDTO { return opIDDTO{Counter: v.Counter, Actor: v.Actor} }
func decodeOpID(v opIDDTO) model.OpID { return model.OpID{Counter: v.Counter, Actor: v.Actor} }
func encodeScalar(v model.ScalarValue) scalarDTO {
	return scalarDTO{Kind: uint8(v.Kind), Bytes: v.Bytes, String: v.String, Int: v.Int, Uint: v.Uint, F64: v.F64, Counter: v.Counter, Time: v.Time, Boolean: v.Boolean, TypeCode: v.TypeCode}
}
func decodeScalar(v scalarDTO) model.ScalarValue {
	return model.ScalarValue{Kind: model.ScalarKind(v.Kind), Bytes: v.Bytes, String: v.String, Int: v.Int, Uint: v.Uint, F64: v.F64, Counter: v.Counter, Time: v.Time, Boolean: v.Boolean, TypeCode: v.TypeCode}
}

func (d *Document) Validate() error                    { return d.graph.Validate() }
func (d *Document) VerificationMode() VerificationMode { return VerificationCheck }

var _ = changegraph.Clock{}
