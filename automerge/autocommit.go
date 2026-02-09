package automerge

import "github.com/cjanietz/automerge-native-go/internal/model"

type AutoCommit struct {
	doc        *Document
	diffCursor []model.ChangeHash
}

func NewAutoCommit() *AutoCommit {
	return &AutoCommit{doc: NewDocument(), diffCursor: nil}
}

func (a *AutoCommit) Document() *Document {
	return a.doc
}

func (a *AutoCommit) Diff(before, after []model.ChangeHash) []Patch {
	return a.doc.Diff(before, after)
}

func (a *AutoCommit) DiffCursor() []model.ChangeHash {
	out := make([]model.ChangeHash, len(a.diffCursor))
	copy(out, a.diffCursor)
	return out
}

func (a *AutoCommit) UpdateDiffCursor() {
	a.diffCursor = a.doc.Heads()
}

func (a *AutoCommit) ResetDiffCursor() {
	a.diffCursor = nil
}

func (a *AutoCommit) DiffIncremental() []Patch {
	after := a.doc.Heads()
	patches := a.doc.Diff(a.diffCursor, after)
	a.diffCursor = after
	return patches
}

func (a *AutoCommit) SetActor(actor uint32) error {
	return a.doc.SetActor(actor)
}

func (a *AutoCommit) Put(obj model.ObjID, key string, value model.ScalarValue) (*Change, error) {
	tx, err := a.doc.Begin()
	if err != nil {
		return nil, err
	}
	if err := tx.Put(obj, key, value); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx.Commit()
}

func (a *AutoCommit) PutObject(obj model.ObjID, key string, typ ObjType) (model.ObjID, *Change, error) {
	tx, err := a.doc.Begin()
	if err != nil {
		return model.ObjID{}, nil, err
	}
	objID, err := tx.PutObject(obj, key, typ)
	if err != nil {
		_ = tx.Rollback()
		return model.ObjID{}, nil, err
	}
	change, err := tx.Commit()
	if err != nil {
		return model.ObjID{}, nil, err
	}
	return objID, change, nil
}

func (a *AutoCommit) Insert(obj model.ObjID, index int, value model.ScalarValue) (*Change, error) {
	tx, err := a.doc.Begin()
	if err != nil {
		return nil, err
	}
	if err := tx.Insert(obj, index, value); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx.Commit()
}

func (a *AutoCommit) InsertObject(obj model.ObjID, index int, typ ObjType) (model.ObjID, *Change, error) {
	tx, err := a.doc.Begin()
	if err != nil {
		return model.ObjID{}, nil, err
	}
	objID, err := tx.InsertObject(obj, index, typ)
	if err != nil {
		_ = tx.Rollback()
		return model.ObjID{}, nil, err
	}
	change, err := tx.Commit()
	if err != nil {
		return model.ObjID{}, nil, err
	}
	return objID, change, nil
}

func (a *AutoCommit) DeleteMap(obj model.ObjID, key string) (*Change, error) {
	tx, err := a.doc.Begin()
	if err != nil {
		return nil, err
	}
	if err := tx.DeleteMap(obj, key); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx.Commit()
}

func (a *AutoCommit) DeleteList(obj model.ObjID, index int) (*Change, error) {
	tx, err := a.doc.Begin()
	if err != nil {
		return nil, err
	}
	if err := tx.DeleteList(obj, index); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx.Commit()
}

func (a *AutoCommit) Increment(obj model.ObjID, key string, by int64) (*Change, error) {
	tx, err := a.doc.Begin()
	if err != nil {
		return nil, err
	}
	if err := tx.Increment(obj, key, by); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx.Commit()
}

func (a *AutoCommit) SpliceText(obj model.ObjID, index int, deleteCount int, insert string) (*Change, error) {
	tx, err := a.doc.Begin()
	if err != nil {
		return nil, err
	}
	if err := tx.SpliceText(obj, index, deleteCount, insert); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx.Commit()
}

func (a *AutoCommit) Mark(obj model.ObjID, start int, end int, name string, value model.ScalarValue) (*Change, error) {
	tx, err := a.doc.Begin()
	if err != nil {
		return nil, err
	}
	if err := tx.Mark(obj, start, end, name, value); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx.Commit()
}
