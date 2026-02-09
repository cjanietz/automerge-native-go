package automerge

import (
	"fmt"

	"github.com/cjanietz/automerge-native-go/internal/changegraph"
	"github.com/cjanietz/automerge-native-go/internal/model"
	inttext "github.com/cjanietz/automerge-native-go/internal/text"
)

type CursorSide uint8

const (
	CursorBefore CursorSide = iota
	CursorAfter
)

type Cursor struct {
	ObjID        model.ObjID
	Anchor       model.OpID
	Side         CursorSide
	FallbackRune int
}

func (d *Document) CursorForText(obj model.ObjID, index int, enc inttext.Encoding) (Cursor, error) {
	return d.cursorForText(obj, index, enc, nil)
}

func (d *Document) CursorForTextAt(obj model.ObjID, index int, enc inttext.Encoding, heads []model.ChangeHash) (Cursor, error) {
	clk, err := d.clockFromHeads(heads)
	if err != nil {
		return Cursor{}, err
	}
	return d.cursorForText(obj, index, enc, clk)
}

func (d *Document) ResolveTextCursor(c Cursor, enc inttext.Encoding) (int, error) {
	return d.resolveTextCursor(c, enc, nil)
}

func (d *Document) ResolveTextCursorAt(c Cursor, enc inttext.Encoding, heads []model.ChangeHash) (int, error) {
	clk, err := d.clockFromHeads(heads)
	if err != nil {
		return 0, err
	}
	return d.resolveTextCursor(c, enc, clk)
}

func (d *Document) cursorForText(obj model.ObjID, index int, enc inttext.Encoding, at *changegraph.Clock) (Cursor, error) {
	typ, ok := d.ops.ObjectType(obj)
	if !ok {
		return Cursor{}, fmt.Errorf("unknown object: %s", obj)
	}
	if typ != ObjText {
		return Cursor{}, fmt.Errorf("wrong object type: have=%s", typ)
	}
	txt := d.ops.Text(obj, at)
	norm := inttext.NormalizeIndexForEncoding(txt, index, enc)
	runeIndex := norm
	if enc == inttext.EncodingUTF16 {
		runeIndex = inttext.UTF16IndexToRune(txt, norm)
	}
	ids := d.ops.SequenceElementIDs(obj, at)
	if len(ids) == 0 {
		return Cursor{ObjID: obj, Side: CursorAfter, FallbackRune: 0}, nil
	}
	if runeIndex <= 0 {
		return Cursor{ObjID: obj, Anchor: ids[0], Side: CursorBefore, FallbackRune: 0}, nil
	}
	if runeIndex >= len(ids) {
		return Cursor{ObjID: obj, Anchor: ids[len(ids)-1], Side: CursorAfter, FallbackRune: len(ids)}, nil
	}
	return Cursor{ObjID: obj, Anchor: ids[runeIndex-1], Side: CursorAfter, FallbackRune: runeIndex}, nil
}

func (d *Document) resolveTextCursor(c Cursor, enc inttext.Encoding, at *changegraph.Clock) (int, error) {
	typ, ok := d.ops.ObjectType(c.ObjID)
	if !ok {
		return 0, fmt.Errorf("unknown object: %s", c.ObjID)
	}
	if typ != ObjText {
		return 0, fmt.Errorf("wrong object type: have=%s", typ)
	}
	ids := d.ops.SequenceElementIDs(c.ObjID, at)
	runeIndex := c.FallbackRune
	for i, id := range ids {
		if id != c.Anchor {
			continue
		}
		if c.Side == CursorBefore {
			runeIndex = i
		} else {
			runeIndex = i + 1
		}
		break
	}
	if runeIndex < 0 {
		runeIndex = 0
	}
	if runeIndex > len(ids) {
		runeIndex = len(ids)
	}
	if enc == inttext.EncodingUTF16 {
		txt := d.ops.Text(c.ObjID, at)
		return inttext.RuneIndexToUTF16(txt, runeIndex), nil
	}
	return runeIndex, nil
}
