package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	ErrBadColumnMeta = errors.New("bad column metadata")
	ErrBadColumnData = errors.New("bad column data")
)

type ColumnValueType uint8

const (
	ColumnUint ColumnValueType = iota
	ColumnInt
	ColumnBool
	ColumnBytes
)

type ColumnSpec struct {
	ID   uint64
	Type ColumnValueType
}

type RawColumn struct {
	Spec ColumnSpec
	Data []byte
}

func EncodeColumnMetadata(cols []RawColumn) []byte {
	out := make([]byte, 0)
	out = appendULEB(out, uint64(len(cols)))
	for _, c := range cols {
		out = appendULEB(out, c.Spec.ID)
		out = append(out, byte(c.Spec.Type))
		out = appendULEB(out, uint64(len(c.Data)))
	}
	return out
}

func DecodeColumnMetadata(data []byte) ([]RawColumn, int, error) {
	offset := 0
	count, n, err := readULEB(data[offset:])
	if err != nil {
		return nil, 0, ErrBadColumnMeta
	}
	offset += n
	cols := make([]RawColumn, 0, count)
	for i := uint64(0); i < count; i++ {
		id, n, err := readULEB(data[offset:])
		if err != nil {
			return nil, 0, ErrBadColumnMeta
		}
		offset += n
		if offset >= len(data) {
			return nil, 0, ErrBadColumnMeta
		}
		typ := ColumnValueType(data[offset])
		offset++
		l, n, err := readULEB(data[offset:])
		if err != nil {
			return nil, 0, ErrBadColumnMeta
		}
		offset += n
		cols = append(cols, RawColumn{Spec: ColumnSpec{ID: id, Type: typ}, Data: make([]byte, int(l))})
	}
	return cols, offset, nil
}

func EncodeRawColumns(cols []RawColumn) []byte {
	meta := EncodeColumnMetadata(cols)
	out := make([]byte, 0, len(meta)+1024)
	out = append(out, meta...)
	for _, c := range cols {
		out = append(out, c.Data...)
	}
	return out
}

func DecodeRawColumns(data []byte) ([]RawColumn, error) {
	cols, offset, err := DecodeColumnMetadata(data)
	if err != nil {
		return nil, err
	}
	for i := range cols {
		l := len(cols[i].Data)
		if len(data)-offset < l {
			return nil, ErrBadColumnData
		}
		cols[i].Data = append([]byte(nil), data[offset:offset+l]...)
		offset += l
	}
	if offset != len(data) {
		return nil, fmt.Errorf("%w: trailing=%d", ErrBadColumnData, len(data)-offset)
	}
	return cols, nil
}

func EncodeUintColumn(vals []uint64) []byte {
	out := make([]byte, 0, len(vals)*8)
	for _, v := range vals {
		out = appendULEB(out, v)
	}
	return out
}

func DecodeUintColumn(data []byte) ([]uint64, error) {
	out := make([]uint64, 0)
	for len(data) > 0 {
		v, n, err := readULEB(data)
		if err != nil {
			return nil, err
		}
		data = data[n:]
		out = append(out, v)
	}
	return out, nil
}

func EncodeBoolColumn(vals []bool) []byte {
	out := make([]byte, 0, (len(vals)+7)/8)
	var cur byte
	bit := uint(0)
	for _, v := range vals {
		if v {
			cur |= 1 << bit
		}
		bit++
		if bit == 8 {
			out = append(out, cur)
			cur = 0
			bit = 0
		}
	}
	if bit > 0 {
		out = append(out, cur)
	}
	return out
}

func DecodeBoolColumn(data []byte, count int) ([]bool, error) {
	out := make([]bool, 0, count)
	for _, b := range data {
		for bit := uint(0); bit < 8 && len(out) < count; bit++ {
			out = append(out, (b&(1<<bit)) != 0)
		}
	}
	if len(out) != count {
		return nil, ErrBadColumnData
	}
	return out, nil
}

func EncodeBytesColumn(vals [][]byte) []byte {
	out := make([]byte, 0)
	out = appendULEB(out, uint64(len(vals)))
	for _, v := range vals {
		out = appendULEB(out, uint64(len(v)))
		out = append(out, v...)
	}
	return out
}

func DecodeBytesColumn(data []byte) ([][]byte, error) {
	count, n, err := readULEB(data)
	if err != nil {
		return nil, err
	}
	data = data[n:]
	out := make([][]byte, 0, count)
	for i := uint64(0); i < count; i++ {
		l, n, err := readULEB(data)
		if err != nil {
			return nil, err
		}
		data = data[n:]
		if len(data) < int(l) {
			return nil, ErrBadColumnData
		}
		buf := append([]byte(nil), data[:l]...)
		if l == 0 {
			buf = []byte{}
		}
		out = append(out, buf)
		data = data[l:]
	}
	if len(data) != 0 {
		return nil, ErrBadColumnData
	}
	return out, nil
}

func appendULEB(dst []byte, v uint64) []byte {
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			dst = append(dst, b|0x80)
		} else {
			dst = append(dst, b)
			break
		}
	}
	return dst
}

func readULEB(data []byte) (uint64, int, error) {
	var out uint64
	var shift uint
	for i, b := range data {
		out |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return out, i + 1, nil
		}
		shift += 7
		if shift > 63 {
			return 0, 0, ErrBadColumnMeta
		}
	}
	return 0, 0, ErrBadColumnMeta
}

func EncodeInt64(v int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return b[:]
}

func DecodeInt64(b []byte) (int64, error) {
	if len(b) != 8 {
		return 0, ErrBadColumnData
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}
