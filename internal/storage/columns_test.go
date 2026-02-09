package storage

import (
	"reflect"
	"testing"
)

func TestRawColumnsRoundTrip(t *testing.T) {
	cols := []RawColumn{
		{Spec: ColumnSpec{ID: 1, Type: ColumnUint}, Data: EncodeUintColumn([]uint64{1, 2, 3})},
		{Spec: ColumnSpec{ID: 2, Type: ColumnBool}, Data: EncodeBoolColumn([]bool{true, false, true, true})},
		{Spec: ColumnSpec{ID: 3, Type: ColumnBytes}, Data: EncodeBytesColumn([][]byte{{1, 2}, {3}})},
	}
	buf := EncodeRawColumns(cols)
	got, err := DecodeRawColumns(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(cols) {
		t.Fatalf("column length mismatch: got %d want %d", len(got), len(cols))
	}
	for i := range cols {
		if got[i].Spec != cols[i].Spec {
			t.Fatalf("spec mismatch at %d: got %#v want %#v", i, got[i].Spec, cols[i].Spec)
		}
		if !reflect.DeepEqual(got[i].Data, cols[i].Data) {
			t.Fatalf("data mismatch at %d", i)
		}
	}
}

func TestTypedColumnHelpers(t *testing.T) {
	u := []uint64{0, 1, 127, 128, 1024}
	ud, err := DecodeUintColumn(EncodeUintColumn(u))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ud, u) {
		t.Fatalf("uint column mismatch: got %#v want %#v", ud, u)
	}

	b := []bool{true, false, true, false, true, true, false, false, true}
	bd, err := DecodeBoolColumn(EncodeBoolColumn(b), len(b))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bd, b) {
		t.Fatalf("bool column mismatch: got %#v want %#v", bd, b)
	}

	bytesIn := [][]byte{{0x01}, {0x02, 0x03}, {}}
	bytesOut, err := DecodeBytesColumn(EncodeBytesColumn(bytesIn))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bytesOut, bytesIn) {
		t.Fatalf("bytes column mismatch: got %#v want %#v", bytesOut, bytesIn)
	}
}
