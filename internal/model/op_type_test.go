package model

import "testing"

func TestOpTypeString(t *testing.T) {
	cases := []struct {
		op   OpType
		want string
	}{
		{OpMakeMap, "MAP"},
		{OpMakeList, "LST"},
		{OpMakeText, "TXT"},
		{OpSet, "SET"},
		{OpDelete, "DEL"},
		{OpIncrement, "INC"},
		{OpMakeTable, "TBL"},
		{OpMark, "MRK"},
	}
	for _, tc := range cases {
		if got := tc.op.String(); got != tc.want {
			t.Fatalf("unexpected op string for %v: got %s want %s", tc.op, got, tc.want)
		}
	}
}
