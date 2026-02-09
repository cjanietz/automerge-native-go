package model

import "testing"

func TestScalarValueEquality(t *testing.T) {
	if !Null().Equal(Null()) {
		t.Fatal("null equality failed")
	}
	if !StringValue("x").Equal(StringValue("x")) {
		t.Fatal("string equality failed")
	}
	if BytesValue([]byte{1, 2, 3}).Equal(BytesValue([]byte{1, 2, 4})) {
		t.Fatal("bytes inequality failed")
	}
	if !UnknownValue(0x44, []byte{9, 9}).Equal(UnknownValue(0x44, []byte{9, 9})) {
		t.Fatal("unknown equality failed")
	}
}
