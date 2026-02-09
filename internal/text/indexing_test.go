package text

import "testing"

func TestUTF16UTF8IndexConversion(t *testing.T) {
	s := "A😀B"
	if got := RuneCount(s); got != 3 {
		t.Fatalf("rune count mismatch: %d", got)
	}
	if got := UTF16CodeUnitCount(s); got != 4 {
		t.Fatalf("utf16 unit count mismatch: %d", got)
	}
	if got := RuneIndexToUTF16(s, 2); got != 3 {
		t.Fatalf("rune->utf16 mismatch: %d", got)
	}
	if got := UTF16IndexToRune(s, 3); got != 2 {
		t.Fatalf("utf16->rune mismatch: %d", got)
	}
}

func TestGraphemeStartsEmojiZWJ(t *testing.T) {
	s := "x👨‍👨‍👧‍👦y"
	starts := GraphemeStarts(s)
	if len(starts) != 3 {
		t.Fatalf("expected 3 grapheme starts (x, family, y), got %d: %#v", len(starts), starts)
	}
	if starts[0] != 0 || starts[1] != 1 {
		t.Fatalf("unexpected starts: %#v", starts)
	}
}

func TestConvertIndexAndClamp(t *testing.T) {
	s := "ae\u0301😀z"
	if got := ConvertIndex(s, 3, EncodingUTF8, EncodingUTF16); got != 3 {
		t.Fatalf("unexpected utf8->utf16 index: %d", got)
	}
	if got := ConvertIndex(s, 4, EncodingUTF16, EncodingUTF8); got != 4 {
		t.Fatalf("unexpected utf16->utf8 index: %d", got)
	}
	if got := ClampToGraphemeStart(s, 2); got != 1 {
		t.Fatalf("expected clamp to start of combining grapheme, got %d", got)
	}
}
