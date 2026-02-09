package text

import (
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

type Encoding uint8

const (
	EncodingUTF8  Encoding = iota // rune index
	EncodingUTF16                 // utf-16 code unit index
)

func RuneCount(s string) int {
	return utf8.RuneCountInString(s)
}

func UTF16CodeUnitCount(s string) int {
	return len(utf16.Encode([]rune(s)))
}

func RuneIndexToUTF16(s string, runeIndex int) int {
	if runeIndex <= 0 {
		return 0
	}
	runes := []rune(s)
	if runeIndex > len(runes) {
		runeIndex = len(runes)
	}
	return len(utf16.Encode(runes[:runeIndex]))
}

func UTF16IndexToRune(s string, utf16Index int) int {
	if utf16Index <= 0 {
		return 0
	}
	runes := []rune(s)
	units := 0
	for i, r := range runes {
		if r > 0xFFFF {
			units += 2
		} else {
			units += 1
		}
		if units >= utf16Index {
			return i + 1
		}
	}
	return len(runes)
}

func NormalizeIndexForEncoding(s string, index int, enc Encoding) int {
	if index < 0 {
		return 0
	}
	switch enc {
	case EncodingUTF16:
		max := UTF16CodeUnitCount(s)
		if index > max {
			return max
		}
		return index
	default:
		max := RuneCount(s)
		if index > max {
			return max
		}
		return index
	}
}

func ConvertIndex(s string, index int, from Encoding, to Encoding) int {
	norm := NormalizeIndexForEncoding(s, index, from)
	if from == to {
		return norm
	}
	if from == EncodingUTF16 && to == EncodingUTF8 {
		return UTF16IndexToRune(s, norm)
	}
	if from == EncodingUTF8 && to == EncodingUTF16 {
		return RuneIndexToUTF16(s, norm)
	}
	return norm
}

func ClampToGraphemeStart(s string, runeIndex int) int {
	runeIndex = NormalizeIndexForEncoding(s, runeIndex, EncodingUTF8)
	starts := GraphemeStarts(s)
	if len(starts) == 0 {
		return 0
	}
	best := 0
	for _, st := range starts {
		if st > runeIndex {
			break
		}
		best = st
	}
	return best
}

// GraphemeStarts returns rune indexes that begin a grapheme cluster.
// This is a lightweight heuristic that handles combining marks, ZWJ sequences,
// and variation selectors for common rich-text cursor movement.
func GraphemeStarts(s string) []int {
	runes := []rune(s)
	if len(runes) == 0 {
		return []int{0}
	}
	starts := []int{0}
	for i := 1; i < len(runes); i++ {
		r := runes[i]
		prev := runes[i-1]
		if unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Me, r) {
			continue
		}
		if r == 0x200D || prev == 0x200D || r == 0xFE0F {
			continue
		}
		starts = append(starts, i)
	}
	return starts
}
