package sync

import (
	"encoding/hex"
	"strconv"

	"github.com/cjanietz/automerge-native-go/internal/model"
)

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
			return 0, 0, ErrStateDecode
		}
	}
	return 0, 0, ErrStateDecode
}

func hashesToStrings(in []model.ChangeHash) []string {
	out := make([]string, len(in))
	for i, h := range in {
		out[i] = h.String()
	}
	return out
}

func stringsToHashes(in []string) ([]model.ChangeHash, error) {
	out := make([]model.ChangeHash, len(in))
	for i, s := range in {
		h, err := model.ChangeHashFromHex(s)
		if err != nil {
			return nil, err
		}
		out[i] = h
	}
	return out, nil
}

func uint64ToHex(v uint64) string {
	var b [8]byte
	for i := 0; i < 8; i++ {
		b[7-i] = byte(v >> (i * 8))
	}
	return hex.EncodeToString(b[:])
}

func hexToUint64(s string) (uint64, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return 0, err
	}
	if len(b) != 8 {
		v, err := strconv.ParseUint(s, 16, 64)
		if err != nil {
			return 0, ErrMessageDecode
		}
		return v, nil
	}
	var v uint64
	for i := 0; i < 8; i++ {
		v = (v << 8) | uint64(b[i])
	}
	return v, nil
}
