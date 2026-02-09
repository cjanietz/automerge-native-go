package model

import (
	"cmp"
	"slices"
)

func SortActorIDs(ids []ActorID) {
	slices.SortFunc(ids, func(a, b ActorID) int { return a.Compare(b) })
}

func SortChangeHashes(hashes []ChangeHash) {
	slices.SortFunc(hashes, func(a, b ChangeHash) int { return a.Compare(b) })
}

func SortOpIDs(ids []OpID) {
	slices.SortFunc(ids, func(a, b OpID) int { return a.Compare(b) })
}

func SortObjIDs(ids []ObjID) {
	slices.SortFunc(ids, func(a, b ObjID) int { return a.Compare(b) })
}

func SortedMapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b string) int { return cmp.Compare(a, b) })
	return keys
}
