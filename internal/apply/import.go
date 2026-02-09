package apply

import "github.com/cjanietz/automerge-native-go/internal/model"

// RemapActor rewrites actor ids in operation identifiers/objects.
func RemapActor(opID model.OpID, actorMap map[uint32]uint32) model.OpID {
	if actorMap == nil {
		return opID
	}
	if mapped, ok := actorMap[opID.Actor]; ok {
		opID.Actor = mapped
	}
	return opID
}

func RemapObjID(obj model.ObjID, actorMap map[uint32]uint32) model.ObjID {
	if obj.Root {
		return obj
	}
	obj.Op = RemapActor(obj.Op, actorMap)
	return obj
}
