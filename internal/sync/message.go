package sync

import (
	"encoding/json"
	"errors"

	"github.com/cjanietz/automerge-native-go/internal/model"
)

const (
	MessageTypeSync   byte = 0x42
	MessageTypeSyncV2 byte = 0x43
)

var ErrMessageDecode = errors.New("sync message decode")

type MessageVersion uint8

const (
	MessageV1 MessageVersion = iota
	MessageV2
)

type Message struct {
	Version               MessageVersion
	Heads                 []model.ChangeHash
	Have                  []Have
	Need                  []model.ChangeHash
	SupportedCapabilities []Capability
	ChangePayload         []byte
	DocumentPayload       []byte
}

type messageDTO struct {
	Heads    []string  `json:"heads"`
	Have     []haveDTO `json:"have"`
	Need     []string  `json:"need"`
	Caps     []uint8   `json:"caps"`
	Changes  []byte    `json:"changes,omitempty"`
	Document []byte    `json:"document,omitempty"`
}

type haveDTO struct {
	LastSync []string   `json:"last_sync"`
	Bloom    [16]string `json:"bloom"`
}

func (m Message) Encode() ([]byte, error) {
	d := messageDTO{Heads: hashesToStrings(m.Heads), Need: hashesToStrings(m.Need), Changes: m.ChangePayload, Document: m.DocumentPayload}
	for _, h := range m.Have {
		hv := haveDTO{LastSync: hashesToStrings(h.LastSync)}
		for i := 0; i < len(h.Bloom.Words); i++ {
			hv.Bloom[i] = uint64ToHex(h.Bloom.Words[i])
		}
		d.Have = append(d.Have, hv)
	}
	for _, c := range m.SupportedCapabilities {
		d.Caps = append(d.Caps, uint8(c))
	}
	payload, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	t := MessageTypeSync
	if m.Version == MessageV2 {
		t = MessageTypeSyncV2
	}
	return append([]byte{t}, payload...), nil
}

func DecodeMessage(in []byte) (Message, error) {
	if len(in) < 1 {
		return Message{}, ErrMessageDecode
	}
	var version MessageVersion
	switch in[0] {
	case MessageTypeSync:
		version = MessageV1
	case MessageTypeSyncV2:
		version = MessageV2
	default:
		return Message{}, ErrMessageDecode
	}
	var d messageDTO
	if err := json.Unmarshal(in[1:], &d); err != nil {
		return Message{}, err
	}
	heads, err := stringsToHashes(d.Heads)
	if err != nil {
		return Message{}, err
	}
	need, err := stringsToHashes(d.Need)
	if err != nil {
		return Message{}, err
	}
	msg := Message{Version: version, Heads: heads, Need: need, ChangePayload: d.Changes, DocumentPayload: d.Document}
	for _, c := range d.Caps {
		msg.SupportedCapabilities = append(msg.SupportedCapabilities, Capability(c))
	}
	for _, hv := range d.Have {
		lastSync, err := stringsToHashes(hv.LastSync)
		if err != nil {
			return Message{}, err
		}
		var bf BloomFilter
		for i := 0; i < len(bf.Words); i++ {
			w, err := hexToUint64(hv.Bloom[i])
			if err != nil {
				return Message{}, err
			}
			bf.Words[i] = w
		}
		msg.Have = append(msg.Have, Have{LastSync: lastSync, Bloom: bf})
	}
	return msg, nil
}
