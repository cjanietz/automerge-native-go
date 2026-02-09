package model

import "fmt"

type OpType uint8

const (
	OpMakeMap OpType = iota
	OpMakeList
	OpMakeText
	OpSet
	OpDelete
	OpIncrement
	OpMakeTable
	OpMark
)

func (o OpType) String() string {
	switch o {
	case OpMakeMap:
		return "MAP"
	case OpMakeList:
		return "LST"
	case OpMakeText:
		return "TXT"
	case OpSet:
		return "SET"
	case OpDelete:
		return "DEL"
	case OpIncrement:
		return "INC"
	case OpMakeTable:
		return "TBL"
	case OpMark:
		return "MRK"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint8(o))
	}
}
