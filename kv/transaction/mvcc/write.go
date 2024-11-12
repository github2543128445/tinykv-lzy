package mvcc

import (
	"encoding/binary"
	"fmt"

	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Write is a representation of a committed write to backing storage.
// A serialized version is stored in the "write" CF of our engine when a write is committed. That allows MvccTxn to find
// the status of a key at a given timestamp.
// Write 是对提交到后端存储的写入的一种表示。
// 当写入被提交时，其序列化版本存储在我们引擎的 “write” 列族中。这使得 MvccTxn 能够在给定的时间戳找到一个键的状态。
type Write struct {
	StartTS uint64
	Kind    WriteKind
}

func (wr *Write) ToBytes() []byte {
	buf := append([]byte{byte(wr.Kind)}, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[1:], wr.StartTS)
	return buf
}

func ParseWrite(value []byte) (*Write, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 9 {
		return nil, fmt.Errorf("mvcc/write/ParseWrite: value is incorrect length, expected 9, found %d", len(value))
	}
	kind := value[0]
	startTs := binary.BigEndian.Uint64(value[1:])

	return &Write{startTs, WriteKind(kind)}, nil
}

type WriteKind int

const (
	WriteKindPut      WriteKind = 1
	WriteKindDelete   WriteKind = 2
	WriteKindRollback WriteKind = 3
)

func (wk WriteKind) ToProto() kvrpcpb.Op {
	switch wk {
	case WriteKindPut:
		return kvrpcpb.Op_Put
	case WriteKindDelete:
		return kvrpcpb.Op_Del
	case WriteKindRollback:
		return kvrpcpb.Op_Rollback
	}

	return -1
}

func WriteKindFromProto(op kvrpcpb.Op) WriteKind {
	switch op {
	case kvrpcpb.Op_Put:
		return WriteKindPut
	case kvrpcpb.Op_Del:
		return WriteKindDelete
	case kvrpcpb.Op_Rollback:
		return WriteKindRollback
	default:
		panic("unsupported type")
	}
}
