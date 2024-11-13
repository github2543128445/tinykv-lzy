package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
// 扫描器用于从存储层读取多个连续的键值对。它知晓存储层的实现，并返回适合用户的结果。
// 不变式：要么扫描器已完成且无法使用，要么它已准备好立即返回一个值。
type Scanner struct {
	// Your Data Here (4C).
	nextKey []byte
	txn     *MvccTxn
	iter    engine_util.DBIterator
	done    bool
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	return &Scanner{
		nextKey: startKey,
		txn:     txn,
		iter:    txn.Reader.IterCF(engine_util.CfWrite),
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.done {
		return nil, nil, nil
	}
	key := scan.nextKey
	scan.iter.Seek(EncodeKey(key, scan.txn.StartTS)) //找到这个时间前最新的write
	if !scan.iter.Valid() {
		scan.done = true
		return nil, nil, nil
	}
	item := scan.iter.Item()
	getKey := DecodeUserKey(item.KeyCopy(nil))
	if !bytes.Equal(getKey, key) {
		scan.nextKey = getKey
		return scan.Next()
	}
	for {
		scan.iter.Next()
		if !scan.iter.Valid() {
			scan.done = true
			break
		}
		item := scan.iter.Item()
		getKey := DecodeUserKey(item.KeyCopy(nil))
		if !bytes.Equal(getKey, key) { //找到第一个不相同的key再退出，即取出最新的kv，跳过老的kv
			scan.nextKey = getKey
			break
		}
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return key, nil, err
	}
	write, err := ParseWrite(val)
	if err != nil {
		return key, nil, err
	}
	if write.Kind == WriteKindDelete {
		return key, nil, nil
	}
	value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS)) //找到最新write更新后的value值
	return key, value, err
}
