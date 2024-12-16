package mvcc

import (
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn         *MvccTxn
	it          engine_util.DBIterator
	current_key []byte
	Need_stop   bool
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := Scanner{
		txn:       txn,
		it:        txn.Reader.IterCF(engine_util.CfWrite),
		Need_stop: false,
	}
	scanner.it.Seek(startKey)

	return &scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.it.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).

	// seek for the current key, iterate the next key
	if scan.it.Valid() {
		commit_ts := decodeTimestamp(scan.it.Item().Key())
		commit_key := DecodeUserKey(scan.it.Item().Key())
		v, _ := scan.it.Item().Value()

		scan.it.Next()
		if !scan.it.Valid() {
			scan.Need_stop = true
		}
		lock, err := scan.txn.GetLock(commit_key)
		if err != nil {
			return nil, nil, err
		}
		if lock != nil && scan.txn.StartTS > lock.Ts {
			return nil, nil, nil
		}

		if scan.txn.StartTS >= commit_ts {
			// filter data that has been traversed through new version
			if reflect.DeepEqual(scan.current_key, commit_key) {
				return nil, nil, nil
			}
			// find start time from write
			write, _ := ParseWrite(v)
			if write.Kind == WriteKindDelete || write.Kind == WriteKindRollback {
				scan.current_key = commit_key
				return nil, nil, nil
			}

			// seek key with start_ts from default
			start_ts := write.StartTS
			start_key := EncodeKey(commit_key, start_ts)
			value_it := scan.txn.Reader.IterCF(engine_util.CfDefault)
			value_it.Seek(start_key)

			if value_it.Valid() {
				value, _ := value_it.Item().Value()
				scan.current_key = commit_key
				return commit_key, value, nil
			}
		}
	} else {
		scan.Need_stop = true
	}
	return nil, nil, nil
}
