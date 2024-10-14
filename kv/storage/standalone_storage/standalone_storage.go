package standalone_storage

import (
	"path"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := path.Join(dbPath, "kv")
	raftPath := path.Join(dbPath, "raft")

	kvDB := engine_util.CreateDB(kvPath, false)
	raftDB := engine_util.CreateDB(raftPath, true)

	ret := StandAloneStorage{
		engine: engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath),
		config: conf,
	}
	return &ret
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	//NO need
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.engine.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engine.Kv.NewTransaction(false)
	sasr := NewStandAloneStorageReader(txn)
	return sasr, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	var err error
	for _, m := range batch {
		cf, key, val := m.Cf(), m.Key(), m.Value()
		_, isPut := m.Data.(storage.Put) //这叫类型断言，_保存了Data的具体值，ok为true说明是Put，否则是false
		if isPut {
			err = engine_util.PutCF(s.engine.Kv, cf, key, val)
		} else {
			err = engine_util.DeleteCF(s.engine.Kv, cf, key)
		}

		if err != nil {
			return err
		}
	}
	// Your Code Here (1).
	return nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		txn: txn,
	}
}
func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}
func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}
func (s *StandAloneStorageReader) Close() {
	s.txn.Discard()
}
