package standalone_storage

import (
	"log"

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
	conf *config.Config
	db   *badger.DB
	err  error
}

// Define a class to implement the interface in StorageReader, using Txn and engine_utils
type ReadBdger struct {
	s   *StandAloneStorage
	txn *badger.Txn
}

func NewReadBdger(i_s *StandAloneStorage) *ReadBdger {
	i_txn := i_s.db.NewTransaction(true)
	return &ReadBdger{
		s:   i_s,
		txn: i_txn,
	}
}

func (reader ReadBdger) GetCF(cf string, key []byte) ([]byte, error) {
	v, _ := engine_util.GetCF(reader.s.db, cf, key)
	return v, nil
}

func (reader ReadBdger) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}

func (reader ReadBdger) Close() {
	reader.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// Open the bager database
	opts := badger.DefaultOptions
	opts.Dir = s.conf.DBPath
	opts.ValueDir = s.conf.DBPath

	s.db, s.err = badger.Open(opts)
	if s.err != nil {
		log.Fatal(s.err)
	}
	return s.err
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewReadBdger(s), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)

	for _, v := range batch {
		var err error
		if _, ok := v.Data.(storage.Delete); ok {
			err = txn.Delete(engine_util.KeyWithCF(v.Cf(), v.Key()))
		} else {
			err = txn.Set(engine_util.KeyWithCF(v.Cf(), v.Key()), v.Value())
		}

		if err != nil {
			return err
		}
	}
	txn.Commit()

	return nil
}
