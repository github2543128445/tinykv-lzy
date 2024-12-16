package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	respone := &kvrpcpb.GetResponse{}
	reader, _ := server.storage.Reader(&kvrpcpb.Context{})

	var keys [][]byte
	keys = append(keys, req.Key)
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	// create a new transaction and lock the key
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)

	if err != nil {
		return respone, err
	}
	if lock != nil && req.Version > lock.Ts {
		respone.Error = &kvrpcpb.KeyError{
			Locked: lock.Info(req.Key),
		}
		return respone, nil
	}

	// get the latest value less than start timestamp
	v, _ := txn.GetValue(req.Key)
	if v == nil {
		respone.NotFound = true
	} else {
		respone.Value = v
	}

	return respone, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	respone := &kvrpcpb.PrewriteResponse{}
	reader, _ := server.storage.Reader(&kvrpcpb.Context{})

	var keys [][]byte
	for _, muta := range req.Mutations {
		keys = append(keys, muta.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// check if the prewrite can succeed
	for _, muta := range req.Mutations {
		// check for if all keys are unlocked
		lock, err := txn.GetLock(muta.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			respone.Errors = append(respone.Errors, &kvrpcpb.KeyError{
				Locked: lock.Info(muta.Key),
			})
			return respone, nil
		}

		// check for if there are commit after start
		start_ts := txn.StartTS
		writes, ts, _ := txn.MostRecentWrite(muta.Key)
		if writes != nil && writes.StartTS <= start_ts && ts > start_ts {
			respone.Errors = append(respone.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: ts,
				},
			})
			return respone, nil
		}

		// prewrite all the keys
		var kind mvcc.WriteKind
		switch muta.Op {
		case kvrpcpb.Op_Put:
			kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			kind = mvcc.WriteKindDelete
		case kvrpcpb.Op_Rollback:
			kind = mvcc.WriteKindRollback
		case kvrpcpb.Op_Lock:
		}
		txn.PutLock(muta.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    kind,
		})
		txn.PutValue(muta.Key, muta.Value)
	}

	// if all successed then presist the writes
	server.storage.Write(req.Context, txn.Writes())

	return respone, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	respone := &kvrpcpb.CommitResponse{}
	reader, _ := server.storage.Reader(&kvrpcpb.Context{})

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// check if the prewrite succeeded
	for _, key := range req.Keys {
		// check for if all keys are unlocked
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			writes, _, _ := txn.MostRecentWrite(key)
			if writes != nil {
				// transaction has been rollbacked by other trans
				if writes.Kind == mvcc.WriteKindRollback {
					respone.Error = &kvrpcpb.KeyError{
						Abort: "rollback",
					}
				}
			}
			// prewrite not successed, because no lock
			return respone, nil
		}

		// need consistent with the trans start version
		if req.StartVersion != lock.Ts {
			respone.Error = &kvrpcpb.KeyError{
				Retryable: "retry",
			}
			return respone, nil
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}

	// if all successed then presist the writes
	server.storage.Write(req.Context, txn.Writes())

	return respone, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	respone := &kvrpcpb.ScanResponse{}
	reader, _ := server.storage.Reader(&kvrpcpb.Context{})
	txn := mvcc.NewMvccTxn(reader, req.Version)

	it := txn.Reader.IterCF(engine_util.CfWrite)
	it.Seek(req.StartKey)
	defer it.Close()

	var kvs_results []*kvrpcpb.KvPair

	// get the scan response through the scanner
	scanner := mvcc.NewScanner(req.StartKey, txn)
	count := 0
	for count < int(req.Limit) {
		key, vlaue, _ := scanner.Next()
		if vlaue != nil {
			kvs_results = append(kvs_results, &kvrpcpb.KvPair{Key: key, Value: vlaue})
			count++
		}
		if scanner.Need_stop {
			break
		}
	}
	respone.Pairs = kvs_results

	return respone, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	respone := &kvrpcpb.CheckTxnStatusResponse{}
	reader, _ := server.storage.Reader(&kvrpcpb.Context{})
	// take the lock_ts as the start timestamp
	txn := mvcc.NewMvccTxn(reader, req.LockTs)

	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		// lock time out, roll back the transaction
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
			respone.Action = kvrpcpb.Action_TTLExpireRollback

			// delete lock and rollback transaction
			txn.DeleteValue(req.PrimaryKey)
			txn.DeleteLock(req.PrimaryKey)
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})
			server.storage.Write(req.Context, txn.Writes())
		}
		return respone, nil
	}

	writes, ts, _ := txn.MostRecentWrite(req.PrimaryKey)
	// transaction has already been committed or rolled back
	if writes != nil {
		if writes.Kind == mvcc.WriteKindRollback {
			respone.LockTtl = 0
			respone.CommitVersion = 0
		} else {
			respone.CommitVersion = ts
		}
		return respone, nil
	}

	// prewrite not succeed, rollback
	respone.Action = kvrpcpb.Action_LockNotExistRollback
	txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
		StartTS: req.LockTs,
		Kind:    mvcc.WriteKindRollback,
	})
	server.storage.Write(req.Context, txn.Writes())

	return respone, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	respone := &kvrpcpb.BatchRollbackResponse{}
	reader, _ := server.storage.Reader(&kvrpcpb.Context{})

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			writes, _, _ := txn.CurrentWrite(key)
			if writes != nil {
				// transaction has already been committed
				if writes.Kind != mvcc.WriteKindRollback {
					respone.Error = &kvrpcpb.KeyError{
						Abort: "abort",
					}
					return respone, nil
				}
			} else {
				// there is no prewrite
				txn.PutWrite(key, req.StartVersion, &mvcc.Write{
					StartTS: req.StartVersion,
					Kind:    mvcc.WriteKindRollback,
				})
			}

		} else if req.StartVersion != lock.Ts {
			// locked by other transaction, also roolback
			txn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
		} else {
			// common rollback transcaction
			txn.DeleteValue(key)
			txn.DeleteLock(key)

			txn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
		}
	}
	// batch write the challenges
	server.storage.Write(req.Context, txn.Writes())

	return respone, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	respone := &kvrpcpb.ResolveLockResponse{}
	reader, _ := server.storage.Reader(&kvrpcpb.Context{})
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	it := txn.Reader.IterCF(engine_util.CfLock)

	for it.Valid() {
		key := it.Item().Key()
		value, _ := it.Item().Value()
		lock, _ := mvcc.ParseLock(value)

		// Unlock the key-value and either commit transaction or rollback transaction
		if lock.Ts == req.StartVersion && req.CommitVersion == 0 {
			txn.DeleteValue(key)
			txn.DeleteLock(key)
			txn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
		} else if lock.Ts == req.StartVersion && req.CommitVersion > lock.Ts {
			txn.DeleteLock(key)
			txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindPut,
			})
		}
		it.Next()
	}
	server.storage.Write(req.Context, txn.Writes())

	return respone, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
