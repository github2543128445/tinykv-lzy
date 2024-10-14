package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	respon := &kvrpcpb.RawGetResponse{}
	var val []byte
	val, err = reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	respon.Value = val
	respon.NotFound = false
	if val == nil {
		respon.NotFound = true
	}
	// Your Code Here (1).
	return respon, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modi := storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	}
	batch := []storage.Modify{modi}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modi := storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	}
	batch := []storage.Modify{modi}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	respon := &kvrpcpb.RawScanResponse{}
	it := reader.IterCF(req.Cf)
	limit := req.Limit
	for it.Seek(req.StartKey); it.Valid() && limit > 0; it.Next() {
		key := it.Item().Key()
		value, _ := it.Item().Value()
		respon.Kvs = append(respon.Kvs, &kvrpcpb.KvPair{Key: key, Value: value})
		limit--
	}
	return respon, nil
}
