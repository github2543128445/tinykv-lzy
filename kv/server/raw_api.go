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
	// Your Code Here (1).
	r := &kvrpcpb.RawGetResponse{
		NotFound: true,
	}

	reader, err := server.storage.Reader(nil)
	if err != nil {
		return r, err
	}

	v, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return r, err
	}

	if v != nil {
		r.Value = v
		r.NotFound = true
	}
	return r, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	})
	r := &kvrpcpb.RawPutResponse{}

	if err != nil {
		return r, err
	}

	return r, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	})
	r := &kvrpcpb.RawDeleteResponse{}

	if err != nil {
		return r, err
	}

	return r, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	it := reader.IterCF(req.Cf)
	it.Seek(req.StartKey)
	defer it.Close()

	var kv_res []*kvrpcpb.KvPair
	for i := 0; i < int(req.Limit); i++ {
		if !it.Valid() {
			break
		}
		v, _ := it.Item().Value()
		kv_res = append(kv_res, &kvrpcpb.KvPair{
			Key: it.Item().Key(), Value: v,
		})
		it.Next()
	}
	r := &kvrpcpb.RawScanResponse{
		Kvs: kv_res,
	}

	return r, err
}
