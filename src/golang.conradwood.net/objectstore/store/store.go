package store

import (
	"context"
	pb "golang.conradwood.net/apis/objectstore"
)

type Store interface {
	// sync db/disk do stuff
	Check()
	// buf, exists or error
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Put(ctx context.Context, key string, buf []byte, expiry uint32) error
	List(ctx context.Context, req *pb.ListRequest) ([]*pb.ObjectMeta, error)
	Evict(ctx context.Context, key string) ([]byte, bool, error)
	HigherOrSameThanVersion(req *pb.ByVersionRequest, srv pb.ObjectStore_HigherOrSameThanVersionServer) error
}



