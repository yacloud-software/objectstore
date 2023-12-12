package main

import (
	"context"
	"flag"
	"fmt"
	"golang.conradwood.net/apis/common"
	pb "golang.conradwood.net/apis/objectstore"
	"golang.conradwood.net/go-easyops/auth"
	"golang.conradwood.net/go-easyops/errors"
	"golang.conradwood.net/go-easyops/server"
	"golang.conradwood.net/go-easyops/utils"
	"golang.conradwood.net/objectstore/store"
	"google.golang.org/grpc"
	"io"
	"os"
	"sync"
)

var (
	debug  = flag.Bool("debug", false, "debug mode")
	port   = flag.Int("port", 4100, "The grpc server port")
	ostore store.Store
	olock  sync.Mutex
)

type objectStoreServer struct {
}

func main() {
	var err error
	flag.Parse()
	fmt.Printf("Starting ObjectStoreServer...\n")
	ostore, err = store.NewDiskStore("/srv/objectstore/")
	utils.Bail("failed to open store", err)

	sd := server.NewServerDef()
	sd.SetPort(*port)
	sd.SetRegister(server.Register(
		func(server *grpc.Server) error {
			e := new(objectStoreServer)
			pb.RegisterObjectStoreServer(server, e)
			return nil
		},
	))
	err = server.ServerStartup(sd)
	utils.Bail("Unable to start server", err)
	os.Exit(0)
}

/************************************
* grpc functions
************************************/

func (e *objectStoreServer) TriggerCheckDisk(ctx context.Context, req *common.Void) (*common.Void, error) {
	go ostore.Check()
	return &common.Void{}, nil
}
func (e *objectStoreServer) PutWithID(ctx context.Context, req *pb.PutWithIDRequest) (*common.Void, error) {
	fmt.Printf("Saving key %s\n", req.ID)
	err := ostore.Put(ctx, req.ID, req.Content, req.Expiry)
	if err != nil {
		return nil, err
	}
	return &common.Void{}, nil
}

func (e *objectStoreServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.Object, error) {
	key := req.ID
	if *debug {
		fmt.Printf("Getting content for key %s\n", key)
	}
	c, b, err := ostore.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !b {
		return nil, errors.NotFound(ctx, "not found")
	}
	o := &pb.Object{ID: key, Content: c}
	return o, nil
}

func (e *objectStoreServer) LGet(req *pb.GetRequest, srv pb.ObjectStore_LGetServer) error {
	key := req.ID
	if *debug {
		fmt.Printf("Getting content for key %s\n", key)
	}
	ctx := srv.Context()
	buf, b, err := ostore.Get(ctx, req.ID)
	if err != nil {
		return err
	}
	if !b {
		return errors.NotFound(ctx, "not found")
	}

	size := 8192
	repeat := true
	offset := 0
	for repeat {
		if offset+size > len(buf) {
			size = len(buf) - offset
			repeat = false
		}
		n := buf[offset : offset+size]
		offset = offset + size
		err := srv.Send(&pb.Object{ID: key, Content: n})
		if err != nil {
			return err
		}
	}
	buf = buf[:0]
	return nil
}

func (e *objectStoreServer) LPutWithID(srv pb.ObjectStore_LPutWithIDServer) error {
	var buf []byte
	var key string
	var exp uint32
	for {
		ct, err := srv.Recv()
		if err == nil {
			if srv.Context().Err() != nil {
				err = srv.Context().Err()
				return err
			}
			if ct.Expiry != 0 {
				exp = ct.Expiry
			}
			if key == "" {
				fmt.Printf("Saving key %s\n", ct.ID)
				key = ct.ID
			}
			buf = append(buf, ct.Content...)
			continue
		}
		if err == io.EOF {
			break
		}
		return err
	}
	err := srv.SendAndClose(&common.Void{})
	if err != nil {
		return err
	}
	fmt.Printf("Received %d bytes for key %s\n", len(buf), key)
	ctx := srv.Context()
	err = ostore.Put(ctx, key, buf, exp)
	if err != nil {
		fmt.Printf("Error for key \"%s\" as submitted by service %s: %s\n", key, auth.Description(auth.GetService(srv.Context())), err)
	}
	buf = buf[:0] // gc?
	return err
}

func (e *objectStoreServer) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	var err error
	var obs []*pb.ObjectMeta
	if req.Prefix == "" {
		obs, err = ostore.List(ctx, req)
	}
	if err != nil {
		return nil, err
	}
	res := &pb.ListResponse{Objects: obs}
	return res, nil
}

func (e *objectStoreServer) PutIfNotExists(ctx context.Context, req *pb.PutWithIDRequest) (*pb.PutResponse, error) {
	olock.Lock()
	defer olock.Unlock()
	key := req.ID
	c, b, err := ostore.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if b {
		return &pb.PutResponse{WasAdded: false, DidExist: true}, nil
	}
	if c != nil {
		return &pb.PutResponse{WasAdded: false, DidExist: true}, nil
	}
	ostore.Put(ctx, req.ID, req.Content, req.Expiry)
	return nil, nil
}
func (e *objectStoreServer) TryGet(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	key := req.ID
	c, b, err := ostore.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !b {
		return &pb.GetResponse{DoesExist: false}, nil
	}
	o := &pb.Object{ID: key, Content: c}
	return &pb.GetResponse{DoesExist: true, Object: o}, nil
}

func (e *objectStoreServer) Evict(ctx context.Context, req *pb.EvictRequest) (*pb.GetResponse, error) {
	c, b, err := ostore.Evict(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	if !b {
		return &pb.GetResponse{DoesExist: false}, nil
	}
	o := &pb.Object{ID: req.ID, Content: c}
	if req.ReturnObject {
		return &pb.GetResponse{DoesExist: true, Object: o}, nil
	}
	return &pb.GetResponse{DoesExist: true}, nil
}
func (e *objectStoreServer) HigherOrSameThanVersion(req *pb.ByVersionRequest, srv pb.ObjectStore_HigherOrSameThanVersionServer) error {
	return ostore.HigherOrSameThanVersion(req, srv)

}
func (e *objectStoreServer) DoesExist(ctx context.Context, req *pb.GetRequest) (*pb.ExistResponse, error) {
	gr, err := e.TryGet(ctx, req)
	if err != nil {
		return nil, err
	}
	er := &pb.ExistResponse{ID: req.ID, DoesExist: gr.DoesExist}
	return er, nil
}




