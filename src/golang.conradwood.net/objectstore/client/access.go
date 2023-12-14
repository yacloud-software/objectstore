package main

import (
	"context"
	os "golang.conradwood.net/apis/objectstore"
	"io"
)

func PutWithID(ctx context.Context, key string, buf []byte, expiry uint32) error {
	ob := osclient()
	stream, err := ob.LPutWithID(ctx)
	if err != nil {
		return err
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
		err := stream.Send(&os.PutWithIDRequest{ID: key, Content: n, Expiry: expiry})
		if err != nil {
			return err
		}
	}
	_, err = stream.CloseAndRecv()
	if err != nil {
		return err
	}
	return err
}
func Get(ctx context.Context, key string) ([]byte, error) {
	ob := osclient()
	gr := &os.GetRequest{ID: key}
	stream, err := ob.LGet(ctx, gr)
	if err != nil {
		return nil, err
	}
	var buf []byte
	for {
		ct, err := stream.Recv()
		if err == nil {
			buf = append(buf, ct.Content...)
			continue
		}
		if err == io.EOF {
			break
		}
		return nil, err

	}
	return buf, nil
}






