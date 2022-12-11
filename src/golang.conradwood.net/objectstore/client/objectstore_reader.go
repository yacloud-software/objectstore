package main

import (
	"context"
	"fmt"
	obj "golang.conradwood.net/apis/objectstore"
	"golang.conradwood.net/go-easyops/client"
)

type ObjectStoreReader struct {
	srv                    obj.ObjectStore_LGetClient
	dchan                  chan *object_store_data
	sending_object         *object_store_data // the one we are currently sending
	bytes_sent_from_object int                // number of bytes sent from current object
}
type object_store_data struct {
	err  error
	data *obj.Object
}

func NewObjectStoreReader(ctx context.Context, key string) (*ObjectStoreReader, error) {
	gr := &obj.GetRequest{ID: key}
	srv, err := client.GetObjectStoreClient().LGet(ctx, gr)
	if err != nil {
		return nil, err
	}
	osr := &ObjectStoreReader{
		srv:   srv,
		dchan: make(chan *object_store_data, 20),
	}
	return osr, nil
}

// retrieve from objectstore and make it available to Read() function
func (osr *ObjectStoreReader) retriever() {
	for {
		dt, err := osr.srv.Recv()
		osd := &object_store_data{data: dt, err: err}
		osr.dchan <- osd
		if err != nil {
			break
		}

	}
}

func (osr *ObjectStoreReader) Close() error {
	return nil
}
func (osr *ObjectStoreReader) Read(buf []byte) (int, error) {
	return 0, fmt.Errorf("foo")
}
