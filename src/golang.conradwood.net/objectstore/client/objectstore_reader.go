package main

import (
	"context"
	"fmt"
	obj "golang.conradwood.net/apis/objectstore"
	"golang.conradwood.net/go-easyops/client"
	"io"
)

type ObjectStoreReader struct {
	srv                  obj.ObjectStore_LGetClient
	dchan                chan *object_store_data
	completed            bool
	total_bytes_received int
}
type object_store_data struct {
	err       error
	data      *obj.Object
	completed bool // if true will send EOF from then on
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
	go osr.retriever()
	return osr, nil
}

// retrieve from objectstore and make it available to Read() function
func (osr *ObjectStoreReader) retriever() {
	var err error
	var dt *obj.Object
	for {
		dt, err = osr.srv.Recv()
		if dt != nil {
			osr.total_bytes_received = osr.total_bytes_received + len(dt.Content)
		}
		osd := &object_store_data{data: dt, err: err}
		osr.dchan <- osd
		if err != nil {
			break
		}
	}
	osd := &object_store_data{err: err}
	osr.dchan <- osd
	osd = &object_store_data{completed: true}
	osr.dchan <- osd
//	fmt.Printf("retrieved %d bytes.\n", osr.total_bytes_received)

}

func (osr *ObjectStoreReader) Close() error {
	return nil
}
func (osr *ObjectStoreReader) Read(buf []byte) (int, error) {
	osd := <-osr.dchan
	if osd.completed {
		osr.completed = true
	}
	if osr.completed {
		fmt.Printf("Sent EOF\n")
		return 0, io.EOF
	}
	n := 0
	if osd.data != nil {
		data := osd.data.Content
		n = len(data)
		if len(data) > len(buf) {
			return 0, fmt.Errorf("objectstorereader - buffer size too small (need at least %d bytes, only got %d bytes buf", len(data), len(buf))
		}
		for i := 0; i < len(osd.data.Content); i++ {
			buf[i] = data[i]
		}
	}
	if n == 0 && osd.err == nil {
		fmt.Printf("ERROR - no data and no error\n")
	}
	return n, osd.err

}






