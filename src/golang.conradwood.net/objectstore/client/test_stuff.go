package main

import (
	"flag"
	"fmt"
	//	obj "golang.conradwood.net/apis/objectstore"
	"golang.conradwood.net/go-easyops/authremote"
	"golang.conradwood.net/go-easyops/utils"
	"io"
	"strings"
	"sync"
	"time"
)

var (
	fname   = flag.String("test_filename", "/tmp/cacheids.txt", "ids to test with")
	workers = 20
)

type worker struct {
	idx int
	ch  chan *work
	wg  *sync.WaitGroup
}
type work struct {
	key  string
	exit bool
}

func test_stuff() {
	ch := make(chan *work)
	wg := sync.WaitGroup{}
	idx := 0
	for i := 0; i < workers; i++ {
		wg.Add(1)
		idx++
		w := &worker{wg: &wg, ch: ch, idx: idx}
		go w.test_worker()
	}
	l, err := utils.ReadFile(*fname)
	utils.Bail("failed to read ids", err)
	keys := strings.Split(string(l), "\n")
	fmt.Printf("Got %d keys to operate on\n", len(keys))
	for _, key := range keys {
		if len(key) < 4 {
			continue
		}
		uw := &work{key: key}
		ch <- uw
	}
	fmt.Printf("Signalling workers to exit...\n")
	for i := 0; i < workers; i++ {
		ch <- &work{exit: true}
	}
	fmt.Printf("Waiting for workers to exit...\n")
	wg.Wait()
}
func (w *worker) test_worker() {
	defer w.wg.Done()
	for {
		wk := <-w.ch
		if wk.exit {
			return
		}
		fmt.Printf("retrieving %s\n", wk.key)
		ctx := authremote.ContextWithTimeout(time.Duration(180) * time.Second)
		or, err := NewObjectStoreReader(ctx, wk.key)
		utils.Bail("failed to get objectstore reader", err)
		outfile, err := utils.OpenWriteFile(fmt.Sprintf("/tmp/x/objs/%d.bin", w.idx))
		utils.Bail("failed to open write file", err)
		_, err = io.Copy(outfile, or)
		or.Close()
		outfile.Close()
		utils.Bail(fmt.Sprintf("failed to iocopy \"%s\"", wk.key), err)
	}
}







