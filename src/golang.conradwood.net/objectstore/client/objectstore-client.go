package main

import (
	"context"
	"flag"
	"fmt"
	"golang.conradwood.net/apis/common"
	pb "golang.conradwood.net/apis/objectstore"
	ar "golang.conradwood.net/go-easyops/authremote"
	"golang.conradwood.net/go-easyops/client"
	"golang.conradwood.net/go-easyops/utils"
	"io"
	"os"
	"time"
)

var (
	echoClient pb.ObjectStoreClient
	id         = flag.String("id", "", "id of object to set or get")
	filename   = flag.String("filename", "/tmp/object.bin", "file to save/load from")
	get        = flag.Bool("get", false, "get object")
	put        = flag.Bool("put", false, "put object")
	list       = flag.Bool("list", false, "list objects")
	check      = flag.Bool("check", false, "trigger a disk/db sync on server")
	min_vers   = flag.Int("min_version", 0, "if non-zero get same or higher than this version")
)

func main() {
	flag.Parse()
	if *get && *put {
		fmt.Printf("Won't get and put at the same time. got some safety issues\n")
		os.Exit(10)
	}
	echoClient = osclient()
	ctx := ar.Context()
	if *min_vers > 0 {
		DoMinVers()
		os.Exit(0)
	}
	if *check {
		_, err := echoClient.TriggerCheckDisk(ctx, &common.Void{})
		utils.Bail("failed to trigger check", err)
		fmt.Printf("Check initiated\n")
	}
	if *list {
		dolist(ctx)
	}

	// a context with authentication
	if *get {
		if *id == "" {
			fmt.Printf("Please specify -id")
			os.Exit(10)
		}
		response, err := Get(ctx, *id)
		utils.Bail("failed to get", err)
		utils.WriteFile(*filename, response)
		fmt.Printf("Saved %d bytes to %s\n", len(response), *filename)
	}
	if *put {
		if *id == "" {
			fmt.Printf("Please specify -id")
			os.Exit(10)
		}
		fmt.Printf("Reading file %s\n", *filename)
		ct, err := utils.ReadFile(*filename)
		utils.Bail("failed to read file", err)
		fmt.Printf("Putting %d bytes of file %s\n", len(ct), *filename)
		err = PutWithID(ctx, *id, ct, uint32(time.Now().Add(time.Duration(30)*time.Second).Unix()))
		utils.Bail("failed to put object", err)
	}

	fmt.Printf("Done.\n")
	os.Exit(0)
}
func dolist(ctx context.Context) {
	lr := &pb.ListRequest{}
	response, err := echoClient.List(ctx, lr)
	utils.Bail("failed to list objects", err)
	fmt.Printf("%d objects\n", len(response.Objects))
	ml := 0
	for _, o := range response.Objects {
		if len(o.Key) > ml {
			ml = len(o.Key)
		}
	}
	q := "%5d %" + fmt.Sprintf("%d", ml) + "s %d %s %s %s %s %s\n"
	for _, o := range response.Objects {
		fmt.Printf(q,
			o.ID,
			o.Key,
			o.Version,
			o.Service,
			o.Creator,
			utils.TimestampString(o.Created),
			utils.TimestampString(o.LastUpdated),
			utils.TimestampString(o.Expiry),
		)
	}
}
func osclient() pb.ObjectStoreClient {
	if echoClient != nil {
		return echoClient
	}
	ec := pb.NewObjectStoreClient(client.Connect("objectstore.ObjectStore"))
	echoClient = ec
	return echoClient
}
func DoMinVers() {
	ctx := ar.Context()
	srv, err := osclient().HigherOrSameThanVersion(ctx, &pb.ByVersionRequest{Version: uint64(*min_vers)})
	utils.Bail("failed to query", err)
	for {
		m, err := srv.Recv()
		if err == io.EOF {
			break
		}
		utils.Bail("failed to receive", err)
		fmt.Printf("Received %d objects\n", len(m.Objects))
		for _, md := range m.Objects {
			fmt.Printf("%15d %05d %s\n", md.ID, md.StoreVersion, md.Key)
		}
	}
	fmt.Printf("Done\n")
}
