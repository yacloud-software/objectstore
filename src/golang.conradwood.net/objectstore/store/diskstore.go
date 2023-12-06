package store

import (
	"context"
	"flag"
	"fmt"
	pb "golang.conradwood.net/apis/objectstore"
	"golang.conradwood.net/go-easyops/auth"
	"golang.conradwood.net/go-easyops/errors"
	"golang.conradwood.net/go-easyops/prometheus"
	"golang.conradwood.net/go-easyops/sql"
	"golang.conradwood.net/go-easyops/utils"
	"golang.conradwood.net/objectstore/db"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	versionGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "objectstore_version",
			Help: "V=1 UNIT=none DESC=current storeversion",
		},
	)
	inc_store_version = flag.Bool("inc_store_version", true, "if true, increase a store version, otherwise keep it at 0")
	auto_delete_files = flag.Bool("auto_delete_files", false, "automatically delete files which do not exist in the database. Use with caution")
	auto_delete_rows  = flag.Bool("auto_delete_rows", false, "automatically delete rows which do not exist on disk. Use with caution")
	dbstore           *db.DBObjectMeta
	lock              sync.Mutex
	psql              *sql.DB
	consistencyCheck  = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "objectstore_disk_inconsistency",
			Help: "V=1 UNIT=none DESC=current inconsistencies on disk vs database",
		},
		[]string{"missing"},
	)
)

func init() {
	prometheus.MustRegister(consistencyCheck, versionGauge)
}

type DiskStore struct {
	cl  sync.Mutex
	dir string
}

func NewDiskStore(dir string) (Store, error) {
	var err error
	res := &DiskStore{dir: dir}
	psql, err = sql.Open()
	if err != nil {
		return nil, err
	}
	dbstore = db.DefaultDBObjectMeta()
	go res.diskstore_cleaner()
	return res, nil
}

func (d *DiskStore) Evict(ctx context.Context, key string) ([]byte, bool, error) {
	c, b, err := d.Get(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if !b {
		return nil, false, nil
	}
	// set expiry to the past:
	err = d.Put(ctx, key, []byte{}, 1) // not 0 because that means unlimited
	if err != nil {
		return nil, true, err
	}
	return c, b, nil
}
func (d *DiskStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	o, err := dbstore.ByKey(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if o == nil || len(o) == 0 {
		return nil, false, nil
	}
	var os *pb.ObjectMeta
	now := time.Now().Unix()
	for _, anyos := range o {
		if anyos.Expiry != 0 && int64(anyos.Expiry) <= now {
			continue
		}
		os = anyos
		break
	}
	if os == nil {
		return nil, false, nil
	}
	os.LastRetrieved = uint32(time.Now().Unix())
	err = dbstore.Update(ctx, os)
	if err != nil {
		return nil, false, err
	}

	//	fmt.Printf("Filename: %s\n", os.StoreKey)
	b, err := utils.ReadFile(d.dir + os.StoreKey)
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}
func (d *DiskStore) Put(ctx context.Context, key string, buf []byte, expiry uint32) error {
	if key == "" {
		return errors.InvalidArgs(ctx, "missing key", "missing key")
	}
	o, err := dbstore.ByKey(ctx, key)
	if err != nil {
		return err
	}
	// if we have an object already, update it:
	if o != nil && len(o) != 0 {
		err = d.update(ctx, o[0], buf, expiry)
		return err
	}
	// if we have none. lock + create
	lock.Lock()
	o, err = dbstore.ByKey(ctx, key)
	if err != nil {
		lock.Unlock()
		return err
	}
	if o != nil && len(o) != 0 {
		lock.Unlock()
		return d.update(ctx, o[0], buf, expiry)
	}
	// create a new one:
	defer lock.Unlock()
	var fname string
	for {
		fname = fmt.Sprintf("%02d/%s", utils.RandomInt(99), utils.RandomString(64))
		if !utils.FileExists(d.dir + fname) {
			b := filepath.Dir(d.dir + fname)
			fmt.Printf("Making %s\n", b)
			err := os.MkdirAll(b, 0777)
			if err != nil {
				fmt.Printf("failed to mkdir %s :%s\n", b, err)
				return err
			}
			break
		}
	}
	user := ""
	service := ""
	u := auth.GetUser(ctx)
	if u != nil {
		user = u.ID
	}
	u = auth.GetService(ctx)
	if u != nil {
		service = u.ID
	}
	om := &pb.ObjectMeta{
		Key:         key,
		Version:     0,
		Service:     service,
		Creator:     user,
		Created:     uint32(time.Now().Unix()),
		LastUpdated: 0,
		StoreID:     1,
		StoreKey:    fname,
		Expiry:      expiry,
	}
	err = utils.WriteFile(d.dir+fname, buf)
	if err != nil {
		return err
	}
	nid, err := get_next_store_version(ctx)
	if err != nil {
		return err
	}
	om.StoreVersion = nid
	_, err = dbstore.Save(ctx, om)
	if err != nil {
		return err
	}
	fmt.Printf("Saved %s under key %s\n", fname, key)
	return nil
}

func (d *DiskStore) update(ctx context.Context, om *pb.ObjectMeta, buf []byte, expiry uint32) error {
	fname := om.StoreKey
	err := utils.WriteFile(d.dir+fname, buf)
	if err != nil {
		return err
	}
	om.LastUpdated = uint32(time.Now().Unix())
	om.Expiry = expiry
	om.Version = om.Version + 1
	nid, err := get_next_store_version(ctx)
	if err != nil {
		return err
	}
	om.StoreVersion = nid
	err = dbstore.Update(ctx, om)
	if err != nil {
		return err
	}
	fmt.Printf("Updated file %s under key %s\n", fname, om.Key)
	return nil
}

func (d *DiskStore) List(ctx context.Context, req *pb.ListRequest) ([]*pb.ObjectMeta, error) {
	var err error
	var res []*pb.ObjectMeta
	if req.Prefix == "" {
		res, err = dbstore.All(ctx)
	} else {
		return nil, errors.NotImplemented(ctx, "cannot yet filter by prefix")
	}
	if err != nil {
		return nil, err
	}
	res = removeExpired(res)
	return res, nil
}
func removeExpired(in []*pb.ObjectMeta) []*pb.ObjectMeta {
	res := in
	sort.Slice(res, func(i, j int) bool {
		if res[i].Expiry == 0 && res[j].Expiry != 0 {
			return true
		}
		if res[j].Expiry == 0 && res[i].Expiry != 0 {
			return false
		}
		return res[i].Expiry > res[j].Expiry
	})
	x := 0
	now := time.Now().Unix()
	for i, k := range res {
		if k.Expiry == 0 {
			continue
		}
		if int64(k.Expiry) < now {
			x = i
			break
		}
	}
	//	fmt.Printf("Cut at %d\n", x)
	res = res[:x]
	return res
}

/*
*****************
the housekeeping
******************
*/
func (d *DiskStore) diskstore_cleaner() {
	time.Sleep(5 * time.Second)
	for {
		d.Check()
		time.Sleep(120 * time.Minute)
	}
}
func (d *DiskStore) Check() {
	fmt.Printf("Requesting lock for consistency check\n")
	d.cl.Lock()
	defer d.cl.Unlock()
	fmt.Printf("Running Consistency check\n")
	err := d.clean_expired()
	if err != nil {
		fmt.Printf("Error cleaning expired: %s\n", err)
	}
	err = d.check_disk_exists()
	if err != nil {
		fmt.Printf("Error check_disk_exists: %s\n", err)
	}
	err = d.check_db_exists()
	if err != nil {
		fmt.Printf("Error check_disk_exists: %s\n", err)
	}
	fmt.Printf("Consistency check complete.\n")
}

// check that all rows exist in db for each file one disk
func (d *DiskStore) check_db_exists() error {
	ct, err := d.check_dir(d.dir)
	if err != nil {
		return err
	}
	consistencyCheck.With(prometheus.Labels{"missing": "db"}).Set(float64(ct))
	if ct != 0 {
		fmt.Printf("Missing %d rows in database\n", ct)
	}
	return nil
}

func (d *DiskStore) check_dir(dir string) (int, error) {
	dir = strings.TrimSuffix(dir, "/")
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	res := 0
	for _, f := range files {
		fname := dir + "/" + f.Name()
		if f.IsDir() {
			tr, err := d.check_dir(fname)
			if err != nil {
				return 0, err
			}
			res = res + tr
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(2)*time.Second)
		defer cancel()
		sk := strings.TrimPrefix(fname, d.dir)
		sk = strings.TrimPrefix(sk, "/")
		obs, err := dbstore.ByStoreKey(ctx, sk)
		if err != nil {
			return 0, err
		}
		if len(obs) == 0 {
			fmt.Printf("no row exists for file: %s\n", sk)
			if *auto_delete_files {
				err = os.Remove(fname)
				if err != nil {
					fmt.Printf("Failed to delete file: %s\n", err)
				} else {
					continue
				}
			}
			res = res + 1
		}
	}
	return res, nil
}

// check that all files exist on disk which are mentioned in db
func (d *DiskStore) check_disk_exists() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(600)*time.Second)
	defer cancel()
	obs, err := dbstore.All(ctx)
	if err != nil {
		return err
	}
	missing := 0
	for _, o := range obs {
		fname := d.dir + o.StoreKey
		if utils.FileExists(fname) {
			continue
		}
		if !*auto_delete_rows {
			missing++
			continue
		}
		fmt.Printf("Delete key %s\n", o.Key)
		err = dbstore.DeleteByID(ctx, o.ID)
		if err == nil {
			continue
		}
		fmt.Printf("Deleted from disk, but failed to delete from db: %s\n", err)
		missing++

	}
	consistencyCheck.With(prometheus.Labels{"missing": "disk"}).Set(float64(missing))
	return nil
}
func (d *DiskStore) clean_expired() error {
	n := time.Now().Unix()
	s := fmt.Sprintf("select "+dbstore.SelectCols()+" from "+dbstore.Tablename()+" where expiry < %d and expiry != 0", n)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(180)*time.Second)
	defer cancel()
	r, err := psql.QueryContext(ctx, "select_expired", s)
	if err != nil {
		return err
	}
	obs, err := dbstore.FromRows(ctx, r)
	if err != nil {
		return err
	}
	fmt.Printf("%d expired\n", len(obs))
	for _, o := range obs {
		fname := d.dir + o.StoreKey
		if !utils.FileExists(fname) {
			//	fmt.Printf("%s (%s) does not exist on disk\n", o.Key, fname)
			continue
		}
		err = os.Remove(fname)
		if err != nil {
			fmt.Printf("Failed to remove file: %s\n", err)
			continue
		}
		err = dbstore.DeleteByID(ctx, o.ID)
		if err != nil {
			fmt.Printf("Deleted from disk, but failed to delete from db: %s\n", err)
			continue
		}
	}
	return nil
}
func (e *DiskStore) HigherOrSameThanVersion(req *pb.ByVersionRequest, srv pb.ObjectStore_HigherOrSameThanVersionServer) error {
	n := req.Version
	lv, err := get_store_version(srv.Context())
	if err != nil {
		return err
	}
	if lv < n {
		// shortcut - nothing to sync
		return nil
	}
	s := fmt.Sprintf("select "+dbstore.SelectCols()+" from "+dbstore.Tablename()+" where storeversion >= %d order by key", n)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(180)*time.Second)
	defer cancel()
	r, err := psql.QueryContext(ctx, "select_by_version", s)
	if err != nil {
		return err
	}
	obs, err := dbstore.FromRows(ctx, r)
	if err != nil {
		return err
	}
	var buf []*pb.ObjectMeta
	MAX_OBJS := 64
	now := time.Now().Unix()
	for _, o := range obs {
		if o.Expiry != 0 && int64(o.Expiry) <= now {
			continue
		}
		if len(buf) < MAX_OBJS {
			buf = append(buf, o)
		} else {
			v := &pb.KeyList{Objects: buf}
			err := srv.Send(v)
			if err != nil {
				return err
			}
			buf = buf[:0]
		}
	}
	if len(buf) > 0 {
		v := &pb.KeyList{Objects: buf}
		err := srv.Send(v)
		if err != nil {
			return err
		}
	}
	return nil
}

func get_next_store_version(ctx context.Context) (uint64, error) {
	if !*inc_store_version {
		return 1, nil
	}
	rows, err := psql.QueryContext(ctx, "select_new_id", `select nextval('store_version_seq')`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, nil
	}
	var nid uint64
	err = rows.Scan(&nid)
	if err != nil {
		return 0, err
	}
	versionGauge.Set(float64(nid))
	return nid, nil

}

func get_store_version(ctx context.Context) (uint64, error) {
	if !*inc_store_version {
		return 1, nil
	}
	rows, err := psql.QueryContext(ctx, "select_current_id", `select last_value from store_version_seq`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, nil
	}
	var nid uint64
	err = rows.Scan(&nid)
	if err != nil {
		return 0, err
	}
	versionGauge.Set(float64(nid))
	return nid, nil

}

