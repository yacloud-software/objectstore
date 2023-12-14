package db

/*
 This file was created by mkdb-client.
 The intention is not to modify thils file, but you may extend the struct DBObjectMeta
 in a seperate file (so that you can regenerate this one from time to time)
*/

/*
 PRIMARY KEY: ID
*/

/*
 postgres:
 create sequence objectmeta_seq;

Main Table:

 CREATE TABLE objectmeta (id integer primary key default nextval('objectmeta_seq'),key text not null  ,version bigint not null  ,service text not null  ,creator text not null  ,lastretrieved integer not null  ,created integer not null  ,lastupdated integer not null  ,storekey text not null  ,storeid integer not null  ,expiry integer not null  ,storeversion bigint not null  );

Alter statements:
ALTER TABLE objectmeta ADD COLUMN key text not null default '';
ALTER TABLE objectmeta ADD COLUMN version bigint not null default 0;
ALTER TABLE objectmeta ADD COLUMN service text not null default '';
ALTER TABLE objectmeta ADD COLUMN creator text not null default '';
ALTER TABLE objectmeta ADD COLUMN lastretrieved integer not null default 0;
ALTER TABLE objectmeta ADD COLUMN created integer not null default 0;
ALTER TABLE objectmeta ADD COLUMN lastupdated integer not null default 0;
ALTER TABLE objectmeta ADD COLUMN storekey text not null default '';
ALTER TABLE objectmeta ADD COLUMN storeid integer not null default 0;
ALTER TABLE objectmeta ADD COLUMN expiry integer not null default 0;
ALTER TABLE objectmeta ADD COLUMN storeversion bigint not null default 0;


Archive Table: (structs can be moved from main to archive using Archive() function)

 CREATE TABLE objectmeta_archive (id integer unique not null,key text not null,version bigint not null,service text not null,creator text not null,lastretrieved integer not null,created integer not null,lastupdated integer not null,storekey text not null,storeid integer not null,expiry integer not null,storeversion bigint not null);
*/

import (
	"context"
	gosql "database/sql"
	"fmt"
	savepb "golang.conradwood.net/apis/objectstore"
	"golang.conradwood.net/go-easyops/sql"
	"os"
)

var (
	default_def_DBObjectMeta *DBObjectMeta
)

type DBObjectMeta struct {
	DB                  *sql.DB
	SQLTablename        string
	SQLArchivetablename string
}

func DefaultDBObjectMeta() *DBObjectMeta {
	if default_def_DBObjectMeta != nil {
		return default_def_DBObjectMeta
	}
	psql, err := sql.Open()
	if err != nil {
		fmt.Printf("Failed to open database: %s\n", err)
		os.Exit(10)
	}
	res := NewDBObjectMeta(psql)
	ctx := context.Background()
	err = res.CreateTable(ctx)
	if err != nil {
		fmt.Printf("Failed to create table: %s\n", err)
		os.Exit(10)
	}
	default_def_DBObjectMeta = res
	return res
}
func NewDBObjectMeta(db *sql.DB) *DBObjectMeta {
	foo := DBObjectMeta{DB: db}
	foo.SQLTablename = "objectmeta"
	foo.SQLArchivetablename = "objectmeta_archive"
	return &foo
}

// archive. It is NOT transactionally save.
func (a *DBObjectMeta) Archive(ctx context.Context, id uint64) error {

	// load it
	p, err := a.ByID(ctx, id)
	if err != nil {
		return err
	}

	// now save it to archive:
	_, e := a.DB.ExecContext(ctx, "archive_DBObjectMeta", "insert into "+a.SQLArchivetablename+"+ (id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion) values ($1,$2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ", p.ID, p.Key, p.Version, p.Service, p.Creator, p.LastRetrieved, p.Created, p.LastUpdated, p.StoreKey, p.StoreID, p.Expiry, p.StoreVersion)
	if e != nil {
		return e
	}

	// now delete it.
	a.DeleteByID(ctx, id)
	return nil
}

// Save (and use database default ID generation)
func (a *DBObjectMeta) Save(ctx context.Context, p *savepb.ObjectMeta) (uint64, error) {
	qn := "DBObjectMeta_Save"
	rows, e := a.DB.QueryContext(ctx, qn, "insert into "+a.SQLTablename+" (key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) returning id", p.Key, p.Version, p.Service, p.Creator, p.LastRetrieved, p.Created, p.LastUpdated, p.StoreKey, p.StoreID, p.Expiry, p.StoreVersion)
	if e != nil {
		return 0, a.Error(ctx, qn, e)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, a.Error(ctx, qn, fmt.Errorf("No rows after insert"))
	}
	var id uint64
	e = rows.Scan(&id)
	if e != nil {
		return 0, a.Error(ctx, qn, fmt.Errorf("failed to scan id after insert: %s", e))
	}
	p.ID = id
	return id, nil
}

// Save using the ID specified
func (a *DBObjectMeta) SaveWithID(ctx context.Context, p *savepb.ObjectMeta) error {
	qn := "insert_DBObjectMeta"
	_, e := a.DB.ExecContext(ctx, qn, "insert into "+a.SQLTablename+" (id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion) values ($1,$2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ", p.ID, p.Key, p.Version, p.Service, p.Creator, p.LastRetrieved, p.Created, p.LastUpdated, p.StoreKey, p.StoreID, p.Expiry, p.StoreVersion)
	return a.Error(ctx, qn, e)
}

func (a *DBObjectMeta) Update(ctx context.Context, p *savepb.ObjectMeta) error {
	qn := "DBObjectMeta_Update"
	_, e := a.DB.ExecContext(ctx, qn, "update "+a.SQLTablename+" set key=$1, version=$2, service=$3, creator=$4, lastretrieved=$5, created=$6, lastupdated=$7, storekey=$8, storeid=$9, expiry=$10, storeversion=$11 where id = $12", p.Key, p.Version, p.Service, p.Creator, p.LastRetrieved, p.Created, p.LastUpdated, p.StoreKey, p.StoreID, p.Expiry, p.StoreVersion, p.ID)

	return a.Error(ctx, qn, e)
}

// delete by id field
func (a *DBObjectMeta) DeleteByID(ctx context.Context, p uint64) error {
	qn := "deleteDBObjectMeta_ByID"
	_, e := a.DB.ExecContext(ctx, qn, "delete from "+a.SQLTablename+" where id = $1", p)
	return a.Error(ctx, qn, e)
}

// get it by primary id
func (a *DBObjectMeta) ByID(ctx context.Context, p uint64) (*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByID"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where id = $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByID: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByID: error scanning (%s)", e))
	}
	if len(l) == 0 {
		return nil, a.Error(ctx, qn, fmt.Errorf("No ObjectMeta with id %v", p))
	}
	if len(l) != 1 {
		return nil, a.Error(ctx, qn, fmt.Errorf("Multiple (%d) ObjectMeta with id %v", len(l), p))
	}
	return l[0], nil
}

// get all rows
func (a *DBObjectMeta) All(ctx context.Context) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_all"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" order by id")
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("All: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, fmt.Errorf("All: error scanning (%s)", e)
	}
	return l, nil
}

/**********************************************************************
* GetBy[FIELD] functions
**********************************************************************/

// get all "DBObjectMeta" rows with matching Key
func (a *DBObjectMeta) ByKey(ctx context.Context, p string) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByKey"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where key = $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByKey: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByKey: error scanning (%s)", e))
	}
	return l, nil
}

// the 'like' lookup
func (a *DBObjectMeta) ByLikeKey(ctx context.Context, p string) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByLikeKey"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where key ilike $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByKey: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByKey: error scanning (%s)", e))
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching Version
func (a *DBObjectMeta) ByVersion(ctx context.Context, p uint64) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByVersion"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where version = $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByVersion: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByVersion: error scanning (%s)", e))
	}
	return l, nil
}

// the 'like' lookup
func (a *DBObjectMeta) ByLikeVersion(ctx context.Context, p uint64) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByLikeVersion"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where version ilike $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByVersion: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByVersion: error scanning (%s)", e))
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching Service
func (a *DBObjectMeta) ByService(ctx context.Context, p string) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByService"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where service = $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByService: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByService: error scanning (%s)", e))
	}
	return l, nil
}

// the 'like' lookup
func (a *DBObjectMeta) ByLikeService(ctx context.Context, p string) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByLikeService"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where service ilike $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByService: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByService: error scanning (%s)", e))
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching Creator
func (a *DBObjectMeta) ByCreator(ctx context.Context, p string) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByCreator"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where creator = $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByCreator: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByCreator: error scanning (%s)", e))
	}
	return l, nil
}

// the 'like' lookup
func (a *DBObjectMeta) ByLikeCreator(ctx context.Context, p string) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByLikeCreator"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where creator ilike $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByCreator: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByCreator: error scanning (%s)", e))
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching LastRetrieved
func (a *DBObjectMeta) ByLastRetrieved(ctx context.Context, p uint32) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByLastRetrieved"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where lastretrieved = $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByLastRetrieved: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByLastRetrieved: error scanning (%s)", e))
	}
	return l, nil
}

// the 'like' lookup
func (a *DBObjectMeta) ByLikeLastRetrieved(ctx context.Context, p uint32) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByLikeLastRetrieved"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where lastretrieved ilike $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByLastRetrieved: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByLastRetrieved: error scanning (%s)", e))
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching Created
func (a *DBObjectMeta) ByCreated(ctx context.Context, p uint32) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByCreated"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where created = $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByCreated: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByCreated: error scanning (%s)", e))
	}
	return l, nil
}

// the 'like' lookup
func (a *DBObjectMeta) ByLikeCreated(ctx context.Context, p uint32) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByLikeCreated"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where created ilike $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByCreated: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByCreated: error scanning (%s)", e))
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching LastUpdated
func (a *DBObjectMeta) ByLastUpdated(ctx context.Context, p uint32) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByLastUpdated"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where lastupdated = $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByLastUpdated: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByLastUpdated: error scanning (%s)", e))
	}
	return l, nil
}

// the 'like' lookup
func (a *DBObjectMeta) ByLikeLastUpdated(ctx context.Context, p uint32) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByLikeLastUpdated"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where lastupdated ilike $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByLastUpdated: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByLastUpdated: error scanning (%s)", e))
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching StoreKey
func (a *DBObjectMeta) ByStoreKey(ctx context.Context, p string) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByStoreKey"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where storekey = $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByStoreKey: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByStoreKey: error scanning (%s)", e))
	}
	return l, nil
}

// the 'like' lookup
func (a *DBObjectMeta) ByLikeStoreKey(ctx context.Context, p string) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByLikeStoreKey"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where storekey ilike $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByStoreKey: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByStoreKey: error scanning (%s)", e))
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching StoreID
func (a *DBObjectMeta) ByStoreID(ctx context.Context, p uint32) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByStoreID"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where storeid = $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByStoreID: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByStoreID: error scanning (%s)", e))
	}
	return l, nil
}

// the 'like' lookup
func (a *DBObjectMeta) ByLikeStoreID(ctx context.Context, p uint32) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByLikeStoreID"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where storeid ilike $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByStoreID: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByStoreID: error scanning (%s)", e))
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching Expiry
func (a *DBObjectMeta) ByExpiry(ctx context.Context, p uint32) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByExpiry"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where expiry = $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByExpiry: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByExpiry: error scanning (%s)", e))
	}
	return l, nil
}

// the 'like' lookup
func (a *DBObjectMeta) ByLikeExpiry(ctx context.Context, p uint32) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByLikeExpiry"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where expiry ilike $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByExpiry: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByExpiry: error scanning (%s)", e))
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching StoreVersion
func (a *DBObjectMeta) ByStoreVersion(ctx context.Context, p uint64) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByStoreVersion"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where storeversion = $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByStoreVersion: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByStoreVersion: error scanning (%s)", e))
	}
	return l, nil
}

// the 'like' lookup
func (a *DBObjectMeta) ByLikeStoreVersion(ctx context.Context, p uint64) ([]*savepb.ObjectMeta, error) {
	qn := "DBObjectMeta_ByLikeStoreVersion"
	rows, e := a.DB.QueryContext(ctx, qn, "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion from "+a.SQLTablename+" where storeversion ilike $1", p)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByStoreVersion: error querying (%s)", e))
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, a.Error(ctx, qn, fmt.Errorf("ByStoreVersion: error scanning (%s)", e))
	}
	return l, nil
}

/**********************************************************************
* Helper to convert from an SQL Query
**********************************************************************/

// from a query snippet (the part after WHERE)
func (a *DBObjectMeta) FromQuery(ctx context.Context, query_where string, args ...interface{}) ([]*savepb.ObjectMeta, error) {
	rows, err := a.DB.QueryContext(ctx, "custom_query_"+a.Tablename(), "select "+a.SelectCols()+" from "+a.Tablename()+" where "+query_where, args...)
	if err != nil {
		return nil, err
	}
	return a.FromRows(ctx, rows)
}

/**********************************************************************
* Helper to convert from an SQL Row to struct
**********************************************************************/
func (a *DBObjectMeta) Tablename() string {
	return a.SQLTablename
}

func (a *DBObjectMeta) SelectCols() string {
	return "id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry, storeversion"
}
func (a *DBObjectMeta) SelectColsQualified() string {
	return "" + a.SQLTablename + ".id," + a.SQLTablename + ".key, " + a.SQLTablename + ".version, " + a.SQLTablename + ".service, " + a.SQLTablename + ".creator, " + a.SQLTablename + ".lastretrieved, " + a.SQLTablename + ".created, " + a.SQLTablename + ".lastupdated, " + a.SQLTablename + ".storekey, " + a.SQLTablename + ".storeid, " + a.SQLTablename + ".expiry, " + a.SQLTablename + ".storeversion"
}

func (a *DBObjectMeta) FromRows(ctx context.Context, rows *gosql.Rows) ([]*savepb.ObjectMeta, error) {
	var res []*savepb.ObjectMeta
	for rows.Next() {
		foo := savepb.ObjectMeta{}
		err := rows.Scan(&foo.ID, &foo.Key, &foo.Version, &foo.Service, &foo.Creator, &foo.LastRetrieved, &foo.Created, &foo.LastUpdated, &foo.StoreKey, &foo.StoreID, &foo.Expiry, &foo.StoreVersion)
		if err != nil {
			return nil, a.Error(ctx, "fromrow-scan", err)
		}
		res = append(res, &foo)
	}
	return res, nil
}

/**********************************************************************
* Helper to create table and columns
**********************************************************************/
func (a *DBObjectMeta) CreateTable(ctx context.Context) error {
	csql := []string{
		`create sequence if not exists ` + a.SQLTablename + `_seq;`,
		`CREATE TABLE if not exists ` + a.SQLTablename + ` (id integer primary key default nextval('` + a.SQLTablename + `_seq'),key text not null  ,version bigint not null  ,service text not null  ,creator text not null  ,lastretrieved integer not null  ,created integer not null  ,lastupdated integer not null  ,storekey text not null  ,storeid integer not null  ,expiry integer not null  ,storeversion bigint not null  );`,
		`CREATE TABLE if not exists ` + a.SQLTablename + `_archive (id integer primary key default nextval('` + a.SQLTablename + `_seq'),key text not null  ,version bigint not null  ,service text not null  ,creator text not null  ,lastretrieved integer not null  ,created integer not null  ,lastupdated integer not null  ,storekey text not null  ,storeid integer not null  ,expiry integer not null  ,storeversion bigint not null  );`,
	}
	for i, c := range csql {
		_, e := a.DB.ExecContext(ctx, fmt.Sprintf("create_"+a.SQLTablename+"_%d", i), c)
		if e != nil {
			return e
		}
	}
	return nil
}

/**********************************************************************
* Helper to meaningful errors
**********************************************************************/
func (a *DBObjectMeta) Error(ctx context.Context, q string, e error) error {
	if e == nil {
		return nil
	}
	return fmt.Errorf("[table="+a.SQLTablename+", query=%s] Error: %s", q, e)
}







