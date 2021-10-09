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

 CREATE TABLE objectmeta (id integer primary key default nextval('objectmeta_seq'),key varchar(2000) not null,version bigint not null,service varchar(2000) not null,creator varchar(2000) not null,lastretrieved integer not null,created integer not null,lastupdated integer not null,storekey varchar(2000) not null,storeid integer not null,expiry integer not null);

Archive Table: (structs can be moved from main to archive using Archive() function)

 CREATE TABLE objectmeta_archive (id integer unique not null,key varchar(2000) not null,version bigint not null,service varchar(2000) not null,creator varchar(2000) not null,lastretrieved integer not null,created integer not null,lastupdated integer not null,storekey varchar(2000) not null,storeid integer not null,expiry integer not null);
*/

import (
	gosql "database/sql"
	"fmt"
	savepb "golang.conradwood.net/apis/objectstore"
	"golang.conradwood.net/go-easyops/sql"
	"golang.org/x/net/context"
)

type DBObjectMeta struct {
	DB *sql.DB
}

func NewDBObjectMeta(db *sql.DB) *DBObjectMeta {
	foo := DBObjectMeta{DB: db}
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
	_, e := a.DB.ExecContext(ctx, "insert_DBObjectMeta", "insert into objectmeta_archive (id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry) values ($1,$2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ", p.ID, p.Key, p.Version, p.Service, p.Creator, p.LastRetrieved, p.Created, p.LastUpdated, p.StoreKey, p.StoreID, p.Expiry)
	if e != nil {
		return e
	}

	// now delete it.
	a.DeleteByID(ctx, id)
	return nil
}

// Save (and use database default ID generation)
func (a *DBObjectMeta) Save(ctx context.Context, p *savepb.ObjectMeta) (uint64, error) {
	rows, e := a.DB.QueryContext(ctx, "DBObjectMeta_Save", "insert into objectmeta (key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) returning id", p.Key, p.Version, p.Service, p.Creator, p.LastRetrieved, p.Created, p.LastUpdated, p.StoreKey, p.StoreID, p.Expiry)
	if e != nil {
		return 0, e
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("No rows after insert")
	}
	var id uint64
	e = rows.Scan(&id)
	if e != nil {
		return 0, fmt.Errorf("failed to scan id after insert: %s", e)
	}
	p.ID = id
	return id, nil
}

// Save using the ID specified
func (a *DBObjectMeta) SaveWithID(ctx context.Context, p *savepb.ObjectMeta) error {
	_, e := a.DB.ExecContext(ctx, "insert_DBObjectMeta", "insert into objectmeta (id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry) values ($1,$2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ", p.ID, p.Key, p.Version, p.Service, p.Creator, p.LastRetrieved, p.Created, p.LastUpdated, p.StoreKey, p.StoreID, p.Expiry)
	return e
}

func (a *DBObjectMeta) Update(ctx context.Context, p *savepb.ObjectMeta) error {
	_, e := a.DB.ExecContext(ctx, "DBObjectMeta_Update", "update objectmeta set key=$1, version=$2, service=$3, creator=$4, lastretrieved=$5, created=$6, lastupdated=$7, storekey=$8, storeid=$9, expiry=$10 where id = $11", p.Key, p.Version, p.Service, p.Creator, p.LastRetrieved, p.Created, p.LastUpdated, p.StoreKey, p.StoreID, p.Expiry, p.ID)

	return e
}

// delete by id field
func (a *DBObjectMeta) DeleteByID(ctx context.Context, p uint64) error {
	_, e := a.DB.ExecContext(ctx, "deleteDBObjectMeta_ByID", "delete from objectmeta where id = $1", p)
	return e
}

// get it by primary id
func (a *DBObjectMeta) ByID(ctx context.Context, p uint64) (*savepb.ObjectMeta, error) {
	rows, e := a.DB.QueryContext(ctx, "DBObjectMeta_ByID", "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry from objectmeta where id = $1", p)
	if e != nil {
		return nil, fmt.Errorf("ByID: error querying (%s)", e)
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, fmt.Errorf("ByID: error scanning (%s)", e)
	}
	if len(l) == 0 {
		return nil, fmt.Errorf("No ObjectMeta with id %d", p)
	}
	if len(l) != 1 {
		return nil, fmt.Errorf("Multiple (%d) ObjectMeta with id %d", len(l), p)
	}
	return l[0], nil
}

// get all rows
func (a *DBObjectMeta) All(ctx context.Context) ([]*savepb.ObjectMeta, error) {
	rows, e := a.DB.QueryContext(ctx, "DBObjectMeta_all", "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry from objectmeta order by id")
	if e != nil {
		return nil, fmt.Errorf("All: error querying (%s)", e)
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
	rows, e := a.DB.QueryContext(ctx, "DBObjectMeta_ByKey", "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry from objectmeta where key = $1", p)
	if e != nil {
		return nil, fmt.Errorf("ByKey: error querying (%s)", e)
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, fmt.Errorf("ByKey: error scanning (%s)", e)
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching Version
func (a *DBObjectMeta) ByVersion(ctx context.Context, p uint64) ([]*savepb.ObjectMeta, error) {
	rows, e := a.DB.QueryContext(ctx, "DBObjectMeta_ByVersion", "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry from objectmeta where version = $1", p)
	if e != nil {
		return nil, fmt.Errorf("ByVersion: error querying (%s)", e)
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, fmt.Errorf("ByVersion: error scanning (%s)", e)
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching Service
func (a *DBObjectMeta) ByService(ctx context.Context, p string) ([]*savepb.ObjectMeta, error) {
	rows, e := a.DB.QueryContext(ctx, "DBObjectMeta_ByService", "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry from objectmeta where service = $1", p)
	if e != nil {
		return nil, fmt.Errorf("ByService: error querying (%s)", e)
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, fmt.Errorf("ByService: error scanning (%s)", e)
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching Creator
func (a *DBObjectMeta) ByCreator(ctx context.Context, p string) ([]*savepb.ObjectMeta, error) {
	rows, e := a.DB.QueryContext(ctx, "DBObjectMeta_ByCreator", "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry from objectmeta where creator = $1", p)
	if e != nil {
		return nil, fmt.Errorf("ByCreator: error querying (%s)", e)
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, fmt.Errorf("ByCreator: error scanning (%s)", e)
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching LastRetrieved
func (a *DBObjectMeta) ByLastRetrieved(ctx context.Context, p uint32) ([]*savepb.ObjectMeta, error) {
	rows, e := a.DB.QueryContext(ctx, "DBObjectMeta_ByLastRetrieved", "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry from objectmeta where lastretrieved = $1", p)
	if e != nil {
		return nil, fmt.Errorf("ByLastRetrieved: error querying (%s)", e)
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, fmt.Errorf("ByLastRetrieved: error scanning (%s)", e)
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching Created
func (a *DBObjectMeta) ByCreated(ctx context.Context, p uint32) ([]*savepb.ObjectMeta, error) {
	rows, e := a.DB.QueryContext(ctx, "DBObjectMeta_ByCreated", "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry from objectmeta where created = $1", p)
	if e != nil {
		return nil, fmt.Errorf("ByCreated: error querying (%s)", e)
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, fmt.Errorf("ByCreated: error scanning (%s)", e)
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching LastUpdated
func (a *DBObjectMeta) ByLastUpdated(ctx context.Context, p uint32) ([]*savepb.ObjectMeta, error) {
	rows, e := a.DB.QueryContext(ctx, "DBObjectMeta_ByLastUpdated", "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry from objectmeta where lastupdated = $1", p)
	if e != nil {
		return nil, fmt.Errorf("ByLastUpdated: error querying (%s)", e)
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, fmt.Errorf("ByLastUpdated: error scanning (%s)", e)
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching StoreKey
func (a *DBObjectMeta) ByStoreKey(ctx context.Context, p string) ([]*savepb.ObjectMeta, error) {
	rows, e := a.DB.QueryContext(ctx, "DBObjectMeta_ByStoreKey", "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry from objectmeta where storekey = $1", p)
	if e != nil {
		return nil, fmt.Errorf("ByStoreKey: error querying (%s)", e)
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, fmt.Errorf("ByStoreKey: error scanning (%s)", e)
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching StoreID
func (a *DBObjectMeta) ByStoreID(ctx context.Context, p uint32) ([]*savepb.ObjectMeta, error) {
	rows, e := a.DB.QueryContext(ctx, "DBObjectMeta_ByStoreID", "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry from objectmeta where storeid = $1", p)
	if e != nil {
		return nil, fmt.Errorf("ByStoreID: error querying (%s)", e)
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, fmt.Errorf("ByStoreID: error scanning (%s)", e)
	}
	return l, nil
}

// get all "DBObjectMeta" rows with matching Expiry
func (a *DBObjectMeta) ByExpiry(ctx context.Context, p uint32) ([]*savepb.ObjectMeta, error) {
	rows, e := a.DB.QueryContext(ctx, "DBObjectMeta_ByExpiry", "select id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry from objectmeta where expiry = $1", p)
	if e != nil {
		return nil, fmt.Errorf("ByExpiry: error querying (%s)", e)
	}
	defer rows.Close()
	l, e := a.FromRows(ctx, rows)
	if e != nil {
		return nil, fmt.Errorf("ByExpiry: error scanning (%s)", e)
	}
	return l, nil
}

/**********************************************************************
* Helper to convert from an SQL Row to struct
**********************************************************************/
func (a *DBObjectMeta) Tablename() string {
	return "objectmeta"
}

func (a *DBObjectMeta) SelectCols() string {
	return "id,key, version, service, creator, lastretrieved, created, lastupdated, storekey, storeid, expiry"
}

func (a *DBObjectMeta) FromRows(ctx context.Context, rows *gosql.Rows) ([]*savepb.ObjectMeta, error) {
	var res []*savepb.ObjectMeta
	for rows.Next() {
		foo := savepb.ObjectMeta{}
		err := rows.Scan(&foo.ID, &foo.Key, &foo.Version, &foo.Service, &foo.Creator, &foo.LastRetrieved, &foo.Created, &foo.LastUpdated, &foo.StoreKey, &foo.StoreID, &foo.Expiry)
		if err != nil {
			return nil, err
		}
		res = append(res, &foo)
	}
	return res, nil
}
