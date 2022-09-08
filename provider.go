package ueprofilesqlserver

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

//

// databaseProvider is a collection of Database.
type databaseProvider struct {
	dbs map[string]sql.Database
	mu  *sync.RWMutex
}

var _ sql.DatabaseProvider = &databaseProvider{}

func NewDatabaseProvider(dbs ...sql.Database) *databaseProvider {
	dbMap := make(map[string]sql.Database, len(dbs))
	for _, db := range dbs {
		dbMap[strings.ToLower(db.Name())] = db
	}
	return &databaseProvider{
		dbs: dbMap,
		mu:  &sync.RWMutex{},
	}
}

// Database returns the Database with the given name if it exists.
func (d *databaseProvider) Database(ctx *sql.Context, name string) (sql.Database, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	db, ok := d.dbs[strings.ToLower(name)]
	if ok {
		return db, nil
	}

	return nil, sql.ErrDatabaseNotFound.New(name)
}

// HasDatabase returns the Database with the given name if it exists.
func (d *databaseProvider) HasDatabase(ctx *sql.Context, name string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	_, ok := d.dbs[strings.ToLower(name)]
	return ok
}

// AllDatabases returns the Database with the given name if it exists.
func (d *databaseProvider) AllDatabases(*sql.Context) []sql.Database {
	d.mu.RLock()
	defer d.mu.RUnlock()

	all := make([]sql.Database, 0, len(d.dbs))
	for _, db := range d.dbs {
		all = append(all, db)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name() < all[j].Name()
	})

	return all
}

func (d *databaseProvider) NewDatabase(ctx *sql.Context, db sql.Database) error {
	if d.HasDatabase(ctx, db.Name()) {
		return fmt.Errorf("database already exists. %s", db.Name())
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.dbs[db.Name()] = db

	return nil
}
