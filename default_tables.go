package ueprofilesqlserver

import (
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

func createProfilesTable(db *memory.Database) *memory.Table {

	const (
		tableName = "profiles"
	)

	table := memory.NewTable(tableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "profile", Type: sql.Text, Nullable: false, Source: tableName, PrimaryKey: true},
		{Name: "timestamp", Type: sql.Timestamp, Nullable: false, Source: tableName},
	}), db.GetForeignKeyCollection())

	db.AddTable(tableName, table)

	return table
}

func createProfileMetadataTable(db *memory.Database) *memory.Table {
	const (
		tableName = "profile_metadata"
	)

	table := memory.NewTable(tableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "profile", Type: sql.Text, Nullable: false, Source: tableName, PrimaryKey: true},
		{Name: "data", Type: sql.JSON, Nullable: false, Source: tableName},
	}), db.GetForeignKeyCollection())

	db.AddTable(tableName, table)

	return table
}
