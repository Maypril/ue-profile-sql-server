package ueprofilesqlserver

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/stoewer/go-strcase"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
)

type Server struct {
	engine   *sqle.Engine
	provider *databaseProvider
	server   *server.Server

	watcher          *fsnotify.Watcher
	closeWatcherChan chan struct{}

	profilesTable        *memory.Table
	profileMetadataTable *memory.Table

	db *memory.Database
}

func NewServer(dir string) (*Server, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("%s directory error. %w", dir, err)
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", dir)
	}

	// todo: setup filewatcher
	db := memory.NewDatabase("db")

	profilesTable := createProfilesTable(db)
	profileMetadataTable := createProfileMetadataTable(db)

	provider := NewDatabaseProvider(db, information_schema.NewInformationSchemaDatabase())

	engine := sqle.NewDefault(provider)

	config := server.Config{
		Protocol: "tcp",
		Address:  "localhost:3306",
	}

	sqlServer, err := server.NewDefaultServer(config, engine)
	if err != nil {
		return nil, fmt.Errorf("failed to create server. %w", err)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create watcher. %w", err)
	}

	if err := watcher.Add(dir); err != nil {
		return nil, fmt.Errorf("failed to add dir: %s to watcher. %w", dir, err)
	}

	server := &Server{
		engine:               engine,
		provider:             provider,
		server:               sqlServer,
		watcher:              watcher,
		closeWatcherChan:     make(chan struct{}, 1),
		profilesTable:        profilesTable,
		profileMetadataTable: profileMetadataTable,
		db:                   db,
	}

	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}

		if d.IsDir() {
			return nil
		}

		if err := server.AddFile(path); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *Server) Start() error {
	go s.watchForChanges()

	if err := s.server.Start(); err != nil {
		return fmt.Errorf("failed to start server. %w", err)
	}

	return nil
}

func (s *Server) watchForChanges() {
	for {
		select {
		case event, ok := <-s.watcher.Events:
			if !ok {
				continue
			}

			if event.Op == fsnotify.Write {
				s.AddFile(event.Name)
			}

		case err, ok := <-s.watcher.Errors:
			if ok {
				log.Printf("watcher error: %v", err)
			}
		case <-s.closeWatcherChan:
			return
		}
	}
}

var fileNameRe = regexp.MustCompile(`(?m)((.*)\((\d{8}_\d{6})\))\.csv`)

func (s *Server) AddFile(filename string) error {
	if filepath.Ext(filename) != ".csv" {
		return nil
	}
	fmt.Println("found csv file", filename)

	matches := fileNameRe.FindStringSubmatch(filepath.Base(filename))

	if len(matches) != 4 {
		return fmt.Errorf("filename doesn't match ue format")
	}

	profileName := strings.NewReplacer("(", "_", ")", "").Replace(matches[1])
	_ = profileName

	fmt.Println("Profile name", profileName)

	timestamp, err := time.Parse("20060102_150405", matches[3])
	if err != nil {
		return fmt.Errorf("failed to parse profile timestamp. %w", err)
	}
	fmt.Println("timestamp", timestamp)

	fileReader, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to add file. %w", err)
	}
	defer fileReader.Close()

	csvReader := csv.NewReader(fileReader)
	csvReader.LazyQuotes = true

	rows := make([][]string, 0)

	err = nil

	i := 0

	for err == nil {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if len(row) == 0 {
			// ignore empty rows

			continue
		}

		if i == 0 && row[0] != "EVENTS" {
			return fmt.Errorf("not a ue profile csv file")
		}

		rows = append(rows, row)
		i++
	}

	if err != nil {
		return err
	}

	if len(rows) == 0 {
		return fmt.Errorf("no data?")
	}

	fmt.Println("number of rows: ", len(rows))

	header := rows[0]
	rows = rows[1:]

	// check if len -1 row first field contains [HasHeaderRowAtEnd]

	removeBottomHeader := false
	metadataAtEnd := false
	lastRow := rows[len(rows)-1]
	if len(lastRow) > 1 && lastRow[0] == "[HasHeaderRowAtEnd]" && lastRow[1] == "1" {
		removeBottomHeader = true
		if len(lastRow)%2 == 0 {
			metadataAtEnd = true
		}
	}

	if removeBottomHeader {
		rows = rows[:len(rows)-2]
	}

	var metadata map[string]string

	ctx := sql.NewEmptyContext()

	s.profilesTable.Insert(ctx, sql.NewRow(profileName, timestamp))

	keyReplacer := strings.NewReplacer("[", "", "]", "", "/", "_", " ", "_")

	if metadataAtEnd {

		metadata = make(map[string]string)
		for i := 0; i < len(lastRow); i += 2 {
			key := strcase.SnakeCase(keyReplacer.Replace(lastRow[i]))
			value := lastRow[i+1]
			metadata[key] = value
		}

		s.profileMetadataTable.Insert(ctx, sql.NewRow(profileName, sql.JSONDocument{Val: metadata}))
	}

	frameTimeField := 0

	for i := 0; i < len(header); i++ {
		header[i] = strcase.SnakeCase(keyReplacer.Replace(header[i]))
		if header[i] == "frame_time" {
			frameTimeField = i
		}
	}

	converters := make(map[int]ConverterFunc)
	// first field is always a string
	converters[0] = converterFuncString

	schema := sql.Schema{
		{Name: "frame", Type: sql.Int64, Nullable: false, Source: profileName, PrimaryKey: true},
		{Name: "timestamp", Type: sql.Timestamp, Nullable: false, Source: profileName},
		{Name: "events", Type: sql.Text, Nullable: false, Source: profileName},
	}

	firstRow := rows[0]

	for i := 1; i < len(firstRow); i++ {
		// just have everything as floats for now
		schema = append(schema, &sql.Column{
			Name:     header[i],
			Type:     sql.Float32,
			Nullable: false,
			Source:   profileName,
		})

		converters[i] = converterFuncFloat
	}

	// use the first row to figure out the schema

	table := memory.NewTable(profileName, sql.NewPrimaryKeySchema(schema), s.db.GetForeignKeyCollection())

	frameTimestamp := timestamp

	// alright. time to fill the table
	for i, row := range rows {
		rowData := make([]any, 0, len(schema))
		rowData = append(rowData, int64(i))

		frameDuration, err := time.ParseDuration(fmt.Sprintf("%vms", row[frameTimeField]))
		if err != nil {
			return fmt.Errorf("failed to parse duration: %v", err)
		}
		frameTimestamp = frameTimestamp.Add(frameDuration)
		rowData = append(rowData, frameTimestamp)

		for c, column := range row {
			data, err := converters[c](column)
			if err != nil {
				return fmt.Errorf("failed to convert row: %d colum: %d. data: %s. %w", i, c, column, err)
			}
			rowData = append(rowData, data)
		}

		if err := table.Insert(ctx, sql.NewRow(rowData...)); err != nil {
			return fmt.Errorf("failed to insert row: %d. %w", i, err)
		}
	}

	s.db.AddTable(profileName, table)

	return nil
}

type Number interface {
	int64 | float64
}

type ConverterFunc func(string) (any, error)

type DataMapper struct {
	Func ConverterFunc
	Type sql.Type
}

func converterFuncString(in string) (any, error) {
	return in, nil
}

func converterFuncFloat(in string) (any, error) {
	v, err := strconv.ParseFloat(in, 32)
	if err != nil {
		return float32(0), err
	}
	return float32(v), err
}

func converterFuncInt(in string) (any, error) {
	return strconv.ParseInt(in, 10, 0)
}

func (s *Server) Close() error {
	close(s.closeWatcherChan)

	if err := s.server.Close(); err != nil {
		return fmt.Errorf("failed to close server. %w", err)
	}

	if err := s.watcher.Close(); err != nil {
		return fmt.Errorf("failed to close watcher. %w", err)
	}

	return nil
}
