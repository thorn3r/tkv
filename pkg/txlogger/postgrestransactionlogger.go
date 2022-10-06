package txlogger

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/go-yaml/yaml"
	_ "github.com/lib/pq"
	klog "k8s.io/klog/v2"
)

type PostgresTxLogger struct {
	db     *sql.DB
	events chan<- Event
	errors <-chan error
	table  string
}

type PostgresTxLoggerConfig struct {
	Host     string `yaml:"host"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DbName   string `yaml:"dbName"`
	Table    string `yaml:"table"`
}

func ParseConfigFromFile(filename string) (PostgresTxLoggerConfig, error) {
	var config PostgresTxLoggerConfig
	bytes, err := os.ReadFile(filename)
	if err != nil {
		return config, fmt.Errorf("failed to read config file: %v", err)
	}

	err = yaml.Unmarshal(bytes, &config)
	if err != nil {
		return config, fmt.Errorf("failed to unmarshall yaml config file: %v", err)
	}
	klog.Infof("debug: config: %v", config)
	return config, nil
}

func NewPostgresTxLogger(configFile string) (TxLogger, error) {
	config, err := ParseConfigFromFile(configFile)
	if err != nil {
		return nil, err
	}
	connectionStr := fmt.Sprintf(
		"host=%s dbname=%s user=%s password=%s sslmode=disable",
		config.Host,
		config.DbName,
		config.User,
		config.Password,
	)

	db, err := sql.Open("postgres", connectionStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %v", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %v", err)
	}

	txlogger := &PostgresTxLogger{db: db, table: config.Table}

	if err = txlogger.createTable(); err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}
	return txlogger, nil
}

func (ptx *PostgresTxLogger) createTable() error {
	execStr := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		sequence BIGSERIAL PRIMARY KEY,
		eventtype SMALLINT NOT NULL,
		key TEXT NOT NULL,
		value TEXT)`, ptx.table)
	_, err := ptx.db.Exec(execStr)
	return err
}

func (ptx *PostgresTxLogger) InsertTransaction(event EventType, key, value string) error {
	query := fmt.Sprintf("INSERT INTO %s (eventtype, key, value) VALUES (%d, '%s', '%s')", ptx.table, event, key, value)
	klog.Infof("debug: inserting: %v", query)
	_, err := ptx.db.Exec(query)
	if err != nil {
		klog.Errorf("failed to write transaction to db: %v", err)
		return fmt.Errorf("failed to write transaction to db: %v", err)
	}
	return nil
}

func (ptx *PostgresTxLogger) WritePut(key, value string) {
	ptx.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (ptx *PostgresTxLogger) WriteDelete(key string) {
	ptx.events <- Event{EventType: EventDelete, Key: key}
}

func (ptx *PostgresTxLogger) Run() {
	events := make(chan Event, 16)
	ptx.events = events

	errors := make(chan error, 1)
	ptx.errors = errors

	go func() {
		for e := range events {
			if err := ptx.InsertTransaction(
				e.EventType,
				e.Key,
				e.Value,
			); err != nil {
				errors <- err
			}
		}
	}()
}

func (ptx *PostgresTxLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)
	outError := make(chan error)
	go func() {
		var e Event
		defer close(outEvent)
		defer close(outError)

		query := fmt.Sprintf("SELECT sequence, eventtype, key, value from %s order by sequence", ptx.table)
		rows, err := ptx.db.Query(query)
		if err != nil {
			klog.Errorf("failed to query db: %v", err)
			outError <- fmt.Errorf("failed to query db: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			err = rows.Scan(&e.Sequence, &e.EventType, &e.Key, &e.Value)
			if err != nil {
				outError <- fmt.Errorf("transaction parse error: %v", err)
			}
			klog.Infof("debug: row scanned: %v", e)

			outEvent <- e
		}
		err = rows.Err()
		if err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}

	}()
	return outEvent, outError
}

func (ptx *PostgresTxLogger) Err() <-chan error {
	return ptx.errors
}
