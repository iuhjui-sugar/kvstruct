package kvstruct

import (
	"time"

	"github.com/boltdb/bolt"
)

type DB struct {
	db *bolt.DB
}

func Open(path string) (*DB, error) {
	db := new(DB)
	err := db.makeDatabase(path)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) makeDatabase(path string) error {
	database, err := bolt.Open(path, 0600, &bolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return err
	}
	db.db = database
	return nil
}

func (db *DB) Update(fn func(*Tx) error) error {
	return db.db.Update(func(dbtx *bolt.Tx) error {
		return fn(newTx(dbtx))
	})
}

func (db *DB) Close() error {
	return db.db.Close()
}
