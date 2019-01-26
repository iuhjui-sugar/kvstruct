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

func (db *DB) Close() error {
	return db.db.Close()
}

var (
	zetKeyPrefix   = []byte{31}
	zetScorePrefix = []byte{29}
)

const HashMapPrefix = []byte{0x30}

type HashMap struct {
	dbtx *bolt.Tx
}

func (hm *HashMap) Hset(name string, key []byte, value []byte) error {

}

func (db *DB) Hset(name string, key, val []byte) error {
	return db.DB.Update(func(tx *bolt.Tx) error {

	})
}
