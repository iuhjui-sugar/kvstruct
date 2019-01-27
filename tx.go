package kvstruct

import (
	"github.com/boltdb/bolt"

	"github.com/keystone-coin/kvstruct/hset"
	"github.com/keystone-coin/kvstruct/zset"
)

type Tx struct {
	dbtx *bolt.Tx
	hset *hset.HSet
	zset *zset.ZSet
}

func newTx(dbtx *bolt.Tx) *Tx {
	tx := new(Tx)
	tx.dbtx = dbtx
	return tx
}

func (tx *Tx) HSet() *hset.HSet {
	if tx.hset != nil {
		return tx.hset
	}
	tx.hset = hset.NewHSet(tx.dbtx)
	return tx.hset
}

func (tx *Tx) ZSet() *zset.ZSet {
	if tx.zset != nil {
		return tx.zset
	}
	tx.zset = zset.NewZSet(tx.dbtx)
	return tx.zset
}
