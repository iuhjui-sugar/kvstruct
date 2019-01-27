package hset

import (
	"bytes"
	"errors"

	"github.com/boltdb/bolt"

	"github.com/keystone-coin/kvstruct/utils"
)

type HSet struct {
	dbtx *bolt.Tx
}

func NewHSet(dbtx *bolt.Tx) *HSet {
	h := new(HSet)
	h.dbtx = dbtx
	return h
}

func (h *HSet) Hset(name string, key []byte, value []byte) error {
	hname := h.hname(name)
	hbucket := h.dbtx.Bucket(hname)
	if hbucket == nil {
		var err error
		hbucket, err = h.dbtx.CreateBucket(hname)
		if err != nil {
			return err
		}
	}
	err := hbucket.Put(key, value)
	if err != nil {
		return err
	}
	return nil
}

func (h *HSet) Hincr(name string, key []byte, step int64) (uint64, error) {
	hname := h.hname(name)
	hbucket := h.dbtx.Bucket(hname)
	if hbucket == nil {
		var err error
		hbucket, err = h.dbtx.CreateBucket(hname)
		if err != nil {
			return 0, err
		}
	}

	var oldnum uint64
	raw := hbucket.Get(key)
	if raw != nil {
		oldnum = utils.B2i(raw)
	}

	if step > 0 {
		if (SCORE_MAX_SIZE - uint64(step)) < oldnum {
			return 0, errors.New("overflow number")
		}
		oldnum = oldnum + uint64(step)
	} else {
		if uint64(-step) > oldnum {
			return 0, errors.New("overflow number")
		}
		oldnum = oldnum - uint64(-step)
	}

	err := hbucket.Put(key, utils.I2b(oldnum))
	if err != nil {
		return 0, err
	}
	return oldnum, nil
}

func (h *HSet) Hdel(name string, key []byte) error {
	hname := h.hname(name)
	hbucket := h.dbtx.Bucket(hname)
	if hbucket != nil {
		return hbucket.Delete(key)
	}
	return nil
}

func (h *HSet) HdelBucket(name string) error {
	hname := h.hname(name)
	err := h.dbtx.DeleteBucket(hname)
	if err != nil {
		return err
	}
	return nil
}

func (h *HSet) Hget(name string, key []byte) ([]byte, error) {
	hname := h.hname(name)
	hbucket := h.dbtx.Bucket(hname)
	if hbucket == nil {
		return nil, errors.New("bucket_not_found")
	}
	value := hbucket.Get(key)
	if value == nil {
		return nil, errors.New("key_not_found")
	}
	return value, nil
}

func (h *HSet) Hsequence(name string) uint64 {
	hname := h.hname(name)
	hbucket := h.dbtx.Bucket(hname)
	if hbucket == nil {
		return 0
	}
	return hbucket.Sequence()
}

func (h *HSet) HsetSequence(name string, value uint64) error {
	hname := h.hname(name)
	hbucket := h.dbtx.Bucket(hname)
	if hbucket == nil {
		var err error
		hbucket, err = h.dbtx.CreateBucket(hname)
		if err != nil {
			return err
		}
	}
	err := hbucket.SetSequence(value)
	if err != nil {
		return err
	}
	return nil
}

func (h *HSet) HnextSequence(name string) (uint64, error) {
	hname := h.hname(name)
	hbucket := h.dbtx.Bucket(hname)
	if hbucket == nil {
		var err error
		hbucket, err = h.dbtx.CreateBucket(hname)
		if err != nil {
			return 0, err
		}
	}
	sequence, err := hbucket.NextSequence()
	if err != nil {
		return 0, err
	}
	return sequence, nil
}

func (h *HSet) Hscan(name string, keystart []byte, limit int) ([][][]byte, error) {
	hname := h.hname(name)
	hbucket := h.dbtx.Bucket(hname)
	if hbucket == nil {
		return nil, errors.New("bucket_not_found")
	}

	cursor := hbucket.Cursor()
	items := make([][][]byte, 0, limit)
	count := int(0)
	for k, v := cursor.Seek(keystart); k != nil; k, v = cursor.Next() {
		if bytes.Compare(k, keystart) == 1 {
			item := make([][]byte, 0, 2)
			item = append(item, k, v)
			items = append(items, item)
			count = count + 1
			if count == limit {
				break
			}
		}
	}
	return items, nil
}

func (h *HSet) Hrscan(name string, keystart []byte, limit int) ([][][]byte, error) {
	hname := h.hname(name)
	hbucket := h.dbtx.Bucket(hname)
	if hbucket != nil {
		return nil, errors.New("bucket_not_found")
	}

	cursor := hbucket.Cursor()

	var startkey = []byte{255}
	var k0, v0 []byte
	if len(keystart) > 0 {
		startkey = make([]byte, len(keystart))
		copy(startkey, keystart)
		k0, v0 = cursor.Seek(startkey)
	} else {
		k0, v0 = cursor.Last()
	}

	items := make([][][]byte, 0, limit)
	count := int(0)

	for k, v := k0, v0; k != nil; k, v = cursor.Prev() {
		if bytes.Compare(k, startkey) == -1 {
			item := make([][]byte, 0, 2)
			item = append(item, k, v)
			items = append(items, item)
			count = count + 1
			if count == limit {
				break
			}
		}
	}
	return items, nil
}

func (h *HSet) hname(name string) []byte {
	return utils.BConnect([]byte{HSET_BUCKET_PREFIX}, []byte(name))
}
