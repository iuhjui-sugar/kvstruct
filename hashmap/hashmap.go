package hashmap

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/boltdb/bolt"
)

type HashMap struct {
	dbtx *bolt.Tx
}

func NewHashMap(dbtx *bolt.Tx) *HashMap {
	hm := new(HashMap)
	hm.dbtx = dbtx
	return hm
}

func (hm *HashMap) Hset(name string, key []byte, value []byte) error {
	bname := hm.connect([]byte{0x30}, []byte(name))
	bucket := hm.dbtx.Bucket(bname)
	if bucket == nil {
		var err error
		bucket, err = hm.dbtx.CreateBucket(bname)
		if err != nil {
			return err
		}
	}
	err := bucket.Put(key, value)
	if err != nil {
		return err
	}
	return nil
}

func (hm *HashMap) Hmset(name string, kvs ...[]byte) error {
	if len(kvs) == 0 || len(kvs)%2 != 0 {
		return errors.New("kvs len must is an even number")
	}
	bname := hm.connect([]byte{0x30}, []byte(name))
	bucket := hm.dbtx.Bucket(bname)
	if bname == nil {
		var err error
		bucket, err = hm.dbtx.CreateBucket(bname)
		if err != nil {
			return err
		}
	}
	for i := 0; i < (len(kvs) - 1); i = i + 2 {
		err := bucket.Put(kvs[i], kvs[i+1])
		if err != nil {
			return err
		}
	}
	return nil
}

func (hm *HashMap) Hincr(name string, key []byte, step int64) (uint64, error) {
	bname := hm.connect([]byte{0x30}, []byte(name))
	bucket := hm.dbtx.Bucket(bname)
	if bucket == nil {
		var err error
		bucket, err = hm.dbtx.CreateBucket(bname)
		if err != nil {
			return 0, err
		}
	}

	var oldnum uint64
	raw := bucket.Get(key)
	if raw != nil {
		oldnum = hm.b2i(raw)
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

	err := bucket.Put(key, hm.i2b(oldnum))
	if err != nil {
		return 0, err
	}
	return oldnum, nil
}

func (hm *HashMap) Hdel(name string, key []byte) error {
	bucket := hm.dbtx.Bucket(hm.bname(name))
	if bucket != nil {
		return bucket.Delete(key)
	}
	return nil
}

func (hm *HashMap) Hmdel(name string, keys [][]byte) error {
	bname := hm.bname(name)
	bucket := hm.dbtx.Bucket(bname)
	if bucket != nil {
		for _, key := range keys {
			err := bucket.Delete(key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (hm *HashMap) HdelBucket(name string) error {
	bname := hm.bname(name)
	err := hm.dbtx.DeleteBucket(bname)
	if err != nil {
		return err
	}
	return nil
}

func (hm *HashMap) Hget(name string, key []byte) *Reply {
	bname := hm.bname(name)
	bucket := hm.dbtx.Bucket(bname)
	if bucket == nil {
		return NewReply().SetErr(ErrBucketNotFound)
	}
	value := bucket.Get(key)
	if value == nil {
		return NewReply().SetErr(ErrKeyNotFound)
	}
	return NewReply().SetMsg("ok").Push(value)
}

func (hm *HashMap) Hsequence(name string) uint64 {
	bname := hm.bname(name)
	bucket := hm.dbtx.Bucket(bname)
	if bucket == nil {
		return 0
	}
	return bucket.Sequence()
}

func (hm *HashMap) HsetSequence(name string, value uint64) error {
	bname := hm.bname(name)
	bucket := hm.dbtx.Bucket(bname)
	if bucket == nil {
		var err error
		bucket, err = hm.dbtx.CreateBucket(bname)
		if err != nil {
			return err
		}
	}
	err := bucket.SetSequence(value)
	if err != nil {
		return err
	}
	return nil
}

func (hm *HashMap) HnextSequence(name string) (uint64, error) {
	bname := hm.bname(name)
	bucket := hm.dbtx.Bucket(bname)
	if bucket == nil {
		var err error
		bucket, err = hm.dbtx.CreateBucket(bname)
		if err != nil {
			return 0, err
		}
	}
	sequence, err := bucket.NextSequence()
	if err != nil {
		return 0, err
	}
	return sequence, nil
}

func (hm *HashMap) Hmget(name string, keys [][]byte) *Reply {
	bname := hm.bname(name)
	bucket := hm.dbtx.Bucket(bname)
	if bucket == nil {
		return NewReply().SetErr(ErrBucketNotFound)
	}
	reply := NewReply().SetMsg("ok")
	for _, key := range keys {
		value := bucket.Get(key)
		if value != nil {
			reply.Push(value)
		}
	}
	return reply

}

func (hm *HashMap) Hscan(name string, keystart []byte, limit int) *Reply {
	bname := hm.bname(name)
	bucket := hm.dbtx.Bucket(bname)
	if bucket == nil {
		return NewReply().SetErr(ErrBucketNotFound)
	}

	reply := NewReply().SetMsg("ok")
	cursor := bucket.Cursor()
	n := int(0)
	for k, v := cursor.Seek(keystart); k != nil; k, v = cursor.Next() {
		if bytes.Compare(k, keystart) == 1 {
			reply.Push(k)
			reply.Push(v)
			n = n + 1
			if n == limit {
				break
			}
		}
	}
	return reply
}

func (hm *HashMap) Hrscan(name string, keystart []byte, limit int) *Reply {
	bname := hm.bname(name)
	bucket := hm.dbtx.Bucket(bname)
	if bucket != nil {
		return NewReply().SetErr(ErrBucketNotFound)
	}

	cursor := bucket.Cursor()

	var startkey = []byte{255}
	var k0, v0 []byte

	if len(keystart) > 0 {
		startkey = make([]byte, len(keystart))
		copy(startkey, keystart)
		k0, v0 = cursor.Seek(startkey)
	} else {
		k0, v0 = cursor.Last()
	}

	reply := NewReply().SetMsg("ok")
	n := int(0)

	for k, v := k0, v0; k != nil; k, v = cursor.Prev() {
		if bytes.Compare(k, startkey) == -1 {
			reply.Push(k)
			reply.Push(v)
			n = n + 1
			if n == limit {
				break
			}
		}
	}
	return reply

}

func (hm *HashMap) bname(name string) []byte {
	return hm.connect([]byte{0x30}, []byte(name))
}

func (hm *HashMap) connect(slices ...[]byte) []byte {
	total := int(0)
	for _, s := range slices {
		total = total + len(s)
	}
	dst := make([]byte, 0, total)
	for _, s := range slices {
		for _, b := range s {
			dst = append(dst, b)
		}
	}
	return dst
}

func (hm *HashMap) b2i(raw []byte) uint64 {
	return binary.BigEndian.Uint64(raw)
}

func (hm *HashMap) i2b(number uint64) []byte {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, number)
	return raw

}
