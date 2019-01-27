package zset

import (
	"bytes"
	"errors"

	"github.com/boltdb/bolt"

	"github.com/keystone-coin/kvstruct/utils"
)

type ZSet struct {
	dbtx *bolt.Tx
}

func NewZSet(dbtx *bolt.Tx) *ZSet {
	z := new(ZSet)
	z.dbtx = dbtx
	return z
}

func (z *ZSet) Zset(name string, key []byte, value uint64) error {
	score := utils.I2b(value)
	kname := z.kname(name)
	sname := z.sname(name)

	kbucket := z.dbtx.Bucket(kname)
	if kbucket == nil {
		var err error
		kbucket, err = z.dbtx.CreateBucket(kname)
		if err != nil {
			return err
		}
	}

	sbucket := z.dbtx.Bucket(sname)
	if sbucket == nil {
		var err error
		sbucket, err = z.dbtx.CreateBucket(sname)
		if err != nil {
			return err
		}
	}

	oldscore := sbucket.Get(key)
	if bytes.Equal(oldscore, score) == true {
		return nil
	}

	newkey := utils.BConnect(score, key)
	err := kbucket.Put(newkey, []byte{})
	if err != nil {
		return err
	}
	err = sbucket.Put(key, score)
	if err != nil {
		return err
	}

	if oldscore != nil {
		oldkey := utils.BConnect(oldscore, key)
		err := kbucket.Delete(oldkey)
		if err != nil {
			return err
		}
	}

	return nil
}

func (z *ZSet) Zincr(name string, key []byte, step int64) (uint64, error) {
	kname := z.kname(name)
	sname := z.sname(name)

	kbucket := z.dbtx.Bucket(kname)
	if kbucket == nil {
		var err error
		kbucket, err = z.dbtx.CreateBucket(kname)
		if err != nil {
			return 0, err
		}
	}

	sbucket := z.dbtx.Bucket(sname)
	if sbucket == nil {
		var err error
		sbucket, err = z.dbtx.CreateBucket(sname)
		if err != nil {
			return 0, err
		}
	}

	var score uint64
	oldscore := sbucket.Get(key)
	if oldscore != nil {
		score = utils.B2i(oldscore)
	}

	if step > 0 {
		if (SCORE_MAX_SIZE - uint64(step)) < score {
			return 0, errors.New("overflow number")
		}
		score = score + uint64(step)
	} else {
		if uint64(-step) > score {
			return 0, errors.New("overflow number")
		}
		score = score - uint64(-step)
	}

	newscore := utils.I2b(score)
	newkey := utils.BConnect(newscore, key)
	err := kbucket.Put(newkey, []byte{})
	if err != nil {
		return 0, err
	}

	err = sbucket.Put(key, newscore)
	if err != nil {
		return 0, err
	}

	if oldscore != nil {
		oldkey := utils.BConnect(oldscore, key)
		err = kbucket.Delete(oldkey)
		if err != nil {
			return 0, err
		}
	}
	return score, nil
}

func (z *ZSet) Zdel(name string, key []byte) error {
	kname := z.kname(name)
	sname := z.sname(name)
	kbucket := z.dbtx.Bucket(kname)
	if kbucket == nil {
		return nil
	}
	sbucket := z.dbtx.Bucket(sname)
	if sbucket == nil {
		return nil
	}

	oldscore := sbucket.Get(key)
	if oldscore == nil {
		return nil
	}

	oldkey := utils.BConnect(oldscore, key)
	err := kbucket.Delete(oldkey)
	if err != nil {
		return err
	}
	err = sbucket.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

func (z *ZSet) ZdelBucket(name string) error {
	kname := z.kname(name)
	sname := z.sname(name)

	err := z.dbtx.DeleteBucket(kname)
	if err != nil {
		return err
	}

	err = z.dbtx.DeleteBucket(sname)
	if err != nil {
		return err
	}
	return nil
}

func (z *ZSet) Zget(name string, key []byte) (uint64, error) {
	sname := z.sname(name)
	sbucket := z.dbtx.Bucket(sname)
	if sbucket == nil {
		return 0, errors.New("bucket_not_found")
	}
	oldscore := sbucket.Get(key)
	if oldscore == nil {
		return 0, errors.New("key_not_found")
	}
	return utils.B2i(oldscore), nil
}

func (z *ZSet) Zsequence(name string) uint64 {
	sname := z.sname(name)
	sbucket := z.dbtx.Bucket(sname)
	if sbucket == nil {
		return 0
	}
	return sbucket.Sequence()
}

func (z *ZSet) ZsetSequence(name string, value uint64) error {
	sname := z.sname(name)
	sbucket := z.dbtx.Bucket(sname)
	if sbucket == nil {
		var err error
		sbucket, err = z.dbtx.CreateBucket(sname)
		if err != nil {
			return err
		}
	}
	err := sbucket.SetSequence(value)
	if err != nil {
		return err
	}
	return nil
}

func (z *ZSet) ZnextSequence(name string) (uint64, error) {
	sname := z.sname(name)
	sbucket := z.dbtx.Bucket(sname)
	if sbucket == nil {
		var err error
		sbucket, err = z.dbtx.CreateBucket(sname)
		if err != nil {
			return 0, err
		}
	}

	sequence, err := sbucket.NextSequence()
	if err != nil {
		return 0, err
	}
	return sequence, nil
}

func (z *ZSet) Zscan(name string, keystart, scorestart []byte, limit int) ([][][]byte, error) {
	kname := z.kname(name)
	kbucket := z.dbtx.Bucket(kname)
	if kbucket == nil {
		return nil, errors.New("bucket_not_found")
	}

	minscore := utils.I2b(SCORE_MIN_SIZE)
	cursor := kbucket.Cursor()
	items := make([][][]byte, 0, limit)
	count := int(0)

	for k, _ := cursor.Seek(minscore); k != nil; k, _ = cursor.Next() {
		if bytes.Compare(k, minscore) == 1 {
			item := make([][]byte, 0, 2)
			item = append(item, k[8:], k[:8])
			items = append(items, item)
			count = count + 1
			if count == limit {
				break
			}
		}
	}
	return items, nil
}

func (z *ZSet) Zrscan(name string, keystart, scorestart []byte, limit int) ([][][]byte, error) {
	kname := z.kname(name)
	kbucket := z.dbtx.Bucket(kname)
	if kbucket == nil {
		return nil, errors.New("bucket_not_found")
	}

	maxscore := utils.I2b(SCORE_MAX_SIZE)
	if len(scorestart) > 0 {
		maxscore = make([]byte, len(scorestart))
		copy(maxscore, scorestart)
	}

	cursor := kbucket.Cursor()
	var k0, v0 []byte
	if len(scorestart) > 0 {
		k0, v0 = cursor.Seek(maxscore)
	} else {
		k0, v0 = cursor.Last()
	}

	startkey := []byte{255}
	if len(keystart) > 0 {
		startkey = make([]byte, len(keystart))
		copy(startkey, keystart)
	}

	maxkey := utils.BConnect(maxscore, startkey)
	items := make([][][]byte, 0, limit)
	count := int(0)

	for k, _ := k0, v0; k != nil; k, _ = cursor.Prev() {
		if bytes.Compare(k, maxkey) == -1 {
			item := make([][]byte, 0, 2)
			item = append(item, k[8:], k[:8])
			items = append(items, item)
			count = count + 1
			if count == limit {
				break
			}
		}
	}

	return items, nil
}

func (z *ZSet) kname(name string) []byte {
	return utils.BConnect([]byte{ZSET_KEY_PREFIX}, []byte(name))
}

func (z *ZSet) sname(name string) []byte {
	return utils.BConnect([]byte{ZSET_SCORE_PREFIX}, []byte(name))
}
