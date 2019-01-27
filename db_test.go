package kvstruct

import (
	"os"
	"testing"
)

func TestHsetBase(t *testing.T) {
	db, err := Open("./tmp.db")
	if err != nil {
		t.Error(err)
		return
	}
	err = db.Update(func(dbtx *Tx) error {
		err := dbtx.HSet().Hset("b1", []byte("iuhjui"), []byte("one"))
		if err != nil {
			return err
		}
		value, err := dbtx.HSet().Hget("b1", []byte("iuhjui"))
		if err != nil {
			return err
		}
		t.Log(string(value))
		return nil
	})
	if err != nil {
		t.Error(err)
		return
	}
	os.Remove("./tmp.db")
}
