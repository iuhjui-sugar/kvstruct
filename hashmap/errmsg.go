package hashmap

import (
	"errors"
)

var ErrBucketNotFound = errors.New("bucket_not_found")
var ErrKeyNotFound = errors.New("key_not_found")
