package graph

import (
	"testing"
)

func TestB254(t *testing.T) {
	src := []byte{0, 0, 0, 1, 0, 0, 0, 2, 4, 6, 255}
	raw := NewB254().Encode(src)
	dst, _ := NewB254().Decode(raw)
	t.Log(dst)
}
