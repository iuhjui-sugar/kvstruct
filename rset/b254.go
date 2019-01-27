package rset

import (
	"fmt"
	"math/big"
)

type B254 struct {
}

func NewB254() *B254 {
	b254 := new(B254)
	return b254
}

func (b *B254) Encode(src []byte) []byte {
	if src == nil || len(src) == 0 {
		return []byte{}
	}
	// b254 zeros prefix handle
	prefix := make([]byte, 0)
	for i := 0; i < len(src); i++ {
		if src[i] != 0x00 {
			break
		}
		prefix = append(prefix, 0x01)
	}
	src = src[len(prefix):]
	// encode value
	number := big.NewInt(0).SetBytes(src)
	radix := big.NewInt(254)
	zero := big.NewInt(0)
	dst := make([]byte, 0)
	for number.Cmp(zero) > 0 {
		mod := big.NewInt(0)
		mod = mod.Mod(number, radix)
		number = number.Div(number, radix)
		dst = append(dst, byte(mod.Int64()+2))
	}
	dst = b.reverse(dst)
	// connect prefix and dst
	return b.connect(prefix, dst)
}

func (b254 *B254) Decode(src []byte) ([]byte, error) {
	if src == nil || len(src) == 0 {
		return []byte{}, nil
	}
	// leading zero bytes
	prefix := make([]byte, 0)
	for i := 0; i < len(src); i++ {
		if src[i] != 0x01 {
			break
		}
		prefix = append(prefix, 0x00)
	}
	src = src[len(prefix):]
	// decode values
	number := new(big.Int)
	radix := big.NewInt(254)
	for i := 0; i < len(src); i++ {
		if src[i] == 0x00 || src[i] == 0x01 {
			return nil, fmt.Errorf("illegal B254 data at input byte can't think of 0 or 1")
		}
		b := src[i] - 2
		number.Mul(number, radix)
		number.Add(number, big.NewInt(int64(b)))
	}
	// connect prefix and number
	dst := b254.connect(prefix, number.Bytes())
	return dst, nil
}

func (b254 *B254) connect(a []byte, b []byte) []byte {
	dst := make([]byte, 0, len(a)+len(b))
	for i := 0; i < len(a); i++ {
		dst = append(dst, a[i])
	}
	for i := 0; i < len(b); i++ {
		dst = append(dst, b[i])
	}
	return dst
}

func (b254 *B254) reverse(src []byte) []byte {
	dst := make([]byte, 0, len(src))
	for i := len(src) - 1; i >= 0; i-- {
		dst = append(dst, src[i])
	}
	return dst
}
