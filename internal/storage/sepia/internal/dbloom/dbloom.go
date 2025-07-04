package dbloom

import (
	"encoding/binary"
	"errors"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/splitmix64"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/wyhash"
)

type Bloom struct {
	m    uint64
	k    uint64
	buf  []byte
	bits []byte
}

func NewBloom(m uint64, k uint64) *Bloom {
	bf := &Bloom{
		m: m,
		k: k,
	}
	bf.buf = make([]byte, ((m+7)/8)+16)
	binary.LittleEndian.PutUint64(bf.buf, m)
	binary.LittleEndian.PutUint64(bf.buf[8:], k)
	bf.bits = bf.buf[16:]
	return bf
}

func (bf *Bloom) Bytes() []byte {
	return bf.buf
}

var ErrInvalidBloom = errors.New("invalid bloom filter")

func FromBytes(buf []byte) (*Bloom, error) {
	if len(buf) < 16 {
		return nil, ErrInvalidBloom
	}
	m := binary.LittleEndian.Uint64(buf)
	k := binary.LittleEndian.Uint64(buf[8:])
	if len(buf) < int(16+(m+7)/8) {
		return nil, ErrInvalidBloom
	}
	bf := &Bloom{
		m:    m,
		k:    k,
		buf:  buf,
		bits: buf[16:],
	}
	return bf, nil
}

func hash(s []byte, i uint64) uint64 {
	seed := splitmix64.Splitmix64(&i)
	return wyhash.Hash(s, seed)
}

func hashstr(s string, i uint64) uint64 {
	seed := splitmix64.Splitmix64(&i)
	return wyhash.HashString(s, seed)
}

func (bf *Bloom) Set(s []byte) {
	for i := uint64(0); i < bf.k; i++ {
		h := hash(s, i) % bf.m
		bf.bits[h/8] |= 1 << (h % 8)
	}
}

func (bf *Bloom) SetString(s string) {
	for i := uint64(0); i < bf.k; i++ {
		h := hashstr(s, i) % bf.m
		bf.bits[h/8] |= 1 << (h % 8)
	}
}

func (bf *Bloom) Get(s []byte) bool {
	for i := uint64(0); i < bf.k; i++ {
		h := hash(s, i) % bf.m
		if bf.bits[h/8]&(1<<(h%8)) == 0 {
			return false
		}
	}
	return true
}

func (bf *Bloom) GetString(s string) bool {
	for i := uint64(0); i < bf.k; i++ {
		h := hashstr(s, i) % bf.m
		if bf.bits[h/8]&(1<<(h%8)) == 0 {
			return false
		}
	}
	return true
}

func (bf *Bloom) Reset() {
	for i := range bf.bits {
		bf.bits[i] = 0
	}
}

func (bf *Bloom) MarshalBinary() (data []byte, err error) {
	return bf.Bytes(), nil
}

func (bf *Bloom) UnmarshalBinary(data []byte) error {
	v, err := FromBytes(data)
	if err != nil {
		return err
	}
	*bf = *v
	return nil
}
