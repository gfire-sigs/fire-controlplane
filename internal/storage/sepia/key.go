package sepia

import (
	"bytes"
	"encoding/binary"
)

// ValueType represents the type of value (Value or Deletion).
type ValueType uint8

const (
	TypeDeletion ValueType = 0
	TypeValue    ValueType = 1
)

// InternalKey is the key used internally by the DB.
// Format: UserKey + Trailer (8 bytes: SequenceNumber << 8 | ValueType)
type InternalKey []byte

// NewInternalKey creates a new InternalKey.
func NewInternalKey(userKey []byte, seq uint64, t ValueType) InternalKey {
	k := make([]byte, len(userKey)+8)
	copy(k, userKey)
	trailer := (seq << 8) | uint64(t)
	binary.BigEndian.PutUint64(k[len(userKey):], trailer)
	return k
}

// UserKey returns the user key part.
func (k InternalKey) UserKey() []byte {
	if len(k) < 8 {
		return k
	}
	return k[:len(k)-8]
}

// Trailer returns the trailer (Seq + Type).
func (k InternalKey) Trailer() uint64 {
	if len(k) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(k[len(k)-8:])
}

// SeqNum returns the sequence number.
func (k InternalKey) SeqNum() uint64 {
	return k.Trailer() >> 8
}

// Type returns the value type.
func (k InternalKey) Type() ValueType {
	return ValueType(k.Trailer() & 0xff)
}

// Comparator defines a total ordering over keys.
type Comparator interface {
	// Compare returns -1 if a < b, 0 if a == b, +1 if a > b.
	Compare(a, b []byte) int
	// Name returns the name of the comparator.
	Name() string
}

// DefaultComparator is the default byte-wise comparator.
type DefaultComparator struct{}

func (c DefaultComparator) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (c DefaultComparator) Name() string {
	return "sepia.DefaultComparator"
}

// InternalKeyComparator compares InternalKeys.
// Order: UserKey ascending, then SeqNum descending (newer first).
type InternalKeyComparator struct {
	UserComparator Comparator
}

func (c InternalKeyComparator) Compare(a, b []byte) int {
	ak := InternalKey(a)
	bk := InternalKey(b)
	r := c.UserComparator.Compare(ak.UserKey(), bk.UserKey())
	if r != 0 {
		return r
	}
	// User keys are equal, compare trailers.
	// Newer (higher seq) comes first, so descending order.
	at := ak.Trailer()
	bt := bk.Trailer()
	if at > bt {
		return -1
	} else if at < bt {
		return 1
	}
	return 0
}

func (c InternalKeyComparator) Name() string {
	return "sepia.InternalKeyComparator"
}

// ExtractTS extracts the timestamp (sequence number) from an InternalKey.
func ExtractTS(key []byte) uint64 {
	return InternalKey(key).SeqNum()
}
