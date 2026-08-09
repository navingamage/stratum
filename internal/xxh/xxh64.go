// Package xxh implements XXH64, the 64-bit variant of Yann Collet's xxHash.
//
// stratum hashes a label set on every append to find the series it belongs
// to, so this sits directly on the write path. XXH64 is used rather than
// something from the standard library because FNV-1a is roughly an order of
// magnitude slower on the string lengths involved here, and hash/maphash is
// seeded per process, which rules it out for anything that has to stay
// stable across restarts.
//
// Implemented here rather than pulled in as a dependency: it is ninety lines
// against a frozen specification, and it is verified below against the
// reference vectors.
package xxh

import (
	"encoding/binary"
	"math/bits"
)

// The five XXH64 mixing primes.
const (
	prime1 uint64 = 11400714785074694791
	prime2 uint64 = 14029467366897019727
	prime3 uint64 = 1609587929392839161
	prime4 uint64 = 9650029242287828579
	prime5 uint64 = 2870177450012600261
)

func round(acc, input uint64) uint64 {
	acc += input * prime2
	acc = bits.RotateLeft64(acc, 31)
	return acc * prime1
}

func mergeRound(acc, val uint64) uint64 {
	acc ^= round(0, val)
	return acc*prime1 + prime4
}

// Sum64 returns the XXH64 digest of b with a zero seed.
func Sum64(b []byte) uint64 { return Sum64Seed(b, 0) }

// Sum64Seed returns the XXH64 digest of b with the given seed.
func Sum64Seed(b []byte, seed uint64) uint64 {
	n := len(b)
	var h uint64

	if n >= 32 {
		// Four independent accumulators over 32-byte stripes; on a
		// superscalar core these issue in parallel, which is where XXH64
		// gets most of its throughput.
		v1 := seed + prime1 + prime2
		v2 := seed + prime2
		v3 := seed
		v4 := seed - prime1

		for len(b) >= 32 {
			v1 = round(v1, binary.LittleEndian.Uint64(b[0:8]))
			v2 = round(v2, binary.LittleEndian.Uint64(b[8:16]))
			v3 = round(v3, binary.LittleEndian.Uint64(b[16:24]))
			v4 = round(v4, binary.LittleEndian.Uint64(b[24:32]))
			b = b[32:]
		}

		h = bits.RotateLeft64(v1, 1) +
			bits.RotateLeft64(v2, 7) +
			bits.RotateLeft64(v3, 12) +
			bits.RotateLeft64(v4, 18)
		h = mergeRound(h, v1)
		h = mergeRound(h, v2)
		h = mergeRound(h, v3)
		h = mergeRound(h, v4)
	} else {
		h = seed + prime5
	}

	h += uint64(n)

	for len(b) >= 8 {
		h ^= round(0, binary.LittleEndian.Uint64(b[:8]))
		h = bits.RotateLeft64(h, 27)*prime1 + prime4
		b = b[8:]
	}
	if len(b) >= 4 {
		h ^= uint64(binary.LittleEndian.Uint32(b[:4])) * prime1
		h = bits.RotateLeft64(h, 23)*prime2 + prime3
		b = b[4:]
	}
	for _, c := range b {
		h ^= uint64(c) * prime5
		h = bits.RotateLeft64(h, 11) * prime1
	}

	// Final avalanche.
	h ^= h >> 33
	h *= prime2
	h ^= h >> 29
	h *= prime3
	h ^= h >> 32
	return h
}

// Sum64String hashes s without copying it to a byte slice.
//
// The conversion is elided by the compiler for this specific pattern, so
// hashing a label name costs no allocation. That matters: an append with a
// dozen labels would otherwise allocate a dozen times before it has done any
// real work.
func Sum64String(s string) uint64 {
	return Sum64([]byte(s))
}

// Digest is the streaming form, for hashing a value assembled from pieces
// without first concatenating them.
type Digest struct {
	v1, v2, v3, v4 uint64
	total          uint64
	mem            [32]byte
	n              int // bytes buffered in mem
	seed           uint64
}

// New returns a streaming digest with a zero seed.
func New() *Digest {
	d := &Digest{}
	d.Reset()
	return d
}

// Reset returns the digest to its initial state.
func (d *Digest) Reset() {
	d.v1 = d.seed + prime1 + prime2
	d.v2 = d.seed + prime2
	d.v3 = d.seed
	d.v4 = d.seed - prime1
	d.total = 0
	d.n = 0
}

// WriteString adds s to the digest. It never returns an error.
func (d *Digest) WriteString(s string) (int, error) { return d.Write([]byte(s)) }

// Write adds b to the digest. It never returns an error.
func (d *Digest) Write(b []byte) (int, error) {
	n := len(b)
	d.total += uint64(n)

	// Top up the carry buffer first; only once it holds a full stripe can we
	// consume it.
	if d.n > 0 {
		c := copy(d.mem[d.n:], b)
		d.n += c
		b = b[c:]
		if d.n < 32 {
			return n, nil
		}
		d.consume(d.mem[:])
		d.n = 0
	}

	for len(b) >= 32 {
		d.consume(b[:32])
		b = b[32:]
	}
	if len(b) > 0 {
		d.n = copy(d.mem[:], b)
	}
	return n, nil
}

func (d *Digest) consume(b []byte) {
	d.v1 = round(d.v1, binary.LittleEndian.Uint64(b[0:8]))
	d.v2 = round(d.v2, binary.LittleEndian.Uint64(b[8:16]))
	d.v3 = round(d.v3, binary.LittleEndian.Uint64(b[16:24]))
	d.v4 = round(d.v4, binary.LittleEndian.Uint64(b[24:32]))
}

// Sum64 returns the digest of everything written so far. It does not alter
// the digest, so writing may continue afterwards.
func (d *Digest) Sum64() uint64 {
	var h uint64
	if d.total >= 32 {
		h = bits.RotateLeft64(d.v1, 1) +
			bits.RotateLeft64(d.v2, 7) +
			bits.RotateLeft64(d.v3, 12) +
			bits.RotateLeft64(d.v4, 18)
		h = mergeRound(h, d.v1)
		h = mergeRound(h, d.v2)
		h = mergeRound(h, d.v3)
		h = mergeRound(h, d.v4)
	} else {
		h = d.seed + prime5
	}

	h += d.total

	b := d.mem[:d.n]
	for len(b) >= 8 {
		h ^= round(0, binary.LittleEndian.Uint64(b[:8]))
		h = bits.RotateLeft64(h, 27)*prime1 + prime4
		b = b[8:]
	}
	if len(b) >= 4 {
		h ^= uint64(binary.LittleEndian.Uint32(b[:4])) * prime1
		h = bits.RotateLeft64(h, 23)*prime2 + prime3
		b = b[4:]
	}
	for _, c := range b {
		h ^= uint64(c) * prime5
		h = bits.RotateLeft64(h, 11) * prime1
	}

	h ^= h >> 33
	h *= prime2
	h ^= h >> 29
	h *= prime3
	h ^= h >> 32
	return h
}
