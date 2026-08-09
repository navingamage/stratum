package wal

import (
	"errors"
	"fmt"
	"math"

	"github.com/navingamage/stratum/internal/encoding"
	"github.com/navingamage/stratum/internal/model"
)

// RecordType tags the payload of a log record. Values are persisted and must
// never be renumbered.
type RecordType uint8

// Record types.
const (
	// RecordInvalid is what an empty or unrecognised record decodes to.
	RecordInvalid RecordType = 0

	// RecordSeries maps series refs to their label sets. One is written the
	// first time a series is seen, so replay can rebuild the index before it
	// starts applying samples.
	RecordSeries RecordType = 1

	// RecordSamples carries observations, addressed by ref.
	RecordSamples RecordType = 2

	// RecordTombstones marks time ranges of a series as deleted.
	RecordTombstones RecordType = 3
)

func (t RecordType) String() string {
	switch t {
	case RecordSeries:
		return "series"
	case RecordSamples:
		return "samples"
	case RecordTombstones:
		return "tombstones"
	}
	return "invalid"
}

// ErrMalformedRecord reports a record whose payload does not decode. This is
// distinct from ErrCorrupt: the framing checksum passed, so the bytes are the
// ones that were written, and the fault is a version mismatch or a writer bug
// rather than storage damage.
var ErrMalformedRecord = errors.New("wal: malformed record payload")

// RefSeries associates a series ref with its labels.
type RefSeries struct {
	Ref    model.SeriesRef
	Labels model.Labels
}

// RefSample is one observation of a series, addressed by ref.
type RefSample struct {
	Ref model.SeriesRef
	T   int64
	V   float64
}

// Tombstone marks a closed time range of a series as deleted.
type Tombstone struct {
	Ref      model.SeriesRef
	Min, Max int64
}

// Encoder builds record payloads. The zero value is ready to use, and every
// method appends to a caller-supplied buffer so ingest can reuse one.
type Encoder struct{}

// Series encodes a batch of series definitions.
func (e *Encoder) Series(series []RefSeries, buf []byte) []byte {
	enc := encoding.Encbuf{B: buf}
	enc.PutByte(byte(RecordSeries))

	for _, s := range series {
		enc.PutBE64(uint64(s.Ref))
		enc.PutUvarint(len(s.Labels))
		for _, l := range s.Labels {
			enc.PutUvarintStr(l.Name)
			enc.PutUvarintStr(l.Value)
		}
	}
	return enc.Get()
}

// Samples encodes a batch of observations.
//
// The batch is delta-encoded against its first entry. Within one append batch
// refs are near-consecutive and timestamps near-identical, so both deltas
// collapse to one or two bytes; only the float value is stored at full width,
// because a WAL record is written once and read once and is not worth the
// cost of a Gorilla-style encoder that would have to carry state across
// records.
func (e *Encoder) Samples(samples []RefSample, buf []byte) []byte {
	enc := encoding.Encbuf{B: buf}
	enc.PutByte(byte(RecordSamples))
	if len(samples) == 0 {
		return enc.Get()
	}

	first := samples[0]
	enc.PutBE64(uint64(first.Ref))
	enc.PutBE64(uint64(first.T))

	for _, s := range samples {
		// Signed deltas: refs within a batch are ascending but not
		// guaranteed to be, and timestamps can go either way when several
		// appenders share a batch.
		enc.PutVarint64(int64(s.Ref) - int64(first.Ref))
		enc.PutVarint64(s.T - first.T)
		enc.PutBE64(math.Float64bits(s.V))
	}
	return enc.Get()
}

// Tombstones encodes a batch of deletion markers.
func (e *Encoder) Tombstones(ts []Tombstone, buf []byte) []byte {
	enc := encoding.Encbuf{B: buf}
	enc.PutByte(byte(RecordTombstones))
	for _, t := range ts {
		enc.PutBE64(uint64(t.Ref))
		enc.PutVarint64(t.Min)
		enc.PutVarint64(t.Max)
	}
	return enc.Get()
}

// Decoder reads record payloads.
type Decoder struct{}

// Type reports the kind of a record.
func (d *Decoder) Type(rec []byte) RecordType {
	if len(rec) == 0 {
		return RecordInvalid
	}
	switch t := RecordType(rec[0]); t {
	case RecordSeries, RecordSamples, RecordTombstones:
		return t
	default:
		return RecordInvalid
	}
}

// Series decodes a series record, appending to into.
//
// Label names and values are copied out of rec rather than aliasing it. The
// zero-copy alternative is a trap: the replayer reuses one buffer for every
// record, and Labels.Copy clones the slice but not the string data it points
// at, so the obvious defensive call at the call site does not actually
// defend. Replay runs once per restart, so the copies are not worth the
// class of bug they avoid.
func (d *Decoder) Series(rec []byte, into []RefSeries) ([]RefSeries, error) {
	dec := encoding.NewDecbuf(rec)
	if RecordType(dec.Byte()) != RecordSeries {
		return nil, fmt.Errorf("%w: not a series record", ErrMalformedRecord)
	}

	for dec.Len() > 0 && dec.Err() == nil {
		ref := dec.Be64()
		n := dec.Uvarint()
		if dec.Err() != nil {
			break
		}
		// Guard before allocating: a corrupt count would otherwise ask for an
		// arbitrarily large slice. Each label costs at least two bytes.
		if n < 0 || n > dec.Len()/2 {
			return nil, fmt.Errorf("%w: series claims %d labels with %d bytes left",
				ErrMalformedRecord, n, dec.Len())
		}

		lbls := make(model.Labels, 0, n)
		for i := 0; i < n; i++ {
			lbls = append(lbls, model.Label{
				Name:  string(dec.UvarintBytes()),
				Value: string(dec.UvarintBytes()),
			})
		}
		if dec.Err() != nil {
			break
		}
		into = append(into, RefSeries{Ref: model.SeriesRef(ref), Labels: lbls})
	}

	if err := dec.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedRecord, err)
	}
	return into, nil
}

// Samples decodes a samples record, appending to into.
func (d *Decoder) Samples(rec []byte, into []RefSample) ([]RefSample, error) {
	dec := encoding.NewDecbuf(rec)
	if RecordType(dec.Byte()) != RecordSamples {
		return nil, fmt.Errorf("%w: not a samples record", ErrMalformedRecord)
	}
	if dec.Len() == 0 {
		return into, nil
	}

	baseRef := dec.Be64()
	baseT := dec.Be64()
	if err := dec.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading batch header: %v", ErrMalformedRecord, err)
	}

	for dec.Len() > 0 && dec.Err() == nil {
		dref := dec.Varint64()
		dt := dec.Varint64()
		val := dec.Be64()
		if dec.Err() != nil {
			break
		}
		into = append(into, RefSample{
			Ref: model.SeriesRef(int64(baseRef) + dref),
			T:   int64(baseT) + dt,
			V:   math.Float64frombits(val),
		})
	}

	if err := dec.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedRecord, err)
	}
	return into, nil
}

// Tombstones decodes a tombstone record, appending to into.
func (d *Decoder) Tombstones(rec []byte, into []Tombstone) ([]Tombstone, error) {
	dec := encoding.NewDecbuf(rec)
	if RecordType(dec.Byte()) != RecordTombstones {
		return nil, fmt.Errorf("%w: not a tombstone record", ErrMalformedRecord)
	}

	for dec.Len() > 0 && dec.Err() == nil {
		ref := dec.Be64()
		lo := dec.Varint64()
		hi := dec.Varint64()
		if dec.Err() != nil {
			break
		}
		into = append(into, Tombstone{Ref: model.SeriesRef(ref), Min: lo, Max: hi})
	}

	if err := dec.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedRecord, err)
	}
	return into, nil
}
