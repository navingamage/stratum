// Package model defines the core data types shared by every layer of
// stratum: label sets, matchers and samples.
package model

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/navingamage/stratum/internal/xxh"
)

// MetricName is the reserved label that carries a series' metric name. It is
// stored as an ordinary label so that the inverted index needs no special
// case for name lookups: `cpu_seconds{host="a"}` and `{__name__="cpu_seconds",
// host="a"}` resolve through exactly the same code path.
const MetricName = "__name__"

// labelSep separates fields when hashing a label set. 0xff cannot appear in
// valid UTF-8, so no combination of label names and values can be made to
// collide by embedding the separator.
const labelSep = 0xff

// Label is a single key/value pair.
type Label struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func (l Label) String() string { return l.Name + "=" + strconv.Quote(l.Value) }

// Labels is a set of labels sorted by name, with no duplicate names.
//
// The sorted invariant is load-bearing rather than cosmetic: it makes Hash
// deterministic across differently-ordered inputs, makes Compare a linear
// scan, and lets the block index store label sets as a delta against the
// previous one.
type Labels []Label

// EmptyLabels returns an empty, non-nil label set.
func EmptyLabels() Labels { return Labels{} }

// New builds a label set from unordered labels, sorting them. It panics on
// duplicate names, which is always a caller bug: silently keeping one of the
// two would produce a series that cannot be looked up by the labels that
// created it.
func New(ls ...Label) Labels {
	set := make(Labels, 0, len(ls))
	set = append(set, ls...)
	sort.Sort(set)
	for i := 1; i < len(set); i++ {
		if set[i-1].Name == set[i].Name {
			panic(fmt.Sprintf("model: duplicate label name %q", set[i].Name))
		}
	}
	return set
}

// FromStrings builds a label set from alternating name/value arguments.
func FromStrings(ss ...string) Labels {
	if len(ss)%2 != 0 {
		panic("model: FromStrings needs an even number of arguments")
	}
	ls := make(Labels, 0, len(ss)/2)
	for i := 0; i < len(ss); i += 2 {
		ls = append(ls, Label{Name: ss[i], Value: ss[i+1]})
	}
	sort.Sort(ls)
	return ls
}

// FromMap builds a label set from a map.
func FromMap(m map[string]string) Labels {
	ls := make(Labels, 0, len(m))
	for k, v := range m {
		ls = append(ls, Label{Name: k, Value: v})
	}
	sort.Sort(ls)
	return ls
}

// Map returns the label set as a map.
func (ls Labels) Map() map[string]string {
	m := make(map[string]string, len(ls))
	for _, l := range ls {
		m[l.Name] = l.Value
	}
	return m
}

// sort.Interface.
func (ls Labels) Len() int           { return len(ls) }
func (ls Labels) Less(i, j int) bool { return ls[i].Name < ls[j].Name }
func (ls Labels) Swap(i, j int)      { ls[i], ls[j] = ls[j], ls[i] }

// Get returns the value of the named label, or the empty string.
//
// The scan is linear rather than a binary search because label sets are
// small - the median is around six labels - and at that size the branch
// predictor beats the extra arithmetic.
func (ls Labels) Get(name string) string {
	for _, l := range ls {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

// Has reports whether the named label is present.
func (ls Labels) Has(name string) bool {
	for _, l := range ls {
		if l.Name == name {
			return true
		}
	}
	return false
}

// Copy returns a deep copy of the label set.
func (ls Labels) Copy() Labels {
	out := make(Labels, len(ls))
	copy(out, ls)
	return out
}

// Equal reports whether two label sets are identical.
func (ls Labels) Equal(other Labels) bool {
	if len(ls) != len(other) {
		return false
	}
	for i := range ls {
		if ls[i] != other[i] {
			return false
		}
	}
	return true
}

// IsEmpty reports whether the set has no labels.
func (ls Labels) IsEmpty() bool { return len(ls) == 0 }

// Compare orders two label sets lexicographically by (name, value) pairs.
// It returns a negative number, zero, or a positive number as a sorts before,
// equal to, or after b.
func Compare(a, b Labels) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if c := strings.Compare(a[i].Name, b[i].Name); c != 0 {
			return c
		}
		if c := strings.Compare(a[i].Value, b[i].Value); c != 0 {
			return c
		}
	}
	// A prefix sorts before the longer set that extends it.
	return len(a) - len(b)
}

// Hash returns a stable 64-bit digest of the label set.
//
// The digest is stable across processes and builds, which the head block
// relies on to map a label set to its series ref without holding every label
// set in memory twice.
func (ls Labels) Hash() uint64 {
	// A stack buffer, so the common case does not allocate at all. This runs
	// once per appended sample; an allocation here would dominate ingest.
	var scratch [1024]byte
	b := scratch[:0]

	for i, l := range ls {
		if len(b)+len(l.Name)+len(l.Value)+2 >= cap(b) {
			// Oversized label set: fall back to the streaming digest rather
			// than growing (and heap-allocating) the buffer.
			d := xxh.New()
			_, _ = d.Write(b)
			for _, l := range ls[i:] {
				_, _ = d.WriteString(l.Name)
				_, _ = d.Write([]byte{labelSep})
				_, _ = d.WriteString(l.Value)
				_, _ = d.Write([]byte{labelSep})
			}
			return d.Sum64()
		}
		b = append(b, l.Name...)
		b = append(b, labelSep)
		b = append(b, l.Value...)
		b = append(b, labelSep)
	}
	return xxh.Sum64(b)
}

// String renders the label set in the query language's own syntax, so that
// output can be pasted straight back into a query.
func (ls Labels) String() string {
	var sb strings.Builder
	name := ls.Get(MetricName)
	sb.WriteString(name)

	rest := len(ls)
	if name != "" {
		rest--
	}
	if rest == 0 {
		if name == "" {
			return "{}"
		}
		return sb.String()
	}

	sb.WriteByte('{')
	first := true
	for _, l := range ls {
		if l.Name == MetricName {
			continue
		}
		if !first {
			sb.WriteString(", ")
		}
		first = false
		sb.WriteString(l.Name)
		sb.WriteByte('=')
		sb.WriteString(strconv.Quote(l.Value))
	}
	sb.WriteByte('}')
	return sb.String()
}

// Validation errors.
var (
	ErrEmptyLabelName   = errors.New("model: empty label name")
	ErrInvalidLabelName = errors.New("model: invalid label name")
	ErrInvalidUTF8      = errors.New("model: label value is not valid UTF-8")
	ErrDuplicateLabel   = errors.New("model: duplicate label name")
	ErrUnsorted         = errors.New("model: labels are not sorted by name")
)

// IsValidLabelName reports whether name matches [a-zA-Z_][a-zA-Z0-9_]*.
func IsValidLabelName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		valid := c == '_' ||
			(c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9' && i > 0)
		if !valid {
			return false
		}
	}
	return true
}

// Validate checks the invariants callers outside this package must uphold:
// well-formed names, valid UTF-8 values, sorted order and no duplicates.
//
// Ingest runs this on every incoming series. Everything downstream - the
// index, the block writer, Hash - assumes it has passed.
func (ls Labels) Validate() error {
	for i, l := range ls {
		if l.Name == "" {
			return fmt.Errorf("%w at position %d", ErrEmptyLabelName, i)
		}
		if !IsValidLabelName(l.Name) {
			return fmt.Errorf("%w: %q", ErrInvalidLabelName, l.Name)
		}
		if !utf8.ValidString(l.Value) {
			return fmt.Errorf("%w: label %q", ErrInvalidUTF8, l.Name)
		}
		if i > 0 {
			switch {
			case ls[i-1].Name == l.Name:
				return fmt.Errorf("%w: %q", ErrDuplicateLabel, l.Name)
			case ls[i-1].Name > l.Name:
				return fmt.Errorf("%w: %q precedes %q", ErrUnsorted, ls[i-1].Name, l.Name)
			}
		}
	}
	return nil
}

// Bytes returns a compact binary form of the label set, used as a map key
// where a hash would risk collisions. The separator is the same 0xff used by
// Hash, so the encoding is unambiguous.
func (ls Labels) Bytes(buf []byte) []byte {
	b := bytes.NewBuffer(buf[:0])
	for _, l := range ls {
		b.WriteByte(labelSep)
		b.WriteString(l.Name)
		b.WriteByte(labelSep)
		b.WriteString(l.Value)
	}
	return b.Bytes()
}

// Builder incrementally derives a label set from a base set. Query operators
// that drop or rewrite labels - aggregations grouping by a subset, binary
// operations dropping the metric name - go through this rather than mutating
// a set in place, because the base set is shared with the index and must not
// be touched.
type Builder struct {
	base Labels
	del  []string
	add  []Label
}

// NewBuilder returns a builder over base.
func NewBuilder(base Labels) *Builder {
	b := &Builder{}
	b.Reset(base)
	return b
}

// Reset re-points the builder at a new base, retaining its scratch slices.
func (b *Builder) Reset(base Labels) {
	b.base = base
	b.del = b.del[:0]
	b.add = b.add[:0]
	// An empty value in the base means the label is absent, so normalise it
	// into a deletion now and keep Labels() simple.
	for _, l := range base {
		if l.Value == "" {
			b.del = append(b.del, l.Name)
		}
	}
}

// Set adds or overwrites a label. Setting an empty value deletes the label,
// matching the convention that an absent label and an empty one are the same
// thing.
func (b *Builder) Set(name, value string) *Builder {
	if value == "" {
		return b.Del(name)
	}
	for i, l := range b.add {
		if l.Name == name {
			b.add[i].Value = value
			return b
		}
	}
	b.add = append(b.add, Label{Name: name, Value: value})
	return b
}

// Del removes labels by name.
func (b *Builder) Del(names ...string) *Builder {
	for _, name := range names {
		for i, l := range b.add {
			if l.Name == name {
				b.add = append(b.add[:i], b.add[i+1:]...)
				break
			}
		}
		b.del = append(b.del, name)
	}
	return b
}

// Keep removes every label except those named.
func (b *Builder) Keep(names ...string) *Builder {
Outer:
	for _, l := range b.base {
		for _, n := range names {
			if l.Name == n {
				continue Outer
			}
		}
		b.del = append(b.del, l.Name)
	}
	return b
}

// Labels returns the resulting set. The base is never mutated.
func (b *Builder) Labels() Labels {
	if len(b.del) == 0 && len(b.add) == 0 {
		return b.base
	}

	out := make(Labels, 0, len(b.base)+len(b.add))
	for _, l := range b.base {
		if slicesContains(b.del, l.Name) || containsName(b.add, l.Name) {
			continue
		}
		out = append(out, l)
	}
	out = append(out, b.add...)
	sort.Sort(out)
	return out
}

func slicesContains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func containsName(ls []Label, name string) bool {
	for _, l := range ls {
		if l.Name == name {
			return true
		}
	}
	return false
}
