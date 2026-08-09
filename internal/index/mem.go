package index

import (
	"sort"
	"sync"

	"github.com/navingamage/stratum/internal/model"
)

// allPostingsKey is the synthetic label pair under which every series ref is
// recorded. Queries built only from negations - `{job!="batch"}` - have no
// positive list to start from and would otherwise need a full scan of the
// series map; this gives them a real postings list to subtract from.
var allPostingsKey = struct{ Name, Value string }{"", ""}

// AllPostingsKey returns the reserved label pair used for the "every series"
// postings list. The block writer needs it to persist the same entry.
func AllPostingsKey() (name, value string) {
	return allPostingsKey.Name, allPostingsKey.Value
}

// MemPostings is the in-memory inverted index used by the head block.
//
// It is a two-level map from label name to label value to sorted series refs.
// Sorted order is maintained on insert rather than on read: appends arrive
// with monotonically increasing refs almost always, so the common insert is a
// bare append, and keeping the lists sorted means a query never has to sort
// anything.
type MemPostings struct {
	mtx sync.RWMutex
	m   map[string]map[string][]model.SeriesRef
}

// NewMemPostings returns an empty index.
func NewMemPostings() *MemPostings {
	return &MemPostings{m: make(map[string]map[string][]model.SeriesRef, 512)}
}

// Add indexes a series under every one of its labels.
func (p *MemPostings) Add(id model.SeriesRef, ls model.Labels) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, l := range ls {
		p.addFor(id, l.Name, l.Value)
	}
	p.addFor(id, allPostingsKey.Name, allPostingsKey.Value)
}

func (p *MemPostings) addFor(id model.SeriesRef, name, value string) {
	vals, ok := p.m[name]
	if !ok {
		vals = make(map[string][]model.SeriesRef, 4)
		p.m[name] = vals
	}
	list := vals[value]

	// Fast path: refs are handed out in increasing order, so an append keeps
	// the list sorted without any comparison work.
	//
	// This is safe against a concurrent iterator even though the iterator
	// holds the same backing array. Append only ever writes at or past the
	// old length, and an iterator captured its slice header with the old
	// length, so the two touch disjoint memory. If append reallocates, the
	// iterator simply keeps reading the old array.
	if len(list) == 0 || list[len(list)-1] < id {
		vals[value] = append(list, id)
		return
	}

	i := sort.Search(len(list), func(i int) bool { return list[i] >= id })
	if i < len(list) && list[i] == id {
		return // already present
	}

	// Out-of-order insert, reachable when refs are assigned by concurrent
	// appenders and land in a shared label pair out of sequence.
	//
	// Unlike the append path this has to build a fresh backing array. Shifting
	// in place would rewrite positions a live iterator has not yet read, so it
	// would see a ref twice or skip one - and worse, it is a genuine data race
	// rather than merely a stale read.
	repl := make([]model.SeriesRef, 0, len(list)+1)
	repl = append(repl, list[:i]...)
	repl = append(repl, id)
	repl = append(repl, list[i:]...)
	vals[value] = repl
}

// Get returns the postings for a single label pair.
//
// The returned iterator may be used after this call returns and while writers
// are active. It observes a snapshot of the list as it stood at this moment:
// refs added later may or may not appear, but the refs it does yield are
// always correct, sorted and free of duplicates. Add and Delete uphold that
// by never rewriting an element a live iterator could still read.
func (p *MemPostings) Get(name, value string) Postings {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	vals, ok := p.m[name]
	if !ok {
		return EmptyPostings()
	}
	return NewListPostings(vals[value])
}

// All returns the postings for every series.
func (p *MemPostings) All() Postings {
	return p.Get(allPostingsKey.Name, allPostingsKey.Value)
}

// LabelNames returns every indexed label name, excluding the reserved
// all-postings key.
func (p *MemPostings) LabelNames() []string {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	out := make([]string, 0, len(p.m))
	for n := range p.m {
		if n == allPostingsKey.Name {
			continue
		}
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// LabelValues returns every value seen for a label name, sorted.
func (p *MemPostings) LabelValues(name string) []string {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	vals, ok := p.m[name]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(vals))
	for v := range vals {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// Postings returns the union of the postings for one label name across
// several values.
func (p *MemPostings) Postings(name string, values ...string) Postings {
	switch len(values) {
	case 0:
		return EmptyPostings()
	case 1:
		return p.Get(name, values[0])
	}

	p.mtx.RLock()
	vals, ok := p.m[name]
	if !ok {
		p.mtx.RUnlock()
		return EmptyPostings()
	}
	its := make([]Postings, 0, len(values))
	for _, v := range values {
		if refs := vals[v]; len(refs) > 0 {
			its = append(its, NewListPostings(refs))
		}
	}
	p.mtx.RUnlock()

	return Merge(its...)
}

// Delete removes a set of series from the index.
//
// Compaction calls this after a block has absorbed the head, so it runs while
// queries may be in flight. It rebuilds each affected list into fresh backing
// storage rather than compacting in place, so an iterator already holding the
// old slice keeps seeing a consistent snapshot instead of shifting refs
// underneath itself.
func (p *MemPostings) Delete(deleted map[model.SeriesRef]struct{}) {
	if len(deleted) == 0 {
		return
	}
	p.mtx.Lock()
	defer p.mtx.Unlock()

	for name, vals := range p.m {
		for value, list := range vals {
			found := false
			for _, id := range list {
				if _, ok := deleted[id]; ok {
					found = true
					break
				}
			}
			if !found {
				continue
			}

			repl := make([]model.SeriesRef, 0, len(list))
			for _, id := range list {
				if _, ok := deleted[id]; !ok {
					repl = append(repl, id)
				}
			}
			if len(repl) == 0 {
				delete(vals, value)
			} else {
				vals[value] = repl
			}
		}
		if len(vals) == 0 {
			delete(p.m, name)
		}
	}
}

// Stats reports index size, for the /metrics endpoint and for deciding when
// the head has grown enough to be worth compacting.
type Stats struct {
	LabelNames   int `json:"labelNames"`
	LabelPairs   int `json:"labelPairs"`
	PostingsRefs int `json:"postingsRefs"`
}

// Stats computes the current index statistics.
func (p *MemPostings) Stats() Stats {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	var s Stats
	for name, vals := range p.m {
		if name != allPostingsKey.Name {
			s.LabelNames++
		}
		for _, list := range vals {
			s.LabelPairs++
			s.PostingsRefs += len(list)
		}
	}
	return s
}
