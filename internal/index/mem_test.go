package index

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/navingamage/stratum/internal/model"
)

// corpus is a small in-memory series set plus the index built over it, so
// tests can compare index answers against brute-force filtering.
type corpus struct {
	idx    *MemPostings
	series map[model.SeriesRef]model.Labels
	order  []model.SeriesRef
}

func newCorpus(sets ...model.Labels) *corpus {
	c := &corpus{
		idx:    NewMemPostings(),
		series: make(map[model.SeriesRef]model.Labels, len(sets)),
	}
	for i, ls := range sets {
		ref := model.SeriesRef(i)
		c.idx.Add(ref, ls)
		c.series[ref] = ls
		c.order = append(c.order, ref)
	}
	return c
}

// brute returns the refs matching ms by testing every series directly. This
// is the oracle: obviously correct, hopelessly slow.
func (c *corpus) brute(ms ...*model.Matcher) []model.SeriesRef {
	var out []model.SeriesRef
	for _, ref := range c.order {
		ls := c.series[ref]
		ok := true
		for _, m := range ms {
			if !m.MatchesLabels(ls) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, ref)
		}
	}
	return out
}

func (c *corpus) lookup(ref model.SeriesRef) (model.Labels, bool) {
	ls, ok := c.series[ref]
	return ls, ok
}

func testCorpus() *corpus {
	return newCorpus(
		model.FromStrings(model.MetricName, "cpu", "host", "web-1", "env", "prod"),
		model.FromStrings(model.MetricName, "cpu", "host", "web-2", "env", "prod"),
		model.FromStrings(model.MetricName, "cpu", "host", "db-1", "env", "staging"),
		model.FromStrings(model.MetricName, "mem", "host", "web-1", "env", "prod"),
		model.FromStrings(model.MetricName, "mem", "host", "db-1"), // no env label
	)
}

func TestMemPostingsGet(t *testing.T) {
	c := testCorpus()

	got, err := ExpandPostings(c.idx.Get(model.MetricName, "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	if want := refs(0, 1, 2); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, _ := ExpandPostings(c.idx.Get("nosuch", "x")); got != nil {
		t.Errorf("unknown label returned %v", got)
	}
	if got, _ := ExpandPostings(c.idx.Get(model.MetricName, "nosuch")); got != nil {
		t.Errorf("unknown value returned %v", got)
	}
}

func TestMemPostingsAll(t *testing.T) {
	c := testCorpus()
	got, err := ExpandPostings(c.idx.All())
	if err != nil {
		t.Fatal(err)
	}
	if want := refs(0, 1, 2, 3, 4); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestMemPostingsLabelNamesAndValues(t *testing.T) {
	c := testCorpus()

	// The reserved all-postings key must never surface as a real label.
	want := []string{model.MetricName, "env", "host"}
	if got := c.idx.LabelNames(); !reflect.DeepEqual(got, want) {
		t.Errorf("LabelNames() = %v, want %v", got, want)
	}

	if got, want := c.idx.LabelValues("host"), []string{"db-1", "web-1", "web-2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("LabelValues(host) = %v, want %v", got, want)
	}
	if got := c.idx.LabelValues("nosuch"); got != nil {
		t.Errorf("LabelValues(nosuch) = %v, want nil", got)
	}
}

func TestMemPostingsUnionAcrossValues(t *testing.T) {
	c := testCorpus()
	got, err := ExpandPostings(c.idx.Postings("host", "web-1", "web-2"))
	if err != nil {
		t.Fatal(err)
	}
	if want := refs(0, 1, 3); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, _ := ExpandPostings(c.idx.Postings("host")); got != nil {
		t.Errorf("Postings with no values = %v, want nil", got)
	}
}

func TestMemPostingsStaysSortedOnOutOfOrderAdd(t *testing.T) {
	p := NewMemPostings()
	for _, id := range []model.SeriesRef{5, 3, 9, 1, 7} {
		p.Add(id, model.FromStrings("k", "v"))
	}
	got, err := ExpandPostings(p.Get("k", "v"))
	if err != nil {
		t.Fatal(err)
	}
	if want := refs(1, 3, 5, 7, 9); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestMemPostingsAddIsIdempotent(t *testing.T) {
	p := NewMemPostings()
	ls := model.FromStrings("k", "v")
	p.Add(7, ls)
	p.Add(3, ls)
	p.Add(7, ls) // duplicate, out of order

	got, _ := ExpandPostings(p.Get("k", "v"))
	if want := refs(3, 7); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestMemPostingsDelete(t *testing.T) {
	c := testCorpus()
	c.idx.Delete(map[model.SeriesRef]struct{}{0: {}, 2: {}})

	got, _ := ExpandPostings(c.idx.Get(model.MetricName, "cpu"))
	if want := refs(1); !reflect.DeepEqual(got, want) {
		t.Errorf("after Delete, cpu = %v, want %v", got, want)
	}
	got, _ = ExpandPostings(c.idx.All())
	if want := refs(1, 3, 4); !reflect.DeepEqual(got, want) {
		t.Errorf("after Delete, All = %v, want %v", got, want)
	}

	// A label pair whose entire postings list was deleted must disappear, or
	// LabelValues reports values no series has.
	if got := c.idx.LabelValues("env"); !reflect.DeepEqual(got, []string{"prod"}) {
		t.Errorf("LabelValues(env) = %v, want [prod]", got)
	}
}

func TestMemPostingsDeleteEmptyIsNoOp(t *testing.T) {
	c := testCorpus()
	before, _ := ExpandPostings(c.idx.All())
	c.idx.Delete(nil)
	after, _ := ExpandPostings(c.idx.All())
	if !reflect.DeepEqual(before, after) {
		t.Errorf("Delete(nil) changed the index: %v -> %v", before, after)
	}
}

// TestMemPostingsDeleteDoesNotDisturbLiveIterators covers the reason Delete
// rebuilds lists instead of compacting them in place: compaction runs while
// queries are in flight, and an iterator holding the old slice must keep
// seeing a consistent snapshot.
func TestMemPostingsDeleteDoesNotDisturbLiveIterators(t *testing.T) {
	p := NewMemPostings()
	ls := model.FromStrings("k", "v")
	for i := 0; i < 10; i++ {
		p.Add(model.SeriesRef(i), ls)
	}

	it := p.Get("k", "v")
	if !it.Next() || it.At() != 0 {
		t.Fatal("iterator did not start at 0")
	}

	p.Delete(map[model.SeriesRef]struct{}{3: {}, 4: {}, 5: {}})

	// The live iterator keeps the pre-delete view.
	rest, err := ExpandPostings(it)
	if err != nil {
		t.Fatal(err)
	}
	if want := refs(1, 2, 3, 4, 5, 6, 7, 8, 9); !reflect.DeepEqual(rest, want) {
		t.Errorf("live iterator saw %v, want the pre-delete snapshot %v", rest, want)
	}

	// A fresh read sees the deletion.
	fresh, _ := ExpandPostings(p.Get("k", "v"))
	if want := refs(0, 1, 2, 6, 7, 8, 9); !reflect.DeepEqual(fresh, want) {
		t.Errorf("fresh read = %v, want %v", fresh, want)
	}
}

func TestMemPostingsStats(t *testing.T) {
	c := testCorpus()
	s := c.idx.Stats()
	if s.LabelNames != 3 {
		t.Errorf("LabelNames = %d, want 3", s.LabelNames)
	}
	// 2 metric names + 3 hosts + 2 envs + 1 reserved all-postings pair.
	if s.LabelPairs != 8 {
		t.Errorf("LabelPairs = %d, want 8", s.LabelPairs)
	}
	// Series 0-3 carry three labels each, series 4 carries two, and every
	// series also appears once under the all-postings key.
	const wantRefs = 4*3 + 2 + 5
	if s.PostingsRefs != wantRefs {
		t.Errorf("PostingsRefs = %d, want %d", s.PostingsRefs, wantRefs)
	}
}

func TestMemPostingsConcurrentAccess(t *testing.T) {
	p := NewMemPostings()
	var wg sync.WaitGroup

	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				ref := model.SeriesRef(w*1000 + i)
				p.Add(ref, model.FromStrings("worker", fmt.Sprint(w), "shard", fmt.Sprint(i%7)))
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_, _ = ExpandPostings(p.All())
				_ = p.LabelNames()
				_ = p.LabelValues("shard")
				_ = p.Stats()
			}
		}()
	}
	wg.Wait()

	all, err := ExpandPostings(p.All())
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 8*200 {
		t.Errorf("All() has %d refs, want %d", len(all), 8*200)
	}
	if !sort.SliceIsSorted(all, func(i, j int) bool { return all[i] < all[j] }) {
		t.Error("All() is not sorted")
	}
}

func BenchmarkMemPostingsAdd(b *testing.B) {
	p := NewMemPostings()
	ls := model.FromStrings(
		model.MetricName, "node_cpu_seconds_total",
		"host", "web-1", "job", "node-exporter", "mode", "user",
	)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Add(model.SeriesRef(i), ls)
	}
}

func BenchmarkMemPostingsGet(b *testing.B) {
	p := NewMemPostings()
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 100_000; i++ {
		p.Add(model.SeriesRef(i), model.FromStrings(
			model.MetricName, "m", "host", fmt.Sprintf("h-%d", rng.Intn(1000))))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := p.Get("host", "h-500")
		for it.Next() {
		}
	}
}
