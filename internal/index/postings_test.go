package index

import (
	"errors"
	"math/rand"
	"reflect"
	"testing"

	"github.com/navingamage/stratum/internal/model"
)

func refs(vs ...uint64) []model.SeriesRef {
	out := make([]model.SeriesRef, len(vs))
	for i, v := range vs {
		out[i] = model.SeriesRef(v)
	}
	return out
}

func expand(t *testing.T, p Postings) []model.SeriesRef {
	t.Helper()
	got, err := ExpandPostings(p)
	if err != nil {
		t.Fatalf("ExpandPostings: %v", err)
	}
	return got
}

func TestListPostings(t *testing.T) {
	p := NewListPostings(refs(1, 3, 5, 7))
	if got, want := expand(t, p), refs(1, 3, 5, 7); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	if got := expand(t, NewListPostings(nil)); got != nil {
		t.Errorf("empty list yielded %v", got)
	}
}

func TestListPostingsSeek(t *testing.T) {
	cases := []struct {
		name   string
		seek   model.SeriesRef
		wantOK bool
		wantAt model.SeriesRef
	}{
		{"before start", 0, true, 1},
		{"exact", 5, true, 5},
		{"between", 4, true, 5},
		{"last", 9, true, 9},
		{"past end", 10, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := NewListPostings(refs(1, 3, 5, 7, 9))
			if got := p.Seek(tc.seek); got != tc.wantOK {
				t.Fatalf("Seek(%d) = %v, want %v", tc.seek, got, tc.wantOK)
			}
			if tc.wantOK && p.At() != tc.wantAt {
				t.Errorf("At() = %d, want %d", p.At(), tc.wantAt)
			}
		})
	}
}

func TestListPostingsSeekDoesNotRewind(t *testing.T) {
	p := NewListPostings(refs(1, 3, 5, 7, 9))
	if !p.Seek(5) {
		t.Fatal("Seek(5) = false")
	}
	if !p.Seek(1) {
		t.Fatal("backwards Seek(1) = false")
	}
	if p.At() != 5 {
		t.Errorf("backwards Seek moved to %d, want to stay at 5", p.At())
	}
}

func TestListPostingsSeekThenNext(t *testing.T) {
	p := NewListPostings(refs(1, 3, 5, 7, 9))
	p.Seek(5)
	if !p.Next() {
		t.Fatal("Next after Seek = false")
	}
	if p.At() != 7 {
		t.Errorf("At() = %d, want 7", p.At())
	}
}

func TestIntersect(t *testing.T) {
	cases := []struct {
		name string
		in   [][]model.SeriesRef
		want []model.SeriesRef
	}{
		{"disjoint", [][]model.SeriesRef{refs(1, 3), refs(2, 4)}, nil},
		{"identical", [][]model.SeriesRef{refs(1, 2, 3), refs(1, 2, 3)}, refs(1, 2, 3)},
		{"partial", [][]model.SeriesRef{refs(1, 2, 3, 4), refs(2, 4, 6)}, refs(2, 4)},
		{"three way", [][]model.SeriesRef{refs(1, 2, 3, 4, 5), refs(2, 3, 5), refs(3, 5, 9)}, refs(3, 5)},
		{"one empty", [][]model.SeriesRef{refs(1, 2), {}}, nil},
		{"single input", [][]model.SeriesRef{refs(1, 2)}, refs(1, 2)},
		{"very uneven", [][]model.SeriesRef{refs(500), makeRange(0, 1000)}, refs(500)},
		{"includes ref zero", [][]model.SeriesRef{refs(0, 1, 2), refs(0, 2)}, refs(0, 2)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			its := make([]Postings, len(tc.in))
			for i, l := range tc.in {
				its[i] = NewListPostings(l)
			}
			if got := expand(t, Intersect(its...)); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}

	if got := expand(t, Intersect()); got != nil {
		t.Errorf("Intersect() with no inputs = %v, want nil", got)
	}
}

func TestIntersectSeek(t *testing.T) {
	p := Intersect(
		NewListPostings(makeRange(0, 100)),
		NewListPostings(refs(10, 20, 30, 40)),
	)
	if !p.Seek(25) {
		t.Fatal("Seek(25) = false")
	}
	if p.At() != 30 {
		t.Errorf("At() = %d, want 30", p.At())
	}
	if !p.Next() || p.At() != 40 {
		t.Errorf("after Next, At() = %d, want 40", p.At())
	}
	if p.Seek(41) {
		t.Error("Seek past the end = true")
	}
}

func TestMerge(t *testing.T) {
	cases := []struct {
		name string
		in   [][]model.SeriesRef
		want []model.SeriesRef
	}{
		{"disjoint", [][]model.SeriesRef{refs(1, 3), refs(2, 4)}, refs(1, 2, 3, 4)},
		{"overlapping dedupes", [][]model.SeriesRef{refs(1, 2, 3), refs(2, 3, 4)}, refs(1, 2, 3, 4)},
		{"identical dedupes", [][]model.SeriesRef{refs(1, 2), refs(1, 2), refs(1, 2)}, refs(1, 2)},
		{"one empty", [][]model.SeriesRef{refs(1, 2), {}}, refs(1, 2)},
		{"all empty", [][]model.SeriesRef{{}, {}}, nil},
		{"single input", [][]model.SeriesRef{refs(3, 4)}, refs(3, 4)},
		{"includes ref zero", [][]model.SeriesRef{refs(0, 2), refs(0, 1)}, refs(0, 1, 2)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			its := make([]Postings, len(tc.in))
			for i, l := range tc.in {
				its[i] = NewListPostings(l)
			}
			if got := expand(t, Merge(its...)); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}

	if got := expand(t, Merge()); got != nil {
		t.Errorf("Merge() with no inputs = %v, want nil", got)
	}
}

func TestMergeSeek(t *testing.T) {
	p := Merge(
		NewListPostings(refs(1, 5, 9)),
		NewListPostings(refs(2, 6, 10)),
	)
	if !p.Seek(6) {
		t.Fatal("Seek(6) = false")
	}
	if p.At() != 6 {
		t.Errorf("At() = %d, want 6", p.At())
	}
	// Seeking backwards must not rewind.
	if !p.Seek(1) || p.At() != 6 {
		t.Errorf("backwards Seek moved to %d, want to stay at 6", p.At())
	}
	if got := expand(t, p); !reflect.DeepEqual(got, refs(9, 10)) {
		t.Errorf("remainder = %v, want [9 10]", got)
	}
}

func TestWithout(t *testing.T) {
	cases := []struct {
		name         string
		full, remove []model.SeriesRef
		want         []model.SeriesRef
	}{
		{"removes middle", refs(1, 2, 3, 4), refs(2, 3), refs(1, 4)},
		{"removes nothing", refs(1, 2, 3), refs(9), refs(1, 2, 3)},
		{"removes everything", refs(1, 2, 3), refs(1, 2, 3), nil},
		{"empty full", nil, refs(1), nil},
		{"empty remove", refs(1, 2), nil, refs(1, 2)},
		{"removes first", refs(1, 2, 3), refs(1), refs(2, 3)},
		{"removes last", refs(1, 2, 3), refs(3), refs(1, 2)},
		{"remove extends past full", refs(1, 2), refs(2, 3, 4), refs(1)},
		{"removes ref zero", refs(0, 1, 2), refs(0), refs(1, 2)},
		// makeRange is inclusive at both ends, so only 100 survives.
		{"long excluded run", makeRange(0, 100), makeRange(0, 99), refs(100)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := expand(t, Without(NewListPostings(tc.full), NewListPostings(tc.remove)))
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWithoutSeek(t *testing.T) {
	p := Without(NewListPostings(makeRange(0, 20)), NewListPostings(refs(5, 6, 7, 8)))
	if !p.Seek(5) {
		t.Fatal("Seek(5) = false")
	}
	// 5 through 8 are excluded, so the first survivor at or after 5 is 9.
	if p.At() != 9 {
		t.Errorf("At() = %d, want 9", p.At())
	}
	if !p.Next() || p.At() != 10 {
		t.Errorf("after Next, At() = %d, want 10", p.At())
	}
}

func TestErrPostingsPropagates(t *testing.T) {
	sentinel := errors.New("boom")
	p := ErrPostings(sentinel)

	if _, err := ExpandPostings(Intersect(p, NewListPostings(refs(1)))); !errors.Is(err, sentinel) {
		t.Errorf("Intersect swallowed the error: %v", err)
	}
	if _, err := ExpandPostings(Merge(p, NewListPostings(refs(1)))); !errors.Is(err, sentinel) {
		t.Errorf("Merge swallowed the error: %v", err)
	}
	if _, err := ExpandPostings(Without(NewListPostings(refs(1)), p)); !errors.Is(err, sentinel) {
		t.Errorf("Without swallowed the error: %v", err)
	}
}

func TestIsEmpty(t *testing.T) {
	if !IsEmpty(EmptyPostings()) {
		t.Error("EmptyPostings is not reported as empty")
	}
	// An error is not emptiness: reporting it as empty would silently turn a
	// read failure into a query that returns no results.
	if IsEmpty(ErrPostings(errors.New("x"))) {
		t.Error("ErrPostings reported as empty")
	}
	if IsEmpty(NewListPostings(refs(1))) {
		t.Error("non-empty list reported as empty")
	}
}

func TestEstimateLen(t *testing.T) {
	if got := EstimateLen(NewListPostings(refs(1, 2, 3))); got != 3 {
		t.Errorf("EstimateLen(list) = %d, want 3", got)
	}
	// An intersection's size is not knowable without running it.
	p := Intersect(NewListPostings(refs(1, 2)), NewListPostings(refs(2, 3)))
	if got := EstimateLen(p); got != -1 {
		t.Errorf("EstimateLen(intersection) = %d, want -1", got)
	}
}

// TestIntersectOrdersBySize checks the planner optimisation actually takes
// effect: the shortest input must drive the iteration, or a selective query
// against a large label pays for the large list.
func TestIntersectOrdersBySize(t *testing.T) {
	small := &countingPostings{Postings: NewListPostings(refs(500))}
	large := &countingPostings{Postings: NewListPostings(makeRange(0, 10000))}

	// Supplied largest-first, so a naive implementation would drive on it.
	if _, err := ExpandPostings(Intersect(large, small)); err != nil {
		t.Fatal(err)
	}
	if large.nexts > 10 {
		t.Errorf("large list was stepped %d times; it should be seeked, not walked", large.nexts)
	}
}

type countingPostings struct {
	Postings
	nexts int
}

func (c *countingPostings) Next() bool {
	c.nexts++
	return c.Postings.Next()
}

func (c *countingPostings) Len() int { return EstimateLen(c.Postings) }

func makeRange(from, to uint64) []model.SeriesRef {
	out := make([]model.SeriesRef, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, model.SeriesRef(i))
	}
	return out
}

// TestPostingsOperatorsAgainstOracle is the test that matters. The iterators
// are fiddly - leapfrog seeks, heap merges, exclusion lists that can run past
// the end - and the failure mode is silently wrong query results rather than
// a crash. So: generate random inputs, compute the answer with obvious set
// operations, and require the iterators to agree.
func TestPostingsOperatorsAgainstOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(20260809))

	randomList := func(maxVal, n int) []model.SeriesRef {
		set := make(map[model.SeriesRef]struct{}, n)
		for i := 0; i < n; i++ {
			set[model.SeriesRef(rng.Intn(maxVal))] = struct{}{}
		}
		out := make([]model.SeriesRef, 0, len(set))
		for v := range set {
			out = append(out, v)
		}
		sortRefs(out)
		return out
	}

	for iter := 0; iter < 500; iter++ {
		maxVal := rng.Intn(200) + 1
		a := randomList(maxVal, rng.Intn(60))
		b := randomList(maxVal, rng.Intn(60))
		c := randomList(maxVal, rng.Intn(60))

		t.Run("", func(t *testing.T) {
			// Intersection.
			want := oracleIntersect(oracleIntersect(a, b), c)
			got := expand(t, Intersect(
				NewListPostings(a), NewListPostings(b), NewListPostings(c)))
			if !equalRefs(got, want) {
				t.Fatalf("Intersect\n a=%v\n b=%v\n c=%v\n got  %v\n want %v", a, b, c, got, want)
			}

			// Union.
			want = oracleUnion(oracleUnion(a, b), c)
			got = expand(t, Merge(
				NewListPostings(a), NewListPostings(b), NewListPostings(c)))
			if !equalRefs(got, want) {
				t.Fatalf("Merge\n a=%v\n b=%v\n c=%v\n got  %v\n want %v", a, b, c, got, want)
			}

			// Difference.
			want = oracleWithout(a, b)
			got = expand(t, Without(NewListPostings(a), NewListPostings(b)))
			if !equalRefs(got, want) {
				t.Fatalf("Without\n a=%v\n b=%v\n got  %v\n want %v", a, b, got, want)
			}

			// A composed expression, which is what a real query produces.
			want = oracleWithout(oracleIntersect(a, b), c)
			got = expand(t, Without(
				Intersect(NewListPostings(a), NewListPostings(b)),
				NewListPostings(c)))
			if !equalRefs(got, want) {
				t.Fatalf("Without(Intersect)\n a=%v\n b=%v\n c=%v\n got  %v\n want %v", a, b, c, got, want)
			}
		})
	}
}

// TestPostingsSeekAgainstOracle exercises the Seek paths specifically, since
// full expansion via Next never reaches most of that code.
func TestPostingsSeekAgainstOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(4242))

	for iter := 0; iter < 300; iter++ {
		a := makeRandomSorted(rng, 300, 40)
		b := makeRandomSorted(rng, 300, 40)

		build := func() (Postings, []model.SeriesRef) {
			switch iter % 3 {
			case 0:
				return Intersect(NewListPostings(a), NewListPostings(b)), oracleIntersect(a, b)
			case 1:
				return Merge(NewListPostings(a), NewListPostings(b)), oracleUnion(a, b)
			default:
				return Without(NewListPostings(a), NewListPostings(b)), oracleWithout(a, b)
			}
		}

		p, want := build()
		target := model.SeriesRef(rng.Intn(320))

		// The oracle answer: the first element >= target.
		wantIdx := -1
		for i, v := range want {
			if v >= target {
				wantIdx = i
				break
			}
		}

		ok := p.Seek(target)
		if err := p.Err(); err != nil {
			t.Fatalf("Seek reported %v", err)
		}
		if (wantIdx >= 0) != ok {
			t.Fatalf("iter %d: Seek(%d) = %v, want %v (a=%v b=%v want=%v)",
				iter, target, ok, wantIdx >= 0, a, b, want)
		}
		if !ok {
			continue
		}
		if p.At() != want[wantIdx] {
			t.Fatalf("iter %d: Seek(%d).At() = %d, want %d", iter, target, p.At(), want[wantIdx])
		}
		// Everything after the seek point must still come out in order.
		rest := expand(t, p)
		if !equalRefs(rest, want[wantIdx+1:]) {
			t.Fatalf("iter %d: after Seek(%d) remainder = %v, want %v",
				iter, target, rest, want[wantIdx+1:])
		}
	}
}

func makeRandomSorted(rng *rand.Rand, maxVal, n int) []model.SeriesRef {
	set := make(map[model.SeriesRef]struct{}, n)
	for i := 0; i < n; i++ {
		set[model.SeriesRef(rng.Intn(maxVal))] = struct{}{}
	}
	out := make([]model.SeriesRef, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sortRefs(out)
	return out
}

func sortRefs(s []model.SeriesRef) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func oracleIntersect(a, b []model.SeriesRef) []model.SeriesRef {
	set := make(map[model.SeriesRef]struct{}, len(b))
	for _, v := range b {
		set[v] = struct{}{}
	}
	var out []model.SeriesRef
	for _, v := range a {
		if _, ok := set[v]; ok {
			out = append(out, v)
		}
	}
	return out
}

func oracleUnion(a, b []model.SeriesRef) []model.SeriesRef {
	set := make(map[model.SeriesRef]struct{}, len(a)+len(b))
	for _, v := range a {
		set[v] = struct{}{}
	}
	for _, v := range b {
		set[v] = struct{}{}
	}
	out := make([]model.SeriesRef, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sortRefs(out)
	return out
}

func oracleWithout(a, b []model.SeriesRef) []model.SeriesRef {
	set := make(map[model.SeriesRef]struct{}, len(b))
	for _, v := range b {
		set[v] = struct{}{}
	}
	var out []model.SeriesRef
	for _, v := range a {
		if _, ok := set[v]; !ok {
			out = append(out, v)
		}
	}
	return out
}

func equalRefs(a, b []model.SeriesRef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func BenchmarkIntersectSelective(b *testing.B) {
	// The shape that matters: a tiny list against a huge one.
	small := refs(500_000)
	large := makeRange(0, 1_000_000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := Intersect(NewListPostings(large), NewListPostings(small))
		for p.Next() {
		}
	}
}

func BenchmarkMergeMany(b *testing.B) {
	lists := make([][]model.SeriesRef, 20)
	for i := range lists {
		l := make([]model.SeriesRef, 0, 1000)
		for j := 0; j < 1000; j++ {
			l = append(l, model.SeriesRef(j*20+i))
		}
		lists[i] = l
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		its := make([]Postings, len(lists))
		for j, l := range lists {
			its[j] = NewListPostings(l)
		}
		p := Merge(its...)
		for p.Next() {
		}
	}
}
