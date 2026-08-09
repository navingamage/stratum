package index

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/navingamage/stratum/internal/model"
)

func eq(name, value string) *model.Matcher {
	return model.MustNewMatcher(model.MatchEqual, name, value)
}
func ne(name, value string) *model.Matcher {
	return model.MustNewMatcher(model.MatchNotEqual, name, value)
}
func re(name, value string) *model.Matcher {
	return model.MustNewMatcher(model.MatchRegexp, name, value)
}
func nre(name, value string) *model.Matcher {
	return model.MustNewMatcher(model.MatchNotRegexp, name, value)
}

func TestPostingsForMatchers(t *testing.T) {
	c := testCorpus()

	cases := []struct {
		name string
		ms   []*model.Matcher
		want []model.SeriesRef
	}{
		{"no matchers", nil, nil},
		{"single equality", []*model.Matcher{eq(model.MetricName, "cpu")}, refs(0, 1, 2)},
		{"conjunction", []*model.Matcher{eq(model.MetricName, "cpu"), eq("env", "prod")}, refs(0, 1)},
		{"no match", []*model.Matcher{eq(model.MetricName, "nosuch")}, nil},
		{"contradiction", []*model.Matcher{eq("env", "prod"), eq("env", "staging")}, nil},

		// Negations must include series that lack the label entirely: series 4
		// has no env label at all and must survive env!="prod".
		{"negation includes missing label", []*model.Matcher{ne("env", "prod")}, refs(2, 4)},
		{"negated regexp", []*model.Matcher{nre("host", "web-.*")}, refs(2, 4)},

		// label!="" means the label must be present and non-empty.
		{"label must be present", []*model.Matcher{ne("env", "")}, refs(0, 1, 2, 3)},
		{"label must be absent", []*model.Matcher{eq("env", "")}, refs(4)},

		{"regexp set path", []*model.Matcher{re("host", "web-1|web-2")}, refs(0, 1, 3)},
		{"regexp scan path", []*model.Matcher{re("host", "web-.*")}, refs(0, 1, 3)},

		// A tautology constrains nothing and must not narrow the result.
		{"tautology alone", []*model.Matcher{re("host", ".*")}, refs(0, 1, 2, 3, 4)},
		{"tautology with a real matcher", []*model.Matcher{re("host", ".*"), eq(model.MetricName, "mem")}, refs(3, 4)},

		{"mixed", []*model.Matcher{eq(model.MetricName, "cpu"), ne("host", "db-1")}, refs(0, 1)},
		{"only negations", []*model.Matcher{ne("host", "web-1"), ne("env", "staging")}, refs(1, 4)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := PostingsForMatchers(c.idx, tc.ms...)
			if err != nil {
				t.Fatalf("PostingsForMatchers: %v", err)
			}
			got, err := ExpandPostings(p)
			if err != nil {
				t.Fatalf("ExpandPostings: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}

			// Cross-check against brute force, except for the no-matchers case
			// where "match everything" and "match nothing" legitimately differ.
			if len(tc.ms) > 0 {
				if brute := c.brute(tc.ms...); !reflect.DeepEqual(got, brute) {
					t.Errorf("index says %v, brute force says %v", got, brute)
				}
			}
		})
	}
}

// TestPostingsForMatchersAgainstOracle generates random corpora and random
// matcher combinations, and requires the index to agree with brute-force
// filtering every time.
//
// This is the test that justifies the whole matcher-resolution design. The
// interesting cases - a negation that has to be applied by subtraction, a
// regexp that enumerates into a union, a tautology that gets dropped - are
// exactly the ones where a plausible-looking implementation returns subtly
// wrong series rather than failing.
func TestPostingsForMatchersAgainstOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(20260809))

	names := []string{"job", "host", "env", "shard"}
	values := map[string][]string{
		"job":   {"api", "web", "batch", ""},
		"host":  {"web-1", "web-2", "db-1", "db-2"},
		"env":   {"prod", "staging"},
		"shard": {"0", "1", "2"},
	}

	// A pool of matchers spanning every resolution strategy.
	pool := []*model.Matcher{
		eq("job", "api"), eq("job", ""), eq("host", "web-1"), eq("env", "prod"),
		ne("job", "api"), ne("job", ""), ne("host", "db-1"), ne("env", "prod"),
		re("host", "web-1|web-2"), re("host", "web-.*"), re("host", ".*"),
		re("job", "api|batch"), re("shard", "[01]"), re("env", ""),
		nre("host", "web-.*"), nre("job", "api|web"), nre("shard", "[01]"),
		eq("nosuch", "x"), ne("nosuch", "x"), re("nosuch", ".*"),
	}

	for iter := 0; iter < 400; iter++ {
		// Build a random corpus. Labels are omitted at random so that the
		// "missing label" semantics get exercised heavily.
		var sets []model.Labels
		for i := 0; i < rng.Intn(40)+1; i++ {
			var ls []model.Label
			for _, n := range names {
				if rng.Intn(4) == 0 {
					continue // omit this label entirely
				}
				vs := values[n]
				ls = append(ls, model.Label{Name: n, Value: vs[rng.Intn(len(vs))]})
			}
			sets = append(sets, model.New(ls...))
		}
		c := newCorpus(sets...)

		// Pick a random conjunction.
		nm := rng.Intn(3) + 1
		ms := make([]*model.Matcher, 0, nm)
		for i := 0; i < nm; i++ {
			ms = append(ms, pool[rng.Intn(len(pool))])
		}

		p, err := PostingsForMatchers(c.idx, ms...)
		if err != nil {
			t.Fatalf("iter %d: PostingsForMatchers(%s): %v", iter, model.MatchersString(ms), err)
		}
		got, err := ExpandPostings(p)
		if err != nil {
			t.Fatalf("iter %d: ExpandPostings: %v", iter, err)
		}

		want := c.brute(ms...)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("iter %d: matchers %s\n got  %v\n want %v\n corpus:\n%s",
				iter, model.MatchersString(ms), got, want, dumpCorpus(c))
		}
	}
}

func dumpCorpus(c *corpus) string {
	s := ""
	for _, ref := range c.order {
		s += fmt.Sprintf("  %d: %s\n", ref, c.series[ref])
	}
	return s
}

func TestLabelValuesFor(t *testing.T) {
	c := testCorpus()

	// Unfiltered.
	got, err := LabelValuesFor(c.idx, c.lookup, "host")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"db-1", "web-1", "web-2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// Restricted to a metric.
	got, err = LabelValuesFor(c.idx, c.lookup, "host", eq(model.MetricName, "mem"))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"db-1", "web-1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// A label that some matching series lack contributes nothing.
	got, err = LabelValuesFor(c.idx, c.lookup, "env", eq(model.MetricName, "mem"))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"prod"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// No matching series.
	got, err = LabelValuesFor(c.idx, c.lookup, "host", eq(model.MetricName, "nosuch"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}

// BenchmarkPostingsForMatchersSetPath measures the optimisation that motivates
// enumerating regexp alternations: a union of a few postings lists instead of
// a scan over every distinct value of a high-cardinality label.
func BenchmarkPostingsForMatchersSetPath(b *testing.B) {
	idx := NewMemPostings()
	for i := 0; i < 100_000; i++ {
		idx.Add(model.SeriesRef(i), model.FromStrings(
			model.MetricName, "http_requests_total",
			"pod", fmt.Sprintf("pod-%d", i%10_000),
			"job", "api",
		))
	}

	b.Run("enumerable alternation", func(b *testing.B) {
		m := re("pod", "pod-1|pod-2|pod-3")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p, err := PostingsForMatchers(idx, m)
			if err != nil {
				b.Fatal(err)
			}
			for p.Next() {
			}
		}
	})

	b.Run("scan over all values", func(b *testing.B) {
		m := re("pod", "pod-[123]$")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p, err := PostingsForMatchers(idx, m)
			if err != nil {
				b.Fatal(err)
			}
			for p.Next() {
			}
		}
	})
}
