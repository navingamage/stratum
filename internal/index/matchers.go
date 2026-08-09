package index

import (
	"fmt"
	"sort"

	"github.com/navingamage/stratum/internal/model"
)

// Reader is the read side of an index, implemented by both the head's
// MemPostings and a persisted block's index. Resolving matchers against
// either goes through the same code, so the head and a block on disk cannot
// disagree about what a query selects.
type Reader interface {
	// Postings returns the union of postings for one label name across the
	// given values.
	Postings(name string, values ...string) Postings

	// LabelValues returns every value seen for a label name, sorted.
	LabelValues(name string) []string

	// LabelNames returns every label name, sorted.
	LabelNames() []string

	// All returns the postings for every series in the index.
	All() Postings
}

// PostingsForMatchers resolves a matcher list to the series it selects.
//
// The shape of the algorithm is dictated by one awkward fact: a matcher that
// accepts the empty string also accepts every series that lacks the label
// entirely. `{env!="prod"}` must select series with no env label at all, and
// there is no postings list for "series that do not have this label". Such
// matchers are therefore inverted - we compute the series to *exclude* and
// subtract them - while matchers that reject the empty string are resolved
// positively.
func PostingsForMatchers(r Reader, ms ...*model.Matcher) (Postings, error) {
	if len(ms) == 0 {
		return EmptyPostings(), nil
	}

	var (
		selects  []Postings // intersected
		excludes []Postings // subtracted
	)

	for _, m := range ms {
		switch {
		// A matcher that accepts everything constrains nothing. Skipping it
		// avoids materialising a full postings list for `{job=~".*"}`.
		case matchesEverything(r, m):
			continue

		case m.Matches(""):
			// Accepts the empty value, so it also accepts series missing the
			// label. Invert: exclude the series whose value it rejects.
			p, err := inversePostingsForMatcher(r, m)
			if err != nil {
				return nil, err
			}
			excludes = append(excludes, p)

		default:
			p, err := postingsForMatcher(r, m)
			if err != nil {
				return nil, err
			}
			if IsEmpty(p) {
				// One empty conjunct makes the whole result empty.
				return EmptyPostings(), nil
			}
			selects = append(selects, p)
		}
	}

	if len(selects) == 0 {
		if len(excludes) == 0 {
			// Every matcher was a tautology.
			return r.All(), nil
		}
		// Only exclusions: start from everything.
		selects = append(selects, r.All())
	}

	it := Intersect(selects...)
	for _, e := range excludes {
		it = Without(it, e)
	}
	return it, nil
}

// matchesEverything reports whether a matcher constrains nothing at all, so
// the planner can drop it.
func matchesEverything(r Reader, m *model.Matcher) bool {
	if m.Type != model.MatchRegexp {
		return false
	}
	// Must accept the empty value, so that series lacking the label qualify.
	if !m.Matches("") {
		return false
	}
	// A finite set is by definition not everything.
	if m.SetMatches() != nil {
		return false
	}
	for _, v := range r.LabelValues(m.Name) {
		if !m.Matches(v) {
			return false
		}
	}
	return true
}

// postingsForMatcher returns the series a matcher selects.
func postingsForMatcher(r Reader, m *model.Matcher) (Postings, error) {
	switch m.Type {
	case model.MatchEqual:
		return r.Postings(m.Name, m.Value), nil

	case model.MatchNotEqual:
		// Reaches here only when m.Value == "", i.e. `label!=""`, meaning the
		// label must be present with any non-empty value.
		vals := r.LabelValues(m.Name)
		out := make([]string, 0, len(vals))
		for _, v := range vals {
			if v != "" {
				out = append(out, v)
			}
		}
		return r.Postings(m.Name, out...), nil

	case model.MatchRegexp, model.MatchNotRegexp:
		// The fast path that makes selective regex queries viable: when the
		// pattern accepts a finite literal set, look those values up directly
		// instead of reading every distinct value of the label.
		if set := m.SetMatches(); set != nil && m.Type == model.MatchRegexp {
			return r.Postings(m.Name, set...), nil
		}
		vals := r.LabelValues(m.Name)
		out := make([]string, 0, len(vals))
		for _, v := range vals {
			if m.Matches(v) {
				out = append(out, v)
			}
		}
		return r.Postings(m.Name, out...), nil
	}

	return nil, fmt.Errorf("index: unsupported matcher type %s", m.Type)
}

// inversePostingsForMatcher returns the series a matcher *rejects*, for the
// matchers that have to be applied by subtraction.
func inversePostingsForMatcher(r Reader, m *model.Matcher) (Postings, error) {
	vals := r.LabelValues(m.Name)
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		if !m.Matches(v) {
			out = append(out, v)
		}
	}
	return r.Postings(m.Name, out...), nil
}

// LabelValuesFor returns the values of `name` restricted to series matching
// ms. It backs the label-values API, which autocompletion hits constantly, so
// it stops as soon as it has seen every value rather than walking the whole
// postings list.
func LabelValuesFor(r Reader, lookup func(model.SeriesRef) (model.Labels, bool), name string, ms ...*model.Matcher) ([]string, error) {
	if len(ms) == 0 {
		return r.LabelValues(name), nil
	}

	p, err := PostingsForMatchers(r, ms...)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{})
	for p.Next() {
		ls, ok := lookup(p.At())
		if !ok {
			continue
		}
		if v := ls.Get(name); v != "" {
			seen[v] = struct{}{}
		}
	}
	if err := p.Err(); err != nil {
		return nil, err
	}

	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out, nil
}
