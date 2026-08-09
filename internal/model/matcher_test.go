package model

import (
	"sort"
	"testing"
)

func TestMatcherMatches(t *testing.T) {
	cases := []struct {
		typ   MatchType
		value string
		in    string
		want  bool
	}{
		{MatchEqual, "web-1", "web-1", true},
		{MatchEqual, "web-1", "web-2", false},
		{MatchEqual, "", "", true},
		{MatchNotEqual, "web-1", "web-2", true},
		{MatchNotEqual, "web-1", "web-1", false},

		{MatchRegexp, "web-.*", "web-1", true},
		{MatchRegexp, "web-.*", "db-1", false},
		{MatchRegexp, "web|db", "web", true},
		{MatchRegexp, "web|db", "api", false},
		{MatchNotRegexp, "web-.*", "db-1", true},
		{MatchNotRegexp, "web-.*", "web-1", false},

		// Anchoring: a regex must match the whole value, not a substring.
		{MatchRegexp, "web", "webserver", false},
		{MatchRegexp, "web", "myweb", false},
		{MatchRegexp, ".*web.*", "mywebserver", true},

		// A dot must not be treated as a literal.
		{MatchRegexp, "a.c", "abc", true},
		{MatchRegexp, "a.c", "a.c", true},

		{MatchRegexp, "", "", true},
		{MatchRegexp, "", "x", false},
	}
	for _, tc := range cases {
		m := MustNewMatcher(tc.typ, "host", tc.value)
		if got := m.Matches(tc.in); got != tc.want {
			t.Errorf("%s.Matches(%q) = %v, want %v", m, tc.in, got, tc.want)
		}
	}
}

func TestMatcherRejectsBadRegexp(t *testing.T) {
	if _, err := NewMatcher(MatchRegexp, "host", "a(b"); err == nil {
		t.Error("NewMatcher with an unbalanced group succeeded, want error")
	}
}

func TestMatcherMatchesLabels(t *testing.T) {
	ls := FromStrings(MetricName, "cpu", "host", "web-1")

	if !MustNewMatcher(MatchEqual, "host", "web-1").MatchesLabels(ls) {
		t.Error("equality matcher failed on a present label")
	}

	// A missing label reads as empty, so a negation selects series that lack
	// the label entirely. This is what makes `{env!="prod"}` behave the way
	// users expect on a partially-labelled corpus.
	if !MustNewMatcher(MatchNotEqual, "env", "prod").MatchesLabels(ls) {
		t.Error("negation did not select a series missing the label")
	}
	if MustNewMatcher(MatchEqual, "env", "prod").MatchesLabels(ls) {
		t.Error("equality matched a series missing the label")
	}
	if !MustNewMatcher(MatchEqual, "env", "").MatchesLabels(ls) {
		t.Error(`env="" did not match a series missing env`)
	}
}

func TestSetMatches(t *testing.T) {
	cases := []struct {
		pattern string
		want    []string // nil means "not enumerable"
	}{
		{"web-1", []string{"web-1"}},
		{"web-1|web-2|web-3", []string{"web-1", "web-2", "web-3"}},
		{"(web-1|web-2)", []string{"web-1", "web-2"}},
		{"", []string{""}},
		{"a|", []string{"a", ""}},
		{"[ab]", []string{"a", "b"}},

		// Not enumerable: unbounded or case-insensitive.
		{"web-.*", nil},
		{"web-[0-9]+", nil},
		{"(?i)web", nil},
		{".*", nil},
	}

	for _, tc := range cases {
		m := MustNewMatcher(MatchRegexp, "host", tc.pattern)
		got := m.SetMatches()

		if tc.want == nil {
			if got != nil {
				t.Errorf("SetMatches(%q) = %v, want nil", tc.pattern, got)
			}
			continue
		}
		sortedGot := append([]string(nil), got...)
		sortedWant := append([]string(nil), tc.want...)
		sort.Strings(sortedGot)
		sort.Strings(sortedWant)
		if len(sortedGot) != len(sortedWant) {
			t.Errorf("SetMatches(%q) = %v, want %v", tc.pattern, got, tc.want)
			continue
		}
		for i := range sortedGot {
			if sortedGot[i] != sortedWant[i] {
				t.Errorf("SetMatches(%q) = %v, want %v", tc.pattern, got, tc.want)
				break
			}
		}
	}
}

// TestSetMatchesAgreesWithRegexp is the property that makes the optimisation
// safe: whenever a set is enumerated, membership in that set must be exactly
// what the anchored regexp would decide. If these ever disagree, queries
// silently return the wrong series.
func TestSetMatchesAgreesWithRegexp(t *testing.T) {
	patterns := []string{
		"web-1", "web-1|web-2", "(a|b|c)", "", "a|", "[ab]", "[a-e]",
	}
	probes := []string{
		"web-1", "web-2", "web-3", "a", "b", "c", "d", "e", "f", "",
		"aa", "ab", "web-1|web-2", "WEB-1",
	}

	for _, p := range patterns {
		m := MustNewMatcher(MatchRegexp, "host", p)
		if m.SetMatches() == nil {
			t.Fatalf("pattern %q was expected to enumerate", p)
		}
		// An independently compiled, anchored regexp is the oracle.
		oracle := MustNewMatcher(MatchRegexp, "host", "(?:"+p+")|\x00never\x00")

		for _, probe := range probes {
			if got, want := m.Matches(probe), oracle.Matches(probe); got != want {
				t.Errorf("pattern %q, probe %q: set path says %v, regexp says %v",
					p, probe, got, want)
			}
		}
	}
}

func TestSetMatchesIsBounded(t *testing.T) {
	// A finite but huge class must not be enumerated, or compiling a matcher
	// becomes a denial-of-service vector.
	m := MustNewMatcher(MatchRegexp, "host", "[\\x00-\\x{10FFFF}]")
	if got := m.SetMatches(); got != nil {
		t.Errorf("SetMatches enumerated %d values for a full-range class", len(got))
	}
}

func TestPrefix(t *testing.T) {
	cases := []struct {
		pattern string
		want    string
	}{
		{"web-.*", "web-"},
		{"web-[0-9]+", "web-"},
		{".*", ""},
		{"[ab].*", ""},
	}
	for _, tc := range cases {
		m := MustNewMatcher(MatchRegexp, "host", tc.pattern)
		if got := m.Prefix(); got != tc.want {
			t.Errorf("Prefix(%q) = %q, want %q", tc.pattern, got, tc.want)
		}
	}

	// Non-regexp matchers have no prefix to offer.
	if got := MustNewMatcher(MatchEqual, "host", "web-1").Prefix(); got != "" {
		t.Errorf("Prefix on an equality matcher = %q, want empty", got)
	}
}

func TestMatchTypeString(t *testing.T) {
	for _, tc := range []struct {
		t    MatchType
		want string
	}{
		{MatchEqual, "="}, {MatchNotEqual, "!="},
		{MatchRegexp, "=~"}, {MatchNotRegexp, "!~"},
		{MatchType(9), "unknown(9)"},
	} {
		if got := tc.t.String(); got != tc.want {
			t.Errorf("MatchType(%d).String() = %q, want %q", tc.t, got, tc.want)
		}
	}
}

func TestIsNegation(t *testing.T) {
	if MatchEqual.IsNegation() || MatchRegexp.IsNegation() {
		t.Error("positive match types reported as negations")
	}
	if !MatchNotEqual.IsNegation() || !MatchNotRegexp.IsNegation() {
		t.Error("negative match types not reported as negations")
	}
}

func TestMatchersString(t *testing.T) {
	ms := []*Matcher{
		MustNewMatcher(MatchEqual, MetricName, "cpu"),
		MustNewMatcher(MatchRegexp, "host", "web-.*"),
	}
	want := `{__name__="cpu", host=~"web-.*"}`
	if got := MatchersString(ms); got != want {
		t.Errorf("MatchersString() = %s, want %s", got, want)
	}
}

func BenchmarkMatcherSetPath(b *testing.B) {
	m := MustNewMatcher(MatchRegexp, "host", "web-1|web-2|web-3|web-4|web-5")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Matches("web-3")
	}
}

func BenchmarkMatcherRegexpPath(b *testing.B) {
	m := MustNewMatcher(MatchRegexp, "host", "web-[0-9]+")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Matches("web-3")
	}
}
