package model

import (
	"fmt"
	"regexp"
	"regexp/syntax"
	"strings"
)

// MatchType is the kind of comparison a Matcher performs.
type MatchType uint8

// Match types, mirroring the query language's operators.
const (
	MatchEqual     MatchType = iota // =
	MatchNotEqual                   // !=
	MatchRegexp                     // =~
	MatchNotRegexp                  // !~
)

func (m MatchType) String() string {
	switch m {
	case MatchEqual:
		return "="
	case MatchNotEqual:
		return "!="
	case MatchRegexp:
		return "=~"
	case MatchNotRegexp:
		return "!~"
	}
	return fmt.Sprintf("unknown(%d)", uint8(m))
}

// IsNegation reports whether the matcher excludes rather than selects. The
// planner needs this because a query made only of negations has no positive
// postings list to start from and must be seeded with every series.
func (m MatchType) IsNegation() bool {
	return m == MatchNotEqual || m == MatchNotRegexp
}

// Matcher tests one label of a series.
type Matcher struct {
	Type  MatchType
	Name  string
	Value string

	re *fastRegexp // non-nil only for the regexp match types
}

// NewMatcher compiles a matcher. Regular expressions are fully anchored: a
// pattern must match the whole label value, so `host=~"web"` does not match
// "webserver". Anchoring is not a convenience - an unanchored regex cannot be
// turned into a set of index lookups, which is where all the query
// performance comes from.
func NewMatcher(t MatchType, name, value string) (*Matcher, error) {
	m := &Matcher{Type: t, Name: name, Value: value}
	if t == MatchRegexp || t == MatchNotRegexp {
		re, err := newFastRegexp(value)
		if err != nil {
			return nil, fmt.Errorf("model: compiling matcher %s%s%q: %w", name, t, value, err)
		}
		m.re = re
	}
	return m, nil
}

// MustNewMatcher is NewMatcher for tests and static definitions.
func MustNewMatcher(t MatchType, name, value string) *Matcher {
	m, err := NewMatcher(t, name, value)
	if err != nil {
		panic(err)
	}
	return m
}

// Matches reports whether the matcher accepts a label value.
func (m *Matcher) Matches(s string) bool {
	switch m.Type {
	case MatchEqual:
		return s == m.Value
	case MatchNotEqual:
		return s != m.Value
	case MatchRegexp:
		return m.re.MatchString(s)
	case MatchNotRegexp:
		return !m.re.MatchString(s)
	}
	return false
}

// MatchesLabels reports whether the matcher accepts a whole series. A missing
// label is treated as an empty value, so `{env!="prod"}` selects series that
// have no env label at all - which is what users expect and what makes
// negation compose sensibly.
func (m *Matcher) MatchesLabels(ls Labels) bool { return m.Matches(ls.Get(m.Name)) }

func (m *Matcher) String() string {
	return fmt.Sprintf("%s%s%q", m.Name, m.Type, m.Value)
}

// SetMatches returns the finite set of values a regexp matcher can accept, or
// nil if the set is not finite or not worth enumerating.
//
// This is the optimisation that makes `pod=~"a|b|c"` cheap: instead of
// reading every distinct value of `pod` out of the index and testing each
// against the regex, the planner unions three postings lists. On a label with
// a hundred thousand distinct values that is the difference between a
// millisecond and a second.
func (m *Matcher) SetMatches() []string {
	if m.re == nil {
		return nil
	}
	return m.re.setMatches
}

// Prefix returns a literal prefix every accepted value must start with, or
// the empty string. The index can use it to bound a scan over sorted label
// values rather than reading them all.
func (m *Matcher) Prefix() string {
	if m.re == nil {
		return ""
	}
	return m.re.prefix
}

// fastRegexp is an anchored regular expression with two fast paths taken
// before the regexp engine is consulted at all.
type fastRegexp struct {
	// setMatches holds the complete set of accepted values when the pattern
	// is a literal or an alternation of literals.
	setMatches []string
	// set is the same values as a lookup table, used by MatchString.
	set map[string]struct{}

	// prefix is a literal every match must begin with.
	prefix string

	re *regexp.Regexp
}

// maxSetMatches bounds the enumerated set. A pattern like `[0-9]{6}` is
// finite but has a million members; enumerating it would cost more than
// running the regexp.
const maxSetMatches = 256

func newFastRegexp(v string) (*fastRegexp, error) {
	f := &fastRegexp{}

	// Parse before anchoring so the syntax tree reflects the user's pattern
	// rather than the wrapper.
	parsed, err := syntax.Parse(v, syntax.Perl)
	if err != nil {
		return nil, err
	}
	parsed = parsed.Simplify()

	if set, ok := literalAlternatives(parsed); ok && len(set) <= maxSetMatches {
		f.setMatches = set
		f.set = make(map[string]struct{}, len(set))
		for _, s := range set {
			f.set[s] = struct{}{}
		}
		// A finite literal set needs no regexp engine at all.
		return f, nil
	}

	re, err := regexp.Compile("^(?:" + v + ")$")
	if err != nil {
		return nil, err
	}
	f.re = re
	f.prefix, _ = re.LiteralPrefix()
	return f, nil
}

func (f *fastRegexp) MatchString(s string) bool {
	if f.set != nil {
		_, ok := f.set[s]
		return ok
	}
	return f.re.MatchString(s)
}

// literalAlternatives extracts the set of exact strings a parsed pattern
// accepts, if that set is finite and made only of literals.
func literalAlternatives(re *syntax.Regexp) ([]string, bool) {
	// Case folding would mean each literal stands for several values, so the
	// enumeration would be wrong.
	if re.Flags&syntax.FoldCase != 0 {
		return nil, false
	}

	switch re.Op {
	case syntax.OpEmptyMatch:
		return []string{""}, true

	case syntax.OpLiteral:
		return []string{string(re.Rune)}, true

	case syntax.OpCapture:
		// A group around the whole pattern: `(a|b)`.
		if len(re.Sub) != 1 {
			return nil, false
		}
		return literalAlternatives(re.Sub[0])

	case syntax.OpConcat:
		// Simplify leaves an empty concat for the empty pattern.
		if len(re.Sub) == 0 {
			return []string{""}, true
		}
		// A concatenation has to be handled as a cross product, and it is
		// not an unusual shape: the regexp parser factors common prefixes
		// out of alternations, so the very common `web-1|web-2|web-3`
		// arrives here as the literal "web-" concatenated with the class
		// [1-3] rather than as three separate literals.
		//
		// The product is bounded at each step, so a pattern engineered to
		// explode bails out early instead of allocating.
		out := []string{""}
		for _, sub := range re.Sub {
			vals, ok := literalAlternatives(sub)
			if !ok || len(out)*len(vals) > maxSetMatches {
				return nil, false
			}
			next := make([]string, 0, len(out)*len(vals))
			for _, prefix := range out {
				for _, v := range vals {
					next = append(next, prefix+v)
				}
			}
			out = next
		}
		return out, true

	case syntax.OpAlternate:
		var out []string
		for _, sub := range re.Sub {
			vals, ok := literalAlternatives(sub)
			if !ok {
				return nil, false
			}
			out = append(out, vals...)
			if len(out) > maxSetMatches {
				return nil, false
			}
		}
		return out, true

	// A single-character class small enough to enumerate, e.g. `[ab]`.
	case syntax.OpCharClass:
		var out []string
		for i := 0; i+1 < len(re.Rune); i += 2 {
			lo, hi := re.Rune[i], re.Rune[i+1]
			if hi-lo > maxSetMatches {
				return nil, false
			}
			for r := lo; r <= hi; r++ {
				out = append(out, string(r))
				if len(out) > maxSetMatches {
					return nil, false
				}
			}
		}
		return out, true
	}

	return nil, false
}

// MatchersString renders a matcher list in query syntax, for error messages
// and query plan output.
func MatchersString(ms []*Matcher) string {
	parts := make([]string, len(ms))
	for i, m := range ms {
		parts[i] = m.String()
	}
	return "{" + strings.Join(parts, ", ") + "}"
}
