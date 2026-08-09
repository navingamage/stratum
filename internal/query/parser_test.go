package query

import (
	"errors"
	"strings"
	"testing"
)

// TestParseRoundTrip is the broadest parser test available for the cost: parse
// a query, print it, parse the printed form, and require the two trees to
// print identically. A precedence or associativity bug almost always shows up
// here first, because the reprint puts the parentheses where the tree actually
// grouped them.
func TestParseRoundTrip(t *testing.T) {
	queries := []string{
		`up`,
		`up{job="api"}`,
		`up{job="api", instance!="a"}`,
		`{__name__="up"}`,
		`up offset 5m`,
		`rate(http_requests_total[5m])`,
		`rate(http_requests_total{job="api"}[5m] offset 1h)`,
		`sum(rate(http_requests_total[5m]))`,
		`sum by (job) (rate(http_requests_total[5m]))`,
		`sum without (instance) (up)`,
		`topk(5, up)`,
		`quantile(0.99, up)`,
		`1 + 2`,
		`1 + 2 * 3`,
		`(1 + 2) * 3`,
		`2 ^ 3 ^ 2`,
		`-1`,
		`up > 0`,
		`up > bool 0`,
		`up{job="api"} / on (instance) up{job="db"}`,
		`up and up`,
		`up unless up`,
		`up or up`,
		`avg_over_time(cpu[1h])`,
		`abs(cpu)`,
		`clamp_min(cpu, 0)`,
		`round(cpu, 0.5)`,
		`time()`,
		`scalar(up)`,
		`vector(1)`,
		`1e5`,
		`0.5`,
		`Inf`,
		`sum(rate(a[5m])) / sum(rate(b[5m]))`,
	}

	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			expr, err := Parse(q)
			if err != nil {
				t.Fatalf("Parse(%q): %v", q, err)
			}
			printed := expr.String()

			again, err := Parse(printed)
			if err != nil {
				t.Fatalf("reparsing %q (printed from %q): %v", printed, q, err)
			}
			if got := again.String(); got != printed {
				t.Errorf("round trip is not stable:\n  original: %s\n  printed:  %s\n  reprinted: %s",
					q, printed, got)
			}
		})
	}
}

func TestParsePrecedence(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{`1 + 2 * 3`, `1 + 2 * 3`},
		{`1 * 2 + 3`, `1 * 2 + 3`},
		{`2 ^ 3 ^ 2`, `2 ^ 3 ^ 2`},
		{`1 - 2 - 3`, `1 - 2 - 3`},
		{`up > 0 and up < 10`, `up > 0 and up < 10`},
		{`a or b and c`, `a or b and c`},
	}
	for _, tc := range cases {
		expr, err := Parse(tc.in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.in, err)
		}
		if got := expr.String(); got != tc.want {
			t.Errorf("Parse(%q).String() = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestParseAssociativity checks the grouping directly on the tree, since
// String does not print redundant parentheses and so cannot distinguish
// left from right association on its own.
func TestParseAssociativity(t *testing.T) {
	t.Run("subtraction groups left", func(t *testing.T) {
		expr := MustParse(`1 - 2 - 3`)
		b, ok := expr.(*BinaryExpr)
		if !ok {
			t.Fatalf("got %T, want a binary expression", expr)
		}
		// (1 - 2) - 3: the left operand is itself a subtraction.
		if _, ok := b.LHS.(*BinaryExpr); !ok {
			t.Errorf("subtraction associated right: LHS is %T", b.LHS)
		}
		if _, ok := b.RHS.(*NumberLiteral); !ok {
			t.Errorf("subtraction associated right: RHS is %T", b.RHS)
		}
	})

	t.Run("exponentiation groups right", func(t *testing.T) {
		expr := MustParse(`2 ^ 3 ^ 2`)
		b, ok := expr.(*BinaryExpr)
		if !ok {
			t.Fatalf("got %T, want a binary expression", expr)
		}
		// 2 ^ (3 ^ 2): the right operand is itself an exponentiation.
		if _, ok := b.RHS.(*BinaryExpr); !ok {
			t.Errorf("exponentiation associated left: RHS is %T", b.RHS)
		}
		if _, ok := b.LHS.(*NumberLiteral); !ok {
			t.Errorf("exponentiation associated left: LHS is %T", b.LHS)
		}
	})
}

func TestParseSelectorMatchers(t *testing.T) {
	expr := MustParse(`http_requests_total{job="api", status=~"5..", env!="dev"}`)
	vs, ok := expr.(*VectorSelector)
	if !ok {
		t.Fatalf("got %T, want a vector selector", expr)
	}
	if vs.Name != "http_requests_total" {
		t.Errorf("name = %q", vs.Name)
	}
	// Three explicit matchers plus the implicit metric-name one.
	if len(vs.Matchers) != 4 {
		t.Errorf("got %d matchers, want 4: %v", len(vs.Matchers), vs.Matchers)
	}
}

func TestParseAggregationModifierPosition(t *testing.T) {
	// Both orderings must produce the same tree.
	a := MustParse(`sum by (job) (up)`)
	b := MustParse(`sum(up) by (job)`)
	if a.String() != b.String() {
		t.Errorf("modifier position changed the result: %q vs %q", a.String(), b.String())
	}
}

func TestParseDurationLiterals(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"1ms", 1},
		{"1s", 1000},
		{"1m", 60_000},
		{"1h", 3_600_000},
		{"1d", 86_400_000},
		{"1w", 604_800_000},
		{"1h30m", 5_400_000},
		{"1d12h", 129_600_000},
		{"90s", 90_000},
	}
	for _, tc := range cases {
		got, err := ParseDuration(tc.in)
		if err != nil {
			t.Errorf("ParseDuration(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseDuration(%q) = %d, want %d", tc.in, got, tc.want)
		}
		// And the rendering must parse back to the same value.
		if back, err := ParseDuration(FormatDuration(got)); err != nil || back != tc.want {
			t.Errorf("FormatDuration(%d) = %q, which parses to %d", got, FormatDuration(got), back)
		}
	}

	for _, bad := range []string{"", "5", "5x", "m", "-5m", "5mm"} {
		if _, err := ParseDuration(bad); err == nil {
			t.Errorf("ParseDuration(%q) succeeded, want an error", bad)
		}
	}
}

// TestLexDurationVersusNumber pins the one genuinely ambiguous lexing
// decision: whether a digit run followed by letters is a duration or a number
// next to an identifier.
func TestLexDurationVersusNumber(t *testing.T) {
	cases := []struct {
		in   string
		want TokenType
	}{
		{"5", tNumber},
		{"5m", tDuration},
		{"5ms", tDuration},
		{"5h30m", tDuration},
		{"1e5", tNumber},
		{"0x1f", tNumber},
		{"0.5", tNumber},
		{".5", tNumber},
	}
	for _, tc := range cases {
		l := newLexer(tc.in)
		tok := l.Next()
		if tok.Type != tc.want {
			t.Errorf("lexing %q gave %s, want %s", tc.in, tok.Type, tc.want)
		}
		if tok.Val != tc.in {
			t.Errorf("lexing %q consumed only %q", tc.in, tok.Val)
		}
	}

	// `5minutes` must not lex as 5m followed by "inutes".
	l := newLexer("5minutes")
	if tok := l.Next(); tok.Type == tDuration && tok.Val == "5m" {
		t.Error("5minutes lexed as the duration 5m followed by an identifier")
	}
}

func TestParseErrors(t *testing.T) {
	cases := []struct {
		in      string
		wantMsg string
	}{
		{``, "empty query"},
		{`{`, "unterminated selector"},
		{`up{`, "unterminated selector"},
		{`up{job}`, "expected one of"},
		{`up{job=api}`, "expected a quoted string"},
		{`sum(`, "unexpected"},
		{`rate(up)`, "must be a range vector"},
		{`sum(rate(up[5m])[5m])`, "expected ) in aggregation"},
		{`up[5m] + 1`, "cannot take a range vector"},
		{`1 + `, "unexpected"},
		{`topk(up)`, "requires two arguments"},
		{`sum(1, up)`, "single argument"},
		{`rate(up[5m], up[5m])`, "takes 1 argument"},
		{`nosuchfunc(up)`, `unknown function "nosuchfunc"`},
		{`1 == 1`, "requires the bool modifier"},
		{`up + bool 1`, "only valid on comparison"},
		{`{job=~".*"}`, "does not match the empty string"},
		{`up[0s]`, "must be positive"},
		{`"unterminated`, "unterminated string"},
		{`up @`, "unexpected character"},
	}

	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			_, err := Parse(tc.in)
			if err == nil {
				t.Fatalf("Parse(%q) succeeded, want an error", tc.in)
			}
			if !errors.Is(err, ErrParse) {
				t.Errorf("error does not wrap ErrParse: %v", err)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("Parse(%q) = %q, want it to mention %q", tc.in, err.Error(), tc.wantMsg)
			}
		})
	}
}

func TestParseErrorHasPosition(t *testing.T) {
	_, err := Parse(`up{job=api}`)
	if err == nil {
		t.Fatal("expected an error")
	}
	var pe *ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("error is %T, want a *ParseError", err)
	}
	if pe.Pos <= 0 {
		t.Errorf("position = %d, want the offset of the offending token", pe.Pos)
	}

	// The pretty form must put the caret under the error.
	pretty := pe.Pretty()
	lines := strings.Split(pretty, "\n")
	if len(lines) != 2 {
		t.Fatalf("Pretty() produced %d lines, want 2:\n%s", len(lines), pretty)
	}
	caret := strings.Index(lines[1], "^")
	if caret != pe.Pos {
		t.Errorf("caret is at column %d, want %d:\n%s", caret, pe.Pos, pretty)
	}
}

func TestParseKeywordsAsIdentifiers(t *testing.T) {
	// Keywords must remain usable as metric and label names, or users cannot
	// name a metric `offset_seconds` or a label `by`.
	for _, q := range []string{
		`up{by="1"}`,
		`up{on="x"}`,
		`up{bool="true"}`,
	} {
		if _, err := Parse(q); err != nil {
			t.Errorf("Parse(%q): %v", q, err)
		}
	}
}

func TestParseComments(t *testing.T) {
	expr, err := Parse("up # this is a comment\n")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := expr.String(); got != "up" {
		t.Errorf("got %q, want %q", got, "up")
	}
}

func TestParseStringEscapes(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{`up{a="b\"c"}`, `b"c`},
		{`up{a="b\nc"}`, "b\nc"},
		{"up{a=`b\\nc`}", `b\nc`}, // backticks are raw
		{`up{a='b"c'}`, `b"c`},
	}
	for _, tc := range cases {
		expr, err := Parse(tc.in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.in, err)
		}
		vs := expr.(*VectorSelector)
		var got string
		for _, m := range vs.Matchers {
			if m.Name == "a" {
				got = m.Value
			}
		}
		if got != tc.want {
			t.Errorf("Parse(%q) label value = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSelectors(t *testing.T) {
	expr := MustParse(`sum(rate(a[5m])) / sum(rate(b[5m])) + c`)
	sels := Selectors(expr)
	if len(sels) != 3 {
		t.Fatalf("found %d selectors, want 3", len(sels))
	}
	names := map[string]bool{}
	for _, s := range sels {
		names[s.Name] = true
	}
	for _, want := range []string{"a", "b", "c"} {
		if !names[want] {
			t.Errorf("selector %q was not found", want)
		}
	}
}

func TestValueTypeChecking(t *testing.T) {
	cases := []struct {
		in   string
		want ValueType
	}{
		{`1`, ValueTypeScalar},
		{`up`, ValueTypeVector},
		{`up[5m]`, ValueTypeMatrix},
		{`rate(up[5m])`, ValueTypeVector},
		{`sum(up)`, ValueTypeVector},
		{`scalar(up)`, ValueTypeScalar},
		{`time()`, ValueTypeScalar},
		{`1 + 1`, ValueTypeScalar},
		{`up + 1`, ValueTypeVector},
	}
	for _, tc := range cases {
		expr, err := Parse(tc.in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.in, err)
		}
		if got := expr.Type(); got != tc.want {
			t.Errorf("Parse(%q).Type() = %s, want %s", tc.in, got, tc.want)
		}
	}
}

// FuzzParser feeds arbitrary text to the parser. A query arrives from an HTTP
// request, so the parser is directly exposed to whatever anyone sends: it must
// return an error, never panic and never hang.
func FuzzParser(f *testing.F) {
	seeds := []string{
		`up`, `rate(up[5m])`, `sum by (job) (up)`, `1 + 2 * 3`,
		`up{a="b"}`, `topk(5, up)`, `{`, ``, `((((((((((`,
		`up[[[[`, `"\\`, `1e`, `5m5m5m`, `sum(sum(sum(sum(up))))`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		// Deeply nested input legitimately recurses; the parser has no depth
		// limit, so keep the fuzzer away from stack exhaustion, which is a
		// separate concern from the parsing logic under test.
		if len(input) > 1024 {
			return
		}
		expr, err := Parse(input)
		if err != nil {
			return
		}
		// Anything that parses must print, and the printed form must parse.
		printed := expr.String()
		if _, err := Parse(printed); err != nil {
			t.Fatalf("a parsed query did not survive a print/parse round trip:\n  input:   %q\n  printed: %q\n  error:   %v",
				input, printed, err)
		}
	})
}

func BenchmarkParse(b *testing.B) {
	const q = `sum by (job, instance) (rate(http_requests_total{job="api", status=~"5.."}[5m])) / sum by (job, instance) (rate(http_requests_total{job="api"}[5m]))`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Parse(q); err != nil {
			b.Fatal(err)
		}
	}
}
