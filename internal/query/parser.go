package query

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/navingamage/stratum/internal/model"
)

// ErrParse wraps every syntax and type error, so callers can distinguish a bad
// query (the user's problem, a 400) from a storage failure (ours, a 500).
var ErrParse = errors.New("query: parse error")

// ParseError carries the position of a syntax error.
type ParseError struct {
	Pos   int
	Query string
	Msg   string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("%d:%d: %s", 1, e.Pos+1, e.Msg)
}

func (e *ParseError) Unwrap() error { return ErrParse }

// Pretty renders the error with the offending position marked, which is worth
// far more to whoever typed the query than a position number.
func (e *ParseError) Pretty() string {
	var sb strings.Builder
	sb.WriteString(e.Query)
	sb.WriteByte('\n')
	for i := 0; i < e.Pos; i++ {
		if i < len(e.Query) && e.Query[i] == '\t' {
			sb.WriteByte('\t')
		} else {
			sb.WriteByte(' ')
		}
	}
	sb.WriteString("^ ")
	sb.WriteString(e.Msg)
	return sb.String()
}

// precedence returns the binding power of a binary operator, or 0 if the token
// is not one. Higher binds tighter.
//
// The levels mirror PromQL, which in turn mirrors C for arithmetic. The set
// logic operators sit below comparison so that `a > 1 and b` groups the way it
// reads.
func precedence(t TokenType) int {
	switch t {
	case tOr:
		return 1
	case tAnd, tUnless:
		return 2
	case tEqual, tNotEqual, tGreater, tLess, tGreaterEqual, tLessEqual:
		return 3
	case tAdd, tSub:
		return 4
	case tMul, tDiv, tMod:
		return 5
	case tPow:
		return 6
	}
	return 0
}

// isRightAssociative reports whether an operator groups to the right.
// Exponentiation is the only one: 2^3^2 is 2^(3^2).
func isRightAssociative(t TokenType) bool { return t == tPow }

func isComparison(t TokenType) bool {
	switch t {
	case tEqual, tNotEqual, tGreater, tLess, tGreaterEqual, tLessEqual:
		return true
	}
	return false
}

func isSetOperator(t TokenType) bool {
	switch t {
	case tAnd, tOr, tUnless:
		return true
	}
	return false
}

// parser is a precedence-climbing recursive-descent parser.
type parser struct {
	lex   *lexer
	input string

	tok    Token
	peeked *Token
	err    error
}

// Parse turns query text into a type-checked expression tree.
func Parse(input string) (Expr, error) {
	if strings.TrimSpace(input) == "" {
		return nil, &ParseError{Pos: 0, Query: input, Msg: "empty query"}
	}

	p := &parser{lex: newLexer(input), input: input}
	p.advance()

	expr := p.parseExpr(0)
	if p.err != nil {
		return nil, p.err
	}
	if p.tok.Type != tEOF {
		return nil, p.errorf(p.tok.Pos, "unexpected %s after a complete expression", p.tok)
	}
	if expr == nil {
		return nil, p.errorf(0, "empty query")
	}
	return expr, nil
}

// MustParse is Parse for tests and static queries.
func MustParse(input string) Expr {
	e, err := Parse(input)
	if err != nil {
		panic(err)
	}
	return e
}

func (p *parser) errorf(pos int, format string, args ...any) error {
	if p.err != nil {
		return p.err // keep the first error; later ones are usually noise
	}
	p.err = &ParseError{Pos: pos, Query: p.input, Msg: fmt.Sprintf(format, args...)}
	return p.err
}

func (p *parser) advance() {
	if p.peeked != nil {
		p.tok = *p.peeked
		p.peeked = nil
	} else {
		p.tok = p.lex.Next()
	}
	if p.tok.Type == tError && p.err == nil {
		p.errorf(p.tok.Pos, "%s", p.tok.Val)
	}
}

func (p *parser) peek() Token {
	if p.peeked == nil {
		t := p.lex.Next()
		p.peeked = &t
	}
	return *p.peeked
}

// expect consumes a token of the given type or records an error.
func (p *parser) expect(t TokenType, context string) Token {
	if p.tok.Type != t {
		p.errorf(p.tok.Pos, "expected %s in %s, got %s", t, context, p.tok)
		return Token{Type: tError}
	}
	tok := p.tok
	p.advance()
	return tok
}

// parseExpr parses with precedence climbing: parse a unary operand, then keep
// absorbing operators that bind at least as tightly as minPrec.
func (p *parser) parseExpr(minPrec int) Expr {
	lhs := p.parseUnary()
	if p.err != nil {
		return nil
	}

	for {
		op := p.tok.Type
		prec := precedence(op)
		if prec == 0 || prec < minPrec {
			return lhs
		}
		opPos := p.tok.Pos
		p.advance()

		be := &BinaryExpr{Op: op, LHS: lhs, Pos: opPos}

		// `bool` turns a filtering comparison into a 0/1 one.
		if p.tok.Type == tBool {
			if !isComparison(op) {
				p.errorf(p.tok.Pos, "bool modifier is only valid on comparison operators, not %s", op)
				return nil
			}
			be.ReturnBool = true
			p.advance()
		}

		// Vector matching: on(...) / ignoring(...).
		if p.tok.Type == tIdentifier && (p.tok.Val == "on" || p.tok.Val == "ignoring") {
			be.On = p.tok.Val == "on"
			p.advance()
			be.MatchingLabels = p.parseLabelList("vector matching")
			if p.err != nil {
				return nil
			}
		}

		// Right-associative operators recurse at the same precedence so that
		// the right operand absorbs another operator at that level.
		next := prec + 1
		if isRightAssociative(op) {
			next = prec
		}
		rhs := p.parseExpr(next)
		if p.err != nil {
			return nil
		}
		be.RHS = rhs

		if err := p.checkBinary(be); err != nil {
			return nil
		}
		lhs = be
	}
}

// checkBinary applies the type rules that cannot be expressed in the grammar.
func (p *parser) checkBinary(b *BinaryExpr) error {
	lt, rt := b.LHS.Type(), b.RHS.Type()

	for _, side := range []struct {
		t   ValueType
		e   Expr
		lbl string
	}{{lt, b.LHS, "left"}, {rt, b.RHS, "right"}} {
		switch side.t {
		case ValueTypeMatrix:
			return p.errorf(side.e.Position(),
				"binary operator %s cannot take a range vector on the %s; wrap it in a function such as rate()",
				b.Op, side.lbl)
		case ValueTypeString:
			return p.errorf(side.e.Position(),
				"binary operator %s cannot take a string on the %s", b.Op, side.lbl)
		}
	}

	if isSetOperator(b.Op) {
		if lt != ValueTypeVector || rt != ValueTypeVector {
			return p.errorf(b.Pos, "set operator %s requires instant vectors on both sides", b.Op)
		}
	}
	if b.ReturnBool && lt == ValueTypeScalar && rt == ValueTypeScalar {
		// Permitted, and the only way to compare two scalars at all.
		return nil
	}
	if isComparison(b.Op) && lt == ValueTypeScalar && rt == ValueTypeScalar && !b.ReturnBool {
		return p.errorf(b.Pos, "comparing two scalars requires the bool modifier")
	}
	return nil
}

func (p *parser) parseUnary() Expr {
	switch p.tok.Type {
	case tAdd, tSub:
		op, pos := p.tok.Type, p.tok.Pos
		p.advance()
		// Unary binds tighter than every binary operator except ^.
		e := p.parseExpr(precedence(tMul) + 1)
		if p.err != nil {
			return nil
		}
		if e.Type() == ValueTypeMatrix || e.Type() == ValueTypeString {
			p.errorf(pos, "unary %s cannot be applied to a %s", op, e.Type())
			return nil
		}
		if op == tAdd {
			return e // unary plus is a no-op
		}
		// Fold the negation of a literal, so -1 prints as -1.
		if n, ok := e.(*NumberLiteral); ok {
			return &NumberLiteral{Val: -n.Val, Pos: pos}
		}
		return &UnaryExpr{Op: op, Expr: e, Pos: pos}
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() Expr {
	switch p.tok.Type {
	case tNumber:
		pos := p.tok.Pos
		v, err := parseNumber(p.tok.Val)
		if err != nil {
			p.errorf(pos, "%v", err)
			return nil
		}
		p.advance()
		return &NumberLiteral{Val: v, Pos: pos}

	case tString:
		tok := p.tok
		p.advance()
		return &StringLiteral{Val: tok.Val, Pos: tok.Pos}

	case tLeftParen:
		pos := p.tok.Pos
		p.advance()
		inner := p.parseExpr(0)
		if p.err != nil {
			return nil
		}
		p.expect(tRightParen, "parenthesised expression")
		if p.err != nil {
			return nil
		}
		return &ParenExpr{Expr: inner, Pos: pos}

	case tLeftBrace:
		return p.parseSelector("")

	case tIdentifier:
		name := p.tok.Val
		pos := p.tok.Pos

		// An aggregation may put its modifier before or after the argument
		// list: `sum by (x) (y)` and `sum(y) by (x)` are both valid.
		if op, ok := aggregateOps[name]; ok {
			next := p.peek().Type
			if next == tLeftParen || next == tBy || next == tWithout {
				return p.parseAggregate(op, pos)
			}
		}
		if fn, ok := functions[name]; ok && p.peek().Type == tLeftParen {
			return p.parseCall(fn, pos)
		}
		// An identifier followed by a parenthesis was meant to be a call.
		// Saying so beats letting it parse as a selector and then failing on
		// the stray "(" several tokens later.
		if p.peek().Type == tLeftParen {
			p.errorf(pos, "unknown function %q", name)
			return nil
		}
		return p.parseSelector(name)

	// Keywords are legal metric names when they appear where a value is
	// expected, which keeps `by` or `on` from being reserved words a user
	// cannot name a metric after.
	case tAnd, tOr, tUnless, tBy, tWithout, tOffset, tBool:
		return p.parseSelector(p.tok.Val)
	}

	p.errorf(p.tok.Pos, "unexpected %s; expected a metric name, number, string or opening parenthesis", p.tok)
	return nil
}

func parseNumber(s string) (float64, error) {
	lower := strings.ToLower(s)
	switch lower {
	case "inf", "+inf":
		return math.Inf(1), nil
	case "-inf":
		return math.Inf(-1), nil
	case "nan":
		return math.NaN(), nil
	}
	if strings.HasPrefix(lower, "0x") {
		n, err := strconv.ParseInt(s[2:], 16, 64)
		if err != nil {
			return 0, fmt.Errorf("malformed hexadecimal number %q", s)
		}
		return float64(n), nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("malformed number %q", s)
	}
	return v, nil
}

// parseSelector parses a vector selector and any range or offset attached to
// it.
func (p *parser) parseSelector(name string) Expr {
	pos := p.tok.Pos
	vs := &VectorSelector{Name: name, Pos: pos}

	if name != "" {
		p.advance() // the metric name
		vs.Matchers = append(vs.Matchers,
			model.MustNewMatcher(model.MatchEqual, model.MetricName, name))
	}

	if p.tok.Type == tLeftBrace {
		ms := p.parseMatchers()
		if p.err != nil {
			return nil
		}
		vs.Matchers = append(vs.Matchers, ms...)
	}

	if len(vs.Matchers) == 0 {
		p.errorf(pos, "a selector must have at least one matcher")
		return nil
	}
	// A selector made only of matchers that accept everything would scan the
	// entire database, which is never what anyone means.
	if !hasSelectiveMatcher(vs.Matchers) {
		p.errorf(pos, "a selector must have at least one matcher that does not match the empty string")
		return nil
	}

	var result Expr = vs

	// Range selector.
	if p.tok.Type == tLeftBracket {
		rangePos := p.tok.Pos
		p.advance()
		if p.tok.Type != tDuration {
			p.errorf(p.tok.Pos, "expected a duration in the range selector, got %s", p.tok)
			return nil
		}
		ms, err := ParseDuration(p.tok.Val)
		if err != nil {
			p.errorf(p.tok.Pos, "%v", err)
			return nil
		}
		if ms <= 0 {
			p.errorf(p.tok.Pos, "range must be positive, got %s", p.tok.Val)
			return nil
		}
		p.advance()
		p.expect(tRightBracket, "range selector")
		if p.err != nil {
			return nil
		}
		result = &MatrixSelector{Selector: vs, Range: ms, Pos: rangePos}
	}

	// The offset binds to the selector but is written after the range.
	if p.tok.Type == tOffset {
		p.advance()
		negative := false
		if p.tok.Type == tSub {
			negative = true
			p.advance()
		}
		if p.tok.Type != tDuration {
			p.errorf(p.tok.Pos, "expected a duration after offset, got %s", p.tok)
			return nil
		}
		ms, err := ParseDuration(p.tok.Val)
		if err != nil {
			p.errorf(p.tok.Pos, "%v", err)
			return nil
		}
		if negative {
			ms = -ms
		}
		vs.Offset = ms
		p.advance()
	}

	return result
}

// hasSelectiveMatcher reports whether at least one matcher rejects the empty
// string, which is what stops a query from selecting every series in the
// database.
func hasSelectiveMatcher(ms []*model.Matcher) bool {
	for _, m := range ms {
		if !m.Matches("") {
			return true
		}
	}
	return false
}

func (p *parser) parseMatchers() []*model.Matcher {
	p.expect(tLeftBrace, "selector")
	if p.err != nil {
		return nil
	}

	var out []*model.Matcher
	for p.tok.Type != tRightBrace {
		if p.tok.Type == tEOF {
			p.errorf(p.tok.Pos, "unterminated selector: expected }")
			return nil
		}

		// Keywords are valid label names.
		if p.tok.Type != tIdentifier && keywords[strings.ToLower(p.tok.Val)] == 0 {
			p.errorf(p.tok.Pos, "expected a label name, got %s", p.tok)
			return nil
		}
		name := p.tok.Val
		p.advance()

		var mt model.MatchType
		switch p.tok.Type {
		case tAssign:
			mt = model.MatchEqual
		case tNotEqual:
			mt = model.MatchNotEqual
		case tMatchRegex:
			mt = model.MatchRegexp
		case tNotRegex:
			mt = model.MatchNotRegexp
		default:
			p.errorf(p.tok.Pos, "expected one of =, !=, =~ or !~ after label %q, got %s", name, p.tok)
			return nil
		}
		p.advance()

		if p.tok.Type != tString {
			p.errorf(p.tok.Pos, "expected a quoted string as the value for label %q, got %s", name, p.tok)
			return nil
		}
		value := p.tok.Val
		p.advance()

		m, err := model.NewMatcher(mt, name, value)
		if err != nil {
			p.errorf(p.tok.Pos, "%v", err)
			return nil
		}
		out = append(out, m)

		if p.tok.Type == tComma {
			p.advance()
			continue
		}
		break
	}

	p.expect(tRightBrace, "selector")
	if p.err != nil {
		return nil
	}
	return out
}

func (p *parser) parseCall(fn *Function, pos int) Expr {
	p.advance() // the function name
	p.expect(tLeftParen, "function call")
	if p.err != nil {
		return nil
	}

	c := &Call{Func: fn, Pos: pos}
	for p.tok.Type != tRightParen {
		if p.tok.Type == tEOF {
			p.errorf(p.tok.Pos, "unterminated argument list for %s()", fn.Name)
			return nil
		}
		arg := p.parseExpr(0)
		if p.err != nil {
			return nil
		}
		c.Args = append(c.Args, arg)

		if p.tok.Type == tComma {
			p.advance()
			continue
		}
		break
	}
	p.expect(tRightParen, "function call")
	if p.err != nil {
		return nil
	}

	if err := p.checkCall(c); err != nil {
		return nil
	}
	return c
}

// checkCall verifies arity and argument types.
func (p *parser) checkCall(c *Call) error {
	fn := c.Func

	minArgs := len(fn.ArgTypes) - fn.OptionalArgs
	if len(c.Args) < minArgs || len(c.Args) > len(fn.ArgTypes) {
		want := strconv.Itoa(minArgs)
		if fn.OptionalArgs > 0 {
			want = fmt.Sprintf("between %d and %d", minArgs, len(fn.ArgTypes))
		}
		return p.errorf(c.Pos, "%s() takes %s argument(s), got %d", fn.Name, want, len(c.Args))
	}

	for i, arg := range c.Args {
		want := fn.ArgTypes[i]
		if got := arg.Type(); got != want {
			return p.errorf(arg.Position(),
				"argument %d of %s() must be a %s, got a %s", i+1, fn.Name, want, got)
		}
	}
	return nil
}

func (p *parser) parseAggregate(op AggregateOp, pos int) Expr {
	p.advance() // the operator name

	agg := &AggregateExpr{Op: op, Pos: pos}

	// Modifier before the argument list: `sum by (x) (...)`.
	modifierFirst := false
	if p.tok.Type == tBy || p.tok.Type == tWithout {
		agg.Without = p.tok.Type == tWithout
		p.advance()
		agg.Grouping = p.parseLabelList("aggregation grouping")
		if p.err != nil {
			return nil
		}
		modifierFirst = true
	}

	p.expect(tLeftParen, "aggregation")
	if p.err != nil {
		return nil
	}

	first := p.parseExpr(0)
	if p.err != nil {
		return nil
	}

	if p.tok.Type == tComma {
		if !op.TakesParameter() {
			p.errorf(p.tok.Pos, "%s() takes a single argument", op)
			return nil
		}
		p.advance()
		agg.Param = first
		agg.Expr = p.parseExpr(0)
		if p.err != nil {
			return nil
		}
	} else {
		if op.TakesParameter() {
			p.errorf(p.tok.Pos, "%s() requires two arguments: a parameter and an instant vector", op)
			return nil
		}
		agg.Expr = first
	}

	p.expect(tRightParen, "aggregation")
	if p.err != nil {
		return nil
	}

	// Modifier after the argument list: `sum(...) by (x)`.
	if !modifierFirst && (p.tok.Type == tBy || p.tok.Type == tWithout) {
		agg.Without = p.tok.Type == tWithout
		p.advance()
		agg.Grouping = p.parseLabelList("aggregation grouping")
		if p.err != nil {
			return nil
		}
	}

	if agg.Expr.Type() != ValueTypeVector {
		p.errorf(agg.Expr.Position(),
			"%s() requires an instant vector, got a %s", op, agg.Expr.Type())
		return nil
	}
	if agg.Param != nil && agg.Param.Type() != ValueTypeScalar {
		p.errorf(agg.Param.Position(),
			"the first argument to %s() must be a scalar, got a %s", op, agg.Param.Type())
		return nil
	}
	return agg
}

func (p *parser) parseLabelList(context string) []string {
	p.expect(tLeftParen, context)
	if p.err != nil {
		return nil
	}

	var out []string
	for p.tok.Type != tRightParen {
		if p.tok.Type == tEOF {
			p.errorf(p.tok.Pos, "unterminated label list in %s", context)
			return nil
		}
		if p.tok.Type != tIdentifier && keywords[strings.ToLower(p.tok.Val)] == 0 {
			p.errorf(p.tok.Pos, "expected a label name in %s, got %s", context, p.tok)
			return nil
		}
		out = append(out, p.tok.Val)
		p.advance()

		if p.tok.Type == tComma {
			p.advance()
			continue
		}
		break
	}
	p.expect(tRightParen, context)
	if p.err != nil {
		return nil
	}
	return out
}
