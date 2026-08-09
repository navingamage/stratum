// Package query implements stratum's query language: a hand-written lexer and
// precedence-climbing parser, a planner that pushes selection into the index,
// and an evaluator over the storage layer's iterators.
//
// The language is deliberately close to PromQL. Anyone operating a metrics
// system already knows that syntax, and a subtly different dialect would be a
// tax on every user for no benefit.
package query

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// TokenType identifies a lexical token.
type TokenType int

// Token types.
const (
	tEOF TokenType = iota
	tError

	tNumber
	tString
	tIdentifier
	tDuration

	tLeftBrace
	tRightBrace
	tLeftParen
	tRightParen
	tLeftBracket
	tRightBracket
	tComma
	tColon

	// Arithmetic.
	tAdd
	tSub
	tMul
	tDiv
	tMod
	tPow

	// Comparison.
	tEqual        // ==
	tNotEqual     // !=
	tGreater      // >
	tLess         // <
	tGreaterEqual // >=
	tLessEqual    // <=

	// Matchers.
	tAssign     // =
	tMatchRegex // =~
	tNotRegex   // !~

	// Keywords.
	tAnd
	tOr
	tUnless
	tBy
	tWithout
	tOffset
	tBool
)

var tokenNames = map[TokenType]string{
	tEOF: "end of input", tError: "error",
	tNumber: "number", tString: "string", tIdentifier: "identifier", tDuration: "duration",
	tLeftBrace: "{", tRightBrace: "}", tLeftParen: "(", tRightParen: ")",
	tLeftBracket: "[", tRightBracket: "]", tComma: ",", tColon: ":",
	tAdd: "+", tSub: "-", tMul: "*", tDiv: "/", tMod: "%", tPow: "^",
	tEqual: "==", tNotEqual: "!=", tGreater: ">", tLess: "<",
	tGreaterEqual: ">=", tLessEqual: "<=",
	tAssign: "=", tMatchRegex: "=~", tNotRegex: "!~",
	tAnd: "and", tOr: "or", tUnless: "unless",
	tBy: "by", tWithout: "without", tOffset: "offset", tBool: "bool",
}

func (t TokenType) String() string {
	if s, ok := tokenNames[t]; ok {
		return s
	}
	return fmt.Sprintf("token(%d)", int(t))
}

// keywords are recognised only when they stand alone as identifiers, so a
// metric named `or_total` or a label called `by` still works.
var keywords = map[string]TokenType{
	"and": tAnd, "or": tOr, "unless": tUnless,
	"by": tBy, "without": tWithout, "offset": tOffset, "bool": tBool,
}

// Token is one lexical token with its position in the input.
type Token struct {
	Type TokenType
	Val  string
	Pos  int
}

func (t Token) String() string {
	switch t.Type {
	case tEOF:
		return "end of input"
	case tError:
		return t.Val
	case tString:
		return strconv.Quote(t.Val)
	}
	if t.Val != "" {
		return t.Val
	}
	return t.Type.String()
}

// lexer turns query text into tokens.
//
// Hand-written rather than generated. The language is small, the tricky parts
// are not the ones a generator helps with - distinguishing a duration from a
// number followed by an identifier, keeping keywords usable as label names -
// and error messages that point at the offending character are worth a great
// deal more here than a grammar file.
type lexer struct {
	input string
	pos   int // byte offset of the next rune
	start int // byte offset of the token being scanned

	// width is how many bytes the last next() consumed. backup un-consumes
	// exactly that rather than re-deriving it from the rune, because the two
	// disagree on invalid UTF-8: a stray 0xff decodes to RuneError after one
	// byte, but RuneError itself encodes as three, so re-deriving drives the
	// position negative and panics on the next read.
	width int
}

func newLexer(input string) *lexer { return &lexer{input: input} }

const eof = rune(-1)

func (l *lexer) next() rune {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = w
	l.pos += w
	return r
}

// backup un-consumes the rune returned by the most recent next().
func (l *lexer) backup() {
	l.pos -= l.width
	l.width = 0
}

func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// accept consumes the next rune if it is r.
func (l *lexer) accept(r rune) bool {
	if l.peek() == r {
		l.next()
		return true
	}
	return false
}

func (l *lexer) emit(t TokenType) Token {
	tok := Token{Type: t, Val: l.input[l.start:l.pos], Pos: l.start}
	l.start = l.pos
	return tok
}

func (l *lexer) errorf(format string, args ...any) Token {
	return Token{Type: tError, Val: fmt.Sprintf(format, args...), Pos: l.start}
}

// Next returns the next token.
func (l *lexer) Next() Token {
	// Skip whitespace and comments.
	for {
		r := l.next()
		if r == eof {
			l.start = l.pos
			return Token{Type: tEOF, Pos: l.pos}
		}
		if r == '#' {
			for {
				r = l.next()
				if r == eof || r == '\n' {
					break
				}
			}
			l.start = l.pos
			continue
		}
		if !unicode.IsSpace(r) {
			l.backup()
			break
		}
		l.start = l.pos
	}

	l.start = l.pos
	r := l.next()

	switch r {
	case '{':
		return l.emit(tLeftBrace)
	case '}':
		return l.emit(tRightBrace)
	case '(':
		return l.emit(tLeftParen)
	case ')':
		return l.emit(tRightParen)
	case '[':
		return l.emit(tLeftBracket)
	case ']':
		return l.emit(tRightBracket)
	case ',':
		return l.emit(tComma)
	case ':':
		return l.emit(tColon)
	case '+':
		return l.emit(tAdd)
	case '-':
		return l.emit(tSub)
	case '*':
		return l.emit(tMul)
	case '/':
		return l.emit(tDiv)
	case '%':
		return l.emit(tMod)
	case '^':
		return l.emit(tPow)

	case '=':
		if l.accept('=') {
			return l.emit(tEqual)
		}
		if l.accept('~') {
			return l.emit(tMatchRegex)
		}
		return l.emit(tAssign)

	case '!':
		if l.accept('=') {
			return l.emit(tNotEqual)
		}
		if l.accept('~') {
			return l.emit(tNotRegex)
		}
		return l.errorf("unexpected %q; expected != or !~", "!"+string(l.peek()))

	case '>':
		if l.accept('=') {
			return l.emit(tGreaterEqual)
		}
		return l.emit(tGreater)

	case '<':
		if l.accept('=') {
			return l.emit(tLessEqual)
		}
		return l.emit(tLess)

	case '"', '\'', '`':
		return l.lexString(r)
	}

	if isDigit(r) || (r == '.' && isDigit(l.peek())) {
		l.backup()
		return l.lexNumberOrDuration()
	}
	if isIdentStart(r) {
		l.backup()
		return l.lexIdentifier()
	}

	return l.errorf("unexpected character %q at position %d", r, l.start)
}

func (l *lexer) lexString(quote rune) Token {
	raw := quote == '`'
	for {
		r := l.next()
		switch r {
		case eof:
			return l.errorf("unterminated string starting at position %d", l.start)
		case '\n':
			if !raw {
				return l.errorf("unterminated string starting at position %d", l.start)
			}
		case '\\':
			if !raw {
				// Consume the escaped rune so a \" does not end the string.
				if l.next() == eof {
					return l.errorf("unterminated escape sequence at position %d", l.pos)
				}
			}
		case quote:
			tok := l.emit(tString)
			unquoted, err := unquote(tok.Val)
			if err != nil {
				return l.errorf("invalid string at position %d: %v", tok.Pos, err)
			}
			tok.Val = unquoted
			return tok
		}
	}
}

// unquote resolves escape sequences. Backticked strings are raw, matching Go
// and PromQL, which matters because regexp matchers are full of backslashes.
func unquote(s string) (string, error) {
	if len(s) >= 2 && s[0] == '`' {
		return s[1 : len(s)-1], nil
	}
	if len(s) >= 2 && s[0] == '\'' {
		// strconv.Unquote treats single quotes as runes, so translate to a
		// double-quoted form first.
		inner := strings.ReplaceAll(s[1:len(s)-1], `"`, `\"`)
		return strconv.Unquote(`"` + inner + `"`)
	}
	return strconv.Unquote(s)
}

// lexNumberOrDuration scans a numeric literal, which becomes a duration if a
// unit suffix follows.
//
// The two cannot be told apart until the suffix is reached: `5` is a number
// and `5m` is a duration, and the difference decides whether the parser is
// looking at a range selector or an arithmetic operand.
func (l *lexer) lexNumberOrDuration() Token {
	// Hexadecimal. This probes two runes ahead, so it rewinds by restoring a
	// saved offset rather than by backing up - backup only un-consumes the
	// most recent rune, and the intervening peek has already moved on.
	if l.peek() == '0' {
		save := l.pos
		l.next()
		if r := l.peek(); r == 'x' || r == 'X' {
			l.next()
			for isHexDigit(l.peek()) {
				l.next()
			}
			return l.emit(tNumber)
		}
		l.pos = save
	}

	digits := 0
	for isDigit(l.peek()) {
		l.next()
		digits++
	}
	if l.peek() == '.' {
		l.next()
		for isDigit(l.peek()) {
			l.next()
			digits++
		}
	}
	if digits == 0 {
		return l.errorf("malformed number at position %d", l.start)
	}

	// A unit suffix makes this a duration. Units are checked longest-first so
	// that `ms` is not read as `m` followed by an identifier.
	if unit := l.scanDurationUnit(); unit {
		// Durations may be compound: 1h30m.
		for {
			save := l.pos
			d := 0
			for isDigit(l.peek()) {
				l.next()
				d++
			}
			if d == 0 || !l.scanDurationUnit() {
				l.pos = save
				break
			}
		}
		return l.emit(tDuration)
	}

	// Scientific notation, but only for plain numbers - `1e5` is a number,
	// while `1e` would have been a malformed duration.
	if r := l.peek(); r == 'e' || r == 'E' {
		save := l.pos
		l.next()
		if r := l.peek(); r == '+' || r == '-' {
			l.next()
		}
		exp := 0
		for isDigit(l.peek()) {
			l.next()
			exp++
		}
		if exp == 0 {
			l.pos = save
		}
	}
	return l.emit(tNumber)
}

// durationUnits are ordered longest-first so that prefixes do not shadow
// longer units.
var durationUnits = []string{"ms", "s", "m", "h", "d", "w", "y"}

func (l *lexer) scanDurationUnit() bool {
	rest := l.input[l.pos:]
	for _, u := range durationUnits {
		if !strings.HasPrefix(rest, u) {
			continue
		}
		// The unit must not run into a longer identifier: `5minutes` is not
		// five minutes followed by "inutes".
		after := rest[len(u):]
		if after != "" {
			r, _ := utf8.DecodeRuneInString(after)
			if isIdentPart(r) && !isDigit(r) {
				return false
			}
		}
		l.pos += len(u)
		return true
	}
	return false
}

func (l *lexer) lexIdentifier() Token {
	for isIdentPart(l.peek()) {
		l.next()
	}
	tok := l.emit(tIdentifier)
	if kw, ok := keywords[strings.ToLower(tok.Val)]; ok {
		tok.Type = kw
	}
	return tok
}

func isDigit(r rune) bool { return r >= '0' && r <= '9' }
func isHexDigit(r rune) bool {
	return isDigit(r) || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
}
func isIdentStart(r rune) bool {
	return r == '_' || r == ':' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}
func isIdentPart(r rune) bool { return isIdentStart(r) || isDigit(r) }

// ParseDuration converts a duration literal to milliseconds.
//
// Days, weeks and years are fixed multiples rather than calendar-aware. A
// query language that quietly changed the width of `1d` across a daylight
// saving boundary would make two runs of the same query incomparable, which
// is worse than being slightly wrong about what a day is.
func ParseDuration(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("query: empty duration")
	}

	var (
		total int64
		i     int
		any   bool
	)
	for i < len(s) {
		start := i
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i == start {
			return 0, fmt.Errorf("query: malformed duration %q: expected a number at position %d", s, i)
		}
		n, err := strconv.ParseInt(s[start:i], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("query: malformed duration %q: %w", s, err)
		}

		unitStart := i
		for i < len(s) && !(s[i] >= '0' && s[i] <= '9') {
			i++
		}
		unit := s[unitStart:i]

		var ms int64
		switch unit {
		case "ms":
			ms = 1
		case "s":
			ms = 1000
		case "m":
			ms = 60 * 1000
		case "h":
			ms = 60 * 60 * 1000
		case "d":
			ms = 24 * 60 * 60 * 1000
		case "w":
			ms = 7 * 24 * 60 * 60 * 1000
		case "y":
			ms = 365 * 24 * 60 * 60 * 1000
		default:
			return 0, fmt.Errorf("query: unknown duration unit %q in %q", unit, s)
		}
		total += n * ms
		any = true
	}
	if !any {
		return 0, fmt.Errorf("query: malformed duration %q", s)
	}
	return total, nil
}

// FormatDuration renders milliseconds in the language's own syntax.
func FormatDuration(ms int64) string {
	if ms == 0 {
		return "0s"
	}
	var sb strings.Builder
	if ms < 0 {
		sb.WriteByte('-')
		ms = -ms
	}
	for _, u := range []struct {
		name string
		size int64
	}{
		{"y", 365 * 24 * 60 * 60 * 1000},
		{"w", 7 * 24 * 60 * 60 * 1000},
		{"d", 24 * 60 * 60 * 1000},
		{"h", 60 * 60 * 1000},
		{"m", 60 * 1000},
		{"s", 1000},
		{"ms", 1},
	} {
		if n := ms / u.size; n > 0 {
			sb.WriteString(strconv.FormatInt(n, 10))
			sb.WriteString(u.name)
			ms -= n * u.size
		}
	}
	return sb.String()
}
