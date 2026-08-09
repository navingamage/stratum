package query

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/navingamage/stratum/internal/model"
)

// ValueType is the kind of value an expression produces.
//
// The distinction is what makes the type check possible at parse time:
// rate() needs a range vector, sum() needs an instant vector, and arithmetic
// needs scalars or instant vectors. Catching `rate(cpu)` before evaluation
// means the error names the mistake instead of surfacing as an empty result.
type ValueType int

// Value types.
const (
	ValueTypeNone ValueType = iota
	ValueTypeScalar
	ValueTypeString
	ValueTypeVector // an instant vector: one sample per series
	ValueTypeMatrix // a range vector: a run of samples per series
)

func (v ValueType) String() string {
	switch v {
	case ValueTypeScalar:
		return "scalar"
	case ValueTypeString:
		return "string"
	case ValueTypeVector:
		return "instant vector"
	case ValueTypeMatrix:
		return "range vector"
	}
	return "none"
}

// Expr is a node in the query's abstract syntax tree.
type Expr interface {
	// Type reports the value the expression produces.
	Type() ValueType

	// String renders the expression back into query syntax. Round-tripping
	// is not decoration: it is how the query appears in logs and in the
	// explain output, and a parser bug usually shows up first as a query that
	// does not print back the way it was written.
	String() string

	// Position returns the byte offset the expression started at.
	Position() int
}

// NumberLiteral is a scalar constant.
type NumberLiteral struct {
	Val float64
	Pos int
}

func (n *NumberLiteral) Type() ValueType { return ValueTypeScalar }
func (n *NumberLiteral) Position() int   { return n.Pos }
func (n *NumberLiteral) String() string {
	return strconv.FormatFloat(n.Val, 'g', -1, 64)
}

// StringLiteral is a string constant.
type StringLiteral struct {
	Val string
	Pos int
}

func (s *StringLiteral) Type() ValueType { return ValueTypeString }
func (s *StringLiteral) Position() int   { return s.Pos }
func (s *StringLiteral) String() string  { return strconv.Quote(s.Val) }

// VectorSelector selects the most recent sample of each matching series.
type VectorSelector struct {
	Name     string
	Matchers []*model.Matcher

	// Offset shifts the evaluation time backwards, in milliseconds.
	Offset int64

	Pos int
}

func (v *VectorSelector) Type() ValueType { return ValueTypeVector }
func (v *VectorSelector) Position() int   { return v.Pos }

func (v *VectorSelector) String() string {
	var sb strings.Builder
	sb.WriteString(v.Name)

	// The metric-name matcher is rendered as the name, not as a label.
	var rest []string
	for _, m := range v.Matchers {
		if m.Name == model.MetricName && m.Type == model.MatchEqual && m.Value == v.Name {
			continue
		}
		rest = append(rest, m.String())
	}
	if len(rest) > 0 {
		sb.WriteByte('{')
		sb.WriteString(strings.Join(rest, ", "))
		sb.WriteByte('}')
	}
	if v.Offset != 0 {
		sb.WriteString(" offset ")
		sb.WriteString(FormatDuration(v.Offset))
	}
	return sb.String()
}

// MatrixSelector selects a window of samples per series.
type MatrixSelector struct {
	Selector *VectorSelector
	Range    int64 // milliseconds
	Pos      int
}

func (m *MatrixSelector) Type() ValueType { return ValueTypeMatrix }
func (m *MatrixSelector) Position() int   { return m.Pos }

func (m *MatrixSelector) String() string {
	// The range brackets bind to the selector, so an offset has to be printed
	// after them rather than inside.
	inner := *m.Selector
	offset := inner.Offset
	inner.Offset = 0

	s := inner.String() + "[" + FormatDuration(m.Range) + "]"
	if offset != 0 {
		s += " offset " + FormatDuration(offset)
	}
	return s
}

// Call is a function application.
type Call struct {
	Func *Function
	Args []Expr
	Pos  int
}

func (c *Call) Type() ValueType { return c.Func.ReturnType }
func (c *Call) Position() int   { return c.Pos }

func (c *Call) String() string {
	args := make([]string, len(c.Args))
	for i, a := range c.Args {
		args[i] = a.String()
	}
	return c.Func.Name + "(" + strings.Join(args, ", ") + ")"
}

// AggregateExpr groups an instant vector and reduces each group.
type AggregateExpr struct {
	Op       AggregateOp
	Expr     Expr
	Grouping []string

	// Without inverts Grouping: group by everything except these labels.
	Without bool

	// Param is the extra argument topk, bottomk and quantile take.
	Param Expr

	Pos int
}

func (a *AggregateExpr) Type() ValueType { return ValueTypeVector }
func (a *AggregateExpr) Position() int   { return a.Pos }

func (a *AggregateExpr) String() string {
	var sb strings.Builder
	sb.WriteString(a.Op.String())
	sb.WriteByte('(')
	if a.Param != nil {
		sb.WriteString(a.Param.String())
		sb.WriteString(", ")
	}
	sb.WriteString(a.Expr.String())
	sb.WriteByte(')')

	if len(a.Grouping) > 0 || a.Without {
		if a.Without {
			sb.WriteString(" without (")
		} else {
			sb.WriteString(" by (")
		}
		sb.WriteString(strings.Join(a.Grouping, ", "))
		sb.WriteByte(')')
	}
	return sb.String()
}

// BinaryExpr applies an operator to two operands.
type BinaryExpr struct {
	Op       TokenType
	LHS, RHS Expr

	// ReturnBool makes a comparison yield 0 or 1 rather than filtering.
	ReturnBool bool

	// MatchingLabels restricts which labels are used to pair up series, and
	// On inverts it from "ignoring" to "on".
	MatchingLabels []string
	On             bool

	Pos int
}

func (b *BinaryExpr) Position() int { return b.Pos }

func (b *BinaryExpr) Type() ValueType {
	// Scalar op scalar is the only combination that stays a scalar; anything
	// touching a vector produces a vector.
	if b.LHS.Type() == ValueTypeScalar && b.RHS.Type() == ValueTypeScalar {
		return ValueTypeScalar
	}
	return ValueTypeVector
}

func (b *BinaryExpr) String() string {
	var sb strings.Builder
	sb.WriteString(b.LHS.String())
	sb.WriteByte(' ')
	sb.WriteString(b.Op.String())
	if b.ReturnBool {
		sb.WriteString(" bool")
	}
	if len(b.MatchingLabels) > 0 {
		if b.On {
			sb.WriteString(" on (")
		} else {
			sb.WriteString(" ignoring (")
		}
		sb.WriteString(strings.Join(b.MatchingLabels, ", "))
		sb.WriteByte(')')
	}
	sb.WriteByte(' ')
	sb.WriteString(b.RHS.String())
	return sb.String()
}

// UnaryExpr negates an operand.
type UnaryExpr struct {
	Op   TokenType
	Expr Expr
	Pos  int
}

func (u *UnaryExpr) Type() ValueType { return u.Expr.Type() }
func (u *UnaryExpr) Position() int   { return u.Pos }
func (u *UnaryExpr) String() string  { return u.Op.String() + u.Expr.String() }

// ParenExpr preserves explicit grouping.
//
// Kept in the tree rather than folded away so that String round-trips a query
// as the user wrote it. Dropping the parentheses would reprint `(a + b) * c`
// as `a + b * c`, which is a different query.
type ParenExpr struct {
	Expr Expr
	Pos  int
}

func (p *ParenExpr) Type() ValueType { return p.Expr.Type() }
func (p *ParenExpr) Position() int   { return p.Pos }
func (p *ParenExpr) String() string  { return "(" + p.Expr.String() + ")" }

// AggregateOp identifies an aggregation.
type AggregateOp int

// Aggregation operators.
const (
	AggSum AggregateOp = iota
	AggAvg
	AggMin
	AggMax
	AggCount
	AggStddev
	AggStdvar
	AggTopK
	AggBottomK
	AggQuantile
	AggGroup
)

var aggNames = map[AggregateOp]string{
	AggSum: "sum", AggAvg: "avg", AggMin: "min", AggMax: "max",
	AggCount: "count", AggStddev: "stddev", AggStdvar: "stdvar",
	AggTopK: "topk", AggBottomK: "bottomk", AggQuantile: "quantile",
	AggGroup: "group",
}

func (a AggregateOp) String() string {
	if s, ok := aggNames[a]; ok {
		return s
	}
	return fmt.Sprintf("aggregate(%d)", int(a))
}

// TakesParameter reports whether the operator needs a leading argument.
func (a AggregateOp) TakesParameter() bool {
	return a == AggTopK || a == AggBottomK || a == AggQuantile
}

var aggregateOps = map[string]AggregateOp{
	"sum": AggSum, "avg": AggAvg, "min": AggMin, "max": AggMax,
	"count": AggCount, "stddev": AggStddev, "stdvar": AggStdvar,
	"topk": AggTopK, "bottomk": AggBottomK, "quantile": AggQuantile,
	"group": AggGroup,
}

// Inspect walks the tree depth-first, calling fn for every node. Returning
// false from fn stops the descent into that node's children.
func Inspect(e Expr, fn func(Expr) bool) {
	if e == nil || !fn(e) {
		return
	}
	switch n := e.(type) {
	case *MatrixSelector:
		Inspect(n.Selector, fn)
	case *Call:
		for _, a := range n.Args {
			Inspect(a, fn)
		}
	case *AggregateExpr:
		Inspect(n.Param, fn)
		Inspect(n.Expr, fn)
	case *BinaryExpr:
		Inspect(n.LHS, fn)
		Inspect(n.RHS, fn)
	case *UnaryExpr:
		Inspect(n.Expr, fn)
	case *ParenExpr:
		Inspect(n.Expr, fn)
	}
}

// Selectors returns every vector selector in a query, which is what the
// planner uses to work out the storage range a query needs.
func Selectors(e Expr) []*VectorSelector {
	var out []*VectorSelector
	Inspect(e, func(n Expr) bool {
		if vs, ok := n.(*VectorSelector); ok {
			out = append(out, vs)
		}
		return true
	})
	return out
}
