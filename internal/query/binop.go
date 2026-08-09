package query

import (
	"fmt"
	"math"

	"github.com/navingamage/stratum/internal/model"
)

// evalBinary dispatches on the operand kinds.
func (c *evalContext) evalBinary(b *BinaryExpr) (Value, error) {
	lhs, err := c.eval(b.LHS)
	if err != nil {
		return nil, err
	}
	rhs, err := c.eval(b.RHS)
	if err != nil {
		return nil, err
	}

	lv, lIsVec := lhs.(Vector)
	rv, rIsVec := rhs.(Vector)
	ls, lIsScalar := lhs.(Scalar)
	rs, rIsScalar := rhs.(Scalar)

	switch {
	case lIsScalar && rIsScalar:
		v, keep := applyOp(b.Op, float64(ls), float64(rs), b.ReturnBool)
		if !keep {
			// A filtering comparison between two scalars has nothing to
			// filter, which the parser already rejects without `bool`.
			return Scalar(math.NaN()), nil
		}
		return Scalar(v), nil

	case lIsVec && rIsScalar:
		return vectorScalar(b, lv, float64(rs), false), nil

	case lIsScalar && rIsVec:
		return vectorScalar(b, rv, float64(ls), true), nil

	case lIsVec && rIsVec:
		if isSetOperator(b.Op) {
			return setOperation(b, lv, rv), nil
		}
		return vectorVector(b, lv, rv)
	}

	return nil, fmt.Errorf("query: operator %s cannot combine a %s and a %s",
		b.Op, lhs.Type(), rhs.Type())
}

// applyOp evaluates one operator on two numbers. The second result reports
// whether the sample survives, which is how a filtering comparison drops
// elements rather than producing a value.
func applyOp(op TokenType, l, r float64, returnBool bool) (float64, bool) {
	switch op {
	case tAdd:
		return l + r, true
	case tSub:
		return l - r, true
	case tMul:
		return l * r, true
	case tDiv:
		return l / r, true // division by zero yields ±Inf, as IEEE 754 says
	case tMod:
		return math.Mod(l, r), true
	case tPow:
		return math.Pow(l, r), true
	}

	var res bool
	switch op {
	case tEqual:
		res = l == r
	case tNotEqual:
		res = l != r
	case tGreater:
		res = l > r
	case tLess:
		res = l < r
	case tGreaterEqual:
		res = l >= r
	case tLessEqual:
		res = l <= r
	default:
		return math.NaN(), false
	}

	if returnBool {
		if res {
			return 1, true
		}
		return 0, true
	}
	// Without `bool`, a comparison filters: the left-hand value passes through
	// unchanged, or the sample is dropped.
	return l, res
}

// vectorScalar applies an operator between every element of a vector and a
// scalar. swapped means the scalar was on the left, which matters for the
// non-commutative operators and for which side a comparison filters on.
func vectorScalar(b *BinaryExpr, vec Vector, scalar float64, swapped bool) Vector {
	out := make(Vector, 0, len(vec))

	for _, s := range vec {
		l, r := s.V, scalar
		if swapped {
			l, r = scalar, s.V
		}

		v, keep := applyOp(b.Op, l, r, b.ReturnBool)
		if !keep {
			continue
		}
		// A filtering comparison returns the vector element's own value, not
		// whichever side happened to be on the left.
		if isComparison(b.Op) && !b.ReturnBool {
			v = s.V
		}

		labels := s.Labels
		// Arithmetic produces a different quantity, so the metric name goes.
		// A filtering comparison does not: it selects series, it does not
		// transform them.
		if !isComparison(b.Op) || b.ReturnBool {
			labels = dropMetricName(labels)
		}
		out = append(out, VectorSample{Labels: labels, T: s.T, V: v})
	}
	return out
}

// signature builds the key two series are paired on.
//
// By default that is every label except the metric name, which is what makes
// `errors / total` line up series that agree on all their labels. `on(...)`
// restricts the key to the listed labels and `ignoring(...)` removes them,
// both of which exist because in practice the two sides rarely have exactly
// the same label set.
func signature(ls model.Labels, matching []string, on bool) string {
	b := model.NewBuilder(ls)
	if on {
		b.Keep(matching...)
	} else {
		b.Del(model.MetricName)
		b.Del(matching...)
	}
	return b.Labels().String()
}

// vectorVector applies an arithmetic or comparison operator between two
// vectors, pairing series by their matching signature.
func vectorVector(b *BinaryExpr, lhs, rhs Vector) (Vector, error) {
	// Index the right side by signature. Duplicates are a user error: with
	// two candidates there is no defensible choice of which to pair with, and
	// silently picking one produces a result that changes between runs.
	index := make(map[string]VectorSample, len(rhs))
	for _, s := range rhs {
		key := signature(s.Labels, b.MatchingLabels, b.On)
		if _, dup := index[key]; dup {
			return nil, fmt.Errorf(
				"query: many-to-many match: several series on the right of %s share the labels %s; add on() or ignoring() to disambiguate",
				b.Op, key)
		}
		index[key] = s
	}

	out := make(Vector, 0, len(lhs))
	seen := make(map[string]struct{}, len(lhs))

	for _, l := range lhs {
		key := signature(l.Labels, b.MatchingLabels, b.On)
		r, ok := index[key]
		if !ok {
			continue // no counterpart: the element drops out
		}
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf(
				"query: many-to-many match: several series on the left of %s share the labels %s; add on() or ignoring() to disambiguate",
				b.Op, key)
		}
		seen[key] = struct{}{}

		v, keep := applyOp(b.Op, l.V, r.V, b.ReturnBool)
		if !keep {
			continue
		}

		// The result carries the matching labels. The metric name always goes:
		// the value is derived from two metrics and belongs to neither.
		labels := dropMetricName(l.Labels)
		if b.On {
			labels = model.NewBuilder(l.Labels).Keep(b.MatchingLabels...).Labels()
		} else if len(b.MatchingLabels) > 0 {
			labels = model.NewBuilder(l.Labels).Del(model.MetricName).Del(b.MatchingLabels...).Labels()
		}

		out = append(out, VectorSample{Labels: labels, T: l.T, V: v})
	}

	out.Sort()
	return out, nil
}

// setOperation implements and, or and unless.
//
// These pair series the same way arithmetic does but return one side's samples
// untouched, so they compose as filters: `up == 1 and rate(errors[5m]) > 0`
// reads as a conjunction and behaves as one.
func setOperation(b *BinaryExpr, lhs, rhs Vector) Vector {
	rightKeys := make(map[string]struct{}, len(rhs))
	for _, s := range rhs {
		rightKeys[signature(s.Labels, b.MatchingLabels, b.On)] = struct{}{}
	}

	switch b.Op {
	case tAnd:
		out := make(Vector, 0, len(lhs))
		for _, l := range lhs {
			if _, ok := rightKeys[signature(l.Labels, b.MatchingLabels, b.On)]; ok {
				out = append(out, l)
			}
		}
		return out

	case tUnless:
		out := make(Vector, 0, len(lhs))
		for _, l := range lhs {
			if _, ok := rightKeys[signature(l.Labels, b.MatchingLabels, b.On)]; !ok {
				out = append(out, l)
			}
		}
		return out

	case tOr:
		// Everything on the left, plus the right-hand series that have no
		// counterpart there.
		leftKeys := make(map[string]struct{}, len(lhs))
		out := make(Vector, 0, len(lhs)+len(rhs))
		for _, l := range lhs {
			leftKeys[signature(l.Labels, b.MatchingLabels, b.On)] = struct{}{}
			out = append(out, l)
		}
		for _, r := range rhs {
			if _, ok := leftKeys[signature(r.Labels, b.MatchingLabels, b.On)]; !ok {
				out = append(out, r)
			}
		}
		out.Sort()
		return out
	}
	return nil
}
