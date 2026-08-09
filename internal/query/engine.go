package query

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/navingamage/stratum/internal/chunk"
	"github.com/navingamage/stratum/internal/model"
	"github.com/navingamage/stratum/internal/tsdb"
)

// Errors the engine reports.
var (
	// ErrTooManySamples reports a query that exceeded its sample budget. It
	// exists so one expensive query cannot exhaust the process: without a
	// budget, `{__name__=~".+"}[30d]` is a denial of service that any user can
	// type into a dashboard.
	ErrTooManySamples = errors.New("query: sample limit exceeded")

	// ErrTimeout reports a query that ran out of time.
	ErrTimeout = errors.New("query: timed out")

	// ErrInvalidRange reports a range query whose parameters make no sense.
	ErrInvalidRange = errors.New("query: invalid range")
)

// EngineOptions configures the evaluator.
type EngineOptions struct {
	// MaxSamples caps how many samples a single query may load. Zero selects
	// fifty million, which is roughly a gigabyte of decoded points.
	MaxSamples int

	// Timeout caps how long a query may run. Zero selects two minutes.
	Timeout time.Duration

	// LookbackDelta is how far back an instant vector selector will look for a
	// sample. Zero selects five minutes.
	//
	// This is what makes an instant query return anything at all: samples land
	// at scrape intervals, essentially never exactly at the evaluation
	// timestamp, so the selector takes the most recent sample within the
	// lookback. Too short and a series with a slow scrape interval vanishes;
	// too long and a series that stopped reporting appears to continue.
	LookbackDelta int64
}

// Defaults for EngineOptions.
const (
	DefaultMaxSamples    = 50_000_000
	DefaultTimeout       = 2 * time.Minute
	DefaultLookbackDelta = int64(5 * 60 * 1000)
)

func (o EngineOptions) withDefaults() EngineOptions {
	if o.MaxSamples <= 0 {
		o.MaxSamples = DefaultMaxSamples
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.LookbackDelta <= 0 {
		o.LookbackDelta = DefaultLookbackDelta
	}
	return o
}

// Queryable is the storage the engine reads through.
type Queryable interface {
	Querier(mint, maxt int64) (tsdb.Querier, error)
}

// Engine evaluates queries against storage.
type Engine struct {
	opts EngineOptions
}

// NewEngine returns an engine.
func NewEngine(opts EngineOptions) *Engine {
	return &Engine{opts: opts.withDefaults()}
}

// Result is the outcome of a query, with the statistics needed to explain why
// it was slow.
type Result struct {
	Value Value `json:"value"`

	Stats Stats `json:"stats"`
}

// Stats records what a query cost.
type Stats struct {
	SeriesSelected int           `json:"seriesSelected"`
	SamplesScanned int           `json:"samplesScanned"`
	Elapsed        time.Duration `json:"elapsed"`
	Steps          int           `json:"steps"`
}

// evalContext carries per-evaluation state.
type evalContext struct {
	ctx     context.Context
	engine  *Engine
	querier tsdb.Querier

	// timestamp is the instant currently being evaluated.
	timestamp int64

	stats *Stats
}

// checkBudget enforces the sample limit and the deadline.
func (c *evalContext) checkBudget(extra int) error {
	c.stats.SamplesScanned += extra
	if c.stats.SamplesScanned > c.engine.opts.MaxSamples {
		return fmt.Errorf("%w: loaded %d samples, limit is %d",
			ErrTooManySamples, c.stats.SamplesScanned, c.engine.opts.MaxSamples)
	}
	select {
	case <-c.ctx.Done():
		if errors.Is(c.ctx.Err(), context.DeadlineExceeded) {
			return ErrTimeout
		}
		return c.ctx.Err()
	default:
	}
	return nil
}

// Instant evaluates expr at a single timestamp.
func (e *Engine) Instant(ctx context.Context, q Queryable, expr Expr, ts int64) (*Result, error) {
	start := time.Now()

	ctx, cancel := context.WithTimeout(ctx, e.opts.Timeout)
	defer cancel()

	// The storage range a query needs is not the evaluation instant: an
	// instant selector looks back, and a range selector reaches back by its
	// own window plus any offset. Working the widest requirement out up front
	// means the querier is opened once, over exactly the range required.
	mint, maxt := e.storageRange(expr, ts, ts)

	querier, err := q.Querier(mint, maxt)
	if err != nil {
		return nil, fmt.Errorf("query: opening storage: %w", err)
	}
	defer querier.Close()

	stats := Stats{Steps: 1}
	ec := &evalContext{ctx: ctx, engine: e, querier: querier, timestamp: ts, stats: &stats}

	val, err := ec.eval(expr)
	if err != nil {
		return nil, err
	}
	if v, ok := val.(Vector); ok {
		v.Sort()
	}

	stats.Elapsed = time.Since(start)
	return &Result{Value: val, Stats: stats}, nil
}

// Range evaluates expr at every step in [start, end].
//
// Each step is an independent instant evaluation over one shared querier. That
// is not the fastest possible shape - a step-aware evaluator could reuse
// decoded chunks between adjacent steps - but it is the one where a range
// query provably agrees with the instant queries it is made of, which is the
// property people actually rely on when a graph disagrees with a number.
func (e *Engine) Range(ctx context.Context, q Queryable, expr Expr, start, end, step int64) (*Result, error) {
	began := time.Now()

	if step <= 0 {
		return nil, fmt.Errorf("%w: step must be positive, got %d", ErrInvalidRange, step)
	}
	if end < start {
		return nil, fmt.Errorf("%w: end %d is before start %d", ErrInvalidRange, end, start)
	}
	steps := (end-start)/step + 1
	if steps > 100_000 {
		return nil, fmt.Errorf("%w: %d steps requested, limit is 100000; use a coarser step",
			ErrInvalidRange, steps)
	}
	if expr.Type() == ValueTypeMatrix {
		return nil, fmt.Errorf("%w: a range query cannot evaluate a range vector; wrap it in a function such as rate()",
			ErrInvalidRange)
	}

	ctx, cancel := context.WithTimeout(ctx, e.opts.Timeout)
	defer cancel()

	mint, maxt := e.storageRange(expr, start, end)
	querier, err := q.Querier(mint, maxt)
	if err != nil {
		return nil, fmt.Errorf("query: opening storage: %w", err)
	}
	defer querier.Close()

	stats := Stats{}
	// Series accumulate across steps, keyed by their label set.
	byLabels := make(map[string]*MatrixSeries)

	for ts := start; ts <= end; ts += step {
		stats.Steps++

		ec := &evalContext{ctx: ctx, engine: e, querier: querier, timestamp: ts, stats: &stats}
		val, err := ec.eval(expr)
		if err != nil {
			return nil, err
		}

		switch v := val.(type) {
		case Vector:
			for _, s := range v {
				key := s.Labels.String()
				series, ok := byLabels[key]
				if !ok {
					series = &MatrixSeries{Labels: s.Labels, RangeStart: start, RangeEnd: end}
					byLabels[key] = series
				}
				series.Samples = append(series.Samples, model.Sample{T: ts, V: s.V})
			}
		case Scalar:
			key := ""
			series, ok := byLabels[key]
			if !ok {
				series = &MatrixSeries{
					Labels: model.EmptyLabels(), RangeStart: start, RangeEnd: end,
				}
				byLabels[key] = series
			}
			series.Samples = append(series.Samples, model.Sample{T: ts, V: float64(v)})
		default:
			return nil, fmt.Errorf("%w: a range query cannot produce a %s", ErrInvalidRange, val.Type())
		}
	}

	out := make(Matrix, 0, len(byLabels))
	for _, s := range byLabels {
		out = append(out, *s)
	}
	out.Sort()

	stats.SeriesSelected = len(out)
	stats.Elapsed = time.Since(began)
	return &Result{Value: out, Stats: stats}, nil
}

// storageRange computes the widest span of storage the query can touch.
func (e *Engine) storageRange(expr Expr, start, end int64) (mint, maxt int64) {
	mint, maxt = start, end

	Inspect(expr, func(n Expr) bool {
		switch v := n.(type) {
		case *MatrixSelector:
			// A range selector at time t covers (t-range, t], shifted by any
			// offset.
			lo := start - v.Range - v.Selector.Offset
			hi := end - v.Selector.Offset
			if lo < mint {
				mint = lo
			}
			if hi > maxt {
				maxt = hi
			}
		case *VectorSelector:
			lo := start - e.opts.LookbackDelta - v.Offset
			hi := end - v.Offset
			if lo < mint {
				mint = lo
			}
			if hi > maxt {
				maxt = hi
			}
		}
		return true
	})
	return mint, maxt
}

// eval dispatches on node type.
func (c *evalContext) eval(expr Expr) (Value, error) {
	if err := c.checkBudget(0); err != nil {
		return nil, err
	}

	switch e := expr.(type) {
	case *NumberLiteral:
		return Scalar(e.Val), nil

	case *StringLiteral:
		return String(e.Val), nil

	case *ParenExpr:
		return c.eval(e.Expr)

	case *VectorSelector:
		return c.evalVectorSelector(e)

	case *MatrixSelector:
		return c.evalMatrixSelector(e)

	case *UnaryExpr:
		return c.evalUnary(e)

	case *BinaryExpr:
		return c.evalBinary(e)

	case *Call:
		return c.evalCall(e)

	case *AggregateExpr:
		return c.evalAggregate(e)
	}

	return nil, fmt.Errorf("query: cannot evaluate %T", expr)
}

// evalVectorSelector returns the most recent sample of each matching series
// within the lookback window.
func (c *evalContext) evalVectorSelector(vs *VectorSelector) (Value, error) {
	ts := c.timestamp - vs.Offset
	mint := ts - c.engine.opts.LookbackDelta

	set := c.querier.Select(vs.Matchers...)

	var (
		out Vector
		it  chunk.Iterator
	)
	for set.Next() {
		s := set.At()
		it = s.Iterator(it)

		// Walk to the newest sample at or before ts. The scan is forward-only
		// because the iterators are, and a chunk holds at most a few hundred
		// samples.
		var (
			found bool
			bestT int64
			bestV float64
			n     int
		)
		for it.Next() {
			t, v := it.At()
			n++
			if t > ts {
				break
			}
			if t >= mint {
				found, bestT, bestV = true, t, v
			}
		}
		if err := it.Err(); err != nil {
			return nil, fmt.Errorf("query: reading samples: %w", err)
		}
		if err := c.checkBudget(n); err != nil {
			return nil, err
		}
		if !found {
			continue
		}

		// The reported timestamp is the evaluation instant, not the sample's
		// own. Two series scraped at different phases must line up for
		// arithmetic between them to mean anything.
		out = append(out, VectorSample{Labels: s.Labels(), T: c.timestamp, V: bestV})
		_ = bestT
	}
	if err := set.Err(); err != nil {
		return nil, fmt.Errorf("query: selecting series: %w", err)
	}

	c.stats.SeriesSelected += len(out)
	return out, nil
}

// evalMatrixSelector returns every sample in the window for each series.
func (c *evalContext) evalMatrixSelector(ms *MatrixSelector) (Value, error) {
	end := c.timestamp - ms.Selector.Offset
	start := end - ms.Range

	set := c.querier.Select(ms.Selector.Matchers...)

	var (
		out Matrix
		it  chunk.Iterator
	)
	for set.Next() {
		s := set.At()
		it = s.Iterator(it)

		var samples []model.Sample
		for it.Next() {
			t, v := it.At()
			if t > end {
				break
			}
			// The window is half-open at the start: (start, end]. A sample
			// exactly at the start belongs to the previous window, which is
			// what keeps adjacent range evaluations from double-counting it.
			if t > start {
				samples = append(samples, model.Sample{T: t, V: v})
			}
		}
		if err := it.Err(); err != nil {
			return nil, fmt.Errorf("query: reading samples: %w", err)
		}
		if err := c.checkBudget(len(samples)); err != nil {
			return nil, err
		}
		if len(samples) == 0 {
			continue
		}

		out = append(out, MatrixSeries{
			Labels:     s.Labels(),
			Samples:    samples,
			RangeStart: start,
			RangeEnd:   end,
		})
	}
	if err := set.Err(); err != nil {
		return nil, fmt.Errorf("query: selecting series: %w", err)
	}

	c.stats.SeriesSelected += len(out)
	return out, nil
}

func (c *evalContext) evalUnary(u *UnaryExpr) (Value, error) {
	val, err := c.eval(u.Expr)
	if err != nil {
		return nil, err
	}
	if u.Op != tSub {
		return val, nil
	}

	switch v := val.(type) {
	case Scalar:
		return -v, nil
	case Vector:
		out := make(Vector, 0, len(v))
		for _, s := range v {
			// Negation changes the quantity, so the metric name goes.
			out = append(out, VectorSample{Labels: dropMetricName(s.Labels), T: s.T, V: -s.V})
		}
		return out, nil
	}
	return nil, fmt.Errorf("query: cannot negate a %s", val.Type())
}

func (c *evalContext) evalCall(call *Call) (Value, error) {
	args := make([]Value, 0, len(call.Args))
	for _, a := range call.Args {
		v, err := c.eval(a)
		if err != nil {
			return nil, err
		}
		args = append(args, v)
	}

	out, err := call.Func.Call(c, args)
	if err != nil {
		return nil, err
	}

	if call.Func.DropsMetricName {
		if v, ok := out.(Vector); ok {
			for i := range v {
				v[i].Labels = dropMetricName(v[i].Labels)
			}
		}
	}
	return out, nil
}

// dropMetricName removes __name__, which functions and arithmetic do because
// the result is no longer the metric it was computed from. Keeping it would
// let a rate and a raw counter collide in an aggregation and silently sum two
// different quantities.
func dropMetricName(ls model.Labels) model.Labels {
	if !ls.Has(model.MetricName) {
		return ls
	}
	return model.NewBuilder(ls).Del(model.MetricName).Labels()
}

func (c *evalContext) evalAggregate(agg *AggregateExpr) (Value, error) {
	val, err := c.eval(agg.Expr)
	if err != nil {
		return nil, err
	}
	vec, ok := val.(Vector)
	if !ok {
		return nil, fmt.Errorf("query: %s() requires an instant vector, got a %s", agg.Op, val.Type())
	}

	var param float64
	if agg.Param != nil {
		pv, err := c.eval(agg.Param)
		if err != nil {
			return nil, err
		}
		s, ok := pv.(Scalar)
		if !ok {
			return nil, fmt.Errorf("query: the parameter to %s() must be a scalar", agg.Op)
		}
		param = float64(s)
	}

	return aggregate(agg, vec, param, c.timestamp)
}

// groupKey builds the label set a sample aggregates into.
func groupKey(ls model.Labels, grouping []string, without bool) model.Labels {
	b := model.NewBuilder(ls)
	if without {
		// `without` also drops the metric name, since the result is a
		// different quantity from the input.
		b.Del(model.MetricName)
		b.Del(grouping...)
		return b.Labels()
	}
	return model.NewBuilder(ls).Keep(grouping...).Labels()
}

type aggGroup struct {
	labels model.Labels

	count int
	sum   float64
	mean  float64
	m2    float64 // for the variance, via Welford
	value float64 // min, max or the running result

	// samples is retained only by the operators that need every value.
	samples []VectorSample
}

func aggregate(agg *AggregateExpr, vec Vector, param float64, ts int64) (Value, error) {
	groups := make(map[string]*aggGroup)
	var order []string

	needsAll := agg.Op == AggTopK || agg.Op == AggBottomK || agg.Op == AggQuantile

	for _, s := range vec {
		key := groupKey(s.Labels, agg.Grouping, agg.Without)
		k := key.String()

		g, ok := groups[k]
		if !ok {
			g = &aggGroup{labels: key, value: s.V}
			groups[k] = g
			order = append(order, k)
		}

		g.count++
		g.sum += s.V

		// Welford, for stddev and stdvar.
		delta := s.V - g.mean
		g.mean += delta / float64(g.count)
		g.m2 += delta * (s.V - g.mean)

		switch agg.Op {
		case AggMin:
			// NaN loses to any real value, matching min_over_time.
			if s.V < g.value || math.IsNaN(g.value) {
				g.value = s.V
			}
		case AggMax:
			if s.V > g.value || math.IsNaN(g.value) {
				g.value = s.V
			}
		}

		if needsAll {
			g.samples = append(g.samples, s)
		}
	}

	out := make(Vector, 0, len(groups))
	for _, k := range order {
		g := groups[k]

		switch agg.Op {
		case AggTopK, AggBottomK:
			n := int(param)
			if n <= 0 {
				continue
			}
			sort.SliceStable(g.samples, func(i, j int) bool {
				if agg.Op == AggTopK {
					return g.samples[i].V > g.samples[j].V
				}
				return g.samples[i].V < g.samples[j].V
			})
			if n > len(g.samples) {
				n = len(g.samples)
			}
			// topk and bottomk keep the original series labels: the point is
			// to identify which series are the outliers.
			out = append(out, g.samples[:n]...)
			continue

		case AggQuantile:
			out = append(out, VectorSample{
				Labels: g.labels, T: ts, V: quantile(param, g.samples),
			})
			continue
		}

		var v float64
		switch agg.Op {
		case AggSum:
			v = g.sum
		case AggAvg:
			v = g.mean
		case AggMin, AggMax:
			v = g.value
		case AggCount:
			v = float64(g.count)
		case AggGroup:
			v = 1
		case AggStddev:
			v = math.Sqrt(g.m2 / float64(g.count))
		case AggStdvar:
			v = g.m2 / float64(g.count)
		default:
			return nil, fmt.Errorf("query: unsupported aggregation %s", agg.Op)
		}
		out = append(out, VectorSample{Labels: g.labels, T: ts, V: v})
	}

	out.Sort()
	return out, nil
}

// quantile computes the phi-quantile by linear interpolation between the two
// nearest ranks, which is what makes quantile(0.5, ...) of an even-sized set
// the mean of the middle pair rather than an arbitrary one of them.
func quantile(phi float64, samples []VectorSample) float64 {
	if len(samples) == 0 {
		return math.NaN()
	}
	if math.IsNaN(phi) {
		return math.NaN()
	}
	if phi < 0 {
		return math.Inf(-1)
	}
	if phi > 1 {
		return math.Inf(1)
	}

	values := make([]float64, len(samples))
	for i, s := range samples {
		values[i] = s.V
	}
	sort.Float64s(values)

	rank := phi * float64(len(values)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	if lower == upper {
		return values[lower]
	}
	weight := rank - float64(lower)
	return values[lower]*(1-weight) + values[upper]*weight
}
