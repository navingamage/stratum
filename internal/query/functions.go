package query

import (
	"math"
	"sort"

	"github.com/navingamage/stratum/internal/model"
)

// Function describes a callable in the query language.
type Function struct {
	Name       string
	ArgTypes   []ValueType
	ReturnType ValueType

	// OptionalArgs is how many trailing arguments may be omitted.
	OptionalArgs int

	// DropsMetricName marks functions whose output is no longer the metric it
	// was computed from. rate(http_requests_total) is a rate, not a count, and
	// keeping the name would let two different quantities collide in an
	// aggregation.
	DropsMetricName bool

	// Call evaluates the function. Range-vector functions receive their
	// argument as a matrix; the evaluator handles the plumbing.
	Call func(ctx *evalContext, args []Value) (Value, error)
}

// functions is the registry the parser resolves names against.
var functions map[string]*Function

// FunctionNames returns the registered function names, sorted. The CLI uses it
// for completion and the API exposes it for tooling.
func FunctionNames() []string {
	out := make([]string, 0, len(functions))
	for n := range functions {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// LookupFunction returns a function by name.
func LookupFunction(name string) (*Function, bool) {
	f, ok := functions[name]
	return f, ok
}

func init() {
	// Built in init rather than as a literal because several entries share
	// implementations that reference the map.
	functions = make(map[string]*Function)

	reg := func(f *Function) { functions[f.Name] = f }

	// Rate-style functions over a range vector.
	reg(&Function{
		Name: "rate", ArgTypes: []ValueType{ValueTypeMatrix},
		ReturnType: ValueTypeVector, DropsMetricName: true,
		Call: rangeFunc(func(ctx *evalContext, s rangeSeries) (float64, bool) {
			return extrapolatedRate(ctx, s, true, true)
		}),
	})
	reg(&Function{
		Name: "increase", ArgTypes: []ValueType{ValueTypeMatrix},
		ReturnType: ValueTypeVector, DropsMetricName: true,
		Call: rangeFunc(func(ctx *evalContext, s rangeSeries) (float64, bool) {
			return extrapolatedRate(ctx, s, true, false)
		}),
	})
	reg(&Function{
		Name: "delta", ArgTypes: []ValueType{ValueTypeMatrix},
		ReturnType: ValueTypeVector, DropsMetricName: true,
		Call: rangeFunc(func(ctx *evalContext, s rangeSeries) (float64, bool) {
			return extrapolatedRate(ctx, s, false, false)
		}),
	})
	reg(&Function{
		Name: "irate", ArgTypes: []ValueType{ValueTypeMatrix},
		ReturnType: ValueTypeVector, DropsMetricName: true,
		Call: rangeFunc(func(ctx *evalContext, s rangeSeries) (float64, bool) {
			return instantRate(s, true)
		}),
	})
	reg(&Function{
		Name: "idelta", ArgTypes: []ValueType{ValueTypeMatrix},
		ReturnType: ValueTypeVector, DropsMetricName: true,
		Call: rangeFunc(func(ctx *evalContext, s rangeSeries) (float64, bool) {
			return instantRate(s, false)
		}),
	})
	reg(&Function{
		Name: "resets", ArgTypes: []ValueType{ValueTypeMatrix},
		ReturnType: ValueTypeVector, DropsMetricName: true,
		Call: rangeFunc(func(_ *evalContext, s rangeSeries) (float64, bool) {
			resets := 0
			for i := 1; i < len(s.samples); i++ {
				if s.samples[i].V < s.samples[i-1].V {
					resets++
				}
			}
			return float64(resets), len(s.samples) > 0
		}),
	})
	reg(&Function{
		Name: "changes", ArgTypes: []ValueType{ValueTypeMatrix},
		ReturnType: ValueTypeVector, DropsMetricName: true,
		Call: rangeFunc(func(_ *evalContext, s rangeSeries) (float64, bool) {
			changes := 0
			for i := 1; i < len(s.samples); i++ {
				prev, cur := s.samples[i-1].V, s.samples[i].V
				// NaN != NaN, so compare bit patterns to avoid counting a run
				// of NaNs as a change on every step.
				if prev != cur && !(math.IsNaN(prev) && math.IsNaN(cur)) {
					changes++
				}
			}
			return float64(changes), len(s.samples) > 0
		}),
	})

	// Aggregations over a range vector.
	overTime := map[string]func([]model.Sample) float64{
		"avg_over_time": func(s []model.Sample) float64 {
			// Running mean rather than sum/n: a long window of large values
			// overflows to +Inf in the naive form long before the mean does.
			var mean float64
			for i, x := range s {
				mean += (x.V - mean) / float64(i+1)
			}
			return mean
		},
		"sum_over_time": func(s []model.Sample) float64 {
			var sum float64
			for _, x := range s {
				sum += x.V
			}
			return sum
		},
		"min_over_time": func(s []model.Sample) float64 {
			out := math.Inf(1)
			for _, x := range s {
				if x.V < out || math.IsNaN(out) {
					out = x.V
				}
			}
			return out
		},
		"max_over_time": func(s []model.Sample) float64 {
			out := math.Inf(-1)
			for _, x := range s {
				if x.V > out || math.IsNaN(out) {
					out = x.V
				}
			}
			return out
		},
		"count_over_time": func(s []model.Sample) float64 { return float64(len(s)) },
		"last_over_time":  func(s []model.Sample) float64 { return s[len(s)-1].V },
		"first_over_time": func(s []model.Sample) float64 { return s[0].V },
		"stddev_over_time": func(s []model.Sample) float64 {
			return math.Sqrt(variance(s))
		},
		"stdvar_over_time": func(s []model.Sample) float64 { return variance(s) },
	}
	for name, fn := range overTime {
		f := fn
		// count_over_time and last_over_time keep meaning the same quantity;
		// the rest are new quantities and drop the name.
		drops := name != "last_over_time" && name != "first_over_time" &&
			name != "min_over_time" && name != "max_over_time"
		reg(&Function{
			Name: name, ArgTypes: []ValueType{ValueTypeMatrix},
			ReturnType: ValueTypeVector, DropsMetricName: drops,
			Call: rangeFunc(func(_ *evalContext, s rangeSeries) (float64, bool) {
				if len(s.samples) == 0 {
					return 0, false
				}
				return f(s.samples), true
			}),
		})
	}

	// Element-wise maths over an instant vector.
	simple := map[string]func(float64) float64{
		"abs": math.Abs, "ceil": math.Ceil, "floor": math.Floor,
		"sqrt": math.Sqrt, "exp": math.Exp,
		"ln": math.Log, "log2": math.Log2, "log10": math.Log10,
		"sgn": func(v float64) float64 {
			switch {
			case v < 0:
				return -1
			case v > 0:
				return 1
			}
			return v // preserves NaN and signed zero
		},
	}
	for name, fn := range simple {
		f := fn
		reg(&Function{
			Name: name, ArgTypes: []ValueType{ValueTypeVector},
			ReturnType: ValueTypeVector, DropsMetricName: true,
			Call: func(_ *evalContext, args []Value) (Value, error) {
				in := args[0].(Vector)
				out := make(Vector, 0, len(in))
				for _, s := range in {
					out = append(out, VectorSample{Labels: s.Labels, T: s.T, V: f(s.V)})
				}
				return out, nil
			},
		})
	}

	reg(&Function{
		Name:     "round",
		ArgTypes: []ValueType{ValueTypeVector, ValueTypeScalar}, OptionalArgs: 1,
		ReturnType: ValueTypeVector, DropsMetricName: true,
		Call: func(_ *evalContext, args []Value) (Value, error) {
			toNearest := 1.0
			if len(args) > 1 {
				toNearest = float64(args[1].(Scalar))
			}
			in := args[0].(Vector)
			out := make(Vector, 0, len(in))
			for _, s := range in {
				v := math.Floor(s.V/toNearest+0.5) * toNearest
				out = append(out, VectorSample{Labels: s.Labels, T: s.T, V: v})
			}
			return out, nil
		},
	})

	clamp := func(name string, apply func(v, bound float64) float64) *Function {
		return &Function{
			Name:     name,
			ArgTypes: []ValueType{ValueTypeVector, ValueTypeScalar},
			// Clamping does not change what the metric measures, so the name
			// is kept.
			ReturnType: ValueTypeVector,
			Call: func(_ *evalContext, args []Value) (Value, error) {
				bound := float64(args[1].(Scalar))
				in := args[0].(Vector)
				out := make(Vector, 0, len(in))
				for _, s := range in {
					out = append(out, VectorSample{Labels: s.Labels, T: s.T, V: apply(s.V, bound)})
				}
				return out, nil
			},
		}
	}
	reg(clamp("clamp_min", math.Max))
	reg(clamp("clamp_max", math.Min))

	reg(&Function{
		Name: "scalar", ArgTypes: []ValueType{ValueTypeVector},
		ReturnType: ValueTypeScalar,
		Call: func(_ *evalContext, args []Value) (Value, error) {
			in := args[0].(Vector)
			// Defined only for a single-element vector; anything else is NaN
			// rather than an error, so a dashboard panel degrades instead of
			// breaking.
			if len(in) != 1 {
				return Scalar(math.NaN()), nil
			}
			return Scalar(in[0].V), nil
		},
	})

	reg(&Function{
		Name: "vector", ArgTypes: []ValueType{ValueTypeScalar},
		ReturnType: ValueTypeVector,
		Call: func(ctx *evalContext, args []Value) (Value, error) {
			return Vector{{
				Labels: model.EmptyLabels(),
				T:      ctx.timestamp,
				V:      float64(args[0].(Scalar)),
			}}, nil
		},
	})

	reg(&Function{
		Name: "time", ArgTypes: nil, ReturnType: ValueTypeScalar,
		Call: func(ctx *evalContext, _ []Value) (Value, error) {
			return Scalar(float64(ctx.timestamp) / 1000), nil
		},
	})

	reg(&Function{
		Name: "timestamp", ArgTypes: []ValueType{ValueTypeVector},
		ReturnType: ValueTypeVector, DropsMetricName: true,
		Call: func(_ *evalContext, args []Value) (Value, error) {
			in := args[0].(Vector)
			out := make(Vector, 0, len(in))
			for _, s := range in {
				out = append(out, VectorSample{Labels: s.Labels, T: s.T, V: float64(s.T) / 1000})
			}
			return out, nil
		},
	})

	reg(&Function{
		Name: "absent", ArgTypes: []ValueType{ValueTypeVector},
		ReturnType: ValueTypeVector, DropsMetricName: true,
		Call: func(ctx *evalContext, args []Value) (Value, error) {
			if len(args[0].(Vector)) > 0 {
				return Vector{}, nil
			}
			return Vector{{Labels: model.EmptyLabels(), T: ctx.timestamp, V: 1}}, nil
		},
	})

	reg(&Function{
		Name:     "label_replace",
		ArgTypes: []ValueType{ValueTypeVector, ValueTypeString, ValueTypeString, ValueTypeString, ValueTypeString},
		// Not implemented as a regexp rewrite yet; the parser accepts it so
		// that queries using it fail with a clear message rather than
		// "unknown function".
		ReturnType: ValueTypeVector,
		Call: func(_ *evalContext, _ []Value) (Value, error) {
			return nil, errUnimplemented("label_replace")
		},
	})

	reg(&Function{
		Name:       "histogram_quantile",
		ArgTypes:   []ValueType{ValueTypeScalar, ValueTypeVector},
		ReturnType: ValueTypeVector, DropsMetricName: true,
		Call: func(_ *evalContext, _ []Value) (Value, error) {
			return nil, errUnimplemented("histogram_quantile")
		},
	})
}

// variance computes the population variance with Welford's algorithm, which
// stays numerically stable where the sum-of-squares form loses all its
// significant digits on values of similar magnitude.
func variance(samples []model.Sample) float64 {
	var (
		count float64
		mean  float64
		m2    float64
	)
	for _, s := range samples {
		count++
		delta := s.V - mean
		mean += delta / count
		m2 += delta * (s.V - mean)
	}
	if count == 0 {
		return math.NaN()
	}
	return m2 / count
}

// rangeSeries is one series' samples inside the evaluation window.
type rangeSeries struct {
	labels  model.Labels
	samples []model.Sample

	// rangeStart and rangeEnd are the window the samples were selected for,
	// which the rate functions need in order to extrapolate.
	rangeStart, rangeEnd int64
}

// rangeFunc adapts a per-series reduction into a Function.Call.
func rangeFunc(f func(*evalContext, rangeSeries) (float64, bool)) func(*evalContext, []Value) (Value, error) {
	return func(ctx *evalContext, args []Value) (Value, error) {
		m := args[0].(Matrix)
		out := make(Vector, 0, len(m))
		for _, s := range m {
			v, ok := f(ctx, rangeSeries{
				labels:     s.Labels,
				samples:    s.Samples,
				rangeStart: s.RangeStart,
				rangeEnd:   s.RangeEnd,
			})
			if !ok {
				continue
			}
			out = append(out, VectorSample{Labels: s.Labels, T: ctx.timestamp, V: v})
		}
		return out, nil
	}
}

// extrapolatedRate implements rate, increase and delta.
//
// The extrapolation matters and is the part most people get wrong. Samples
// rarely land exactly on the window boundaries, so the observed change covers
// slightly less than the requested window; scaling it up to the full window is
// what makes rate() comparable across series with different scrape phases.
// The extrapolation is capped at half a sample interval beyond the outermost
// samples, so a series that stopped reporting does not have its last known
// rate projected indefinitely.
func extrapolatedRate(ctx *evalContext, s rangeSeries, isCounter, isRate bool) (float64, bool) {
	// Two samples are the minimum from which any change can be observed.
	if len(s.samples) < 2 {
		return 0, false
	}

	first, last := s.samples[0], s.samples[len(s.samples)-1]

	var delta float64
	if isCounter {
		// A counter reset shows up as a drop. The counter climbed to the
		// pre-reset value and then restarted from zero, so that value is
		// added back and the plain end-minus-start difference accounts for
		// the rest.
		//
		// For 0, 5, 10, 2, 7 the reset contributes 10 and the difference
		// contributes 7, giving 17 - which is the 10 counted before the reset
		// plus the 7 after it.
		prev := first.V
		for _, smpl := range s.samples[1:] {
			if smpl.V < prev {
				delta += prev
			}
			prev = smpl.V
		}
		delta += last.V - first.V
	} else {
		delta = last.V - first.V
	}

	sampledRange := float64(last.T - first.T)
	if sampledRange == 0 {
		return 0, false
	}
	windowRange := float64(s.rangeEnd - s.rangeStart)

	// Average interval between the samples we did see.
	avgInterval := sampledRange / float64(len(s.samples)-1)

	// The gap at each end is filled in one of two ways. If it is about one
	// scrape interval or less, the series was almost certainly reporting
	// throughout and the whole gap is covered - this is the case that makes a
	// regularly-scraped counter return its exact rate rather than one
	// systematically biased low. If the gap is larger, the series was
	// probably not reporting, and only half an interval is assumed rather
	// than projecting a stale rate across a hole in the data.
	//
	// The 1.1 factor is slack for scrape jitter: a gap of exactly one
	// interval plus a few milliseconds should still count as "reporting".
	threshold := avgInterval * 1.1

	extrapolateTo := sampledRange
	toStart := float64(first.T - s.rangeStart)
	toEnd := float64(s.rangeEnd - last.T)

	if toStart < threshold {
		extrapolateTo += toStart
	} else {
		extrapolateTo += avgInterval / 2
	}
	if toEnd < threshold {
		extrapolateTo += toEnd
	} else {
		extrapolateTo += avgInterval / 2
	}

	if sampledRange > 0 {
		delta *= extrapolateTo / sampledRange
	}
	if isRate {
		if windowRange <= 0 {
			return 0, false
		}
		delta /= windowRange / 1000 // per second
	}
	return delta, true
}

// instantRate implements irate and idelta from the last two samples only.
func instantRate(s rangeSeries, isRate bool) (float64, bool) {
	if len(s.samples) < 2 {
		return 0, false
	}
	last := s.samples[len(s.samples)-1]
	prev := s.samples[len(s.samples)-2]

	delta := last.V - prev.V
	if isRate && delta < 0 {
		// A counter reset: the increase since the reset is the new value.
		delta = last.V
	}
	if !isRate {
		return delta, true
	}

	elapsed := float64(last.T-prev.T) / 1000
	if elapsed <= 0 {
		return 0, false
	}
	return delta / elapsed, true
}
