package query

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/navingamage/stratum/internal/model"
	"github.com/navingamage/stratum/internal/tsdb"
	"github.com/navingamage/stratum/internal/wal"
)

// testDB builds a database from a compact textual description, so that a test
// reads as the data it is about rather than as thirty lines of appends.
//
// Each line is `<labels> <v1> <v2> ...`, with samples placed at interval
// milliseconds starting from zero. A value of `_` leaves a gap.
func testDB(t *testing.T, interval int64, spec string) *tsdb.DB {
	t.Helper()

	db, err := tsdb.Open(t.TempDir(), tsdb.Options{
		NoWAL:              true,
		WALSync:            wal.SyncNever,
		BackgroundInterval: time.Hour,
		SamplesPerChunk:    8,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("opening the database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	app := db.Appender()
	for _, line := range strings.Split(strings.TrimSpace(spec), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		ls := parseTestLabels(t, fields[0])

		for i, f := range fields[1:] {
			if f == "_" {
				continue
			}
			var v float64
			if _, err := fmt.Sscanf(f, "%g", &v); err != nil {
				t.Fatalf("parsing value %q in %q: %v", f, line, err)
			}
			if _, err := app.Append(0, ls, int64(i)*interval, v); err != nil {
				t.Fatalf("appending %s at %d: %v", ls, int64(i)*interval, err)
			}
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("committing: %v", err)
	}
	return db
}

// parseTestLabels reads `name{a="b",c="d"}` into a label set.
func parseTestLabels(t *testing.T, s string) model.Labels {
	t.Helper()

	name, rest, hasBrace := strings.Cut(s, "{")
	pairs := []string{model.MetricName, name}
	if hasBrace {
		rest = strings.TrimSuffix(rest, "}")
		for _, kv := range strings.Split(rest, ",") {
			if kv == "" {
				continue
			}
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				t.Fatalf("malformed label %q in %q", kv, s)
			}
			pairs = append(pairs, k, strings.Trim(v, `"`))
		}
	}
	return model.FromStrings(pairs...)
}

func evalInstant(t *testing.T, db *tsdb.DB, q string, ts int64) Value {
	t.Helper()
	expr, err := Parse(q)
	if err != nil {
		t.Fatalf("Parse(%q): %v", q, err)
	}
	res, err := NewEngine(EngineOptions{}).Instant(context.Background(), db, expr, ts)
	if err != nil {
		t.Fatalf("evaluating %q: %v", q, err)
	}
	return res.Value
}

// vectorMap flattens a vector into label string to value.
func vectorMap(t *testing.T, v Value) map[string]float64 {
	t.Helper()
	vec, ok := v.(Vector)
	if !ok {
		t.Fatalf("got a %s, want an instant vector", v.Type())
	}
	out := make(map[string]float64, len(vec))
	for _, s := range vec {
		out[s.Labels.String()] = s.V
	}
	return out
}

func approx(a, b float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	if math.IsInf(a, 0) || math.IsInf(b, 0) {
		return a == b
	}
	return math.Abs(a-b) < 1e-9
}

func TestEvalVectorSelector(t *testing.T) {
	db := testDB(t, 1000, `
		cpu{host="a"} 1 2 3 4 5
		cpu{host="b"} 10 20 30 40 50
		mem{host="a"} 100 200 300 400 500
	`)

	got := vectorMap(t, evalInstant(t, db, `cpu`, 4000))
	want := map[string]float64{
		`cpu{host="a"}`: 5,
		`cpu{host="b"}`: 50,
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, v := range want {
		if !approx(got[k], v) {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}

	// A matcher narrows the result.
	got = vectorMap(t, evalInstant(t, db, `cpu{host="a"}`, 4000))
	if len(got) != 1 || !approx(got[`cpu{host="a"}`], 5) {
		t.Errorf("cpu{host=\"a\"} = %v", got)
	}
}

// TestEvalLookback covers the rule that makes instant queries work at all:
// samples land at scrape times, not at the evaluation instant, so the selector
// takes the most recent sample within the lookback window.
func TestEvalLookback(t *testing.T) {
	db := testDB(t, 1000, `
		cpu{host="a"} 1 2 3
	`)

	// Between samples: the most recent one at or before the instant.
	if got := vectorMap(t, evalInstant(t, db, `cpu`, 1500)); !approx(got[`cpu{host="a"}`], 2) {
		t.Errorf("at t=1500 got %v, want 2", got)
	}
	// Exactly on a sample.
	if got := vectorMap(t, evalInstant(t, db, `cpu`, 2000)); !approx(got[`cpu{host="a"}`], 3) {
		t.Errorf("at t=2000 got %v, want 3", got)
	}
	// Just inside the lookback window after the last sample.
	if got := vectorMap(t, evalInstant(t, db, `cpu`, 2000+DefaultLookbackDelta-1)); len(got) != 1 {
		t.Errorf("just inside the lookback window the series vanished: %v", got)
	}
	// Past it: the series is gone rather than reported as stale-but-present.
	if got := vectorMap(t, evalInstant(t, db, `cpu`, 2000+DefaultLookbackDelta+1)); len(got) != 0 {
		t.Errorf("past the lookback window the series is still reported: %v", got)
	}
	// Before any sample.
	if got := vectorMap(t, evalInstant(t, db, `cpu`, 0-1)); len(got) != 0 {
		t.Errorf("before the first sample got %v", got)
	}
}

func TestEvalOffset(t *testing.T) {
	db := testDB(t, 1000, `
		cpu{host="a"} 1 2 3 4 5
	`)
	// At t=4000 with a 2s offset, the value is the one from t=2000.
	got := vectorMap(t, evalInstant(t, db, `cpu offset 2s`, 4000))
	if !approx(got[`cpu{host="a"}`], 3) {
		t.Errorf("cpu offset 2s at t=4000 = %v, want 3", got)
	}
}

func TestEvalMatrixSelector(t *testing.T) {
	db := testDB(t, 1000, `
		cpu{host="a"} 1 2 3 4 5
	`)

	expr := MustParse(`cpu[3s]`)
	res, err := NewEngine(EngineOptions{}).Instant(context.Background(), db, expr, 4000)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := res.Value.(Matrix)
	if !ok {
		t.Fatalf("got a %s, want a range vector", res.Value.Type())
	}
	if len(m) != 1 {
		t.Fatalf("got %d series, want 1", len(m))
	}

	// The window is half-open at the start: (1000, 4000] gives t=2000..4000.
	want := []model.Sample{{T: 2000, V: 3}, {T: 3000, V: 4}, {T: 4000, V: 5}}
	if len(m[0].Samples) != len(want) {
		t.Fatalf("got %v, want %v", m[0].Samples, want)
	}
	for i := range want {
		if m[0].Samples[i] != want[i] {
			t.Errorf("sample %d = %v, want %v", i, m[0].Samples[i], want[i])
		}
	}
}

func TestEvalRate(t *testing.T) {
	// A counter climbing by 10 every second.
	db := testDB(t, 1000, `
		requests{job="api"} 0 10 20 30 40 50
	`)

	got := vectorMap(t, evalInstant(t, db, `rate(requests[5s])`, 5000))
	if len(got) != 1 {
		t.Fatalf("got %v, want one series", got)
	}
	// rate() drops the metric name, so the key is the remaining labels.
	//
	// The exact 10 is the point of the extrapolation. The window (0, 5000]
	// holds only the samples at 1000..5000: a 4s span carrying a change of
	// 40. Scaling that up to the full 5s window is legitimate because the gap
	// to the window start is one scrape interval, so the series was certainly
	// reporting through it. Without the extrapolation the answer would be a
	// systematically low 8.
	v := got[`{job="api"}`]
	if !approx(v, 10) {
		t.Errorf("rate = %v, want 10 per second", v)
	}
}

// TestEvalRateExtrapolationIsCapped covers the other half of the rule: when a
// series stops reporting, its last known rate must not be projected across the
// hole that leaves.
func TestEvalRateExtrapolationIsCapped(t *testing.T) {
	db := testDB(t, 1000, `
		requests{job="api"} 0 10 20 30
	`)

	// A 20s window at t=20000 contains all four samples, but nine tenths of
	// it is empty. The result must be far below the 10/s the samples show.
	got := vectorMap(t, evalInstant(t, db, `rate(requests[20s])`, 20000))
	v := got[`{job="api"}`]
	if v >= 10 {
		t.Errorf("rate over a mostly-empty window = %v; a stale rate is being projected", v)
	}
	if v <= 0 {
		t.Errorf("rate = %v, want a positive value", v)
	}
}

func TestEvalRateHandlesCounterResets(t *testing.T) {
	// The counter resets between 30 and 5.
	db := testDB(t, 1000, `
		requests{job="api"} 0 10 20 30 5 15
	`)

	got := vectorMap(t, evalInstant(t, db, `increase(requests[5s])`, 5000))
	// Real increase: 0->30 is 30, then the reset, then 5->15 within the new
	// run. The sample at t=0 is excluded by the half-open window, so the
	// observed span is 10..15 across (0, 5000].
	v := got[`{job="api"}`]
	if v <= 0 {
		t.Errorf("increase across a counter reset = %v, want a positive value", v)
	}
	// Without reset handling the naive difference would be 15 - 10 = 5.
	if v < 20 {
		t.Errorf("increase = %v; the counter reset does not appear to be accounted for", v)
	}
}

func TestEvalRateNeedsTwoSamples(t *testing.T) {
	db := testDB(t, 1000, `
		requests{job="api"} 5
	`)
	got := vectorMap(t, evalInstant(t, db, `rate(requests[5s])`, 0))
	if len(got) != 0 {
		t.Errorf("rate over a single sample returned %v; a rate needs two points", got)
	}
}

func TestEvalIrate(t *testing.T) {
	db := testDB(t, 1000, `
		requests{job="api"} 0 10 20 100
	`)
	// irate uses only the last two samples: (100 - 20) / 1s.
	got := vectorMap(t, evalInstant(t, db, `irate(requests[4s])`, 3000))
	if !approx(got[`{job="api"}`], 80) {
		t.Errorf("irate = %v, want 80", got[`{job="api"}`])
	}
}

func TestEvalOverTimeFunctions(t *testing.T) {
	db := testDB(t, 1000, `
		v{host="a"} 1 2 3 4 5
	`)

	// Evaluated at t=4000, a [5s] window is (-1000, 4000], so every sample
	// from t=0 onwards is inside it: values 1, 2, 3, 4, 5.
	cases := []struct {
		q    string
		want float64
	}{
		{`avg_over_time(v[5s])`, 3},
		{`sum_over_time(v[5s])`, 15},
		{`min_over_time(v[5s])`, 1},
		{`max_over_time(v[5s])`, 5},
		{`count_over_time(v[5s])`, 5},
		{`last_over_time(v[5s])`, 5},
		{`first_over_time(v[5s])`, 1},
		{`stddev_over_time(v[5s])`, math.Sqrt(2)},
		{`stdvar_over_time(v[5s])`, 2},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			got := vectorMap(t, evalInstant(t, db, tc.q, 4000))
			if len(got) != 1 {
				t.Fatalf("got %v, want one series", got)
			}
			for _, v := range got {
				if !approx(v, tc.want) {
					t.Errorf("%s = %v, want %v", tc.q, v, tc.want)
				}
			}
		})
	}

	// And a window that genuinely excludes the first sample: at t=4000 a [4s]
	// window is (0, 4000], so t=0 falls outside it. The half-open start is
	// what keeps adjacent windows from counting a sample twice.
	t.Run("half-open at the start", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `count_over_time(v[4s])`, 4000))
		for _, v := range got {
			if !approx(v, 4) {
				t.Errorf("count_over_time(v[4s]) = %v, want 4", v)
			}
		}
	})
}

func TestEvalAggregations(t *testing.T) {
	db := testDB(t, 1000, `
		cpu{host="a",dc="eu"} 1
		cpu{host="b",dc="eu"} 2
		cpu{host="c",dc="us"} 3
		cpu{host="d",dc="us"} 4
	`)

	t.Run("sum", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `sum(cpu)`, 0))
		if len(got) != 1 || !approx(got["{}"], 10) {
			t.Errorf("sum(cpu) = %v, want 10", got)
		}
	})

	t.Run("sum by", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `sum by (dc) (cpu)`, 0))
		want := map[string]float64{`{dc="eu"}`: 3, `{dc="us"}`: 7}
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for k, v := range want {
			if !approx(got[k], v) {
				t.Errorf("%s = %v, want %v", k, got[k], v)
			}
		}
	})

	t.Run("sum without", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `sum without (host) (cpu)`, 0))
		want := map[string]float64{`{dc="eu"}`: 3, `{dc="us"}`: 7}
		for k, v := range want {
			if !approx(got[k], v) {
				t.Errorf("%s = %v, want %v", k, got[k], v)
			}
		}
	})

	for _, tc := range []struct {
		q    string
		want float64
	}{
		{`avg(cpu)`, 2.5},
		{`min(cpu)`, 1},
		{`max(cpu)`, 4},
		{`count(cpu)`, 4},
		{`stdvar(cpu)`, 1.25},
		{`stddev(cpu)`, math.Sqrt(1.25)},
		{`group(cpu)`, 1},
	} {
		t.Run(tc.q, func(t *testing.T) {
			got := vectorMap(t, evalInstant(t, db, tc.q, 0))
			if !approx(got["{}"], tc.want) {
				t.Errorf("%s = %v, want %v", tc.q, got["{}"], tc.want)
			}
		})
	}

	t.Run("topk keeps original labels", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `topk(2, cpu)`, 0))
		if len(got) != 2 {
			t.Fatalf("topk(2) returned %d series, want 2", len(got))
		}
		// The two largest, still identifiable by their own labels.
		if !approx(got[`cpu{dc="us", host="d"}`], 4) || !approx(got[`cpu{dc="us", host="c"}`], 3) {
			t.Errorf("topk(2, cpu) = %v", got)
		}
	})

	t.Run("bottomk", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `bottomk(1, cpu)`, 0))
		if len(got) != 1 || !approx(got[`cpu{dc="eu", host="a"}`], 1) {
			t.Errorf("bottomk(1, cpu) = %v", got)
		}
	})

	t.Run("quantile interpolates", func(t *testing.T) {
		// Values 1,2,3,4: the median is the mean of the middle pair.
		got := vectorMap(t, evalInstant(t, db, `quantile(0.5, cpu)`, 0))
		if !approx(got["{}"], 2.5) {
			t.Errorf("quantile(0.5, cpu) = %v, want 2.5", got["{}"])
		}
	})
}

func TestEvalBinaryScalar(t *testing.T) {
	db := testDB(t, 1000, `cpu{host="a"} 4`)

	cases := []struct {
		q    string
		want float64
	}{
		{`1 + 2`, 3},
		{`10 - 3`, 7},
		{`3 * 4`, 12},
		{`10 / 4`, 2.5},
		{`10 % 3`, 1},
		{`2 ^ 10`, 1024},
		{`1 / 0`, math.Inf(1)},
		{`1 == bool 1`, 1},
		{`1 == bool 2`, 0},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			v := evalInstant(t, db, tc.q, 0)
			s, ok := v.(Scalar)
			if !ok {
				t.Fatalf("got a %s, want a scalar", v.Type())
			}
			if !approx(float64(s), tc.want) {
				t.Errorf("%s = %v, want %v", tc.q, float64(s), tc.want)
			}
		})
	}
}

func TestEvalBinaryVectorScalar(t *testing.T) {
	db := testDB(t, 1000, `
		cpu{host="a"} 10
		cpu{host="b"} 20
	`)

	t.Run("arithmetic drops the metric name", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `cpu * 2`, 0))
		if !approx(got[`{host="a"}`], 20) || !approx(got[`{host="b"}`], 40) {
			t.Errorf("cpu * 2 = %v", got)
		}
	})

	t.Run("scalar on the left", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `100 - cpu`, 0))
		if !approx(got[`{host="a"}`], 90) || !approx(got[`{host="b"}`], 80) {
			t.Errorf("100 - cpu = %v", got)
		}
	})

	t.Run("comparison filters and keeps the name", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `cpu > 15`, 0))
		if len(got) != 1 {
			t.Fatalf("cpu > 15 returned %v, want one series", got)
		}
		if !approx(got[`cpu{host="b"}`], 20) {
			t.Errorf("cpu > 15 = %v; a filtering comparison keeps the metric name and value", got)
		}
	})

	t.Run("bool comparison yields 0 or 1", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `cpu > bool 15`, 0))
		if len(got) != 2 {
			t.Fatalf("cpu > bool 15 returned %v, want both series", got)
		}
		if !approx(got[`{host="a"}`], 0) || !approx(got[`{host="b"}`], 1) {
			t.Errorf("cpu > bool 15 = %v", got)
		}
	})
}

func TestEvalBinaryVectorVector(t *testing.T) {
	db := testDB(t, 1000, `
		errors{job="api",inst="1"} 5
		errors{job="api",inst="2"} 10
		total{job="api",inst="1"} 100
		total{job="api",inst="2"} 200
		total{job="api",inst="3"} 300
	`)

	t.Run("matches on all labels but the name", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `errors / total`, 0))
		// inst=3 has no errors counterpart and drops out.
		if len(got) != 2 {
			t.Fatalf("errors / total = %v, want two series", got)
		}
		if !approx(got[`{inst="1", job="api"}`], 0.05) {
			t.Errorf("inst=1 ratio = %v, want 0.05", got[`{inst="1", job="api"}`])
		}
		if !approx(got[`{inst="2", job="api"}`], 0.05) {
			t.Errorf("inst=2 ratio = %v, want 0.05", got[`{inst="2", job="api"}`])
		}
	})

	t.Run("on restricts the matching labels", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `errors / on (inst) total`, 0))
		if len(got) != 2 {
			t.Fatalf("got %v, want two series", got)
		}
		// The result carries only the matching labels.
		if !approx(got[`{inst="1"}`], 0.05) {
			t.Errorf("got %v", got)
		}
	})

	t.Run("duplicate matches are an error", func(t *testing.T) {
		// Matching on job alone leaves two candidates on each side.
		expr := MustParse(`errors / on (job) total`)
		_, err := NewEngine(EngineOptions{}).Instant(context.Background(), db, expr, 0)
		if err == nil {
			t.Fatal("an ambiguous match succeeded, want an error")
		}
		if !strings.Contains(err.Error(), "many-to-many") {
			t.Errorf("error = %v, want it to mention the ambiguity", err)
		}
	})
}

func TestEvalSetOperators(t *testing.T) {
	db := testDB(t, 1000, `
		a{k="1"} 1
		a{k="2"} 2
		b{k="2"} 20
		b{k="3"} 30
	`)

	t.Run("and", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `a and b`, 0))
		if len(got) != 1 || !approx(got[`a{k="2"}`], 2) {
			t.Errorf("a and b = %v, want a{k=\"2\"}", got)
		}
	})

	t.Run("unless", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `a unless b`, 0))
		if len(got) != 1 || !approx(got[`a{k="1"}`], 1) {
			t.Errorf("a unless b = %v, want a{k=\"1\"}", got)
		}
	})

	t.Run("or", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `a or b`, 0))
		if len(got) != 3 {
			t.Fatalf("a or b = %v, want three series", got)
		}
		// The left side wins where both have a series.
		if !approx(got[`a{k="2"}`], 2) {
			t.Errorf("or preferred the right side for k=2: %v", got)
		}
	})
}

func TestEvalFunctions(t *testing.T) {
	db := testDB(t, 1000, `
		v{host="a"} -4
		v{host="b"} 9
	`)

	cases := []struct {
		q    string
		key  string
		want float64
	}{
		{`abs(v)`, `{host="a"}`, 4},
		{`ceil(v)`, `{host="a"}`, -4},
		{`floor(v)`, `{host="a"}`, -4},
		{`sqrt(v)`, `{host="b"}`, 3},
		{`sgn(v)`, `{host="a"}`, -1},
		{`clamp_min(v, 0)`, `v{host="a"}`, 0},
		{`clamp_max(v, 0)`, `v{host="b"}`, 0},
		{`round(v)`, `{host="b"}`, 9},
		{`ln(v)`, `{host="b"}`, math.Log(9)},
		{`log2(v)`, `{host="b"}`, math.Log2(9)},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			got := vectorMap(t, evalInstant(t, db, tc.q, 0))
			if !approx(got[tc.key], tc.want) {
				t.Errorf("%s[%s] = %v, want %v (full: %v)", tc.q, tc.key, got[tc.key], tc.want, got)
			}
		})
	}

	t.Run("scalar", func(t *testing.T) {
		// Defined only for a one-element vector.
		v := evalInstant(t, db, `scalar(v{host="a"})`, 0)
		if !approx(float64(v.(Scalar)), -4) {
			t.Errorf("scalar = %v, want -4", v)
		}
		v = evalInstant(t, db, `scalar(v)`, 0)
		if !math.IsNaN(float64(v.(Scalar))) {
			t.Errorf("scalar of a two-element vector = %v, want NaN", v)
		}
	})

	t.Run("vector", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `vector(42)`, 1234))
		if len(got) != 1 || !approx(got["{}"], 42) {
			t.Errorf("vector(42) = %v", got)
		}
	})

	t.Run("time", func(t *testing.T) {
		v := evalInstant(t, db, `time()`, 5000)
		if !approx(float64(v.(Scalar)), 5) {
			t.Errorf("time() at t=5000 = %v, want 5 seconds", v)
		}
	})

	t.Run("absent", func(t *testing.T) {
		got := vectorMap(t, evalInstant(t, db, `absent(nosuch{a="b"})`, 0))
		if len(got) != 1 || !approx(got["{}"], 1) {
			t.Errorf("absent of a missing series = %v, want 1", got)
		}
		got = vectorMap(t, evalInstant(t, db, `absent(v)`, 0))
		if len(got) != 0 {
			t.Errorf("absent of a present series = %v, want nothing", got)
		}
	})
}

func TestEvalUnimplementedFunction(t *testing.T) {
	db := testDB(t, 1000, `v{a="b"} 1`)

	expr := MustParse(`histogram_quantile(0.9, v)`)
	_, err := NewEngine(EngineOptions{}).Instant(context.Background(), db, expr, 0)
	if err == nil {
		t.Fatal("expected an error")
	}
	// The distinction matters: the query is valid, this build cannot run it.
	if !IsUnimplemented(err) {
		t.Errorf("error = %v, want it to be reported as unimplemented", err)
	}
}

func TestRangeQuery(t *testing.T) {
	db := testDB(t, 1000, `
		cpu{host="a"} 1 2 3 4 5 6 7 8 9 10
	`)

	expr := MustParse(`cpu`)
	res, err := NewEngine(EngineOptions{}).Range(context.Background(), db, expr, 0, 9000, 3000)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	m, ok := res.Value.(Matrix)
	if !ok {
		t.Fatalf("got a %s, want a matrix", res.Value.Type())
	}
	if len(m) != 1 {
		t.Fatalf("got %d series, want 1", len(m))
	}

	want := []model.Sample{{T: 0, V: 1}, {T: 3000, V: 4}, {T: 6000, V: 7}, {T: 9000, V: 10}}
	if len(m[0].Samples) != len(want) {
		t.Fatalf("got %v, want %v", m[0].Samples, want)
	}
	for i := range want {
		if m[0].Samples[i] != want[i] {
			t.Errorf("step %d = %v, want %v", i, m[0].Samples[i], want[i])
		}
	}
	if res.Stats.Steps != 4 {
		t.Errorf("Steps = %d, want 4", res.Stats.Steps)
	}
}

// TestRangeQueryAgreesWithInstant is the property that makes a graph
// trustworthy: every point on it must equal the instant query at that
// timestamp.
func TestRangeQueryAgreesWithInstant(t *testing.T) {
	db := testDB(t, 1000, `
		requests{job="api"} 0 10 25 25 40 60 60 90 100 130
		requests{job="db"}  0 1 2 3 4 5 6 7 8 9
	`)

	queries := []string{
		`requests`,
		`rate(requests[5s])`,
		`sum(rate(requests[5s]))`,
		`avg_over_time(requests[3s])`,
		`requests > 5`,
		`requests * 2`,
	}
	engine := NewEngine(EngineOptions{})

	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			expr := MustParse(q)

			const (
				start = int64(2000)
				end   = int64(9000)
				step  = int64(1000)
			)
			res, err := engine.Range(context.Background(), db, expr, start, end, step)
			if err != nil {
				t.Fatalf("Range: %v", err)
			}
			m := res.Value.(Matrix)

			// Index the range result by (labels, timestamp).
			rangeVals := make(map[string]float64)
			for _, s := range m {
				for _, smpl := range s.Samples {
					rangeVals[fmt.Sprintf("%s@%d", s.Labels, smpl.T)] = smpl.V
				}
			}

			for ts := start; ts <= end; ts += step {
				inst, err := engine.Instant(context.Background(), db, expr, ts)
				if err != nil {
					t.Fatalf("Instant at %d: %v", ts, err)
				}
				vec, ok := inst.Value.(Vector)
				if !ok {
					t.Fatalf("instant query produced a %s", inst.Value.Type())
				}

				for _, s := range vec {
					key := fmt.Sprintf("%s@%d", s.Labels, ts)
					got, present := rangeVals[key]
					if !present {
						t.Fatalf("the range query is missing %s, which the instant query returned as %v",
							key, s.V)
					}
					if !approx(got, s.V) {
						t.Fatalf("%s: range says %v, instant says %v", key, got, s.V)
					}
					delete(rangeVals, key)
				}
			}
			if len(rangeVals) != 0 {
				t.Errorf("the range query returned %d points no instant query produced: %v",
					len(rangeVals), rangeVals)
			}
		})
	}
}

func TestRangeQueryRejectsBadParameters(t *testing.T) {
	db := testDB(t, 1000, `cpu{host="a"} 1`)
	engine := NewEngine(EngineOptions{})

	cases := []struct {
		name             string
		q                string
		start, end, step int64
	}{
		{"zero step", `cpu`, 0, 1000, 0},
		{"negative step", `cpu`, 0, 1000, -1},
		{"end before start", `cpu`, 1000, 0, 100},
		{"too many steps", `cpu`, 0, 1_000_000_000, 1},
		{"range vector result", `cpu[5s]`, 0, 1000, 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expr := MustParse(tc.q)
			_, err := engine.Range(context.Background(), db, expr, tc.start, tc.end, tc.step)
			if !errors.Is(err, ErrInvalidRange) {
				t.Errorf("Range = %v, want ErrInvalidRange", err)
			}
		})
	}
}

func TestSampleLimit(t *testing.T) {
	db := testDB(t, 1000, `
		v{host="a"} 1 2 3 4 5 6 7 8 9 10
		v{host="b"} 1 2 3 4 5 6 7 8 9 10
	`)

	engine := NewEngine(EngineOptions{MaxSamples: 5})
	expr := MustParse(`v[20s]`)
	_, err := engine.Instant(context.Background(), db, expr, 10_000)
	if !errors.Is(err, ErrTooManySamples) {
		t.Errorf("a query over the sample limit = %v, want ErrTooManySamples", err)
	}
}

func TestQueryTimeout(t *testing.T) {
	db := testDB(t, 1000, `v{host="a"} 1 2 3`)

	engine := NewEngine(EngineOptions{Timeout: time.Nanosecond})
	expr := MustParse(`v`)

	// The deadline has certainly passed by the time evaluation starts.
	time.Sleep(time.Millisecond)
	_, err := engine.Instant(context.Background(), db, expr, 2000)
	if err == nil {
		t.Fatal("expected a timeout")
	}
	if !errors.Is(err, ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want a timeout", err)
	}
}

func TestQueryStats(t *testing.T) {
	db := testDB(t, 1000, `
		cpu{host="a"} 1 2 3 4 5
		cpu{host="b"} 1 2 3 4 5
	`)

	expr := MustParse(`cpu`)
	res, err := NewEngine(EngineOptions{}).Instant(context.Background(), db, expr, 4000)
	if err != nil {
		t.Fatal(err)
	}
	if res.Stats.SeriesSelected != 2 {
		t.Errorf("SeriesSelected = %d, want 2", res.Stats.SeriesSelected)
	}
	if res.Stats.SamplesScanned == 0 {
		t.Error("SamplesScanned = 0; the scan cost should be reported")
	}
	// Elapsed is deliberately not asserted to be non-zero. This query runs in
	// well under a millisecond, and Windows' wall clock has a granularity of
	// roughly 15ms, so a zero here is the clock's answer rather than a missing
	// measurement.
	if res.Stats.Elapsed < 0 {
		t.Errorf("Elapsed = %v, want a non-negative duration", res.Stats.Elapsed)
	}
}

func TestEmptyResults(t *testing.T) {
	db := testDB(t, 1000, `cpu{host="a"} 1`)

	for _, q := range []string{
		`nosuch{a="b"}`,
		`sum(nosuch{a="b"})`,
		`rate(nosuch{a="b"}[5m])`,
		`nosuch{a="b"} + cpu`,
	} {
		t.Run(q, func(t *testing.T) {
			v := evalInstant(t, db, q, 0)
			vec, ok := v.(Vector)
			if !ok {
				t.Fatalf("got a %s", v.Type())
			}
			if len(vec) != 0 {
				t.Errorf("%s returned %v, want nothing", q, vec)
			}
		})
	}
}

func BenchmarkInstantQuery(b *testing.B) {
	db, err := tsdb.Open(b.TempDir(), tsdb.Options{
		NoWAL:              true,
		BackgroundInterval: time.Hour,
		SamplesPerChunk:    120,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const (
		series  = 1000
		samples = 240
	)
	app := db.Appender()
	for j := 0; j < samples; j++ {
		for i := 0; i < series; i++ {
			ls := model.FromStrings(
				model.MetricName, "http_requests_total",
				"job", "api",
				"instance", fmt.Sprintf("i-%04d", i),
			)
			if _, err := app.Append(0, ls, int64(j)*15_000, float64(i*j)); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := app.Commit(); err != nil {
		b.Fatal(err)
	}

	engine := NewEngine(EngineOptions{})
	ts := int64(samples-1) * 15_000

	for _, q := range []string{
		`http_requests_total`,
		`sum(http_requests_total)`,
		`rate(http_requests_total[5m])`,
		`sum by (job) (rate(http_requests_total[5m]))`,
	} {
		expr := MustParse(q)
		b.Run(q, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := engine.Instant(context.Background(), db, expr, ts); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
