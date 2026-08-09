// Package api serves the HTTP interface: ingest, queries and status.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/navingamage/stratum/internal/buildinfo"
	"github.com/navingamage/stratum/internal/memtable"
	"github.com/navingamage/stratum/internal/model"
	"github.com/navingamage/stratum/internal/query"
	"github.com/navingamage/stratum/internal/tsdb"
)

// Options configures the API.
type Options struct {
	// MaxBodyBytes caps an ingest request. Zero selects 32MiB.
	MaxBodyBytes int64

	// Logger receives request and error logs.
	Logger *slog.Logger
}

func (o Options) withDefaults() Options {
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = 32 << 20
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

// API holds the handlers' dependencies.
type API struct {
	db     *tsdb.DB
	engine *query.Engine
	opts   Options
	log    *slog.Logger

	started time.Time

	// Counters for the status endpoint. Atomic rather than mutex-guarded
	// because they are incremented on every request and read almost never.
	samplesIngested atomic.Uint64
	queriesRun      atomic.Uint64
	queryErrors     atomic.Uint64
}

// New returns an API over a database and query engine.
func New(db *tsdb.DB, engine *query.Engine, opts Options) *API {
	o := opts.withDefaults()
	return &API{db: db, engine: engine, opts: o, log: o.Logger, started: time.Now()}
}

// Handler returns the HTTP routes.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v1/write", a.handleWrite)
	mux.HandleFunc("GET /api/v1/query", a.handleQuery)
	mux.HandleFunc("POST /api/v1/query", a.handleQuery)
	mux.HandleFunc("GET /api/v1/query_range", a.handleQueryRange)
	mux.HandleFunc("POST /api/v1/query_range", a.handleQueryRange)
	mux.HandleFunc("GET /api/v1/series", a.handleSeries)
	mux.HandleFunc("GET /api/v1/labels", a.handleLabels)
	mux.HandleFunc("GET /api/v1/label/{name}/values", a.handleLabelValues)
	mux.HandleFunc("GET /api/v1/status", a.handleStatus)
	mux.HandleFunc("GET /api/v1/functions", a.handleFunctions)
	mux.HandleFunc("GET /healthz", a.handleHealth)

	return a.withLogging(mux)
}

// withLogging records each request's outcome. It also recovers panics: a bug
// in one handler should return 500 for that request rather than take the
// process down and lose every in-flight write.
func (a *API) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		defer func() {
			if rec.status >= 500 {
				a.log.Error("request failed",
					"method", r.Method, "path", r.URL.Path,
					"status", rec.status, "elapsed", time.Since(start))
				return
			}
			a.log.Debug("request",
				"method", r.Method, "path", r.URL.Path,
				"status", rec.status, "elapsed", time.Since(start))
		}()

		defer func() {
			if v := recover(); v != nil {
				a.log.Error("handler panicked",
					"method", r.Method, "path", r.URL.Path, "panic", v)
				if !rec.wrote {
					http.Error(w, `{"status":"error","error":"internal error"}`,
						http.StatusInternalServerError)
					rec.status = http.StatusInternalServerError
				}
			}
		}()

		next.ServeHTTP(rec, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.wrote {
		return
	}
	r.status = code
	r.wrote = true
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	r.wrote = true
	return r.ResponseWriter.Write(b)
}

// response is the envelope every endpoint returns, so a client has one shape
// to handle whether the request succeeded or not.
type response struct {
	Status    string `json:"status"` // "success" or "error"
	Data      any    `json:"data,omitempty"`
	ErrorType string `json:"errorType,omitempty"`
	Error     string `json:"error,omitempty"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// The status line is already sent, so there is nothing to correct;
		// the client will see a truncated body and its own decode error.
		return
	}
}

func (a *API) writeError(w http.ResponseWriter, code int, kind string, err error) {
	writeJSON(w, code, response{Status: "error", ErrorType: kind, Error: err.Error()})
}

// classify maps an error to an HTTP status.
//
// The distinction is the whole point of having it: a malformed query is the
// caller's problem and must not page anyone, while a storage failure is ours
// and must.
func (a *API) classify(err error) (int, string) {
	switch {
	case errors.Is(err, query.ErrParse):
		return http.StatusBadRequest, "bad_data"
	case errors.Is(err, query.ErrInvalidRange):
		return http.StatusBadRequest, "bad_data"
	case errors.Is(err, query.ErrTooManySamples):
		return http.StatusUnprocessableEntity, "execution"
	case errors.Is(err, query.ErrTimeout):
		return http.StatusServiceUnavailable, "timeout"
	case query.IsUnimplemented(err):
		return http.StatusNotImplemented, "unimplemented"
	case errors.Is(err, tsdb.ErrClosed):
		return http.StatusServiceUnavailable, "unavailable"
	default:
		return http.StatusInternalServerError, "internal"
	}
}

// WriteRequest is the ingest payload.
type WriteRequest struct {
	Series []WriteSeries `json:"series"`
}

// WriteSeries is one series' worth of samples.
type WriteSeries struct {
	Labels  map[string]string `json:"labels"`
	Samples []model.Sample    `json:"samples"`
}

// WriteResponse reports what an ingest request achieved.
type WriteResponse struct {
	Accepted int      `json:"accepted"`
	Rejected int      `json:"rejected"`
	Errors   []string `json:"errors,omitempty"`
}

// handleWrite ingests samples.
//
// Partial success is reported rather than treated as failure. A batch is a
// convenience for the sender, not a transaction: rejecting a thousand good
// samples because one carried a bad label would make an agent retry the whole
// batch forever and never make progress.
func (a *API) handleWrite(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, a.opts.MaxBodyBytes)

	var req WriteRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		a.writeError(w, http.StatusBadRequest, "bad_data",
			fmt.Errorf("decoding the request body: %w", err))
		return
	}

	app := a.db.Appender()
	out := WriteResponse{}

	for i, s := range req.Series {
		if len(s.Labels) == 0 {
			out.Rejected += len(s.Samples)
			out.Errors = appendErr(out.Errors, fmt.Sprintf("series %d has no labels", i))
			continue
		}
		ls := model.FromMap(s.Labels)
		if err := ls.Validate(); err != nil {
			out.Rejected += len(s.Samples)
			out.Errors = appendErr(out.Errors, fmt.Sprintf("series %d: %v", i, err))
			continue
		}

		var ref model.SeriesRef
		for _, smpl := range s.Samples {
			newRef, err := app.Append(ref, ls, smpl.T, smpl.V)
			if err != nil {
				out.Rejected++
				out.Errors = appendErr(out.Errors, fmt.Sprintf("series %d at %d: %v", i, smpl.T, err))
				continue
			}
			// Reusing the ref turns every subsequent append for this series
			// into an integer lookup instead of a label-set hash.
			ref = newRef
			out.Accepted++
		}
	}

	if err := app.Commit(); err != nil {
		// A commit failure after the appends means the samples reached the
		// log but not the head, or not even that. Either way the caller
		// should retry, so it is an error rather than a partial success.
		if errors.Is(err, memtable.ErrOutOfBounds) {
			// A concurrent flush raised the floor. Report it as a rejection
			// rather than a server fault: retrying will not help, the data is
			// simply too old now.
			a.writeError(w, http.StatusBadRequest, "bad_data", err)
			return
		}
		a.writeError(w, http.StatusInternalServerError, "internal", err)
		return
	}

	a.samplesIngested.Add(uint64(out.Accepted))
	writeJSON(w, http.StatusOK, response{Status: "success", Data: out})
}

// appendErr keeps the reported error list bounded. A malformed batch of a
// million samples would otherwise produce a million-line response.
func appendErr(errs []string, msg string) []string {
	const max = 20
	if len(errs) >= max {
		return errs
	}
	if len(errs) == max-1 {
		return append(errs, "... further errors suppressed")
	}
	return append(errs, msg)
}

// QueryResult is what the query endpoints return.
type QueryResult struct {
	ResultType string      `json:"resultType"`
	Result     any         `json:"result"`
	Stats      query.Stats `json:"stats"`
}

func (a *API) handleQuery(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.writeError(w, http.StatusBadRequest, "bad_data", err)
		return
	}

	expr, err := query.Parse(r.FormValue("query"))
	if err != nil {
		a.writeError(w, http.StatusBadRequest, "bad_data", err)
		return
	}

	ts, err := parseTimeParam(r.FormValue("time"), time.Now())
	if err != nil {
		a.writeError(w, http.StatusBadRequest, "bad_data", err)
		return
	}

	a.queriesRun.Add(1)
	res, err := a.engine.Instant(r.Context(), a.db, expr, ts)
	if err != nil {
		a.queryErrors.Add(1)
		code, kind := a.classify(err)
		a.writeError(w, code, kind, err)
		return
	}

	writeJSON(w, http.StatusOK, response{Status: "success", Data: QueryResult{
		ResultType: res.Value.Type().String(),
		Result:     res.Value,
		Stats:      res.Stats,
	}})
}

func (a *API) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.writeError(w, http.StatusBadRequest, "bad_data", err)
		return
	}

	expr, err := query.Parse(r.FormValue("query"))
	if err != nil {
		a.writeError(w, http.StatusBadRequest, "bad_data", err)
		return
	}

	now := time.Now()
	start, err := parseTimeParam(r.FormValue("start"), now.Add(-time.Hour))
	if err != nil {
		a.writeError(w, http.StatusBadRequest, "bad_data", fmt.Errorf("start: %w", err))
		return
	}
	end, err := parseTimeParam(r.FormValue("end"), now)
	if err != nil {
		a.writeError(w, http.StatusBadRequest, "bad_data", fmt.Errorf("end: %w", err))
		return
	}
	step, err := parseDurationParam(r.FormValue("step"), 15*time.Second)
	if err != nil {
		a.writeError(w, http.StatusBadRequest, "bad_data", fmt.Errorf("step: %w", err))
		return
	}

	a.queriesRun.Add(1)
	res, err := a.engine.Range(r.Context(), a.db, expr, start, end, step)
	if err != nil {
		a.queryErrors.Add(1)
		code, kind := a.classify(err)
		a.writeError(w, code, kind, err)
		return
	}

	writeJSON(w, http.StatusOK, response{Status: "success", Data: QueryResult{
		ResultType: res.Value.Type().String(),
		Result:     res.Value,
		Stats:      res.Stats,
	}})
}

func (a *API) handleSeries(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.writeError(w, http.StatusBadRequest, "bad_data", err)
		return
	}

	matches := r.Form["match[]"]
	if len(matches) == 0 {
		a.writeError(w, http.StatusBadRequest, "bad_data",
			errors.New("at least one match[] parameter is required"))
		return
	}

	now := time.Now()
	start, err := parseTimeParam(r.FormValue("start"), now.Add(-time.Hour))
	if err != nil {
		a.writeError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	end, err := parseTimeParam(r.FormValue("end"), now)
	if err != nil {
		a.writeError(w, http.StatusBadRequest, "bad_data", err)
		return
	}

	q, err := a.db.Querier(start, end)
	if err != nil {
		code, kind := a.classify(err)
		a.writeError(w, code, kind, err)
		return
	}
	defer q.Close()

	seen := make(map[string]model.Labels)
	for _, m := range matches {
		expr, err := query.Parse(m)
		if err != nil {
			a.writeError(w, http.StatusBadRequest, "bad_data", err)
			return
		}
		vs, ok := expr.(*query.VectorSelector)
		if !ok {
			a.writeError(w, http.StatusBadRequest, "bad_data",
				fmt.Errorf("match[] must be a series selector, got %q", m))
			return
		}

		set := q.Select(vs.Matchers...)
		for set.Next() {
			ls := set.At().Labels()
			seen[ls.String()] = ls
		}
		if err := set.Err(); err != nil {
			code, kind := a.classify(err)
			a.writeError(w, code, kind, err)
			return
		}
	}

	out := make([]model.Labels, 0, len(seen))
	for _, ls := range seen {
		out = append(out, ls)
	}
	writeJSON(w, http.StatusOK, response{Status: "success", Data: out})
}

func (a *API) handleLabels(w http.ResponseWriter, r *http.Request) {
	q, err := a.db.Querier(model.MinTime, model.MaxTime)
	if err != nil {
		code, kind := a.classify(err)
		a.writeError(w, code, kind, err)
		return
	}
	defer q.Close()

	names, err := q.LabelNames()
	if err != nil {
		code, kind := a.classify(err)
		a.writeError(w, code, kind, err)
		return
	}
	if names == nil {
		names = []string{}
	}
	writeJSON(w, http.StatusOK, response{Status: "success", Data: names})
}

func (a *API) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !model.IsValidLabelName(name) {
		a.writeError(w, http.StatusBadRequest, "bad_data",
			fmt.Errorf("%q is not a valid label name", name))
		return
	}

	q, err := a.db.Querier(model.MinTime, model.MaxTime)
	if err != nil {
		code, kind := a.classify(err)
		a.writeError(w, code, kind, err)
		return
	}
	defer q.Close()

	values, err := q.LabelValues(name)
	if err != nil {
		code, kind := a.classify(err)
		a.writeError(w, code, kind, err)
		return
	}
	if values == nil {
		values = []string{}
	}
	writeJSON(w, http.StatusOK, response{Status: "success", Data: values})
}

// StatusResponse is the operational summary.
type StatusResponse struct {
	Version string        `json:"version"`
	Uptime  time.Duration `json:"uptime"`

	Storage tsdb.Stats `json:"storage"`

	SamplesIngested uint64 `json:"samplesIngested"`
	QueriesRun      uint64 `json:"queriesRun"`
	QueryErrors     uint64 `json:"queryErrors"`

	Blocks []blockSummary `json:"blocks"`
}

type blockSummary struct {
	ID      string `json:"id"`
	MinTime int64  `json:"minTime"`
	MaxTime int64  `json:"maxTime"`
	Level   int    `json:"level"`
	Series  uint64 `json:"series"`
	Samples uint64 `json:"samples"`
}

func (a *API) handleStatus(w http.ResponseWriter, r *http.Request) {
	blocks := a.db.Blocks()
	summaries := make([]blockSummary, 0, len(blocks))
	for _, b := range blocks {
		summaries = append(summaries, blockSummary{
			ID:      b.ID.String(),
			MinTime: b.MinTime,
			MaxTime: b.MaxTime,
			Level:   b.Compaction.Level,
			Series:  b.Stats.NumSeries,
			Samples: b.Stats.NumSamples,
		})
	}

	writeJSON(w, http.StatusOK, response{Status: "success", Data: StatusResponse{
		Version:         buildinfo.String(),
		Uptime:          time.Since(a.started).Truncate(time.Second),
		Storage:         a.db.Stats(),
		SamplesIngested: a.samplesIngested.Load(),
		QueriesRun:      a.queriesRun.Load(),
		QueryErrors:     a.queryErrors.Load(),
		Blocks:          summaries,
	}})
}

func (a *API) handleFunctions(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, response{Status: "success", Data: query.FunctionNames()})
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "ok")
}

// parseTimeParam accepts either epoch seconds (possibly fractional) or
// RFC 3339, matching what monitoring tooling tends to send.
func parseTimeParam(s string, fallback time.Time) (int64, error) {
	if s == "" {
		return fallback.UnixMilli(), nil
	}
	if secs, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(secs * 1000), nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0, fmt.Errorf("%q is neither epoch seconds nor RFC 3339", s)
	}
	return t.UnixMilli(), nil
}

// parseDurationParam accepts a bare number of seconds or a duration literal.
func parseDurationParam(s string, fallback time.Duration) (int64, error) {
	if s == "" {
		return fallback.Milliseconds(), nil
	}
	if secs, err := strconv.ParseFloat(s, 64); err == nil {
		if secs <= 0 {
			return 0, fmt.Errorf("must be positive, got %s", s)
		}
		return int64(secs * 1000), nil
	}
	ms, err := query.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return 0, err
	}
	if ms <= 0 {
		return 0, fmt.Errorf("must be positive, got %s", s)
	}
	return ms, nil
}
