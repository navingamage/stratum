package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/navingamage/stratum/internal/query"
	"github.com/navingamage/stratum/internal/tsdb"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	db, err := tsdb.Open(t.TempDir(), tsdb.Options{
		NoWAL:              true,
		BackgroundInterval: time.Hour,
		SamplesPerChunk:    8,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("opening the database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	a := New(db, query.NewEngine(query.EngineOptions{}),
		Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})

	srv := httptest.NewServer(a.Handler())
	t.Cleanup(srv.Close)
	return srv
}

// post sends a JSON body and returns the status and decoded envelope.
func post(t *testing.T, srv *httptest.Server, path, body string) (int, response) {
	t.Helper()
	resp, err := srv.Client().Post(srv.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()

	var out response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding the response to POST %s: %v", path, err)
	}
	return resp.StatusCode, out
}

func get(t *testing.T, srv *httptest.Server, path string, params url.Values) (int, response) {
	t.Helper()
	u := srv.URL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	resp, err := srv.Client().Get(u)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
	}
	defer resp.Body.Close()

	var out response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding the response to GET %s: %v", u, err)
	}
	return resp.StatusCode, out
}

// seed writes a small corpus and returns the timestamp of the last sample.
func seed(t *testing.T, srv *httptest.Server) int64 {
	t.Helper()

	const base = int64(1_700_000_000_000)
	var series []string
	for h := 0; h < 3; h++ {
		var samples []string
		for i := 0; i < 10; i++ {
			samples = append(samples, fmt.Sprintf(`{"t":%d,"v":%d}`, base+int64(i)*1000, h*100+i))
		}
		series = append(series, fmt.Sprintf(
			`{"labels":{"__name__":"cpu","host":"web-%d","job":"api"},"samples":[%s]}`,
			h, strings.Join(samples, ",")))
	}
	body := fmt.Sprintf(`{"series":[%s]}`, strings.Join(series, ","))

	code, out := post(t, srv, "/api/v1/write", body)
	if code != http.StatusOK {
		t.Fatalf("seeding returned %d: %+v", code, out)
	}
	return base + 9000
}

func TestWriteAndQuery(t *testing.T) {
	srv := newTestServer(t)
	last := seed(t, srv)

	code, out := get(t, srv, "/api/v1/query", url.Values{
		"query": {"cpu"},
		"time":  {fmt.Sprintf("%.3f", float64(last)/1000)},
	})
	if code != http.StatusOK {
		t.Fatalf("query returned %d: %+v", code, out)
	}
	if out.Status != "success" {
		t.Fatalf("status = %q: %+v", out.Status, out)
	}

	var qr struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Labels []struct{ Name, Value string } `json:"labels"`
			V      float64                        `json:"v"`
		} `json:"result"`
		Stats query.Stats `json:"stats"`
	}
	raw, _ := json.Marshal(out.Data)
	if err := json.Unmarshal(raw, &qr); err != nil {
		t.Fatalf("decoding the result: %v", err)
	}

	if qr.ResultType != "instant vector" {
		t.Errorf("resultType = %q", qr.ResultType)
	}
	if len(qr.Result) != 3 {
		t.Fatalf("got %d series, want 3", len(qr.Result))
	}
	if qr.Stats.SeriesSelected != 3 {
		t.Errorf("stats report %d series selected, want 3", qr.Stats.SeriesSelected)
	}
}

func TestWriteReportsPartialSuccess(t *testing.T) {
	srv := newTestServer(t)

	// One good series, one with an unusable label name.
	body := `{"series":[
		{"labels":{"__name__":"ok"},"samples":[{"t":1000,"v":1},{"t":2000,"v":2}]},
		{"labels":{"not a name":"x"},"samples":[{"t":1000,"v":1}]},
		{"labels":{},"samples":[{"t":1000,"v":1}]}
	]}`

	code, out := post(t, srv, "/api/v1/write", body)
	if code != http.StatusOK {
		t.Fatalf("write returned %d: %+v", code, out)
	}

	var wr WriteResponse
	raw, _ := json.Marshal(out.Data)
	if err := json.Unmarshal(raw, &wr); err != nil {
		t.Fatal(err)
	}

	// A batch is a convenience for the sender, not a transaction: rejecting
	// the good samples because of a bad neighbour would make an agent retry
	// forever without making progress.
	if wr.Accepted != 2 {
		t.Errorf("Accepted = %d, want 2", wr.Accepted)
	}
	if wr.Rejected != 2 {
		t.Errorf("Rejected = %d, want 2", wr.Rejected)
	}
	if len(wr.Errors) != 2 {
		t.Errorf("Errors = %v, want two entries", wr.Errors)
	}
}

func TestWriteRejectsMalformedBody(t *testing.T) {
	srv := newTestServer(t)

	for _, body := range []string{
		`not json`,
		`{"series":[{"labels":{"a":"b"},"unknown":1}]}`,
		``,
	} {
		code, out := post(t, srv, "/api/v1/write", body)
		if code != http.StatusBadRequest {
			t.Errorf("POST %q returned %d, want 400 (%+v)", body, code, out)
		}
		if out.ErrorType != "bad_data" {
			t.Errorf("errorType = %q, want bad_data", out.ErrorType)
		}
	}
}

// TestErrorClassification is the property that decides whether a failure pages
// anyone: a bad query is the caller's problem, a storage failure is ours.
func TestErrorClassification(t *testing.T) {
	srv := newTestServer(t)
	seed(t, srv)

	cases := []struct {
		name     string
		query    string
		wantCode int
		wantKind string
	}{
		{"syntax error", `cpu{`, http.StatusBadRequest, "bad_data"},
		{"type error", `rate(cpu)`, http.StatusBadRequest, "bad_data"},
		{"unknown function", `nosuchfn(cpu)`, http.StatusBadRequest, "bad_data"},
		{"empty query", ``, http.StatusBadRequest, "bad_data"},
		{"unimplemented", `histogram_quantile(0.9, cpu)`, http.StatusNotImplemented, "unimplemented"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out := get(t, srv, "/api/v1/query", url.Values{"query": {tc.query}})
			if code != tc.wantCode {
				t.Errorf("status = %d, want %d (%+v)", code, tc.wantCode, out)
			}
			if out.ErrorType != tc.wantKind {
				t.Errorf("errorType = %q, want %q", out.ErrorType, tc.wantKind)
			}
			if out.Error == "" {
				t.Error("the error message is empty")
			}
		})
	}
}

func TestQueryRange(t *testing.T) {
	srv := newTestServer(t)
	last := seed(t, srv)

	code, out := get(t, srv, "/api/v1/query_range", url.Values{
		"query": {"cpu"},
		"start": {fmt.Sprintf("%.3f", float64(last-9000)/1000)},
		"end":   {fmt.Sprintf("%.3f", float64(last)/1000)},
		"step":  {"3"},
	})
	if code != http.StatusOK {
		t.Fatalf("range query returned %d: %+v", code, out)
	}

	var qr struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Samples []struct{ T int64 } `json:"samples"`
		} `json:"result"`
	}
	raw, _ := json.Marshal(out.Data)
	if err := json.Unmarshal(raw, &qr); err != nil {
		t.Fatal(err)
	}
	if qr.ResultType != "range vector" {
		t.Errorf("resultType = %q", qr.ResultType)
	}
	if len(qr.Result) != 3 {
		t.Fatalf("got %d series, want 3", len(qr.Result))
	}
	// start..end in 3s steps over a 9s span is 4 points.
	if n := len(qr.Result[0].Samples); n != 4 {
		t.Errorf("got %d points per series, want 4", n)
	}
}

func TestQueryRangeRejectsBadParameters(t *testing.T) {
	srv := newTestServer(t)
	seed(t, srv)

	cases := []struct {
		name   string
		params url.Values
	}{
		{"zero step", url.Values{"query": {"cpu"}, "start": {"0"}, "end": {"10"}, "step": {"0"}}},
		{"negative step", url.Values{"query": {"cpu"}, "start": {"0"}, "end": {"10"}, "step": {"-1"}}},
		{"end before start", url.Values{"query": {"cpu"}, "start": {"10"}, "end": {"0"}, "step": {"1"}}},
		{"unparseable time", url.Values{"query": {"cpu"}, "start": {"yesterday"}, "end": {"10"}, "step": {"1"}}},
		{"too many steps", url.Values{"query": {"cpu"}, "start": {"0"}, "end": {"1000000"}, "step": {"0.001"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out := get(t, srv, "/api/v1/query_range", tc.params)
			if code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400 (%+v)", code, out)
			}
		})
	}
}

func TestTimeParameterFormats(t *testing.T) {
	srv := newTestServer(t)
	last := seed(t, srv)

	// Epoch seconds and RFC 3339 must select the same instant.
	epoch := fmt.Sprintf("%.3f", float64(last)/1000)
	rfc := time.UnixMilli(last).UTC().Format(time.RFC3339Nano)

	var results []string
	for _, ts := range []string{epoch, rfc} {
		code, out := get(t, srv, "/api/v1/query", url.Values{"query": {"cpu"}, "time": {ts}})
		if code != http.StatusOK {
			t.Fatalf("time=%q returned %d: %+v", ts, code, out)
		}
		// Only the result is compared. The envelope also carries timing
		// statistics, which differ between any two runs by construction.
		var qr struct {
			Result json.RawMessage `json:"result"`
		}
		raw, _ := json.Marshal(out.Data)
		if err := json.Unmarshal(raw, &qr); err != nil {
			t.Fatal(err)
		}
		results = append(results, string(qr.Result))
	}
	if results[0] != results[1] {
		t.Errorf("epoch and RFC 3339 timestamps gave different results:\n  %s\n  %s",
			results[0], results[1])
	}
}

func TestLabelsAndValues(t *testing.T) {
	srv := newTestServer(t)
	seed(t, srv)

	code, out := get(t, srv, "/api/v1/labels", nil)
	if code != http.StatusOK {
		t.Fatalf("labels returned %d", code)
	}
	var names []string
	raw, _ := json.Marshal(out.Data)
	json.Unmarshal(raw, &names)
	if len(names) != 3 {
		t.Errorf("LabelNames = %v, want three names", names)
	}

	code, out = get(t, srv, "/api/v1/label/host/values", nil)
	if code != http.StatusOK {
		t.Fatalf("label values returned %d", code)
	}
	var values []string
	raw, _ = json.Marshal(out.Data)
	json.Unmarshal(raw, &values)
	if len(values) != 3 {
		t.Errorf("values of host = %v, want three", values)
	}

	// An invalid label name is a client error, not an empty result.
	code, _ = get(t, srv, "/api/v1/label/not%20a%20name/values", nil)
	if code != http.StatusBadRequest {
		t.Errorf("an invalid label name returned %d, want 400", code)
	}
}

func TestLabelsOnEmptyDatabaseReturnsAnArray(t *testing.T) {
	srv := newTestServer(t)

	// An empty JSON array, not null: a client iterating the result should not
	// have to special-case the empty case.
	resp, err := srv.Client().Get(srv.URL + "/api/v1/labels")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"data":[]`) {
		t.Errorf("empty labels response = %s, want an empty array", body)
	}
}

func TestSeriesEndpoint(t *testing.T) {
	srv := newTestServer(t)
	seed(t, srv)

	code, out := get(t, srv, "/api/v1/series", url.Values{
		"match[]": {`cpu{job="api"}`},
		"start":   {"1699999999"},
		"end":     {"1700000010"},
	})
	if code != http.StatusOK {
		t.Fatalf("series returned %d: %+v", code, out)
	}
	var series []any
	raw, _ := json.Marshal(out.Data)
	json.Unmarshal(raw, &series)
	if len(series) != 3 {
		t.Errorf("got %d series, want 3", len(series))
	}

	// match[] is required.
	code, _ = get(t, srv, "/api/v1/series", nil)
	if code != http.StatusBadRequest {
		t.Errorf("a request without match[] returned %d, want 400", code)
	}

	// match[] must be a selector, not an arbitrary expression.
	code, _ = get(t, srv, "/api/v1/series", url.Values{"match[]": {`sum(cpu)`}})
	if code != http.StatusBadRequest {
		t.Errorf("a non-selector match[] returned %d, want 400", code)
	}
}

func TestStatusAndHealth(t *testing.T) {
	srv := newTestServer(t)
	seed(t, srv)
	get(t, srv, "/api/v1/query", url.Values{"query": {"cpu"}})

	code, out := get(t, srv, "/api/v1/status", nil)
	if code != http.StatusOK {
		t.Fatalf("status returned %d", code)
	}
	var st StatusResponse
	raw, _ := json.Marshal(out.Data)
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	if st.SamplesIngested != 30 {
		t.Errorf("SamplesIngested = %d, want 30", st.SamplesIngested)
	}
	if st.QueriesRun == 0 {
		t.Error("QueriesRun = 0")
	}
	if st.Storage.Head.Series != 3 {
		t.Errorf("head series = %d, want 3", st.Storage.Head.Series)
	}
	if st.Version == "" {
		t.Error("Version is empty")
	}

	resp, err := srv.Client().Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz returned %d", resp.StatusCode)
	}
}

func TestFunctionsEndpoint(t *testing.T) {
	srv := newTestServer(t)
	code, out := get(t, srv, "/api/v1/functions", nil)
	if code != http.StatusOK {
		t.Fatalf("functions returned %d", code)
	}
	var fns []string
	raw, _ := json.Marshal(out.Data)
	json.Unmarshal(raw, &fns)

	if len(fns) < 20 {
		t.Errorf("got %d functions, want at least 20", len(fns))
	}
	found := false
	for _, f := range fns {
		if f == "rate" {
			found = true
		}
	}
	if !found {
		t.Error("rate is missing from the function list")
	}
}

func TestMethodNotAllowed(t *testing.T) {
	srv := newTestServer(t)

	// The write endpoint is POST-only.
	resp, err := srv.Client().Get(srv.URL + "/api/v1/write")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET /api/v1/write returned %d, want 405", resp.StatusCode)
	}
}

func TestQueryAcceptsPost(t *testing.T) {
	srv := newTestServer(t)
	seed(t, srv)

	// Long queries exceed what a URL can carry, so the endpoint takes a form
	// body as well.
	resp, err := srv.Client().Post(srv.URL+"/api/v1/query",
		"application/x-www-form-urlencoded",
		strings.NewReader(url.Values{"query": {"cpu"}}.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("POST query returned %d: %s", resp.StatusCode, body)
	}
}

func TestParseTimeParam(t *testing.T) {
	fallback := time.UnixMilli(1234)

	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{"", 1234, false},
		{"1700000000", 1_700_000_000_000, false},
		{"1700000000.5", 1_700_000_000_500, false},
		{"2023-11-14T22:13:20Z", 1_700_000_000_000, false},
		{"tomorrow", 0, true},
	}
	for _, tc := range cases {
		got, err := parseTimeParam(tc.in, fallback)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseTimeParam(%q) succeeded, want an error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseTimeParam(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseTimeParam(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestParseDurationParam(t *testing.T) {
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{"", 15_000, false},
		{"30", 30_000, false},
		{"1m", 60_000, false},
		{"1h30m", 5_400_000, false},
		{"0", 0, true},
		{"-5", 0, true},
		{"soon", 0, true},
	}
	for _, tc := range cases {
		got, err := parseDurationParam(tc.in, 15*time.Second)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseDurationParam(%q) succeeded, want an error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseDurationParam(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseDurationParam(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestAppendErrIsBounded(t *testing.T) {
	// A malformed batch of a million samples must not produce a
	// million-line response.
	var errs []string
	for i := 0; i < 1000; i++ {
		errs = appendErr(errs, fmt.Sprintf("error %d", i))
	}
	if len(errs) > 20 {
		t.Errorf("the error list grew to %d entries", len(errs))
	}
	if errs[len(errs)-1] != "... further errors suppressed" {
		t.Errorf("the truncation is not signalled: %q", errs[len(errs)-1])
	}
}
