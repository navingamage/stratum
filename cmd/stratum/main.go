// Command stratum is a command-line client for a stratum server: a one-shot
// query runner and an interactive shell.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/navingamage/stratum/internal/buildinfo"
	"github.com/navingamage/stratum/internal/query"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "stratum: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		server      string
		timeout     time.Duration
		asJSON      bool
		showStats   bool
		showVersion bool
		rangeFrom   string
		rangeTo     string
		step        string
	)

	fs := flag.NewFlagSet("stratum", flag.ContinueOnError)
	fs.StringVar(&server, "server", "http://localhost:9090", "address of the stratum server")
	fs.DurationVar(&timeout, "timeout", time.Minute, "request timeout")
	fs.BoolVar(&asJSON, "json", false, "print raw JSON instead of a table")
	fs.BoolVar(&showStats, "stats", false, "print query statistics")
	fs.StringVar(&rangeFrom, "from", "", "start of a range query, e.g. -1h or an RFC 3339 timestamp")
	fs.StringVar(&rangeTo, "to", "", "end of a range query; defaults to now")
	fs.StringVar(&step, "step", "15s", "step of a range query")
	fs.BoolVar(&showVersion, "version", false, "print the version and exit")

	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), `%s

Usage:
  stratum [flags] '<query>'    run one query and exit
  stratum [flags]              start an interactive shell

Flags:
`, buildinfo.String())
		fs.PrintDefaults()
		fmt.Fprintf(fs.Output(), `
Examples:
  stratum 'up'
  stratum 'sum by (job) (rate(http_requests_total[5m]))'
  stratum -from -1h -step 1m 'rate(http_requests_total[5m])'
  stratum -json 'up' | jq .
`)
	}

	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if showVersion {
		fmt.Println(buildinfo.String())
		return nil
	}

	c := &client{
		base: strings.TrimRight(server, "/"),
		http: &http.Client{Timeout: timeout},
	}

	opts := printOptions{asJSON: asJSON, stats: showStats}

	if fs.NArg() > 0 {
		q := strings.Join(fs.Args(), " ")
		if rangeFrom != "" {
			return c.runRange(q, rangeFrom, rangeTo, step, opts)
		}
		return c.runInstant(q, opts)
	}
	return c.repl(opts)
}

type client struct {
	base string
	http *http.Client
}

type apiResponse struct {
	Status    string          `json:"status"`
	Data      json.RawMessage `json:"data"`
	ErrorType string          `json:"errorType"`
	Error     string          `json:"error"`
}

func (c *client) get(path string, params url.Values) (json.RawMessage, error) {
	u := c.base + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	resp, err := c.http.Get(u)
	if err != nil {
		return nil, fmt.Errorf("contacting the server: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, fmt.Errorf("reading the response: %w", err)
	}

	var out apiResponse
	if err := json.Unmarshal(body, &out); err != nil {
		// A non-JSON body means something other than the API answered - a
		// proxy, or the wrong port. Showing the status and a snippet is far
		// more useful than a JSON decode error.
		snippet := strings.TrimSpace(string(body))
		if len(snippet) > 200 {
			snippet = snippet[:200] + "..."
		}
		return nil, fmt.Errorf("unexpected response (%s): %s", resp.Status, snippet)
	}
	if out.Status != "success" {
		return nil, fmt.Errorf("%s: %s", out.ErrorType, out.Error)
	}
	return out.Data, nil
}

type printOptions struct {
	asJSON bool
	stats  bool
}

type queryResult struct {
	ResultType string          `json:"resultType"`
	Result     json.RawMessage `json:"result"`
	Stats      query.Stats     `json:"stats"`
}

func (c *client) runInstant(q string, opts printOptions) error {
	data, err := c.get("/api/v1/query", url.Values{"query": {q}})
	if err != nil {
		return err
	}
	return printResult(data, opts)
}

func (c *client) runRange(q, from, to, step string, opts printOptions) error {
	params := url.Values{"query": {q}, "step": {step}}

	start, err := resolveTime(from)
	if err != nil {
		return fmt.Errorf("-from: %w", err)
	}
	params.Set("start", start)

	if to != "" {
		end, err := resolveTime(to)
		if err != nil {
			return fmt.Errorf("-to: %w", err)
		}
		params.Set("end", end)
	}

	data, err := c.get("/api/v1/query_range", params)
	if err != nil {
		return err
	}
	return printResult(data, opts)
}

// resolveTime accepts a relative offset like -1h, an RFC 3339 timestamp, or
// epoch seconds, and returns epoch seconds for the API.
func resolveTime(s string) (string, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "-") || strings.HasPrefix(s, "+") {
		d, err := time.ParseDuration(s)
		if err != nil {
			return "", fmt.Errorf("%q is not a duration: %w", s, err)
		}
		return strconv.FormatFloat(float64(time.Now().Add(d).UnixMilli())/1000, 'f', 3, 64), nil
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return s, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return "", fmt.Errorf("%q is neither a relative duration, epoch seconds, nor RFC 3339", s)
	}
	return strconv.FormatInt(t.Unix(), 10), nil
}

func printResult(data json.RawMessage, opts printOptions) error {
	if opts.asJSON {
		var buf bytes.Buffer
		if err := json.Indent(&buf, data, "", "  "); err != nil {
			return err
		}
		fmt.Println(buf.String())
		return nil
	}

	var qr queryResult
	if err := json.Unmarshal(data, &qr); err != nil {
		return fmt.Errorf("decoding the result: %w", err)
	}

	switch qr.ResultType {
	case "instant vector":
		if err := printVector(qr.Result); err != nil {
			return err
		}
	case "range vector":
		if err := printMatrix(qr.Result); err != nil {
			return err
		}
	case "scalar":
		var v float64
		if err := json.Unmarshal(qr.Result, &v); err != nil {
			return err
		}
		fmt.Println(strconv.FormatFloat(v, 'g', -1, 64))
	default:
		fmt.Println(string(qr.Result))
	}

	if opts.stats {
		fmt.Fprintf(os.Stderr, "\n%d series, %d samples scanned, %d steps, %v\n",
			qr.Stats.SeriesSelected, qr.Stats.SamplesScanned, qr.Stats.Steps, qr.Stats.Elapsed)
	}
	return nil
}

type jsonLabel struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type jsonVectorSample struct {
	Labels []jsonLabel `json:"labels"`
	T      int64       `json:"t"`
	V      float64     `json:"v"`
}

type jsonMatrixSeries struct {
	Labels  []jsonLabel `json:"labels"`
	Samples []struct {
		T int64   `json:"t"`
		V float64 `json:"v"`
	} `json:"samples"`
}

func renderLabels(ls []jsonLabel) string {
	var name string
	parts := make([]string, 0, len(ls))
	for _, l := range ls {
		if l.Name == "__name__" {
			name = l.Value
			continue
		}
		parts = append(parts, l.Name+"="+strconv.Quote(l.Value))
	}
	sort.Strings(parts)
	if len(parts) == 0 {
		if name == "" {
			return "{}"
		}
		return name
	}
	return name + "{" + strings.Join(parts, ", ") + "}"
}

func printVector(raw json.RawMessage) error {
	var vec []jsonVectorSample
	if err := json.Unmarshal(raw, &vec); err != nil {
		return err
	}
	if len(vec) == 0 {
		fmt.Println("(no data)")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "SERIES\tVALUE\tTIMESTAMP")
	for _, s := range vec {
		fmt.Fprintf(w, "%s\t%s\t%s\n",
			renderLabels(s.Labels),
			strconv.FormatFloat(s.V, 'g', -1, 64),
			time.UnixMilli(s.T).UTC().Format(time.RFC3339))
	}
	return w.Flush()
}

func printMatrix(raw json.RawMessage) error {
	var m []jsonMatrixSeries
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	if len(m) == 0 {
		fmt.Println("(no data)")
		return nil
	}

	for i, s := range m {
		if i > 0 {
			fmt.Println()
		}
		fmt.Println(renderLabels(s.Labels))

		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		for _, smpl := range s.Samples {
			fmt.Fprintf(w, "  %s\t%s\n",
				time.UnixMilli(smpl.T).UTC().Format(time.RFC3339),
				strconv.FormatFloat(smpl.V, 'g', -1, 64))
		}
		if err := w.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// repl runs the interactive shell.
func (c *client) repl(opts printOptions) error {
	fmt.Printf("%s\nConnected to %s. Type \\h for help, \\q to quit.\n\n", buildinfo.String(), c.base)

	// Fail fast if the server is not reachable, rather than letting the user
	// type a query first and only then find out.
	if _, err := c.get("/api/v1/status", nil); err != nil {
		fmt.Fprintf(os.Stderr, "warning: %v\n\n", err)
	}

	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)

	for {
		fmt.Print("stratum> ")
		if !sc.Scan() {
			fmt.Println()
			return sc.Err()
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "\\") {
			done, err := c.command(line, &opts)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
			}
			if done {
				return nil
			}
			continue
		}

		if err := c.runInstant(line, opts); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}
	}
}

// command handles the backslash commands. The second result reports whether
// the shell should exit.
func (c *client) command(line string, opts *printOptions) (bool, error) {
	fields := strings.Fields(line)
	switch fields[0] {
	case "\\q", "\\quit":
		return true, nil

	case "\\h", "\\help":
		fmt.Print(`Commands:
  \h, \help            show this help
  \q, \quit            exit
  \l, \labels          list label names
  \v, \values <name>   list the values of a label
  \s, \status          show server status
  \f, \functions       list the query functions
  \json                toggle raw JSON output
  \stats               toggle query statistics

Anything else is evaluated as a query.
`)
		return false, nil

	case "\\json":
		opts.asJSON = !opts.asJSON
		fmt.Printf("json output %s\n", onOff(opts.asJSON))
		return false, nil

	case "\\stats":
		opts.stats = !opts.stats
		fmt.Printf("query statistics %s\n", onOff(opts.stats))
		return false, nil

	case "\\l", "\\labels":
		data, err := c.get("/api/v1/labels", nil)
		if err != nil {
			return false, err
		}
		return false, printStringList(data)

	case "\\v", "\\values":
		if len(fields) < 2 {
			return false, errors.New("usage: \\values <label name>")
		}
		data, err := c.get("/api/v1/label/"+url.PathEscape(fields[1])+"/values", nil)
		if err != nil {
			return false, err
		}
		return false, printStringList(data)

	case "\\f", "\\functions":
		data, err := c.get("/api/v1/functions", nil)
		if err != nil {
			return false, err
		}
		return false, printStringList(data)

	case "\\s", "\\status":
		data, err := c.get("/api/v1/status", nil)
		if err != nil {
			return false, err
		}
		var buf bytes.Buffer
		if err := json.Indent(&buf, data, "", "  "); err != nil {
			return false, err
		}
		fmt.Println(buf.String())
		return false, nil
	}

	return false, fmt.Errorf("unknown command %q; type \\h for help", fields[0])
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func printStringList(raw json.RawMessage) error {
	var items []string
	if err := json.Unmarshal(raw, &items); err != nil {
		return err
	}
	if len(items) == 0 {
		fmt.Println("(none)")
		return nil
	}
	for _, s := range items {
		fmt.Println(s)
	}
	return nil
}
