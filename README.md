# stratum

An embedded time-series database engine, written from scratch in Go with no
dependencies outside the standard library.

Samples land in a write-ahead log and an in-memory head block, get compressed
with delta-of-delta and XOR encoding, and are periodically flushed into
immutable on-disk blocks that a background compactor merges into larger time
ranges. Queries go through a PromQL-like language with its own lexer, parser,
type checker and evaluator.

```
$ stratum 'sum by (job) (rate(http_requests_total[5m]))'
SERIES          VALUE               TIMESTAMP
{job="api"}     2.907407407407408   2026-08-09T14:18:33Z
{job="worker"}  2.8962962962962964  2026-08-09T14:18:33Z
```

## Why

I wanted to understand how Prometheus's storage engine actually works, and
reading the source only gets you so far — you find out what you have not
understood when you try to build it and it does not work. Most of the
interesting parts of this project are in the commit messages and the comments,
which record why each design decision went the way it did, including the ones I
got wrong first.

It is a learning project, not a production database. See
[Limitations](#limitations).

## Getting started

```bash
go build ./cmd/...
./bin/stratumd -data-dir ./data
```

Write some samples:

```bash
curl -X POST localhost:9090/api/v1/write -d '{
  "series": [{
    "labels": {"__name__": "cpu_percent", "host": "web-1"},
    "samples": [{"t": 1786109637000, "v": 42.5}]
  }]
}'
```

Query them:

```bash
./bin/stratum 'cpu_percent'
./bin/stratum -from -1h -step 1m 'avg_over_time(cpu_percent[5m])'
./bin/stratum                      # interactive shell; \h for help
```

## Architecture

```
                writes
                   |
                   v
        +---------------------+
        |  write-ahead log    |   segmented, page-framed, CRC-32C
        |  internal/wal       |   torn-write recovery on replay
        +----------+----------+
                   |
                   v
        +---------------------+
        |  head block         |   in memory, writable
        |  internal/memtable  |   512-way sharded series map
        +----------+----------+   compressed chunks, inverted index
                   |
                   | flush, every block duration
                   v
        +---------------------+
        |  immutable blocks   |   meta.json + chunks + index
        |  internal/block     |   mmap'd, written once
        +----------+----------+
                   |
                   | compaction, 2h -> 6h -> 18h -> ...
                   v
        +---------------------+
        |  larger blocks      |
        +---------------------+

reads:  queries merge the head and every overlapping block through one
        interface, so the boundary is invisible above internal/tsdb
```

| Package | What it does |
|---|---|
| `internal/encoding` | Bit-level stream, checked byte buffers, CRC-32C |
| `internal/xxh` | XXH64, verified against the reference vectors |
| `internal/chunk` | Gorilla delta-of-delta and XOR sample encoding |
| `internal/model` | Label sets, matchers, samples |
| `internal/index` | Inverted index and lazy postings operators |
| `internal/wal` | Segmented write-ahead log |
| `internal/memtable` | The writable head block |
| `internal/block` | Immutable on-disk blocks |
| `internal/compact` | Head flush, leveled compaction, retention |
| `internal/tsdb` | Database facade, lifecycle, merged queries |
| `internal/query` | Lexer, parser, evaluator |
| `internal/api` | HTTP interface |

## Design notes

The decisions worth defending, and why.

**Compression is Gorilla with different bucket widths.** Timestamps are stored
as delta-of-delta and values as an XOR against the previous value. The paper
uses 7/9/12/32-bit buckets for the delta-of-delta, tuned for 64-second
resolution; stratum stores epoch milliseconds, where a second of scrape jitter
is already ±1000 and overflows a 7-bit bucket on nearly every sample. At 14
bits a two-bit prefix absorbs ±8s of drift, which covers essentially all real
jitter.

**The write-ahead log is page-framed for exactly one reason.** A process killed
mid-write leaves a partial record. Because every record carries its own length
and checksum inside a page of known size, replay can tell "the log ends here,
incompletely" — expected after any crash, recovered by dropping the tail — from
"bytes changed after they were written", which is data loss and gets reported
rather than silently swallowed. `TestTornWriteRecovery` truncates the log at
127 offsets and requires an exact prefix each time.

**Postings iterators stay lazy.** An intersection seeks each input to the
running maximum, so a ref missing from any list is skipped without visiting the
refs in between, and the cost tracks the size of the result rather than of the
inputs. Regular expressions that accept a finite set of literals are enumerated
at parse time and become a union of point lookups: on a label with 10,000
distinct values that is 480ns instead of 974µs.

**Blocks are immutable and reference counted.** A block is written under a
`.tmp` suffix and renamed into place only once every file is fsynced, so a
crash leaves either a complete block or a directory that startup deletes.
Because blocks are memory-mapped, unmapping one underneath a running query is
not a stale read — it is a segmentation fault. The database holds one
reference, each querier holds another, and compaction can delete a block's
files while a query keeps reading its mapping.

**Writes are durable before they are visible.** The appender buffers, writes
the WAL record, and only then makes samples readable. The other order would let
a query observe a sample that a crash a millisecond later erases — worse than
losing the write, because a caller can retry a lost write and nothing can
un-answer a query.

**`rate()` extrapolates, carefully.** Samples rarely land on the window
boundaries, so the observed change covers slightly less than the window asked
for. If the gap at either end is within about one scrape interval the whole gap
is filled in, which is what makes a regularly-scraped counter return its exact
rate rather than one biased low. Beyond that only half an interval is assumed,
so a series that stopped reporting does not get its last rate projected across
the hole.

Longer write-ups are in [`docs/`](docs/).

## Testing

```bash
make test        # race detector
make cover
make fuzz        # 30s per target; FUZZTIME=5m for a longer soak
make bench
```

Roughly 9,500 of the project's 22,800 lines are tests, covering 78.7% of
statements. The approaches that earned their place:

- **Differential testing.** The postings operators are checked against a
  brute-force oracle over 500 random inputs, with a separate pass targeting the
  `Seek` paths that full expansion never reaches. Matcher resolution is checked
  the same way against random corpora with labels omitted at random — the
  failure mode there is silently wrong results, not a crash.
- **Round-trip properties.** Every query is parsed, printed, and reparsed, with
  the two printed forms required to match. A precedence bug shows up
  immediately, because the reprint puts the parentheses where the tree really
  grouped.
- **Cross-tier agreement.** Every point of a range query is checked against the
  instant query at that timestamp. That is the property people rely on when a
  graph disagrees with a number.
- **Fuzzing.** Five targets. The parser fuzzer found a real crash: on invalid
  UTF-8, `DecodeRuneInString` returns `RuneError` after consuming one byte, but
  `RuneError` itself encodes as three, so backing up by the rune's own length
  drove the offset negative and panicked. Query text arrives straight from an
  HTTP request, so anyone could have reached it.
- **The race detector.** Three genuine bugs came from it, all of them cases
  where I had written a comment confidently asserting the code was safe.

## Measurements

Apple M-series, Go 1.26, `go test -bench`. These are single-machine numbers
from a laptop, not a benchmark of anything against anything.

**Compression**, 1000 samples against a raw 16-byte timestamp/value pair:

| Series shape | Bytes/sample | vs raw |
|---|---|---|
| Constant value, fixed interval | 0.27 | 59.7x |
| Integer-valued gauge, jittered interval | 4.37 | 3.7x |
| Full-entropy mantissa (worst case) | 8.74 | 1.8x |

That last row is the real floor: XOR encoding has nothing to exploit when every
mantissa is unrelated, and the entire saving comes from the timestamp side.

**Throughput and latency:**

| Operation | Result |
|---|---|
| Ingest, 500-series batch | 4.37M samples/sec |
| Chunk append | 9.7 ns, 0 allocs |
| Chunk iterate | 389 MB/s, 0 allocs |
| Label set hash (7 labels) | 39 ns, 0 allocs |
| XXH64, 256 bytes | 20.9 GB/s |
| Postings point lookup, 20k-series block | 44 ns |
| Selective intersection, 1-in-1,000,000 | 114 ns |
| Enumerable regexp vs value scan (10k values) | 479 ns vs 975 µs |
| Query parse (nested aggregation) | 2.7 µs |

## HTTP API

| Endpoint | Purpose |
|---|---|
| `POST /api/v1/write` | Ingest samples |
| `GET\|POST /api/v1/query` | Instant query |
| `GET\|POST /api/v1/query_range` | Range query |
| `GET /api/v1/series?match[]=` | Series matching a selector |
| `GET /api/v1/labels` | Label names |
| `GET /api/v1/label/{name}/values` | Label values |
| `GET /api/v1/status` | Storage and runtime status |
| `GET /api/v1/functions` | Supported query functions |
| `GET /healthz` | Liveness |

Status codes are chosen so a failure's cause is visible from the code alone: a
syntax or type error is 400, exceeding the sample budget is 422, a timeout is
503, a recognised-but-unimplemented function is 501, and anything unclassified
is 500.

## Query language

A PromQL subset. Anyone operating a metrics system already knows the syntax,
and a subtly different dialect would be a tax on every user for no benefit.

```promql
cpu_percent{host=~"web-.*", env!="dev"}
rate(http_requests_total[5m] offset 1h)
sum by (job) (rate(http_requests_total{status=~"5.."}[5m]))
sum(rate(errors[5m])) / sum(rate(requests[5m]))
topk(5, avg_over_time(cpu_percent[1h]))
up == 1 and rate(errors[5m]) > 0
```

Supported: selectors with all four matcher types, range vectors, offsets;
`rate` `irate` `increase` `delta` `idelta` `resets` `changes`; the
`*_over_time` family; `abs` `ceil` `floor` `sqrt` `exp` `ln` `log2` `log10`
`sgn` `round` `clamp_min` `clamp_max` `scalar` `vector` `time` `timestamp`
`absent`; `sum` `avg` `min` `max` `count` `group` `stddev` `stdvar` `topk`
`bottomk` `quantile` with `by`/`without`; arithmetic, filtering and `bool`
comparisons, `and`/`or`/`unless`, and `on`/`ignoring` vector matching.

Type errors are caught at parse time, so `rate(cpu)` is rejected with
`argument 1 of rate() must be a range vector, got an instant vector` rather
than evaluating to nothing and looking like missing data.

## Limitations

Honest list, since this is a learning project:

- **No clustering or replication.** Single node, single writer.
- **No histograms.** `histogram_quantile` parses and returns 501.
- **No `label_replace`.** Same.
- **No out-of-order ingest.** A sample older than a series' newest is rejected.
  Real systems keep a separate out-of-order head; this one does not.
- **No downsampling.** Retention deletes whole blocks; it does not roll old
  data up to a coarser resolution.
- **Range queries are evaluated step by step.** A step-aware evaluator could
  reuse decoded chunks between adjacent steps. I chose the simpler shape
  because it makes a range query provably agree with the instant queries it is
  made of.
- **The block index is loaded eagerly at open.** Fine for the block sizes here;
  a very high-cardinality block would want the symbol table paged in on demand.

## Licence

MIT. See [LICENSE](LICENSE).
