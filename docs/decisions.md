# Decisions

The choices that were not obvious, what the alternatives were, and what I
learned by getting some of them wrong first.

---

## Delta-of-delta buckets are 14/17/20/64, not the paper's 7/9/12/32

**Context.** The Gorilla paper stores a timestamp's delta-of-delta in one of
four bucket widths, chosen so that a regular scrape interval costs a single
control bit.

**Problem.** Gorilla measured at 64-second resolution. stratum stores epoch
milliseconds. A second of scrape jitter — entirely ordinary — is a
delta-of-delta of ±1000, which overflows a 7-bit bucket (±63) on nearly every
sample. The paper's tuning applied literally would push almost all timestamps
into the wider buckets and give up most of the compression it exists to
provide.

**Decision.** Widen to 14/17/20/64. A two-bit prefix plus 14 bits absorbs
±8191ms, which covers essentially all real scrape drift.

**Cost.** A perfectly regular series pays the same single bit either way, so
nothing is lost there. A jittery one pays 16 bits instead of 9 in a
hypothetical world where 9 would have sufficed — but in this world it would
have paid 35.

---

## The WAL is page-framed

**Context.** The log needs to survive a process killed mid-write.

**Alternatives.** A flat sequence of length-prefixed records is simpler. So is
writing a length, then the payload, then a trailing marker.

**Problem with the simple options.** After a crash you find a truncated record
and cannot tell whether the writer stopped there or whether something ate the
bytes. If you assume the former you silently discard corruption; if the latter,
every unclean shutdown looks like data loss.

**Decision.** LevelDB-style pages: 32 KiB, records fragmented across page
boundaries, each fragment carrying its own type, length and checksum. Replay can
then distinguish a stream that ends part-way through a fragment (a torn write,
recovered by dropping the tail) from a checksum failure or an impossible
fragment sequence (real damage, reported).

**What I got wrong.** My first implementation concatenated segments into one
stream and padded short ones back to page alignment. The padding bytes satisfy
the declared length of a half-written record, so a torn write came back as a
checksum failure — exactly the wrong verdict, and the failure the format exists
to prevent. Fixed by giving each segment its own reader, which is sound because
records never span segments.

---

## Sync policy defaults to periodic, not per-write

**Context.** `fsync` on every batch guarantees no acknowledged write is ever
lost, at the cost of a device round-trip per batch.

**Decision.** Default to a 200ms timer, with `always` and `never` available.

**Reasoning.** Time-series ingest is a firehose of individually near-worthless
samples. Losing 200ms of scrapes is a gap in a graph. Paying a device
round-trip per batch to prevent that costs more throughput than the data is
worth. This is the opposite of the right default for a transactional database,
and it is right here for the same reason: the value of an individual record.

---

## Postings are lazy iterators, not materialised sets

**Context.** Resolving `{job="api", status=~"5..", pod="x"}` means combining
three postings lists.

**Alternative.** Materialise each into a slice and intersect.

**Decision.** Iterators with `Next` and `Seek`, combined by a leapfrog join:
seek every input to the running maximum, and a ref missing from any list is
skipped without visiting the refs in between. Inputs are ordered smallest-first.

**Why it matters.** The cost tracks the size of the *result*, not of the inputs.
Intersecting a 1-element list into a 1,000,000-element one runs in 114ns.
Materialising would have cost a million appends to answer a one-series query.

**Consequence.** Persisted postings are fixed-width big-endian rather than
varints, because `Seek` binary searches them and that needs constant-time
indexing. Varints would have turned every leapfrog step into a linear decode and
lost the whole advantage.

---

## Regular expressions are enumerated when they are finite

**Context.** `pod=~"a|b|c"` naively means reading every distinct value of `pod`
and testing each against the regex.

**Decision.** At matcher-compile time, try to enumerate the exact set of strings
the pattern accepts. If that set is finite and small, look those values up
directly and union the postings.

**Measurement.** On a label with 10,000 distinct values: 479ns instead of 975µs.

**Subtlety.** The Go regexp parser factors common prefixes out of alternations,
so `web-1|web-2|web-3` arrives as the literal `web-` concatenated with the class
`[1-3]`, not as three literals. Enumeration therefore has to handle
concatenation as a bounded cross product. `TestSetMatchesAgreesWithRegexp`
pins the safety property: wherever the set path is taken, membership must agree
exactly with the anchored regexp, since a disagreement silently returns wrong
series.

---

## Blocks are reference counted

**Context.** Compaction deletes a block's files as soon as its output is
published. A query started a moment earlier may still be reading them.

**What makes this sharp.** Blocks are memory-mapped. Unmapping one underneath a
running query is not a stale read — it is a segmentation fault.

**Alternatives.** Hold a lock across whole queries (blocks ingest and
compaction for the duration of the slowest query). Copy everything a query needs
up front (defeats the point of mapping).

**Decision.** The database holds one reference per open block and each querier
holds another. Compaction can unlink the files; the mapping survives until the
last reference goes. Pinning happens under the read lock that reload needs the
write lock to invalidate, so there is no window between seeing a block and
pinning it.

---

## A querier snapshots the head, but only pins blocks

**Context.** The head is mutable and gets truncated after a flush.

**What went wrong first.** The head series set read chunk lists lazily. A flush
truncating mid-query left series emptied out underneath it, and the query
silently returned short results — no error, no crash, just missing data.

**Decision.** Materialise the head's matching series at `Select`: label sets and
chunk references, captured up front. Chunks are immutable once handed out and
the open chunk is already a copy, so holding the references pins one consistent
instant of the head regardless of what maintenance does next.

**Asymmetry.** Blocks are reference counted rather than snapshotted because a
block can be gigabytes and its chunks are already stable on disk; the head is
bounded by one block duration and its chunks are not. Different problems,
different answers.

---

## The head raises its floor before a flush reads it

**Context.** A flush reads the head's chunks, writes a block, then truncates
the range it wrote.

**Bug.** A sample committed between the read and the truncation was
acknowledged to the caller and then thrown away. This is exactly the failure
mode the WAL ordering exists to prevent, reintroduced one layer up.

**Decision.** Raise the head's floor first, so the range about to be captured
stops accepting samples; then flush; then truncate. Commits take a read lock
that the floor raise takes for writing, so the two cannot interleave. A
straggler is now *rejected with an error* rather than accepted and lost.

**Principle.** A write either succeeds or says why. Silently dropping an
acknowledged write is worse than refusing it, because the caller can retry a
refusal.

---

## Range queries are evaluated step by step

**Context.** A range query evaluates the same expression at many timestamps.

**Alternative.** A step-aware evaluator that decodes each chunk once and reuses
it across adjacent steps. Meaningfully faster.

**Decision.** Evaluate each step independently over one shared querier.

**Reasoning.** With this shape, a range query provably agrees with the instant
queries it is made of — checked by `TestRangeQueryAgreesWithInstant`. That is
the property people actually rely on when a graph disagrees with a number on a
dashboard, and it is the first thing anyone doubts when debugging. The faster
shape can be added later behind that test; adding the test later to a faster
shape is much harder.

---

## `rate()` extrapolates to the window, with a threshold

**Context.** Samples rarely land on a window's boundaries, so the observed
change covers slightly less time than the window asked for.

**Naive options.** Divide the observed change by the observed span (ignores the
window, so two series with different scrape phases are incomparable). Divide by
the window (systematically biased low).

**Decision.** Scale the observed change up to the window, but bound the
extrapolation: if the gap at either end is within about one scrape interval
(1.1x, for jitter), fill the whole gap in; beyond that, assume only half an
interval.

**Why both halves matter.** The first makes a counter that plainly increments
by 10/s return exactly 10 rather than 8. The second stops a series that
*stopped reporting* from having its last known rate projected across the hole.
Both are tested.

---

## Compaction windows align on absolute time

**Context.** Blocks are grouped into a compaction when they fall in the same
window of the target duration.

**Alternative.** Align relative to the oldest block.

**Decision.** Align on absolute epoch time.

**Reasoning.** Which group a block belongs to must never change as blocks come
and go. Relative alignment lets the same data get rewritten into
differently-shaped blocks on every pass — write amplification with no benefit,
and a compactor that never reaches a fixed point.

---

## No dependencies outside the standard library

**Context.** `cespare/xxhash` and `oklog/ulid` would have saved a few hundred
lines.

**Decision.** Implement XXH64 and the ULID-shaped IDs.

**Reasoning.** For a project whose purpose is understanding how this kind of
system works, a dependency is a part of the machine I do not get to look inside.
XXH64 is ninety lines against a frozen specification, verified against the
published reference vectors — checked against the spec rather than against
itself, since a round-trip test passes just as happily on a hash that is
self-consistent and wrong.

**Where I would not do this.** Anything cryptographic, anything with a
specification that moves, and anything where being subtly wrong is silent.
Compression and hashing have exhaustive test vectors; TLS does not.
