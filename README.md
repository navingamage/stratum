# stratum

An embedded time-series database engine, written from scratch in Go.

Storage is an LSM tree specialised for time-series: samples land in a
write-ahead log and an in-memory head block, get compressed with
delta-of-delta + XOR encoding, and are periodically flushed into immutable
on-disk blocks that a background compactor merges into larger time ranges.
Queries go through a small PromQL-like language with its own lexer, parser,
planner and vectorized executor.

> Status: work in progress. See `docs/` for design notes.
