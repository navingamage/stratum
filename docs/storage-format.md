# Storage format

The on-disk layout, in enough detail to write a reader against.

Everything is big-endian unless stated otherwise. Big-endian is deliberate for
fixed-width index fields: byte order matches sort order, so a binary search over
the serialised form does not have to decode.

## Data directory

```
data/
  wal/
    00000000            segment files, zero-padded so lexical order is numeric order
    00000001
  01KZKBNTGW3CNJWQTAZ/  a block, named by its ID
    meta.json
    chunks
    index
  01KZKBNTHHVPP4CYPA4.tmp/   a block being written; deleted on startup
```

## Block IDs

16 bytes rendered as 26 characters of Crockford base32: a 48-bit millisecond
timestamp followed by 80 bits of randomness, in the manner of a ULID.

IDs minted within the same millisecond increment the random field rather than
redrawing it. The point of both choices is that **lexical order is creation
order**, so listing the data directory yields blocks oldest-first with no
sorting and no metadata reads — which is where compaction planning starts.

The alphabet excludes `I`, `L`, `O` and `U`, so an ID read off a terminal
cannot be mistranscribed.

## Write-ahead log

A segment is a sequence of 32 KiB pages. Records are packed into pages and
fragmented across page boundaries when they do not fit.

Each fragment:

```
+--------+------------+-------------+------------------+
| type   | length     | CRC-32C     | payload          |
| 1 byte | 2 bytes BE | 4 bytes BE  | `length` bytes   |
+--------+------------+-------------+------------------+
```

| Type | Meaning |
|---|---|
| 0 | Page padding; the rest of the page is unused |
| 1 | A complete record |
| 2 | The first fragment of a record |
| 3 | A middle fragment |
| 4 | The final fragment |

The checksum covers the type and length fields as well as the payload, so a
flipped bit in a header is caught rather than steering the decoder into the
wrong byte count.

### Why pages

To tell a torn write from corruption. A process killed mid-write leaves a
partial record at the end of the log. Because every fragment carries its own
length and checksum inside a page of known size, replay can distinguish:

- **the stream ends part-way through a fragment** — a torn write. The record was
  never acknowledged, so dropping it is correct, and replay ends cleanly.
- **a checksum fails, or a fragment sequence occurs that the writer cannot
  produce** (a middle without a first, padding inside a fragmented record) —
  bytes changed after they were written. That is data loss and is reported as
  `ErrCorrupt`.

Getting this distinction wrong in either direction is bad: treating corruption
as a torn write silently discards data, and treating a torn write as corruption
makes every unclean shutdown look like a disaster.

### Segments

Records never span segments — the writer cuts a new segment before writing one
that would not fit. Replay therefore reads each segment with its own reader,
which matters because page offsets are relative to a segment's start and a
segment left short by a crash would otherwise throw off the alignment of every
segment after it.

### Record payloads

The first byte of a record's payload is its type.

**Series (1)** — written the first time a series is seen, so replay can rebuild
the index before applying samples.

```
uint8    1
repeated:
  uint64 BE   series ref
  uvarint     label count
  repeated:
    uvarint-prefixed name
    uvarint-prefixed value
```

**Samples (2)** — delta-encoded against the batch's first entry. Within one
append batch refs are near-consecutive and timestamps near-identical, so both
deltas collapse to one or two bytes.

```
uint8    2
uint64 BE   base ref
uint64 BE   base timestamp
repeated:
  varint    ref - base ref
  varint    timestamp - base timestamp
  uint64 BE float64 bits
```

**Tombstones (3)** — `uint64 BE` ref, `varint` min, `varint` max.

## Chunks

The Gorilla encoding. A chunk holds one series' samples over a bounded time
range.

```
uint16 BE   sample count
varint      first timestamp
64 bits     first value, raw
uvarint     second timestamp, as a delta
<value>     second value
(<dod> <value>)*
```

Everything after the two-byte header is bit-packed, so the stream is only
byte-aligned by coincidence.

### Timestamps: delta-of-delta

| Prefix | Following bits | Range |
|---|---|---|
| `0` | none | delta-of-delta is 0 |
| `10` | 14 | ±8191 |
| `110` | 17 | ±65535 |
| `1110` | 20 | ±524287 |
| `1111` | 64 | anything |

The bucket widths are 14/17/20/64 rather than the paper's 7/9/12/32. Gorilla
assumed 64-second resolution; stratum stores epoch milliseconds, where a second
of scrape jitter is already ±1000 and overflows a 7-bit bucket on nearly every
sample. At 14 bits a two-bit prefix absorbs ±8s of drift.

### Values: XOR

Each value is XOR'd against the previous one.

- `0` — the XOR is zero; the value is unchanged. One bit. This is the common
  case for a gauge that has not moved and for any counter sampled faster than
  it increments.
- `10` — the XOR fits inside the previous leading/trailing zero window; the
  meaningful bits follow at the established width.
- `11` — a new window: 5 bits of leading-zero count, 6 bits of significand
  width, then the significand.

The significand width is in [1, 64] because the XOR is non-zero, so 64 is
encoded as 0 and the 6-bit field stays wide enough. The leading-zero count is
clamped to 31 to fit its 5-bit field, which costs at most a bit of compression
and never correctness.

### Sealed chunks

A chunk loaded from disk rejects appends. The encoder's state — the running
delta and the zero window — is not part of the serialised form, and
reconstructing it would mean trusting bytes that have only just been
checksummed. Head chunks are replayed from the WAL instead.

## Block: chunks file

```
magic  uint32 BE  0x53545243 ("STRC")
version uint8    1
padding [3]byte   so the first chunk starts 8-byte aligned

repeated:
  uvarint    payload length
  uint8      encoding
  bytes      payload
  uint32 BE  CRC-32C over the encoding byte and the payload
```

A chunk reference is the byte offset of its length prefix.

Checksums are per chunk rather than per file so that a query verifies only what
it reads. Verifying a 512 MiB chunk file to answer a query touching one series
would make checksums cost more than they are worth, and operators would switch
them off.

## Block: index

```
magic   uint32 BE  0x53545249 ("STRI")
version uint8      1
padding [3]byte

<symbols>
<series>
<series offset table>
<postings>
<postings offset table>
<label index>
<table of contents>          fixed size, at the end of the file
```

Each section ends with a CRC-32C over its own contents.

**Symbols** — `uvarint` count, then that many `uvarint`-prefixed strings, sorted.
Series reference symbols by index, so a label name repeated across a million
series costs one copy plus a million small integers.

**Series** — one record per series, in ascending label order (which the reader
relies on for binary search):

```
uvarint    label count
repeated:  uvarint name symbol, uvarint value symbol
uvarint    chunk count
repeated:
  first:   varint minTime, uvarint chunk ref
  others:  varint (minTime - previous maxTime), uvarint (ref - previous ref)
  always:  uvarint (maxTime - minTime)
uint32 BE  CRC-32C
```

Chunk metadata is delta-encoded against the previous chunk of the same series.
Chunks of one series are contiguous in the chunk file and cover consecutive time
ranges, so both deltas are small.

**Series offset table** — `uvarint` count, then delta-encoded file offsets. This
maps the dense series index that postings use to a byte offset.

**Postings** — for each label pair, in sorted order:

```
uint32 BE  entry count
uint32 BE  series index, repeated
uint32 BE  CRC-32C
```

Fixed-width, not varints. The reader binary searches these lists during a
`Seek`, which needs constant-time indexing; varints would turn every leapfrog
step into a linear decode and lose exactly the advantage the lazy iterators were
built for.

The reserved label pair `("", "")` holds every series, so a query built only
from negations has something to subtract from.

**Postings offset table** — `uvarint` count, then `(name, value, offset)`
triples with the strings length-prefixed.

**Label index** — `uvarint` name count, then for each: the name, a value count,
and the values. Backs `LabelValues` without touching the postings.

**Table of contents** — six `uint64 BE` section offsets and a `uint32 BE`
checksum, at a fixed size from the end of the file. Reading it is the first
thing the reader does.

## Block: meta.json

```json
{
  "version": 1,
  "id": "01KZKBNTGW3CNJWQTAZCCT64YK",
  "minTime": 1786109637000,
  "maxTime": 1786116837000,
  "stats": { "numSamples": 480000, "numSeries": 2000, "numChunks": 4000 },
  "compaction": {
    "level": 2,
    "sources": ["01KZK...", "01KZK..."],
    "parents": ["01KZK...", "01KZK..."]
  },
  "createdAt": "2026-08-09T14:18:33Z"
}
```

`meta.json` is written last and is what makes a directory a block. A directory
without it is a write that did not finish, and `List` skips it.

`sources` are the level-1 ancestors, carried forward through every compaction.
The lineage is what lets a restart tell an already-absorbed input from a fresh
one after a compaction crashed half-way.

## Durability

The orderings that matter:

1. **A sample is logged before it is visible.** The appender writes the WAL
   record, then applies to the head. The other order would let a query observe
   a sample that a crash would erase.
2. **A block is complete before it is published.** Files are written and fsynced
   under a `.tmp` directory, then renamed. The parent directory is fsynced after
   the rename, since a rename is only durable once the directory entry is.
3. **The head's floor rises before a flush reads it.** Otherwise a sample
   committed between the read and the truncation would be acknowledged and then
   discarded.
4. **Compaction publishes its output before deleting its inputs.** A crash
   between the two leaves data present twice, which queries deduplicate and the
   next compaction cleans up. Deleting first would lose it.
