# Lossy float compression for Parquet partitions

## Status

Design proposal. No implementation yet.

## Summary

Add optional, per-column **lossy** compression for `DOUBLE`/`FLOAT` columns, applied
when a partition is converted to Parquet. The user specifies an accepted error
(a relative tolerance), and the encoder discards numeric precision below that
threshold so the generic Parquet compressor (ZSTD et al.) can shrink the column
far beyond what lossless encoding achieves.

The feature is inspired by the `LnQ16` codec family in the arctic TickStore
(`arctic/tickstore/coding.py`), which logarithmically quantizes financial tick
values to ~16-bit integers at a near-constant relative error (~1 basis point)
across a very large dynamic range.

## Motivation

Financial tick data (prices, quantities) is stored at far higher precision than
anyone queries it at. A price recorded to 15 significant decimal digits is
wasteful when downstream analysis only cares to ~1 basis point (1e-4). The
excess mantissa bits are effectively noise, and noise does not compress: a raw
`DOUBLE` column of prices stored losslessly in Parquet still costs close to its
full 8 bytes per value after ZSTD, because the low-order mantissa bits are
high-entropy.

Discarding precision the user has declared irrelevant turns that noise into
zeros (or into a narrow integer range), which compresses dramatically. The
arctic codec reaches roughly 2 bytes per value at ~1 bp error on real tick
series. This proposal brings the same trade-off to QuestDB without disturbing
its hot-path storage.

## Goals

- Per-column, opt-in lossy compression for `DOUBLE`/`FLOAT`, specified as an
  accepted relative error.
- A closed-form mapping from accepted error to codec parameters, plus a cheap
  calibration step to predict the resulting byte size.
- Correct handling of NULL (the `NaN` sentinel), and of zero / negative values.
- No change to QuestDB's native (`.d`) storage path or its hot-write/random-access
  performance characteristics.
- Round-trippable: a decoded value is guaranteed within the declared tolerance
  of the original.

## Non-goals

- Lossy compression of native `.d` columns. See "Why Parquet only" below.
- Lossy compression of non-floating types.
- Automatic, age-based conversion of partitions to Parquet. That is a separate,
  pre-existing future item (`SqlCompilerImpl.java:4943`).

## Background: current QuestDB storage

QuestDB has two partition storage tiers:

- **Native (`.d` files)** — the default for every partition. `DOUBLE` columns are
  raw IEEE-754 little-endian values, memory-mapped 1:1 and addressed as
  `base + i*8` (`MemoryPARWImpl`, write path `TableWriter.putDouble`,
  `TableWriter.java:13758`). There is **no compression layer** in native storage.
  The format depends on fixed-width random access, which a variable-rate codec
  would break.
- **Parquet partitions** — opt-in, per-partition, via DDL:
  `ALTER TABLE t CONVERT PARTITION TO PARQUET [WHERE ...]` and the inverse
  `CONVERT PARTITION TO NATIVE` (`AlterOperation.java:71-72`,
  `TableWriter.convertPartitionNativeToParquet` ~`1531`). Conversion is manual;
  there is no automatic tiering. The active (latest) partition stays native on
  non-WAL tables; WAL tables may have a Parquet active partition, merged via the
  O3 path. Direct appends to a Parquet partition are not allowed
  (`TableWriter.java:8802`).

### Why Parquet only

Converting a partition to Parquet is already an explicit "freeze this cold data
to save disk, and accept slower access" decision. That is precisely the moment a
user would also accept losing irrelevant precision. Native storage is the speed
tier (fast ingest, O(1) random access, SIMD/JIT execution over raw layout); a
lossy variable-rate codec there would fight everything native is good at, and
native has no compression layer to extend in the first place. So lossy
compression is scoped as an extension of the Parquet conversion path.

## What TickStore does, and how it maps onto QuestDB

TickStore predates having a columnar file format underneath it, so it hand-rolls
three techniques. Two of the three are already provided by Parquet as standard
encodings; only the third is genuinely new.

### 1. Row-masks -> already provided by Parquet nulls

TickStore stores a per-column packed bitmap (`ROWMASK`, lz4-compressed) marking
which rows have a value, so each sparse column stores only its present values
(`tickstore.py:1407`, `rm = ~np.isnan(val)`).

QuestDB's Parquet encoder already does exactly this. A NULL double (the
`Double.NaN` sentinel) maps to a real Parquet null at definition level 0:

- `parquet_write/encoders/numeric.rs:510` — the `f64` encoder's `is_null()` is
  `self.is_nan()`.
- `parquet_write/simd.rs:836` — `encode_f64_def_levels()` records NaN positions
  in the RLE/bit-packed definition-level bitmap.
- `parquet_write/encoders/numeric.rs:316` — null values are filtered out of the
  value buffer entirely.

So an absent value costs ~0 data bytes plus a bit in an RLE bitmap — the rowmask,
standardized. **Do not port it.** (Caveat: the sparse-column win only
materializes if the schema actually leaves untouched fields NULL per row; that is
a data-modeling decision, not a codec one.)

### 2. Index delta-zigzag -> supported, but not the default

TickStore delta-encodes the monotonic timestamp index and varint-packs it
(`tickstore.py:1385`, `np.diff(idx, prepend=0)`).

Parquet's `DELTA_BINARY_PACKED` is exactly delta + zigzag + bit-packing for
integer columns, and QuestDB validates it as legal for the timestamp type
(`schema.rs:549-563`, `Timestamp` is in the accepted set). **But it is not the
default.** `encoding_map` (`schema.rs:691-699`) hands every numeric column
`Plain`:

```rust
match data_type.tag() {
    Symbol                    => RleDictionary,
    Binary | Varchar | String => DeltaLengthByteArray,
    _                         => Plain,   // Long, Int, Timestamp, Double, ...
}
```

So the designated timestamp — a sorted, monotonic microsecond `int64`, about the
most delta-friendly column that exists — is written as raw 8-byte values today.
A user can already opt in by setting that column's encoding to id 4
(`DELTA_BINARY_PACKED`) via the per-column config, but it is never chosen
automatically. **Do not reimplement; change the default.**

### 3. LnQ logarithmic quantization -> genuinely new

Parquet has no lossy float encoding. This is the only piece worth building.

## Design

### Two codec tiers

An IEEE-754 double is already a companded (log-like) representation: exponent
plus mantissa. That gives two natural realizations of "constant relative error,"
at different aggression levels.

**Tier A — mantissa bit-rounding ("bit grooming").** Zero the low mantissa bits,
keeping the top `k`. The value remains a valid IEEE-754 double, so:

- the relative error is bounded by `2^-(k+1)`,
- the Parquet **read path needs zero changes** (it is still a normal double),
- the zeroed low bytes become constant and compress to almost nothing under
  `BYTE_STREAM_SPLIT` + ZSTD.

This is the simple, low-risk 80%. It does not reach arctic's ratios because the
value is still physically a 64-bit double, but it requires no inverse transform.

**Tier B — companded narrow-integer codec (the arctic-style path).** Map each
value through a companding curve, quantize to a narrow integer (int16/int32),
store that integer column in Parquet (with `DELTA_BINARY_PACKED`), and apply the
inverse transform on read. This reaches ~2 bytes per value but changes the
on-disk type and adds a decode step to every read of the column.

For the companding curve, **prefer mu-law over TickStore's raw
`ln(x * 2^prescale + preadd)`**:

```
mu-law:   y = sign(x) * ln(1 + mu*|x|/Xmax) / ln(1 + mu)      (encode, y in [-1,1])
inverse:  x = sign(y) * (Xmax/mu) * ((1+mu)^|y| - 1)          (decode)
```

mu-law improves on the raw-log formulation because:

- It handles **sign and zero natively** (symmetric, smooth through 0), removing
  TickStore's `abs()` + `*sign` + `preadd` workarounds that exist only because
  `ln(0) = -inf`.
- It is **bounded** to `[-1,1]`, eliminating the `prescale` tuning that exists
  only to stop float32 log math overflowing (TickStore overflows at ~2.2e27).
- Its range parameter `Xmax` can be **derived from Parquet per-page min/max
  statistics**, which the encoder already computes — nothing extra to store.

The trade-off to validate: raw log gives constant *relative* error across the
entire range; mu-law gives constant relative error only in its logarithmic
region `|x| >> Xmax/mu` and transitions to constant *absolute* error near zero.
For tick prices this is usually acceptable or even preferable (more robust than a
log blow-up near zero), but the realized error profile must be plotted against a
real price/quantity distribution before declaring it equivalent to the flat-bps
guarantee.

### Sizing the quantizer from an accepted error (closed form)

The quantizer parameters follow directly from the accepted relative error `eps`
and the column's log-dynamic-range `R = ln(xmax/xmin)`; no search is needed.

**Raw log / LnQ.** Stored code `i = round(ln(x) * K)`, `K = 2^n / R`. The
log-domain step is `1/K`, and a half-step error in log space approximates the
relative error, so:

```
rtol ~= R / 2^(n+1)        =>        n = ceil( log2( ln(xmax/xmin) / (2*eps) ) )
```

This reproduces TickStore's own numbers: its `loss` parameter is `2^16/K`, giving
`rtol ~= loss / 2^17` — `loss=15` yields 114 ppm, matching its "LnQ15, 115 ppm"
comment. Worked example: prices spanning `1e-3 .. 1e6` (`R ~= 20.7`) at
`eps = 1 bp = 1e-4` need `n = ceil(log2(20.7 / 2e-4)) = 17` bits per value before
delta and entropy coding.

**mu-law.** Relative-error floor in the log region `~= ln(1+mu) / 2^n`, with the
relative/absolute crossover at `|x| ~= Xmax/mu`. So pick `mu` from the dynamic
range you want kept relative, and `n` from `eps` — two one-line formulas.

**mantissa rounding.** `eps ~= 2^-(k+1)` => `k = ceil(log2(1/(2*eps)))` mantissa
bits kept. Trivial.

### Predicting the byte size (calibration)

The formulas above size the *quantizer* — its entropy ceiling in bits per
sample. They do **not** predict the final file size, because after delta-coding,
ZSTD exploits temporal autocorrelation that is entirely data-dependent. So the
workflow is:

1. Closed-form: pick `n` / `mu` / `loss` / `k` for the error target (instant).
2. Calibration sweep: encode a representative sample at 3-4 settings, measure
   realized rtol and compressed bytes, interpolate. Encoding is fast, so this is
   a seconds-long step and the only empirical part.

## Configuration surface

The accepted error is a per-column property supplied at conversion time. Two
delivery mechanisms, not mutually exclusive:

- **DDL `WITH` clause** on the conversion statement (per-column, explicit):

  ```sql
  ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE timestamp < '2026-01-01'
    WITH (price LOSSY 1bps, size LOSSY 1bps);   -- illustrative syntax
  ```

- **Per-column Parquet config defaults** via `cairo.partition.encoder.parquet.*`
  configuration, for tables converted without an explicit clause.

Both feed the existing per-column config plumbing: `TableColumnMetadata`
`parquetEncodingConfig` (`TableColumnMetadata.java:45`), packed by
`TableUtils.packParquetConfig` (bit layout `TableUtils.java:249`). The packed
i32 currently uses bits 0-25 (encoding, compression, level, explicit flag, bloom
flag — `schema.rs:581-597`); the lossy codec id and a precision parameter need a
home in the spare high bits, or a widening of this config word. This is a
schema/forward-compat decision to settle early because the packed config is
persisted.

## Implementation plan

Three independent, separately shippable pieces, smallest and most reusable first.

### PR 1 — lossless prerequisites (no precision loss)

Self-contained and useful on its own, independent of any lossy work:

- **PR1a (done):** default the designated timestamp to `DELTA_BINARY_PACKED`.
  Implemented as a `default_encoding(data_type, is_designated_timestamp)` helper
  in `to_encodings` (`schema.rs`); non-designated timestamps stay PLAIN since
  they are not guaranteed sorted. The encode dispatch already supported
  `(DeltaBinaryPacked, Timestamp)` (`encode.rs:503-523`), so this was a default
  change only. Covered by Rust unit tests in `schema.rs` and a Java round-trip
  test `PartitionEncoderTest.testDesignatedTimestampDefaultsToDeltaBinaryPacked`.
- **PR1b (done):** implement `BYTE_STREAM_SPLIT` for `Double`/`Float`, both
  encoder and decoder. Encoder transposes the Plain little-endian bytes into K
  per-byte streams (`encoders/numeric.rs` `encode_data`, threaded through
  `encoders/plain/primitive.rs` and dispatched in `encode.rs`); `validate_encoding`
  accepts it for floating types only. Decoder un-transposes the page back to the
  contiguous layout and reuses the existing `PlainPrimitiveDecoder`
  (`parquet_read/decode.rs` `byte_stream_split_to_plain`). Tests: schema
  validation, a self round-trip through our own decoder (Double + Float with NaN
  nulls), and an independent `arrow`-reader round-trip that confirms the bytes are
  spec-compliant rather than merely symmetric with our decoder.

  It was previously *declared*
  (config id 5 parses) but **not implemented**: `validate_encoding` has no arm
  for it and the dispatch in `encode.rs` only matches PLAIN/RleDictionary, so it
  silently falls back to PLAIN (`schema.rs:1057` test confirms). BYTE_STREAM_SPLIT
  is the encoding that makes Tier A's rounded doubles compress well, so it is a
  prerequisite for Tier A and a lossless win regardless.

### PR 2 — Tier A: mantissa bit-rounding

- Add a lossy codec id + precision (kept-mantissa-bits or accepted rtol) to the
  packed per-column config.
- In the Rust encoder, apply the rounding pass to `Double`/`Float` columns before
  encoding; the output remains a valid double, so the read path is untouched.
- Pair with `BYTE_STREAM_SPLIT` + ZSTD from PR 1.
- DDL `WITH (... LOSSY ...)` parsing.

### PR 3 — Tier B: mu-law companded narrow-int codec

- Encode `Double`/`Float` as a companded int16/int32 column plus the metadata
  needed to invert (`mu`, `Xmax` from page stats, `n`).
- Inverse transform on the Parquet read path for these columns.
- This is the larger change: it touches the read path and introduces a stored
  type distinct from the logical column type, so it needs careful handling in the
  Parquet reader and in anything that inspects Parquet column types.

## Read path

- Tier A requires no read changes: the stored value is a normal double already
  within tolerance.
- Tier B requires the Parquet reader to recognize the companded column and apply
  the inverse transform, materializing a `DOUBLE` for the rest of the engine. The
  logical column type stays `DOUBLE`; only the physical Parquet representation
  differs. Predicate pushdown / page-stats pruning on these columns must account
  for the transform (min/max in companded space map monotonically to value space,
  so range pruning is still valid, but the comparison must be done correctly).

## Testing

Per repository conventions (`assertMemoryLeak`, error-path resource cleanup):

- Round-trip error bound: for each codec and a range of `eps`, assert every
  decoded value is within the declared tolerance of the original, across the full
  dynamic range including subnormals, very large, and very small magnitudes.
- NULL handling: `NaN` must round-trip as a Parquet null (Tier A) or be excluded
  from companding and restored as NULL (Tier B). Confirm null doubles still cost
  ~0 data bytes.
- Sign and zero: signed inputs and exact zeros for Tier B (mu-law) round-trip
  correctly.
- Non-finite guard: `+inf`/`-inf` inputs must fail fast or be treated as NULL,
  never silently corrupt (cf. TickStore's `_assert_finite`).
- Resource cleanup on every error path in the encoder and the
  convert-to-Parquet / convert-back-to-native flows.
- Convert-to-Parquet-then-back-to-native: define and test the semantics —
  conversion back to native cannot recover lost precision; the rounded values are
  what persist.
- Calibration: a test asserting the closed-form `rtol` formula matches measured
  round-trip error within a small margin for each codec.

## Risks and trade-offs

- **Irreversible precision loss.** Once a partition is converted with a lossy
  codec, the original precision is gone; converting back to native restores the
  rounded values, not the originals. This must be explicit in the DDL and
  documented; it is the one genuinely destructive operation here.
- **Tier B read cost.** The inverse transform runs on every read of a companded
  column, trading CPU for the smaller on-disk size. Acceptable for cold archival
  data, but it is a real cost and should be measured, not assumed negligible.
- **mu-law near-zero behaviour.** Constant-relative only in the log region; the
  flat-bps guarantee does not hold for tiny magnitudes. Validate against real
  distributions before claiming arctic-equivalent error.
- **Persisted config format.** The packed per-column config is written to disk;
  the lossy codec id and precision must be laid out with forward compatibility in
  mind, and old files without the field must read back correctly.
- **Interoperability.** Tier A output is plain Parquet readable by any tool.
  Tier B stores integers plus QuestDB-specific transform metadata; external
  readers see integers, not the intended doubles, unless they know the transform.
  This narrows Tier B's interop benefit relative to staying lossless.

## Open questions

- Error specification units in DDL: relative tolerance (`1bps`, `1e-4`),
  kept-significant-bits, or kept-decimal-digits? Relative tolerance maps cleanly
  to all three codecs via the closed-form sizing.
- Should `Xmax` for mu-law be per-page (from Parquet stats, adapts to local
  range) or per-column (one value, simpler, stored once)?
- Is Tier B worth the read-path complexity and interop cost, or does Tier A +
  `BYTE_STREAM_SPLIT` + ZSTD capture enough of the benefit for the target
  workloads? Decide with the calibration sweep on real data before committing to
  PR 3.

## References

- arctic TickStore codecs: `arctic/tickstore/coding.py` (`LnQ16_VQL`, `ln_q16` /
  `e_q16`, `log_q16` / `exp_q16`), rowmask and index encoding in
  `arctic/tickstore/tickstore.py`.
- QuestDB Parquet encoder: `core/rust/qdbr/src/parquet_write/`
  (`schema.rs`, `encode.rs`, `simd.rs`, `encoders/numeric.rs`).
- Parquet config plumbing: `TableColumnMetadata.java`, `TableUtils.java`
  (`packParquetConfig`), `PartitionEncoder.java`, `ParquetEncoding.java`,
  `ParquetCompression.java`.
