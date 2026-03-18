# Posting Index Format

## Overview

The Posting index is an inverted index that maps symbol keys to sorted lists of
row IDs. It replaces the legacy block-linked-list bitmap index with a compressed,
generation-based format that uses delta encoding and Frame-of-Reference (FoR)
bitpacking to store postings lists compactly.

Each indexed symbol column produces two files:

- `.bk` (key file) -- 64-byte header + generation directory
- `.bv` (value file) -- concatenated generation data

The format requires no symbol table or dictionary; compression is purely
arithmetic, operating on the integer structure of sorted row IDs.

Source files:

- `PostingIndexUtils.java` -- format constants, encoding/decoding
- `PostingIndexWriter.java` -- writer with commit/seal/compact lifecycle
- `PostingIndexFwdReader.java` -- forward reader with block-buffered decode
- `FORBitmapIndexUtils.java` -- bitpacking primitives (pack/unpack)


## On-Disk Format

### Key File (.bk)

```
Offset  Size  Field
------  ----  -----
  0      1B   Signature (0xfb)
  1      7B   Padding
  8      8B   Sequence number (writer increments on every header update)
 16      8B   Value file logical size (valueMemSize)
 24      4B   Block capacity (always 64)
 28      4B   Key count (total symbol keys seen)
 32      8B   Sequence check (matches sequence for atomic reads)
 40      8B   Max value (highest row ID written)
 48      4B   Generation count
 52      4B   Format version (1)
 56      8B   Reserved
------  ----  ----- (total: 64 bytes)

[Generation directory: genCount x 24B entries starting at offset 64]

  Per-entry layout (24 bytes):
    +0   8B   File offset into .bv where this generation's data starts
    +8   4B   Data size in bytes
   +12   4B   Key count (positive = dense gen, negative = sparse gen;
              |keyCount| = number of active keys if sparse)
   +16   4B   Min key ID in this generation
   +20   4B   Max key ID in this generation
```

Readers use the sequence/sequenceCheck pair as a spin-lock to read a consistent
snapshot of the header fields. The writer increments the sequence before updating
fields and writes sequenceCheck last, with store fences between updates.


### Value File (.bv)

The value file is a sequence of generation data blobs. Each commit appends one
generation. Seal merges all generations into one.

```
+---------------------------+
| Generation 0 data         |
+---------------------------+
| Generation 1 data         |
+---------------------------+
| ...                       |
+---------------------------+
| Generation N-1 data       |
+---------------------------+
```

The file offset and size of each generation are recorded in the key file's
generation directory.


## Generation Formats

### Sparse Format (commit-time)

Each `commit()` writes a sparse generation containing only the keys that
received values since the last commit. The key count in the generation directory
is stored as a negative number to signal sparse format (e.g., -1000 means 1000
active keys).

```
+--------------------------------------------------+
| keyIds[]:   activeKeyCount x 4B (sorted, for     |
|             binary search by readers)             |
+--------------------------------------------------+
| counts[]:   activeKeyCount x 4B (value count     |
|             per key, indexed by position)         |
+--------------------------------------------------+
| offsets[]:  activeKeyCount x 4B (byte offset to  |
|             each key's encoded data, relative to  |
|             start of data section)                |
+--------------------------------------------------+
| key 0 BP-encoded data                            |
| key 1 BP-encoded data                            |
| ...                                              |
+--------------------------------------------------+
```

Sparse format header size = `activeKeyCount * 4B * 3` (keyIds + counts + offsets).


### Dense Format (seal-time, stride-indexed)

Seal produces a single dense generation covering all keys, organized into
strides of 256 keys. The key count in the generation directory is stored as a
positive number.

```
+-------------------------------------------------------------+
| Stride index: (strideCount + 1) x 4B                       |
|   strideIndex[s] = byte offset of stride block s            |
|   strideIndex[strideCount] = sentinel (total stride data)   |
+-------------------------------------------------------------+
| Stride block 0  (keys 0..255)                               |
+-------------------------------------------------------------+
| Stride block 1  (keys 256..511)                             |
+-------------------------------------------------------------+
| ...                                                         |
+-------------------------------------------------------------+
| Stride block N-1  (last stride, may have < 256 keys)        |
+-------------------------------------------------------------+
```

Where `strideCount = ceil(keyCount / 256)`.

Each stride block independently chooses between two encoding modes at seal time,
whichever produces fewer bytes.


#### BP Mode (mode = 0x00)

Per-key delta + FoR bitpacking. Best when different keys have different value
distributions (e.g., high-cardinality scenarios where some keys are dense and
others are sparse).

```
+-----------------------------------------------------------+
| mode:     1B = 0x00                                       |
| reserved: 1B = 0x00                                       |
| padding:  2B                                              |
+-----------------------------------------------------------+
| counts[]:  ks x 4B (value count per key in stride)        |
+-----------------------------------------------------------+
| offsets[]: (ks + 1) x 4B (prefix-sum byte offsets to each |
|            key's BP-encoded data; sentinel at end)         |
+-----------------------------------------------------------+
| BP-encoded data for key 0                                 |
| BP-encoded data for key 1                                 |
| ...                                                       |
+-----------------------------------------------------------+
```

Where `ks` = number of keys in this stride (256 for all but the last stride).


#### Packed Mode (mode = 0x01)

Global FoR bitpacking of raw values across all keys in the stride. Best when
all values in the stride share a narrow range (e.g., round-robin insertion where
neighboring keys have similar row IDs).

```
+-----------------------------------------------------------+
| mode:           1B = 0x01                                 |
| bitWidth:       1B (bits per packed value)                |
| padding:        2B                                        |
+-----------------------------------------------------------+
| baseValue:      8B (minimum value across all values in    |
|                 the stride, subtracted before packing)    |
+-----------------------------------------------------------+
| prefixCounts[]: (ks + 1) x 4B (cumulative value count    |
|                 per key; prefixCounts[0] = 0)             |
+-----------------------------------------------------------+
| packed data:    totalValues x bitWidth bits, contiguous   |
|                 bit-packed (value - baseValue) for all    |
|                 keys in key order                         |
+-----------------------------------------------------------+
```

To read key K in a Packed stride: compute `startIdx = prefixCounts[localKey]`
and `count = prefixCounts[localKey+1] - startIdx`, then unpack `count` values
starting at bit position `startIdx * bitWidth` in the packed data, adding back
`baseValue`.


## Per-Key BP Encoding

Each key's postings are encoded independently using delta + FoR bitpacking with
a block size of 64 values. This is the encoding used in both sparse generations
and BP-mode stride blocks.

### Layout

```
+--------------------------------------------------+
| blockCount:      2B (unsigned short)             |
+--------------------------------------------------+
| valueCounts[]:   blockCount x 1B (1..64 values   |
|                  per block)                       |
+--------------------------------------------------+
| firstValues[]:   blockCount x 8B (first absolute |
|                  value in each block)             |
+--------------------------------------------------+
| minDeltas[]:     blockCount x 8B (FoR reference  |
|                  = minimum delta in each block)   |
+--------------------------------------------------+
| bitWidths[]:     blockCount x 1B (bits needed    |
|                  per residual in each block)      |
+--------------------------------------------------+
| packedBlock[0]:  variable-size bitpacked          |
|                  residuals for block 0            |
| packedBlock[1]:  ...                              |
| ...                                              |
+--------------------------------------------------+
```

### Encoding Algorithm

For a key with N sorted values:

1. **Compute deltas**: `delta[i] = value[i] - value[i-1]` for i > 0.

2. **Split into blocks of 64**: each block has up to 64 values. The last block
   may have fewer.

3. **Per block** (with `count` values in this block):
   - Store `firstValues[b] = value[blockStart]` (the absolute first value).
   - Consider only `count - 1` inter-value deltas: `delta[blockStart+1]` through
     `delta[blockEnd-1]`. (The first value is already stored in `firstValues`.)
   - Find `minDelta` and `maxDelta` among these deltas.
   - Store `minDeltas[b] = minDelta`.
   - Compute `range = maxDelta - minDelta`.
   - Store `bitWidths[b] = bitsNeeded(range)` (0 if range == 0).
   - Bitpack the `count - 1` residuals: `(delta[i] - minDelta)` using
     `bitWidth` bits each.

4. **Packed data size per block**: `ceil((count - 1) * bitWidth / 8)` bytes.

### Decoding Algorithm

1. Read `blockCount` (2B).
2. Read all metadata arrays: `valueCounts`, `firstValues`, `minDeltas`,
   `bitWidths`.
3. For each block `b`:
   - Emit `firstValues[b]` as the first value.
   - Unpack `count - 1` residuals from the packed data.
   - Reconstruct deltas: `delta[i] = residual[i] + minDeltas[b]`.
   - Cumulative sum from `firstValues[b]` to recover absolute row IDs.

### Example

Consider a key with values `[100, 103, 106, 109, 112]` (5 values, 1 block):

```
blockCount    = 1
valueCounts   = [5]
firstValues   = [100]
deltas        = [3, 3, 3, 3]     (all inter-value deltas are 3)
minDeltas     = [3]
maxDeltas     = [3]
range         = 0
bitWidths     = [0]
packedBlocks  = (empty)           (bitWidth=0 means all residuals are 0)

Total encoded size: 2 + 1 + 8 + 8 + 1 + 0 = 20 bytes
```

For constant deltas (common with round-robin insertion), `bitWidth = 0` and the
packed data section is empty. The entire key is encoded in just the metadata:
20 bytes for any number of blocks.


## Stride-Indexed Layout

### Why Strides?

Without strides, a dense generation would need a per-key header entry for all
`keyCount` keys, even keys with zero values. With 5M keys, that is 40 MB of
header alone (count + offset per key).

Strides group keys into blocks of 256. The stride index is only
`(ceil(keyCount/256) + 1) * 4B` -- about 78 KB for 5M keys. Within each stride,
only the keys present in that stride need per-key metadata.

### Adaptive Mode Selection

At seal time, the writer trial-encodes each stride in both modes:

- **BP mode size** = `strideBPHeaderSize(ks) + sum(BP-encoded data for each key)`
- **Packed mode size** = `stridePackedHeaderSize(ks) + ceil(totalValues * bitWidth / 8)`

Whichever is smaller wins. This decision is made independently per stride.

**When Packed wins**: Values across all keys in the stride share a narrow range.
This happens with round-robin or time-ordered insertion where keys 0..255 all
have row IDs in a similar neighborhood. The per-stride `baseValue` is subtracted,
and the residuals fit in few bits.

**When BP wins**: Different keys have very different value ranges or delta
patterns. Per-key delta encoding adapts to each key independently, which is
better when one key in the stride has 10,000 values and another has 2.


## Generation Lifecycle

### Commit (creates sparse generations)

Each call to `commit()`:

1. Sorts the active key IDs (only keys that received values since last commit).
2. Encodes each active key's values using BP encoding.
3. Writes a sparse generation to the value file (keyIds + counts + offsets + data).
4. Appends a generation directory entry to the key file.
5. Atomically updates the header (sequence, valueMemSize, keyCount, genCount).
6. Clears pending buffers.
7. If `genCount > 256`, triggers an automatic `seal()`.

Only active keys appear in a sparse generation. If a partition has 50,000 keys
but only 1,000 are active per commit, each generation contains data for just
those 1,000 keys.

### Seal (merges into single dense generation)

`seal()` merges all generations into a single dense, stride-indexed generation:

1. **Count phase**: scans all generations to compute total values per key.
2. **Decode phase**: decodes all values from all generations into a flat buffer,
   grouped by key.
3. **Encode phase**: encodes into stride-indexed format with adaptive per-stride
   mode selection (BP vs Packed).

Seal writes the new generation AFTER existing data (append-only) so that
concurrent readers with active cursors are not disrupted. Old generation data
becomes dead space until compaction.

**Incremental seal**: when generation 0 is already dense and subsequent
generations are all sparse, the writer marks dirty strides (those touched by
sparse generations) and only re-encodes them. Clean strides are copied verbatim
from generation 0. This avoids re-encoding the entire index when only a small
fraction of keys changed.

### Compact (on close)

`compactValueFile()` runs during `close()`. If there is exactly one generation
and it does not start at file offset 0 (i.e., dead space exists from a
previous seal), compact copies the live generation data to offset 0 and updates
the generation directory. The file is then truncated to its live size.


## Spill Buffer

The writer maintains a pending buffer of `blockCapacity` (64) values per key in
native memory. When a hot key fills its pending buffer before a global commit,
instead of flushing all keys, the writer spills just that key's values into
a per-key overflow buffer.

This prevents generation-count explosion when a single hot key drives repeated
overflows. Without spilling, each overflow would trigger a full commit creating
a new generation for all active keys.

At the next `commit()`, spilled values are merged with pending values and
encoded together into the sparse generation.


## Reader Lookup Tiers

The forward reader (`PostingIndexFwdReader`) uses `PostingGenLookup` to
efficiently locate a key across multiple sparse generations. Three tiers are
selected based on key count and memory budget:

- **Tier 1 (Per-key CSR index)**: for small key counts. Builds an inverted index
  from key to the list of sparse generations containing it. Cursor jumps directly
  to relevant generations -- O(hitGens) per key with zero binary searches.

- **Tier 2 (Per-gen Split Block Bloom Filter)**: for large key counts within
  memory budget. Probes each generation's SBBF to skip generations that
  definitely do not contain the key. Falls back to binary search on Bloom filter
  positives.

- **Tier 3 (Binary search + bounds check)**: fallback. Checks min/max key
  bounds per generation, then binary-searches the sparse keyIds array.


## Performance Characteristics

Benchmark data below is from `IndexComparisonBenchmark` running five scenarios
with three formats (Legacy, FSST, Posting). All sizes are sealed.

### Storage

```
Scenario                        Legacy     FSST    Posting
S1: 5M keys x 4 v/k            534 MB    125 MB    195 MB
S2: 512 keys x 7K v/k           32 MB     17 MB      1 MB
S3: Streaming (50K, 500 cmts)    54 MB     22 MB     14 MB
S4: 8K keys x 10K v/k         1000 MB    357 MB     22 MB
S5: Zipfian (1K, 10M rows)      172 MB     48 MB     15 MB
```

**S1 (5M keys, 4 v/k)**: Posting is 195 MB, larger than FSST's 125 MB. With
only 4 values per key, the per-key BP metadata (blockCount 2B + firstValue 8B +
minDelta 8B + bitWidth 1B + valueCount 1B = 20B minimum per key) dominates. FSST
amortizes its dictionary cost across many keys more efficiently at this ratio.

**S2 (market data, round-robin)**: Posting achieves 1 MB -- a 32x reduction from
Legacy. Round-robin insertion produces constant deltas across consecutive row
IDs, so `bitWidth = 0` for virtually every block. Entire keys are stored in
metadata alone (20 bytes per block of 64 values regardless of row ID magnitude).
Packed mode further helps because all 512 keys in each stride have similar
row ID ranges.

**S3 (streaming, sparse commits)**: 14 MB vs Legacy's 54 MB. The sparse
generation format stores only the 1,000 active keys per commit (2% of 50K),
not all 50,000. Each generation's header is just 12 KB (1000 keys x 3 x 4B).

**S4 (dense instruments, round-robin)**: 22 MB vs Legacy's 1,000 MB -- 46x
compression. With 8K keys and 10K values per key, round-robin produces constant
deltas. Packed stride mode wins because all 256 keys in each stride have similar
row ID magnitudes, allowing a single global FoR bitwidth for the entire stride.

**S5 (Zipfian)**: 15 MB vs Legacy's 172 MB. Per-key BP encoding adapts
independently: hot keys (many values, regular deltas) compress to near-zero bits
per delta, while cold keys (few values) pay the 20B metadata floor but have
negligible total cost.

### Bytes per Value (sealed)

```
Scenario                        Legacy    FSST    Posting
S1: 5M keys x 4 v/k             28.0      6.6      10.2
S2: 512 keys x 7K v/k            9.2      4.8       0.3
S3: Streaming                    11.3      4.6       2.9
S4: 8K keys x 10K v/k           13.1      4.7       0.3
S5: Zipfian                      18.0      5.0       1.5
```

### Write Throughput

```
Scenario                        Legacy    FSST    Posting
S1: 5M keys x 4 v/k            1274 ms    956 ms   1627 ms
S2: 512 keys x 7K v/k            38 ms     86 ms     36 ms
S3: Streaming                    116 ms    251 ms    256 ms
S4: 8K keys x 10K v/k           840 ms   1270 ms    480 ms
S5: Zipfian                      114 ms    210 ms    134 ms
```

Posting write time includes the encoding overhead (delta computation, FoR, and
bitpacking). For S1 (single large commit), this is ~1.3x Legacy. For S4,
Posting is faster because the highly compressible data (constant deltas, bitWidth
= 0) makes encoding trivially cheap -- most blocks write zero packed bytes.

### Read: Full Scan (sealed, all keys)

```
Scenario                        Legacy    FSST    Posting
S1: 5M keys x 4 v/k            953 ms    316 ms    199 ms
S2: 512 keys x 7K v/k           14 ms     38 ms     13 ms
S3: Streaming                    31 ms     67 ms     25 ms
S4: 8K keys x 10K v/k          323 ms    816 ms    288 ms
S5: Zipfian                      38 ms    102 ms     44 ms
```

Posting full scans are faster than Legacy because the stride-indexed layout
provides sequential access -- the reader walks strides in order, and within
each stride, keys are contiguous in memory. Legacy's linked-block layout causes
random I/O when blocks for different keys are interleaved in the value file.

### Read: Range Scan (sealed, middle 50% of row range)

```
Scenario                        Legacy    FSST    Posting
S1: 5M keys x 4 v/k             3.1 ms    1.5 ms   16.9 ms
S2: 512 keys x 7K v/k           3.3 ms   22.9 ms    7.6 ms
S3: Streaming                    1.0 ms    1.3 ms    0.6 ms
S4: 8K keys x 10K v/k          23.5 ms   72.7 ms   21.4 ms
S5: Zipfian                     20.2 ms   68.7 ms   26.5 ms
```

Range scans are the one area where Posting can be slower than Legacy. Legacy's
block-linked-list structure allows direct seeking to the block containing the
range start (via the last-value pointer in each block header). Posting must
decode blocks sequentially from the beginning of each key until reaching the
range, since the BP encoding does not store per-block max values for random
access. For small keys (S3), this is negligible; for large keys with many blocks
(S1, S5), the decode overhead is measurable.


## Seal Benefit

### Posting (minimal seal benefit for size)

Posting's sparse generation format only stores active keys per commit. If a
partition has 50K keys and 1,000 are active per commit, each generation is just
the data for those 1,000 keys plus a 12 KB header. The unsealed size is already
compact -- seal primarily reorganizes data into the stride-indexed layout for
faster reads, not for dramatic size reduction.

The main exception is S1 (5M keys, single commit), where unsealed Posting is
2,556 MB because all 20M values are in a single sparse generation without stride
optimization. Seal compresses this to 195 MB by applying packed stride encoding.

### FSST (large seal benefit for size)

FSST's unsealed format stores a per-key entry (count + offset) for ALL keys in
every generation, regardless of how many are active. With 50K keys and 500
commits, the unsealed header overhead is `50K * 8B * 500 = 200 MB` of counts
and offsets alone. Seal merges into a single generation, reducing this to one
set of 50K entries.

```
Unsealed size comparison:
Scenario     FSST Unsealed    Posting Unsealed
S2           19 MB            2 MB
S3           218 MB           23 MB
S4           358 MB           626 MB
S5           59 MB            79 MB
```

For S3 (streaming), FSST inflates to 218 MB unsealed vs Posting's 23 MB,
because FSST writes a full 50K-key header per generation. Posting's sparse
format stores only the 1,000 active keys per generation.
