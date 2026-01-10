# ILP Wire Protocol Optimization Research

Research into optimizing QuestDB's ILP (InfluxDB Line Protocol) wire format for improved throughput.

## Executive Summary

**Problem:** Current ILP is row-oriented text format where each row repeats schema (column names). This is wasteful for bandwidth and requires server-side parsing/transpose.

**Recommendation:** Custom columnar binary protocol (ILP v4) that:
- Groups rows by table, sends columnar data
- Uses Gorilla delta-of-delta encoding for timestamps (12x compression)
- Supports multiple tables in a single batch
- Requires no external dependencies

**Key Finding:** Apache Arrow IPC cannot support multiple schemas in a single stream, making it unsuitable for QuestDB's multi-table ingestion model.

---

## Current State

QuestDB ILP Sender uses InfluxDB Line Protocol (text-based, row-oriented):
```
trades,symbol=ETH-USD,side=sell price=2615.54,amount=0.00044 1234567890000000000
```

**Pain points:**
- Each row repeats column names
- Text encoding is verbose (numbers as strings)
- No columnar layout = WAL must transpose on write
- No compression

---

## Options Evaluated

### 1. Apache Arrow IPC

**Pros:** Industry standard, zero-copy, columnar, built-in compression (LZ4/Zstd)

**Cons:**
- **Critical:** Cannot support multiple schemas in single stream
- FlatBuffers dependency
- Overhead for small batches

**Verdict:** Not suitable for multi-table ingestion

### 2. Apache Flight (Arrow + gRPC)

**Pros:** Arrow benefits + bidirectional streaming, back-pressure

**Cons:** Heavy gRPC dependency, same multi-schema limitation as Arrow

**Note:** InfluxDB v3 uses Flight but for queries, not writes. They still accept text line protocol for ingestion.

### 3. Custom Protocol with FlatBuffers/Cap'n Proto

**Pros:** Maximum control, zero-copy reads, can optimize for time-series

**Cons:** Must maintain spec, less ecosystem support

| Aspect | FlatBuffers | Cap'n Proto |
|--------|-------------|-------------|
| Decode | 19ns | 830ns |
| Size | 432 bytes | 440 bytes |
| Evolution | vtables | fixed offsets |

### 4. Gorilla Compression

Facebook's time-series compression achieves **12x reduction** (16 bytes → 1.37 bytes):

- **Timestamps:** Delta-of-delta encoding, 96% compress to 1 bit
- **Values:** XOR encoding, 59% identical to previous value

Can be layered on top of any format.

### 5. Custom ILP v4 (Recommended)

Evolution of current ILP with columnar batching, no external dependencies.

---

## How InfluxDB v3 Works

InfluxDB v3 **did NOT change the wire protocol**:
```
Client → [Text Line Protocol] → Ingester → Parse → Arrow (memory) → Parquet (disk)
```

Arrow is used internally AFTER parsing. Server does the row→columnar conversion.

Our approach moves columnar conversion to the **client side**, reducing:
- Wire bandwidth (binary + compression)
- Server CPU (no parsing, no transpose)

---

## Recommended Design: ILP v4

### Wire Format

```
┌─────────────────────────────────────────────────────────┐
│ HEADER                                                  │
│  - Magic: "ILP4" (4 bytes)                              │
│  - Version: uint8                                       │
│  - Flags: uint16 (compression, etc.)                    │
│  - Table count: varint                                  │
└─────────────────────────────────────────────────────────┘
│ TABLE BLOCK 1                                           │
│  - Table name (length-prefixed)                         │
│  - Schema (column names + types)                        │
│  - Row count: varint                                    │
│  - COLUMN DATA (columnar layout)                        │
│    - Symbols: dictionary encoded                        │
│    - Doubles: binary float64 array                      │
│    - Longs: binary int64 or varint                      │
│    - Strings: offset array + data                       │
│    - Timestamps: delta-of-delta (Gorilla)               │
│    - Nulls: bitmap                                      │
│ TABLE BLOCK 2...                                        │
└─────────────────────────────────────────────────────────┘
│ OPTIONAL: LZ4 compressed payload                        │
└─────────────────────────────────────────────────────────┘
```

### Key Features

1. **Multi-table support:** Each batch can contain multiple table blocks
2. **Schema caching:** Send schema hash or full schema if changed
3. **Columnar within table:** Each table block has columnar layout
4. **Gorilla timestamps:** Delta-of-delta encoding (12x compression)
5. **Dictionary encoding:** Symbols sent as IDs with string table
6. **Varint for integers:** Small values use fewer bytes
7. **Optional LZ4:** Compression flag for entire payload
8. **Protocol evolution:** Version byte + flags for extensions

### Encoding Details

**Delta-of-delta timestamps (Gorilla):**
```
D = (t[n] - t[n-1]) - (t[n-1] - t[n-2])

if D == 0:          write '0'                (1 bit)
if D in [-63,64]:   write '10' + 7 bits      (9 bits)
if D in [-255,256]: write '110' + 9 bits     (11 bits)
if D in [-2047,2048]: write '1110' + 12 bits (16 bits)
else:               write '1111' + 32 bits   (36 bits)
```
Result: 96% of regular timestamps → 1 bit

**Symbol dictionary:**
```
First occurrence:  0x00 + varint(id) + length + string
Subsequent:        0x01 + varint(id)
```

### Sender API Changes

Minimal - internal buffering changes only:

```java
// Current API (unchanged)
sender.table("trades")
    .symbol("symbol", "ETH-USD")
    .doubleColumn("price", 2615.54)
    .atNow();

// Internal: rows buffered by table, transposed to columnar on flush()
sender.flush(); // Encodes as ILP v4 columnar batch
```

---

## Comparison Matrix

| Aspect | Arrow IPC | Flight | Custom/FB | Gorilla | ILP v4 |
|--------|-----------|--------|-----------|---------|--------|
| Columnar | Yes | Yes | Yes | No | Yes |
| Multi-table | **No** | **No** | Yes | N/A | Yes |
| Schema-first | Yes | Yes | Yes | N/A | Yes |
| Compression | LZ4/Zstd | LZ4/Zstd | Custom | XOR/Delta | LZ4 |
| Zero-copy | Yes | Yes | Yes | No | Partial |
| Dependencies | Arrow lib | gRPC+Arrow | FB/Capnp | None | None |
| API change | Minimal | Medium | Minimal | None | Minimal |

---

## Next Steps

1. **Prototype encoder** in Java (Sender side)
2. **Prototype decoder** (Server side)
3. **Benchmark** vs current ILP:
   - Encode/decode throughput
   - Wire size comparison
   - Memory allocation
4. **Validate** Gorilla compression ratio on real workloads
5. **Evaluate** LZ4 benefit vs CPU cost

---

## References

- [Gorilla Paper (VLDB)](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf) - Facebook's time-series compression
- [gorilla-tsc Java](https://github.com/burmanm/gorilla-tsc) - Java implementation of Gorilla encoding
- [Cap'n Proto vs FlatBuffers](https://capnproto.org/news/2014-06-17-capnproto-flatbuffers-sbe.html) - Zero-copy format comparison
- [Arrow IPC Format](https://arrow.apache.org/docs/format/IPC.html) - Arrow specification
- [Arrow Multi-Schema Issue](https://github.com/apache/arrow/issues/40853) - Arrow limitation documentation
- [InfluxDB v3 Architecture](https://www.infoq.com/articles/timeseries-db-rust/) - InfluxDB IOx design
- [Time-series Compression Explained](https://www.tigerdata.com/blog/time-series-compression-algorithms-explained) - Compression overview