# ILP v4 Implementation Plan

Test-driven, iterative implementation of the ILP v4 columnar binary protocol.

## Guiding Principles

1. **Iterative Development**: Each iteration produces working, tested code
2. **Test-Driven**: Tests written first, implementation follows. No iteration is complete until all tests pass
3. **No Shortcuts**: Design problems are solved through understanding, not workarounds
4. **Integration-Ready**: Each iteration integrates with existing QuestDB infrastructure

## Architecture Overview

ILP v4 introduces a columnar binary format that differs fundamentally from the row-oriented text protocol. The implementation requires new components while reusing existing infrastructure where appropriate.

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                    Existing Infrastructure              │
                    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
                    │  │ CairoEngine │  │ WalWriter   │  │ LineTcpReceiver │  │
                    │  └─────────────┘  └─────────────┘  └─────────────────┘  │
                    └─────────────────────────────────────────────────────────┘
                                              ▲
                                              │
                    ┌─────────────────────────┴───────────────────────────────┐
                    │                    ILP v4 Components                    │
                    │                                                         │
                    │  ┌──────────────────┐    ┌────────────────────────────┐ │
                    │  │ CapabilityNegot. │───▶│ IlpV4ConnectionContext     │ │
                    │  └──────────────────┘    └────────────────────────────┘ │
                    │                                     │                   │
                    │                                     ▼                   │
                    │  ┌──────────────────┐    ┌────────────────────────────┐ │
                    │  │ Decompressor     │◀───│ IlpV4MessageDecoder        │ │
                    │  │ (LZ4/Zstd)       │    └────────────────────────────┘ │
                    │  └──────────────────┘               │                   │
                    │                                     ▼                   │
                    │  ┌──────────────────┐    ┌────────────────────────────┐ │
                    │  │ SchemaCache      │◀───│ IlpV4TableBlockDecoder     │ │
                    │  └──────────────────┘    └────────────────────────────┘ │
                    │                                     │                   │
                    │                                     ▼                   │
                    │                          ┌────────────────────────────┐ │
                    │                          │ IlpV4ColumnDecoder         │ │
                    │                          │ (per-type decoders)        │ │
                    │                          └────────────────────────────┘ │
                    │                                     │                   │
                    │                                     ▼                   │
                    │                          ┌────────────────────────────┐ │
                    │                          │ IlpV4WalAppender           │ │
                    │                          │ (columnar write path)      │ │
                    │                          └────────────────────────────┘ │
                    └─────────────────────────────────────────────────────────┘
```

---

## Transport Architecture

ILP v4 is a TCP-based protocol. This section defines transport-layer decisions.

### Transport Modes

| Mode | Description | Priority |
|------|-------------|----------|
| **TCP** | Primary transport, same port as text ILP (default 9009) | P0 - Required |
| **TCP + TLS** | Encrypted TCP, same as existing ILP/TCP TLS | P0 - Required |
| **HTTP** | ILP v4 over HTTP POST (future consideration) | P2 - Deferred |
| **UDP** | Not supported (ILP v4 requires responses) | N/A |

### Connection Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Connection State Machine                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│    ┌──────────┐     4 bytes      ┌─────────────────────────────────────┐   │
│    │ ACCEPTED │ ───────────────▶ │ DETECTING                           │   │
│    └──────────┘                  │ (peek first 4 bytes)                │   │
│                                  └─────────────────────────────────────┘   │
│                                         │                                   │
│                    ┌────────────────────┼────────────────────┐              │
│                    │                    │                    │              │
│                    ▼                    ▼                    ▼              │
│            ┌─────────────┐      ┌─────────────┐      ┌─────────────┐       │
│            │ "ILP?"      │      │ "ILP4"      │      │ [a-zA-Z]    │       │
│            │ Handshake   │      │ Direct msg  │      │ Text proto  │       │
│            └─────────────┘      └─────────────┘      └─────────────┘       │
│                    │                    │                    │              │
│                    ▼                    │                    ▼              │
│            ┌─────────────┐              │           ┌─────────────────┐    │
│            │ NEGOTIATING │              │           │ DELEGATE TO     │    │
│            │ (send resp) │              │           │ LineTcpContext  │    │
│            └─────────────┘              │           └─────────────────┘    │
│                    │                    │                                   │
│                    ▼                    ▼                                   │
│            ┌───────────────────────────────────────┐                       │
│            │ AUTHENTICATED                         │                       │
│            │ (if auth enabled, challenge/response) │                       │
│            └───────────────────────────────────────┘                       │
│                              │                                              │
│                              ▼                                              │
│            ┌───────────────────────────────────────┐                       │
│            │ RECEIVING                             │◀──────┐               │
│            │ (read message header + payload)       │       │               │
│            └───────────────────────────────────────┘       │               │
│                              │                             │               │
│                              ▼                             │               │
│            ┌───────────────────────────────────────┐       │               │
│            │ PROCESSING                            │       │               │
│            │ (decode + write to WAL)               │       │               │
│            └───────────────────────────────────────┘       │               │
│                              │                             │               │
│                              ▼                             │               │
│            ┌───────────────────────────────────────┐       │               │
│            │ RESPONDING                            │───────┘               │
│            │ (send status code)                    │                       │
│            └───────────────────────────────────────┘                       │
│                              │                                              │
│                              ▼                                              │
│            ┌───────────────────────────────────────┐                       │
│            │ CLOSED                                │                       │
│            └───────────────────────────────────────┘                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Pipelining

Per spec Section 9.3, clients may send up to 4 batches before waiting for responses.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Pipelining Example (max 4 in-flight)                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Client                                              Server                 │
│    │                                                    │                   │
│    │──── Batch 1 ─────────────────────────────────────▶│                   │
│    │──── Batch 2 ─────────────────────────────────────▶│                   │
│    │──── Batch 3 ─────────────────────────────────────▶│                   │
│    │──── Batch 4 ─────────────────────────────────────▶│                   │
│    │                                                    │ Processing...     │
│    │◀─── Response 1 ───────────────────────────────────│                   │
│    │──── Batch 5 ─────────────────────────────────────▶│ (slot freed)      │
│    │◀─── Response 2 ───────────────────────────────────│                   │
│    │──── Batch 6 ─────────────────────────────────────▶│                   │
│    │◀─── Response 3 ───────────────────────────────────│                   │
│    │◀─── Response 4 ───────────────────────────────────│                   │
│    │◀─── Response 5 ───────────────────────────────────│                   │
│    │◀─── Response 6 ───────────────────────────────────│                   │
│    │                                                    │                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Receive Buffer Strategy

ILP v4 messages can be up to 16MB. Buffer management is critical.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Receive Buffer States                                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. WAITING_HEADER (need 12 bytes)                                          │
│     ┌─────────────────────────────────────────────────────────────────┐    │
│     │ [ received bytes... ]                    [ empty ]              │    │
│     └─────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  2. WAITING_PAYLOAD (header parsed, need payload_length bytes)              │
│     ┌─────────────────────────────────────────────────────────────────┐    │
│     │ [HDR][ received payload bytes... ]       [ empty ]              │    │
│     └─────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  3. MESSAGE_COMPLETE (ready to process)                                     │
│     ┌─────────────────────────────────────────────────────────────────┐    │
│     │ [HDR][ complete payload ]                                       │    │
│     └─────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  Strategy:                                                                  │
│  - Initial buffer: 64KB (handles most batches)                              │
│  - Grow on demand: up to max_batch_size (16MB default)                      │
│  - Reuse buffer across messages (reset position, keep capacity)             │
│  - Consider direct ByteBuffer for zero-copy to decoder                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Back-Pressure Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Back-Pressure Handling                                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Server detects overload:                                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ WAL queue depth > threshold  OR  Memory pressure  OR  IO saturation │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Return OVERLOADED (0x07) response                                   │   │
│  │ Stop reading from socket (apply TCP back-pressure)                  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Client receives OVERLOADED                                          │   │
│  │ Exponential backoff: 100ms, 200ms, 400ms, ... up to 10s            │   │
│  │ Retry same batch                                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### TLS Integration

ILP v4 uses the same TLS configuration as existing ILP/TCP:

- Reuse `DelegatingTlsChannel` from existing implementation
- TLS handshake occurs before protocol detection
- Certificate validation per existing `ilp.tls.*` configuration
- No TLS-specific changes for v4

### Authentication Integration

ILP v4 uses the same authentication as existing ILP/TCP:

- Token-based challenge/response after capability negotiation
- Reuse `EllipticCurveAuthenticator` from existing implementation
- Auth occurs after v4 handshake, before message processing
- No auth-specific changes for v4

---

## Iteration 0: Foundation & Varint Utilities

**Goal**: Establish core binary encoding primitives used throughout ILP v4.

### Scope

- Varint encoder/decoder (unsigned LEB128)
- ZigZag encoder/decoder (for signed varints)
- Bit-level reader/writer utilities
- XXH64 wrapper for schema hashing

### Test Requirements

```
IlpV4VarintTest
├── testEncodeDecodeZero
├── testEncodeDecode127 (1-byte boundary)
├── testEncodeDecode128 (2-byte boundary)
├── testEncodeDecode16383 (2-byte max)
├── testEncodeDecode16384 (3-byte boundary)
├── testEncodeLargeValues (up to Long.MAX_VALUE)
├── testRoundTripRandomValues (1000 random longs)
├── testDecodeFromBuffer (ByteBuffer integration)
├── testEncodeToBuffer (ByteBuffer integration)
├── testDecodeIncompleteVarint (error handling)
└── testDecodeOverflow (too many continuation bytes)

IlpV4ZigZagTest
├── testEncodeDecodeZero
├── testEncodePositive
├── testEncodeNegative
├── testEncodeMinLong
├── testEncodeMaxLong
├── testSymmetry (encode then decode equals original)
└── testRoundTripRandomValues

IlpV4BitWriterReaderTest
├── testWriteReadSingleBit
├── testWriteReadMultipleBits
├── testWriteReadCrossByteBoundary
├── testWriteReadSignedValues
├── testBitAlignment
├── testFlushPartialByte
└── testReadBeyondWritten (error handling)

IlpV4SchemaHashTest
├── testEmptySchema
├── testSingleColumn
├── testMultipleColumns
├── testColumnOrderMatters
├── testTypeAffectsHash
├── testNameAffectsHash
└── testDeterministic (same input = same hash)
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4Varint.java
├── IlpV4ZigZag.java
├── IlpV4BitWriter.java
├── IlpV4BitReader.java
└── IlpV4SchemaHash.java

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4VarintTest.java
├── IlpV4ZigZagTest.java
├── IlpV4BitWriterReaderTest.java
└── IlpV4SchemaHashTest.java
```

### Acceptance Criteria

- [ ] All varint tests pass
- [ ] All zigzag tests pass
- [ ] All bit writer/reader tests pass
- [ ] All schema hash tests pass
- [ ] Zero allocations in encode/decode hot paths (verified via profiler or allocation tracker)
- [ ] Performance: varint decode < 10ns per value (benchmark)

### Design Notes

- Use `long` internally for all varint operations (QuestDB convention)
- BitWriter/BitReader operate on `long` buffers for SIMD-friendliness
- Schema hash uses existing XXH64 implementation if available, otherwise implement per Appendix C

---

## Iteration 1: Message Header Parsing

**Goal**: Parse and validate ILP v4 message headers.

### Scope

- Magic byte validation ("ILP4")
- Version extraction and validation
- Flag parsing (compression, Gorilla timestamps)
- Table count and payload length extraction
- Header constants and error codes

### Test Requirements

```
IlpV4MessageHeaderTest
├── testValidHeader
├── testMagicBytes (exact match required)
├── testInvalidMagic (various wrong values)
├── testVersion1 (current version)
├── testUnsupportedVersion (future versions)
├── testFlagLZ4
├── testFlagZstd
├── testFlagGorilla
├── testFlagCombinations (multiple flags)
├── testTableCountZero
├── testTableCountMax (65535)
├── testPayloadLengthZero
├── testPayloadLengthMax (16MB default limit)
├── testPayloadLengthExceedsLimit
├── testHeaderTooShort (< 12 bytes)
├── testParseFromDirectBuffer
├── testParseFromHeapBuffer
└── testHeaderToString (debugging)

IlpV4ConstantsTest
├── testMagicBytesValue
├── testHeaderSize
├── testFlagBitPositions
├── testTypeCodes
└── testStatusCodes
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4Constants.java
├── IlpV4MessageHeader.java
└── IlpV4ParseException.java

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4MessageHeaderTest.java
└── IlpV4ConstantsTest.java
```

### Acceptance Criteria

- [ ] All header parsing tests pass
- [ ] Invalid headers produce clear error messages
- [ ] Header parsing is zero-allocation (reusable header object)
- [ ] Endianness handled correctly (little-endian)
- [ ] Constants match specification exactly

### Design Notes

- `IlpV4MessageHeader` is a mutable, reusable object (QuestDB pattern)
- Consider making header parsing a static method that populates a provided object
- Error messages include byte offset for debugging

---

## Iteration 2: Capability Negotiation

**Goal**: Implement connection handshake for ILP v4 protocol detection and feature negotiation.

### Scope

- Parse CAPABILITY_REQUEST ("ILP?" magic)
- Generate CAPABILITY_RESPONSE ("ILP!" magic)
- Version negotiation logic
- Feature flag negotiation
- Fallback detection ("ILP0" for old servers)

### Test Requirements

```
IlpV4CapabilityNegotiationTest
├── testParseCapabilityRequest
├── testRequestMagicBytes
├── testRequestMinMaxVersion
├── testRequestFlags (LZ4, Zstd, Gorilla)
├── testRequestReservedBitsIgnored
├── testGenerateCapabilityResponse
├── testResponseMagicBytes
├── testVersionNegotiation_ClientAndServerSame
├── testVersionNegotiation_ClientHigherThanServer
├── testVersionNegotiation_ServerHigherThanClient
├── testVersionNegotiation_NoOverlap (connection fails)
├── testFlagNegotiation_BothSupportLZ4
├── testFlagNegotiation_OnlyClientSupportsZstd
├── testFlagNegotiation_IntersectionOfFlags
├── testFallbackResponse (ILP0 magic)
├── testRequestTooShort
├── testRequestTooLong (extra bytes ignored or error?)
└── testThreadSafety (concurrent negotiations)

IlpV4ProtocolDetectorTest
├── testDetectIlpV4Request (starts with "ILP?")
├── testDetectIlpV4Direct (starts with "ILP4")
├── testDetectTextProtocol (starts with table name)
├── testDetectWithPartialData (need more bytes)
├── testDetectEmptyBuffer
└── testDetectBinaryGarbage
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4CapabilityRequest.java
├── IlpV4CapabilityResponse.java
├── IlpV4Negotiator.java
└── IlpV4ProtocolDetector.java

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4CapabilityNegotiationTest.java
└── IlpV4ProtocolDetectorTest.java
```

### Acceptance Criteria

- [ ] All negotiation tests pass
- [ ] Protocol detection works with minimal bytes (4 bytes sufficient)
- [ ] Version negotiation follows spec exactly
- [ ] Fallback path tested and working
- [ ] Negotiator is stateless and thread-safe

### Design Notes

- Protocol detector returns enum: `V4_HANDSHAKE`, `V4_DIRECT`, `TEXT_PROTOCOL`, `NEED_MORE_DATA`, `UNKNOWN`
- Negotiator should be integrated into existing `LineTcpConnectionContext` pattern
- Consider whether to support "direct batch" (no handshake) in initial implementation

---

## Iteration 2.5: Transport Layer & Buffer Management

**Goal**: Implement transport-layer primitives for receiving ILP v4 messages over TCP.

### Scope

- Receive buffer with grow-on-demand strategy
- Message framing (header + payload accumulation)
- Pipelining queue for in-flight batches
- Response write buffer
- Back-pressure detection hooks

### Test Requirements

```
IlpV4ReceiveBufferTest
├── testInitialCapacity (64KB default)
├── testGrowOnDemand
├── testGrowToMaxLimit (16MB)
├── testGrowBeyondMaxLimit (error)
├── testResetWithoutShrink
├── testShrinkAfterIdlePeriod
├── testAccumulatePartialHeader
├── testAccumulateCompleteHeader
├── testAccumulatePartialPayload
├── testAccumulateCompleteMessage
├── testAccumulateMultipleMessages
├── testCompactAfterConsume
├── testDirectBufferBackedMemory
└── testZeroCopySlice

IlpV4MessageFramerTest
├── testFrameStateWaitingHeader
├── testFrameStateWaitingPayload
├── testFrameStateComplete
├── testFramePartialHeaderRead
├── testFrameCompleteHeaderRead
├── testFramePartialPayloadRead
├── testFrameCompletePayloadRead
├── testFrameMultipleMessagesInBuffer
├── testFramePayloadLengthValidation
├── testFrameResetAfterConsume
├── testFrameWithCompressedPayload
└── testFrameErrorOnOversizedPayload

IlpV4PipelineQueueTest
├── testEnqueueBatch
├── testDequeueInOrder
├── testMaxInFlightLimit (4 batches)
├── testBlockWhenFull
├── testUnblockWhenSlotFreed
├── testResponseCorrelation
├── testPipelineEmpty
├── testPipelineDrain
└── testConcurrentEnqueueDequeue

IlpV4ResponseWriterTest
├── testWriteOkResponse
├── testWriteErrorResponse
├── testWritePartialFailure
├── testBufferResponse
├── testFlushToSocket
├── testPartialFlush
├── testFlushMultipleResponses
└── testWriteBufferGrow

IlpV4BackPressureTest
├── testDetectWalQueueDepth
├── testDetectMemoryPressure
├── testBackPressureThreshold
├── testBackPressureRelease
├── testStopReadingOnBackPressure
├── testResumeReadingOnRelease
└── testOverloadedResponseTrigger
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4ReceiveBuffer.java
├── IlpV4MessageFramer.java
├── IlpV4PipelineQueue.java
├── IlpV4ResponseWriter.java
└── IlpV4BackPressureDetector.java

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4ReceiveBufferTest.java
├── IlpV4MessageFramerTest.java
├── IlpV4PipelineQueueTest.java
├── IlpV4ResponseWriterTest.java
└── IlpV4BackPressureTest.java
```

### Acceptance Criteria

- [ ] All receive buffer tests pass
- [ ] All message framer tests pass
- [ ] All pipeline queue tests pass
- [ ] All response writer tests pass
- [ ] All back-pressure tests pass
- [ ] Buffer growth does not cause data loss
- [ ] Pipelining respects max in-flight limit
- [ ] Zero-copy where possible (direct buffer slices)
- [ ] Integration with existing `IODispatcher` patterns

### Design Notes

- `IlpV4ReceiveBuffer` wraps QuestDB's memory primitives (e.g., `DirectUtf8Sink` or raw `MemoryA`)
- Message framer is a state machine: `WAITING_HEADER` → `WAITING_PAYLOAD` → `MESSAGE_READY`
- Pipeline queue tracks batch boundaries for response correlation
- Back-pressure detector integrates with WAL commit queue depth metrics
- Consider reusing `AdaptiveRecvBuffer` patterns from existing ILP implementation

---

## Iteration 3: Table Header & Schema Parsing

**Goal**: Parse table blocks including full schema and schema reference modes.

### Scope

- Table header parsing (name, row count, column count)
- Full schema parsing (column names and types)
- Schema reference parsing (hash lookup)
- Schema caching infrastructure
- Column type code validation

### Test Requirements

```
IlpV4TableHeaderTest
├── testParseTableName
├── testTableNameEmpty (error)
├── testTableNameMaxLength
├── testTableNameUtf8
├── testTableNameInvalidUtf8 (error handling)
├── testRowCountZero
├── testRowCountVarint
├── testRowCountMax
├── testColumnCountZero (valid? empty table block)
├── testColumnCountVarint
├── testColumnCountMax (2048)
├── testColumnCountExceedsMax (error)
└── testParseMultipleTableHeaders

IlpV4SchemaTest
├── testParseFullSchema
├── testParseSingleColumn
├── testParseMultipleColumns
├── testParseAllColumnTypes (0x01-0x0F)
├── testParseNullableType (high bit set)
├── testColumnNameEmpty (error)
├── testColumnNameMaxLength
├── testColumnNameUtf8
├── testSchemaReferenceMode
├── testSchemaHashExtraction
├── testUnknownSchemaMode (error)
├── testInvalidColumnType (error)
└── testSchemaToColumnList

IlpV4SchemaCacheTest
├── testCacheEmpty
├── testCachePut
├── testCacheGet
├── testCacheHit
├── testCacheMiss
├── testCacheByTableAndHash
├── testCacheEviction (LRU or size-based)
├── testCacheInvalidation
├── testCacheThreadSafety
└── testCacheMetrics (hit rate)
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4TableHeader.java
├── IlpV4Schema.java
├── IlpV4ColumnDef.java
├── IlpV4SchemaCache.java
└── IlpV4TypeCode.java (enum with validation)

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4TableHeaderTest.java
├── IlpV4SchemaTest.java
└── IlpV4SchemaCacheTest.java
```

### Acceptance Criteria

- [ ] All table header tests pass
- [ ] All schema tests pass
- [ ] All schema cache tests pass
- [ ] Schema parsing handles all 15 type codes
- [ ] Nullable flag (0x80) correctly detected
- [ ] Schema cache is thread-safe
- [ ] Schema hash computed correctly per spec

### Design Notes

- `IlpV4Schema` should be immutable once parsed (safe for caching)
- `IlpV4ColumnDef` is a simple record: name, type, nullable
- Schema cache key is `(tableName, schemaHash)` tuple
- Consider cache size limits based on typical deployment (100-1000 schemas)

---

## Iteration 4: Fixed-Width Column Decoders

**Goal**: Decode fixed-width column data (BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, UUID, LONG256).

### Scope

- Null bitmap parsing
- Fixed-width value array parsing
- Endianness handling
- Type-specific validation

### Test Requirements

```
IlpV4NullBitmapTest
├── testEmptyBitmap (0 rows)
├── testAllNulls
├── testNoNulls
├── testMixedNulls
├── testBitmapBitOrder (LSB first)
├── testBitmapByteAlignment
├── testBitmapSizeCalculation
└── testBitmapWithPartialLastByte

IlpV4FixedWidthDecoderTest
├── testDecodeByteColumn
├── testDecodeByteColumnWithNulls
├── testDecodeShortColumn
├── testDecodeShortColumnWithNulls
├── testDecodeIntColumn
├── testDecodeIntColumnWithNulls
├── testDecodeLongColumn
├── testDecodeLongColumnWithNulls
├── testDecodeFloatColumn
├── testDecodeFloatColumnWithNulls
├── testDecodeFloatSpecialValues (NaN, Inf)
├── testDecodeDoubleColumn
├── testDecodeDoubleColumnWithNulls
├── testDecodeDoubleSpecialValues (NaN, Inf)
├── testDecodeDateColumn
├── testDecodeDateColumnWithNulls
├── testDecodeUuidColumn
├── testDecodeUuidColumnWithNulls
├── testDecodeUuidEndianness (big-endian)
├── testDecodeLong256Column
├── testDecodeLong256ColumnWithNulls
├── testDecodeLong256Endianness (big-endian)
├── testDecodeEmptyColumn (0 rows)
├── testDecodeInsufficientData (error)
└── testDecodeLargeColumn (100K rows)

IlpV4BooleanDecoderTest
├── testDecodeBooleanColumn
├── testDecodeBooleanWithNulls
├── testBooleanBitPacking
├── testBooleanPartialLastByte
└── testBooleanLargeColumn
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4NullBitmap.java
├── IlpV4ColumnDecoder.java (interface)
├── IlpV4FixedWidthDecoder.java
└── IlpV4BooleanDecoder.java

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4NullBitmapTest.java
├── IlpV4FixedWidthDecoderTest.java
└── IlpV4BooleanDecoderTest.java
```

### Acceptance Criteria

- [ ] All null bitmap tests pass
- [ ] All fixed-width decoder tests pass
- [ ] All boolean decoder tests pass
- [ ] Correct endianness for each type (little-endian except UUID/LONG256)
- [ ] Null values at correct positions
- [ ] Performance: decode 1M values in < 10ms

### Design Notes

- `IlpV4ColumnDecoder` interface allows different decode strategies
- Decoders write directly to `MemoryA` or similar QuestDB memory abstraction
- Consider SIMD-friendly memory layout for bulk decoding
- Null bitmap indices match row indices exactly

---

## Iteration 5: Variable-Width Column Decoders

**Goal**: Decode STRING, VARCHAR, and SYMBOL columns.

### Scope

- Offset array parsing for STRING/VARCHAR
- String data extraction
- Symbol dictionary parsing
- Symbol index array decoding

### Test Requirements

```
IlpV4StringDecoderTest
├── testDecodeEmptyStringColumn
├── testDecodeSingleString
├── testDecodeMultipleStrings
├── testDecodeEmptyStrings
├── testDecodeUtf8Strings
├── testDecodeWithNulls
├── testOffsetArrayValidation
├── testOffsetArrayOutOfBounds (error)
├── testStringMaxLength
├── testStringLargeColumn (10K strings)
└── testDecodeVarcharSameAsString

IlpV4SymbolDecoderTest
├── testDecodeEmptySymbolColumn
├── testDecodeSingleSymbol
├── testDecodeMultipleSymbols
├── testDictionaryParsing
├── testDictionaryEmpty
├── testDictionaryLarge (1000 entries)
├── testSymbolIndexMapping
├── testSymbolWithNulls (via bitmap)
├── testSymbolNullIndex (max varint alternative)
├── testSymbolRepeatedValues
├── testSymbolUtf8
├── testInvalidDictionaryIndex (error)
└── testSymbolLargeColumn
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4StringDecoder.java
└── IlpV4SymbolDecoder.java

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4StringDecoderTest.java
└── IlpV4SymbolDecoderTest.java
```

### Acceptance Criteria

- [ ] All string decoder tests pass
- [ ] All symbol decoder tests pass
- [ ] UTF-8 validation correct
- [ ] Symbol dictionary correctly indexed
- [ ] Null handling works for both null mechanisms (bitmap, index -1)
- [ ] Performance: decode 100K strings in < 100ms

### Design Notes

- String decoder outputs to `DirectUtf8Sequence` or similar
- Symbol decoder can integrate with existing QuestDB symbol tables
- Consider lazy string materialization for performance
- Validate UTF-8 on decode (security requirement from spec)

---

## Iteration 6: Timestamp Column Decoder (Gorilla Encoding)

**Goal**: Decode timestamp columns with Gorilla delta-of-delta compression.

### Scope

- Uncompressed timestamp decoding
- Gorilla encoding detection
- Delta-of-delta decoding
- All bucket sizes (1-bit, 9-bit, 12-bit, 16-bit, 36-bit)

### Test Requirements

```
IlpV4TimestampDecoderTest
├── testDecodeUncompressedTimestamps
├── testDecodeUncompressedWithNulls
├── testDecodeGorillaEncodingFlag
├── testDecodeGorillaTwoTimestamps (no DoD)
├── testDecodeGorillaDeltaZero (1-bit case)
├── testDecodeGorillaDeltaSmall (9-bit case, [-63,64])
├── testDecodeGorillaDeltaMedium (12-bit case, [-255,256])
├── testDecodeGorillaDeltaLarge (16-bit case, [-2047,2048])
├── testDecodeGorillaDeltaHuge (36-bit case)
├── testDecodeGorillaMixedBuckets
├── testDecodeGorillaRegularInterval (e.g., 1 second)
├── testDecodeGorillaIrregularInterval
├── testDecodeGorillaWithNulls
├── testDecodeGorillaSignedValues
├── testDecodeGorillaBitAlignment
├── testDecodeGorillaLargeColumn (100K timestamps)
├── testDecodeGorillaRoundTrip (encode then decode)
├── testDecodeGorillaOutOfOrder (fallback handling)
├── testDecodeGorillaOverflow (fallback handling)
└── testDecodeIncompleteData (error)

IlpV4GorillaEncoderTest (for round-trip testing)
├── testEncodeConstantDelta
├── testEncodeVaryingDelta
├── testEncodeAllBucketSizes
├── testEncodeLargeDataset
└── testEncodeMatchesSpec
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4TimestampDecoder.java
└── IlpV4GorillaDecoder.java

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4TimestampDecoderTest.java
└── IlpV4GorillaEncoderTest.java
```

### Acceptance Criteria

- [ ] All timestamp decoder tests pass
- [ ] All Gorilla encoder tests pass (for round-trip verification)
- [ ] Correct bucket detection and decoding
- [ ] Signed delta-of-delta handled correctly
- [ ] Fallback to uncompressed works
- [ ] Performance: decode 1M Gorilla timestamps in < 50ms

### Design Notes

- Gorilla decoding is bit-level, use `IlpV4BitReader` from Iteration 0
- First two timestamps are always uncompressed
- Delta-of-delta can be negative (two's complement)
- Spec issue: ranges are asymmetric, implementation should match spec exactly first, then fix if spec is updated

---

## Iteration 7: GeoHash Column Decoder

**Goal**: Decode GEOHASH columns with variable precision.

### Scope

- GeoHash precision extraction
- Packed geohash value decoding
- Precision-based value interpretation
- Integration with QuestDB GeoHash types

### Test Requirements

```
IlpV4GeoHashDecoderTest
├── testDecodeGeoHashByte (5-bit precision)
├── testDecodeGeoHashShort (10-bit precision)
├── testDecodeGeoHashInt (20-bit precision)
├── testDecodeGeoHashLong (40-bit precision)
├── testDecodeGeoHashWithNulls
├── testDecodePrecisionExtraction
├── testDecodePackedValue
├── testDecodeVariablePrecision
├── testDecodeInvalidPrecision (error)
├── testDecodeEmptyColumn
└── testDecodeLargeColumn
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
└── IlpV4GeoHashDecoder.java

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
└── IlpV4GeoHashDecoderTest.java
```

### Acceptance Criteria

- [ ] All GeoHash decoder tests pass
- [ ] Precision correctly determines value size
- [ ] Integration with existing GeoHash utilities
- [ ] Null handling correct

### Design Notes

- GeoHash encoding in spec is underspecified; may need to reverse-engineer from sender implementation or clarify spec
- QuestDB has existing GeoHash types (GEOBYTE, GEOSHORT, GEOINT, GEOLONG)
- Map precision ranges to appropriate QuestDB type

---

## Iteration 8: Decompression (LZ4 and Zstd)

**Goal**: Decompress compressed payloads before parsing.

### Scope

- LZ4 block decompression
- Zstd block decompression
- Uncompressed size validation (security)
- Decompression buffer management

### Test Requirements

```
IlpV4DecompressorTest
├── testDecompressLZ4
├── testDecompressLZ4Empty
├── testDecompressLZ4Large (1MB)
├── testDecompressLZ4InvalidData (error)
├── testDecompressLZ4SizeMismatch (error)
├── testDecompressZstd
├── testDecompressZstdEmpty
├── testDecompressZstdLarge (1MB)
├── testDecompressZstdInvalidData (error)
├── testDecompressZstdSizeMismatch (error)
├── testDecompressBomb (reject oversized output)
├── testDecompressBasedOnFlags
├── testNoDecompressWhenFlagNotSet
├── testBufferReuse
└── testConcurrentDecompression
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
└── IlpV4Decompressor.java

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
└── IlpV4DecompressorTest.java
```

### Acceptance Criteria

- [ ] All decompressor tests pass
- [ ] LZ4 block format (not frame) supported
- [ ] Zstd block format supported
- [ ] Decompression bombs rejected (size limit)
- [ ] Buffer reuse for zero-allocation hot path

### Design Notes

- QuestDB may have existing LZ4/Zstd bindings; reuse if available
- If not, use JNI to native libraries or pure Java implementations
- Decompression happens once per message, before table block parsing
- Pre-validate uncompressed size against limit before allocating buffer

---

## Iteration 9: Complete Table Block Decoder

**Goal**: Integrate all column decoders into a complete table block decoder.

### Scope

- Table block parsing orchestration
- Schema resolution (full or cached)
- Column decoder dispatch by type
- Row-by-row or columnar data access

### Test Requirements

```
IlpV4TableBlockDecoderTest
├── testDecodeEmptyTableBlock
├── testDecodeSingleRowSingleColumn
├── testDecodeSingleRowMultipleColumns
├── testDecodeMultipleRows
├── testDecodeAllColumnTypes (comprehensive)
├── testDecodeWithFullSchema
├── testDecodeWithSchemaReference
├── testDecodeSchemaReferenceNotCached (error)
├── testDecodeMixedNullableNonNullable
├── testDecodeColumnOrderPreserved
├── testDecodeInvalidSchema (error)
├── testDecodeIncompleteData (error)
├── testDecodeLargeBlock (10K rows, 50 columns)
├── testDecodeMultipleTableBlocks
├── testColumnAccessByIndex
├── testColumnAccessByName
├── testRowIteration
└── testColumnIteration
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4TableBlockDecoder.java
├── IlpV4DecodedTableBlock.java
└── IlpV4DecodedColumn.java

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
└── IlpV4TableBlockDecoderTest.java
```

### Acceptance Criteria

- [ ] All table block decoder tests pass
- [ ] All column types correctly decoded
- [ ] Schema caching integrated
- [ ] Error handling with clear messages
- [ ] Memory efficient (columnar access, no row materialization)

### Design Notes

- `IlpV4DecodedTableBlock` provides columnar access to data
- Column data remains in wire format until accessed (lazy decoding option)
- Row iteration should be efficient for WAL writing
- Consider memory mapping for large blocks

---

## Iteration 10: Complete Message Decoder

**Goal**: Parse complete ILP v4 messages including multi-table batches.

### Scope

- Message parsing orchestration
- Multi-table batch handling
- Decompression integration
- Error aggregation

### Test Requirements

```
IlpV4MessageDecoderTest
├── testDecodeEmptyMessage (0 tables)
├── testDecodeSingleTableMessage
├── testDecodeMultiTableMessage
├── testDecodeMaxTablesMessage (256 tables)
├── testDecodeWithLZ4Compression
├── testDecodeWithZstdCompression
├── testDecodeWithGorillaTimestamps
├── testDecodeAllFlagCombinations
├── testDecodePayloadLengthMismatch (error)
├── testDecodeTableCountMismatch (error)
├── testDecodeCorruptedPayload (error)
├── testDecodeTruncatedMessage (error)
├── testDecodeLargeMessage (16MB)
├── testDecodeIterateTableBlocks
├── testDecodeMemoryManagement
└── testDecodePerformance (benchmark)
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4MessageDecoder.java
└── IlpV4DecodedMessage.java

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
└── IlpV4MessageDecoderTest.java
```

### Acceptance Criteria

- [ ] All message decoder tests pass
- [ ] Multi-table messages correctly parsed
- [ ] Compression correctly applied
- [ ] Payload length validated
- [ ] Performance: decode 16MB message in < 100ms

### Design Notes

- `IlpV4DecodedMessage` is iterable over table blocks
- Consider streaming decode for very large messages
- Decoder should be reusable (reset and reuse pattern)

---

## Iteration 11: WAL Integration (Columnar Write Path)

**Goal**: Write decoded ILP v4 data directly to WAL.

### Scope

- Columnar data to WAL segment writing
- Type mapping from ILP v4 to QuestDB types
- Null handling in WAL format
- Timestamp column identification

### Test Requirements

```
IlpV4WalAppenderTest
├── testAppendEmptyBlock
├── testAppendSingleRow
├── testAppendMultipleRows
├── testAppendAllColumnTypes
├── testAppendWithNulls
├── testAppendNewTable (auto-create)
├── testAppendExistingTable
├── testAppendColumnMismatch (new columns added)
├── testAppendTypeMismatch (error or widening)
├── testAppendTypeWidening (BYTE→SHORT→INT→LONG)
├── testAppendTimestampColumn
├── testAppendDesignatedTimestamp
├── testAppendOutOfOrderTimestamp
├── testAppendSymbolIntegration
├── testAppendLargeBlock (100K rows)
├── testAppendMultipleBlocks
├── testAppendTransaction
├── testAppendRollback
└── testAppendConcurrent (multiple tables)
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
└── IlpV4WalAppender.java

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
└── IlpV4WalAppenderTest.java
```

### Acceptance Criteria

- [ ] All WAL appender tests pass
- [ ] Columnar data written efficiently (minimal copying)
- [ ] Type widening works per spec
- [ ] Symbol tables correctly updated
- [ ] Transaction semantics preserved
- [ ] Performance: write 100K rows in < 50ms

### Design Notes

- Reuse patterns from existing `LineWalAppender` where possible
- Columnar format should allow memcpy-like transfers for matching types
- Handle designated timestamp column identification
- Consider batch commit for multiple table blocks

---

## Iteration 12: Response Generation

**Goal**: Generate ILP v4 response messages.

### Scope

- Status code responses (OK, PARTIAL, errors)
- Partial failure response format
- Error message encoding
- Response serialization

### Test Requirements

```
IlpV4ResponseTest
├── testGenerateOkResponse
├── testGeneratePartialResponse
├── testGenerateSchemaRequiredResponse
├── testGenerateSchemaMismatchResponse
├── testGenerateTableNotFoundResponse
├── testGenerateParseErrorResponse
├── testGenerateInternalErrorResponse
├── testGenerateOverloadedResponse
├── testPartialResponseMultipleTables
├── testPartialResponseErrorMessages
├── testResponseSerialization
├── testResponseParsing (client-side)
└── testResponseRoundTrip
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4Response.java
├── IlpV4ResponseEncoder.java
└── IlpV4StatusCode.java

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
└── IlpV4ResponseTest.java
```

### Acceptance Criteria

- [ ] All response tests pass
- [ ] All status codes supported
- [ ] Partial failure includes per-table details
- [ ] Error messages are UTF-8 encoded
- [ ] Response format matches spec

### Design Notes

- Response encoder should be zero-allocation for OK case
- Error messages should be bounded in length
- Consider response pooling for high-throughput

---

## Iteration 13: Connection Context Integration

**Goal**: Integrate ILP v4 into the existing TCP receiver infrastructure.

### Scope

- `IlpV4ConnectionContext` implementation
- Protocol detection integration
- Handshake state machine
- Message receive and dispatch
- Integration with `LineTcpReceiver`

### Test Requirements

```
IlpV4ConnectionContextTest
├── testHandshakeSuccess
├── testHandshakeFallback
├── testHandshakeTimeout
├── testReceiveMessage
├── testReceiveMultipleMessages
├── testReceivePartialMessage
├── testReceiveLargeMessage
├── testReceiveCompressedMessage
├── testResponseSending
├── testConnectionClose
├── testConnectionReset
├── testPipelining (multiple in-flight)
├── testBackpressure
├── testAuthenticationIntegration
└── testTLSIntegration

IlpV4ReceiverIntegrationTest
├── testProtocolAutoDetection
├── testV4AndTextProtocolMixed
├── testMultipleV4Connections
├── testConnectionPooling
├── testWorkerThreadDistribution
├── testMetricsCollection
└── testGracefulShutdown
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4ConnectionContext.java
└── IlpV4ReceiverConfiguration.java

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4ConnectionContextTest.java
└── IlpV4ReceiverIntegrationTest.java
```

### Acceptance Criteria

- [ ] All connection context tests pass
- [ ] All receiver integration tests pass
- [ ] Protocol detection seamless
- [ ] Handshake completes correctly
- [ ] Messages processed end-to-end
- [ ] Backward compatibility with text protocol

### Design Notes

- Extend or parallel existing `LineTcpConnectionContext`
- State machine: `DETECTING` → `HANDSHAKING` → `RECEIVING`
- Reuse existing IO dispatcher and worker infrastructure
- Configuration should mirror `LineTcpReceiverConfiguration`

---

## Iteration 14: End-to-End Integration Tests

**Goal**: Comprehensive integration testing with real network I/O.

### Scope

- Full protocol flow testing
- Performance benchmarking
- Error scenario coverage
- Interoperability testing

### Test Requirements

```
IlpV4EndToEndTest
├── testSingleRowInsertion
├── testBatchInsertion
├── testMultiTableBatch
├── testAllDataTypes
├── testNullValues
├── testSchemaEvolution
├── testSchemaCache
├── testCompression
├── testGorillaTimestamps
├── testLargePayload
├── testHighThroughput
├── testConcurrentConnections
├── testConnectionRecovery
├── testServerRestart
├── testBackpressureHandling
├── testErrorRecovery
├── testPartialFailure
└── testQueryAfterInsert

IlpV4PerformanceTest
├── testThroughputSmallMessages
├── testThroughputLargeMessages
├── testLatencyP50P99P999
├── testCompressionRatio
├── testCpuUtilization
├── testMemoryUsage
├── testGcPauses
└── testCompareToTextProtocol
```

### Files to Create

```
core/src/test/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4EndToEndTest.java
└── IlpV4PerformanceTest.java
```

### Acceptance Criteria

- [ ] All end-to-end tests pass
- [ ] Performance meets spec expectations (Section 14)
- [ ] Wire size reduction verified
- [ ] CPU reduction verified
- [ ] No memory leaks
- [ ] No GC pressure on hot path

### Design Notes

- Use real sockets, not mocks
- Performance tests should be repeatable and stable
- Consider running performance tests in separate CI job
- Document baseline numbers for regression detection

---

## Iteration 15: Client Sender Implementation

**Goal**: Implement ILP v4 client sender for testing and SDK.

### Scope

- `IlpV4Sender` implementation
- Columnar buffering
- Schema tracking
- Batch encoding
- Compression

### Test Requirements

```
IlpV4SenderTest
├── testCreateSender
├── testSendSingleRow
├── testSendMultipleRows
├── testSendMultipleTables
├── testBufferAllTypes
├── testBufferNulls
├── testSchemaGeneration
├── testSchemaHashing
├── testBatchEncoding
├── testCompressionLZ4
├── testCompressionZstd
├── testGorillaEncoding
├── testFlush
├── testAutoFlush
├── testResponseHandling
├── testRetryOnOverloaded
├── testSchemaRequiredRetry
└── testSenderClose

IlpV4SenderReceiverTest
├── testRoundTripAllTypes
├── testRoundTripNulls
├── testRoundTripCompression
├── testRoundTripLargeBatch
└── testRoundTripMultiTable
```

### Files to Create

```
core/src/main/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4Sender.java
├── IlpV4TableBuffer.java
├── IlpV4MessageEncoder.java
└── IlpV4GorillaEncoder.java

core/src/test/java/io/questdb/cutlass/line/tcp/v4/
├── IlpV4SenderTest.java
└── IlpV4SenderReceiverTest.java
```

### Acceptance Criteria

- [ ] All sender tests pass
- [ ] Sender/receiver round-trip works
- [ ] API is consistent with existing `LineTcpSender`
- [ ] Columnar buffering efficient
- [ ] Compression applied correctly

### Design Notes

- Consider extending `AbstractLineSender` or creating parallel hierarchy
- Sender buffers rows in columnar format from the start
- Flush triggers encode and send
- Response parsing for retry logic

---

## Iteration 16: Documentation and Finalization

**Goal**: Complete documentation, configuration, and production readiness.

### Scope

- Configuration documentation
- Performance tuning guide
- Migration guide
- Monitoring and metrics
- Code cleanup and final review

### Deliverables

- `ILP_V4_CONFIGURATION.md` - All configuration options
- `ILP_V4_MIGRATION.md` - How to enable and test
- `ILP_V4_PERFORMANCE.md` - Tuning recommendations
- Updated `CHANGELOG.md`
- Javadoc for public APIs

### Acceptance Criteria

- [ ] All tests pass
- [ ] Documentation complete
- [ ] Configuration options documented
- [ ] Metrics exposed
- [ ] Code review complete
- [ ] Performance validated against spec

---

## Risk Register

| Risk | Mitigation |
|------|------------|
| Gorilla encoding complexity | Extensive test coverage with known test vectors |
| Schema cache memory growth | Implement LRU eviction, configurable size |
| Spec ambiguities (GeoHash, Symbol nulls) | Document assumptions, clarify with spec author |
| Performance regression | Continuous benchmarking, comparison to text protocol |
| Backward compatibility | Protocol detection ensures text protocol still works |
| Native library dependencies (LZ4/Zstd) | Pure Java fallback, or bundle natives |

---

## Dependency Graph

```
Iteration 0 (Foundation)
    │
    ├── Iteration 1 (Header)
    │       │
    │       └── Iteration 2 (Negotiation)
    │               │
    │               └── Iteration 2.5 (Transport/Buffers)
    │                       │
    │                       └── Iteration 13 (Connection Context) ─────────┐
    │                                                                      │
    ├── Iteration 3 (Schema) ──────────────────────────────────────────────┤
    │       │                                                              │
    │       ├── Iteration 4 (Fixed-Width)                                  │
    │       │       │                                                      │
    │       ├── Iteration 5 (Variable-Width)                               │
    │       │       │                                                      │
    │       ├── Iteration 6 (Timestamp/Gorilla)                            │
    │       │       │                                                      │
    │       ├── Iteration 7 (GeoHash)                                      │
    │       │       │                                                      │
    │       └───────┴──────────┬───────────────────────────────────────────┤
    │                          │                                           │
    │               Iteration 9 (Table Block) ─────────────────────────────┤
    │                          │                                           │
    ├── Iteration 8 (Decompression)                                        │
    │               │                                                      │
    │               └──────────┴───────────────────────────────────────────┤
    │                          │                                           │
    │               Iteration 10 (Message Decoder)                         │
    │                          │                                           │
    │                          └───────────────────────────────────────────┤
    │                                                                      │
    │                                            Iteration 11 (WAL) ───────┤
    │                                                  │                   │
    │                                            Iteration 12 (Response)───┤
    │                                                  │                   │
    │                                                  └───────────────────┤
    │                                                                      │
    │                                            Iteration 14 (E2E) ◄──────┘
    │                                                  │
    │                                            Iteration 15 (Sender)
    │                                                  │
    └─────────────────────────────────────────── Iteration 16 (Docs)
```

### Critical Path

The critical path for a working end-to-end system is:

```
0 → 1 → 2 → 2.5 → 3 → 4 → 9 → 10 → 11 → 12 → 13 → 14
```

This path delivers:
- Protocol negotiation and detection (0-2)
- Transport layer with buffering and pipelining (2.5)
- Basic schema and fixed-width types (3-4)
- Table and message decoding (9-10)
- WAL integration and responses (11-12)
- Full connection handling (13)
- End-to-end validation (14)

Iterations 5-8 (variable-width, timestamps, geohash, compression) can be added incrementally after the critical path is working with fixed-width types only.

---

## Appendix: Test Naming Convention

All test classes follow the pattern `IlpV4*Test.java` and use JUnit 5.

Test methods follow:
- `test<Scenario>` for happy path
- `test<Scenario>_<Condition>` for variations
- `test<ErrorCondition>` with "(error)" comment for expected failures

Example:
```java
@Test
public void testDecodeValidHeader() { ... }

@Test
public void testDecodeHeader_CompressionFlagSet() { ... }

@Test
public void testDecodeInvalidMagic() {
    // Expects IlpV4ParseException
    assertThrows(IlpV4ParseException.class, () -> ...);
}
```