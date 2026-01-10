# ILP v4 Migration Guide

This guide explains how to enable, test, and migrate to the ILP v4 columnar binary protocol.

## Overview

ILP v4 is a columnar binary protocol that provides:
- **Reduced wire size**: 40-60% smaller than text protocol
- **Lower CPU usage**: No text parsing overhead
- **Gorilla compression**: Efficient timestamp encoding
- **Schema caching**: Reduced overhead for repeated writes
- **Batch responses**: Confirmation of successful writes

## Compatibility

ILP v4 runs on the same port as the existing text protocol (default: 9009). Protocol detection is automatic based on the first bytes received:

- Existing text protocol clients continue to work unchanged
- New v4 clients can connect to the same port
- No server restart required to support both protocols

## Enabling ILP v4

### Server Side

ILP v4 is enabled by default when the ILP TCP receiver is running. No additional configuration is required.

To verify ILP v4 support:
1. Connect a v4 client to port 9009
2. The handshake response will indicate supported capabilities

### Client Side

Replace the text protocol sender with the v4 sender:

**Before (Text Protocol):**
```java
try (LineSender sender = LineSender.builder()
        .address("localhost:9009")
        .build()) {
    sender.table("weather")
          .symbol("city", "London")
          .doubleColumn("temperature", 23.5)
          .atNow();
}
```

**After (ILP v4):**
```java
try (IlpV4Sender sender = IlpV4Sender.connect("localhost", 9009)) {
    sender.table("weather")
          .symbol("city", "London")
          .doubleColumn("temperature", 23.5)
          .atNow();
    sender.flush();
}
```

## Key Differences from Text Protocol

### 1. Explicit Flush Required

Unlike the text protocol which sends each line immediately, ILP v4 batches data:

```java
// Multiple rows can be buffered
sender.table("t").longColumn("x", 1).atNow();
sender.table("t").longColumn("x", 2).atNow();
sender.table("t").longColumn("x", 3).atNow();

// Flush sends all buffered data
sender.flush();
```

### 2. Response Handling

ILP v4 provides acknowledgment responses:

```java
try {
    sender.flush();
    // Data successfully written
} catch (IOException e) {
    // Check error message for status code
    // OVERLOADED: retry with backoff
    // SCHEMA_MISMATCH: fix column types
}
```

### 3. Multi-Table Batches

A single flush can write to multiple tables:

```java
sender.table("weather").symbol("city", "London").doubleColumn("temp", 20.0).atNow();
sender.table("sensors").symbol("id", "S1").longColumn("reading", 100).atNow();
sender.table("weather").symbol("city", "Paris").doubleColumn("temp", 22.0).atNow();
sender.flush(); // Writes to both tables atomically
```

### 4. Column Type Declaration

Column types are explicitly declared in the schema. Type widening follows these rules:
- BYTE → SHORT → INT → LONG
- FLOAT → DOUBLE

## Testing Migration

### Step 1: Verify Protocol Detection

```java
// Test that v4 handshake succeeds
IlpV4Sender sender = IlpV4Sender.connect("localhost", 9009);
System.out.println("Negotiated version: " + sender.getNegotiatedVersion());
System.out.println("Gorilla enabled: " + sender.isGorillaEnabled());
sender.close();
```

### Step 2: Test Basic Write

```java
try (IlpV4Sender sender = IlpV4Sender.connect("localhost", 9009)) {
    sender.table("ilp_v4_test")
          .symbol("tag", "test")
          .longColumn("value", 42)
          .atNow();
    sender.flush();
}

// Verify data
// SELECT * FROM ilp_v4_test;
```

### Step 3: Test Error Handling

```java
try (IlpV4Sender sender = IlpV4Sender.connect("localhost", 9009)) {
    // Write with one type
    sender.table("type_test").longColumn("x", 1).atNow();
    sender.flush();

    // Try writing incompatible type (should fail)
    sender.table("type_test").stringColumn("x", "hello").atNow();
    sender.flush(); // Throws IOException with SCHEMA_MISMATCH
}
```

### Step 4: Performance Comparison

```java
// Benchmark v4 vs text protocol
int rows = 100_000;

// V4
long v4Start = System.nanoTime();
try (IlpV4Sender sender = IlpV4Sender.connect("localhost", 9009)) {
    for (int i = 0; i < rows; i++) {
        sender.table("perf_test")
              .symbol("tag", "v4")
              .longColumn("i", i)
              .doubleColumn("value", i * 0.1)
              .atNow();
    }
    sender.flush();
}
long v4Time = System.nanoTime() - v4Start;

System.out.println("V4 time: " + v4Time / 1_000_000 + " ms");
```

## Rollback Plan

If issues are encountered:

1. **Client rollback**: Revert to text protocol sender
2. **No server changes needed**: Text protocol continues to work
3. **Data compatibility**: Data written via v4 is identical to text protocol data

## Troubleshooting

### Connection Refused
- Verify ILP TCP receiver is running on port 9009
- Check firewall rules

### Handshake Timeout
- Increase client timeout
- Verify network latency

### SCHEMA_REQUIRED Response
- Client sent schema reference but server doesn't have it cached
- Solution: Resend with full schema (automatic in IlpV4Sender)

### SCHEMA_MISMATCH Response
- Column type in message doesn't match existing table
- Solution: Use compatible types or drop/recreate table

### OVERLOADED Response
- Server is under heavy load
- Solution: Implement exponential backoff retry

## Feature Comparison

| Feature | Text Protocol | ILP v4 |
|---------|--------------|--------|
| Wire format | Text | Binary columnar |
| Compression | None | Gorilla timestamps |
| Batching | Per-line | Multi-row, multi-table |
| Response | None | Status + errors |
| Schema | Implicit | Explicit |
| Pipelining | Unlimited | Up to 4 messages |
| Port | 9009 | 9009 (same) |
