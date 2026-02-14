# ILP v4 WebSocket Sender Internals

This document describes the internal architecture of `QwpWebSocketSender`, focusing on data flow, buffering, flushing behavior, and flow control.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           User Thread                                        │
│  table().symbol().longColumn().at()  →  TableBuffer  →  MicrobatchBuffer    │
└─────────────────────────────────────────────┬───────────────────────────────┘
                                              │ seal & enqueue
                                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SendQueue (bounded)                                  │
│                      ArrayBlockingQueue<MicrobatchBuffer>                   │
│                           capacity = 16 batches                              │
└─────────────────────────────────────────────┬───────────────────────────────┘
                                              │ dequeue
                                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          I/O Thread                                          │
│  1. Add to InFlightWindow (may block if full)                               │
│  2. Send over WebSocket                                                      │
│  3. Mark buffer as recycled                                                  │
└─────────────────────────────────────────────┬───────────────────────────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     InFlightWindow                                           │
│              Tracks batches awaiting server ACK                              │
│                    maxSize = 8 batches                                       │
└─────────────────────────────────────────────┬───────────────────────────────┘
                                              │ ACK received
                                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Response Reader Thread                                  │
│  Reads ACK frames from server, calls inFlightWindow.acknowledge()           │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Double Buffering

The sender uses **two MicrobatchBuffers** (`buffer0` and `buffer1`) that alternate:

```
Time →

buffer0: [FILLING]──seal──[SEALED]──enqueue──[SENDING]──────[RECYCLED]──reset──[FILLING]...
buffer1: [FILLING]────────────────────[FILLING]──seal──[SEALED]──enqueue──[SENDING]...
                                         ↑
                          user swaps to buffer1 while buffer0 is being sent
```

**Buffer States:**
- `FILLING` - User thread is writing data
- `SEALED` - Buffer is complete, waiting to be sent
- `SENDING` - I/O thread is transmitting
- `RECYCLED` - Transmission complete, ready for reuse

## Auto-Flush Triggers

The sender automatically flushes (seals current buffer and enqueues for sending) when ANY of these conditions is met:

| Trigger | Default | Configurable | Description |
|---------|---------|--------------|-------------|
| `autoFlushRows` | 500 rows | ✅ Yes | Flush after N rows accumulated |
| `autoFlushBytes` | 1 MB | ✅ Yes | Flush when buffer reaches N bytes |
| `autoFlushInterval` | 100 ms | ✅ Yes | Flush if oldest row is older than N |

### ILP v4 Message Structure

In ILP v4, each row is encoded as a **separate binary message** with its own header:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Single WebSocket Frame (one batch)                       │
├─────────────────────────────────────────────────────────────────────────────┤
│ [Msg1 Header][Msg1 Data] [Msg2 Header][Msg2 Data] ... [MsgN Header][MsgN Data]
│      ↑            ↑           ↑            ↑
│   4 bytes    variable     4 bytes    variable
│  (length)     (row 1)    (length)    (row 2)
└─────────────────────────────────────────────────────────────────────────────┘
```

Each message contains:
- **4-byte length prefix** - total message size
- **Table name** - length-prefixed string
- **Columns** - type tag + name + value for each column
- **Timestamp** - 8-byte designated timestamp

**Example:** A row like `metrics,host=server1 value=42i 1704067200000000` becomes approximately 50-100 bytes depending on column names and types.

With `autoFlushRows=500` (the default), a single batch contains **500 concatenated messages** in one WebSocket binary frame. The server parses them sequentially and commits them atomically as a single transaction.

**Why this matters:**
- More rows per batch = better throughput (fewer round-trips)
- Fewer rows per batch = lower latency (faster commits)
- Each batch is one WebSocket frame = one TCP send = one server ACK

### Configuration Example

```java
// High throughput: large batches, less frequent flushes
QwpWebSocketSender.connectAsync(host, port, false,
    10_000,  // autoFlushRows: 10,000 rows per batch
    0,       // autoFlushBytes: no byte limit
    0);      // autoFlushInterval: no time limit

// Low latency: small batches, frequent flushes
QwpWebSocketSender.connectAsync(host, port, false,
    100,              // autoFlushRows: 100 rows per batch
    0,                // autoFlushBytes: no byte limit
    100_000_000L);    // autoFlushInterval: 100ms
```

## Flow Control: InFlightWindow

The `InFlightWindow` limits how many unacknowledged batches can be in flight:

```
┌────────────────────────────────────────────────────────────────┐
│                    InFlightWindow (max=8)                       │
│                                                                 │
│  [batch0] [batch1] [batch2] [batch3] [batch4] [batch5] [batch6] [batch7]
│     ↑                                                      ↑
│   oldest                                                newest
│  (awaiting ACK)                                      (just sent)
│                                                                 │
│  When ACK for batch0 arrives → window slides, batch8 can enter │
└────────────────────────────────────────────────────────────────┘
```

### What Happens When Window is Full?

When the I/O thread tries to add a batch to a full window:

1. **I/O thread blocks** in `addInFlight()` waiting for space
2. **SendQueue backs up** because I/O thread can't dequeue
3. **User thread blocks** in `enqueue()` when SendQueue is full (capacity=16)
4. **Backpressure propagates** to the user application

This prevents unbounded memory growth and ensures the client doesn't overwhelm the server.

### Capacity Calculation

| Setting | Default | Configurable | Description |
|---------|---------|--------------|-------------|
| **InFlightWindow** | 8 batches | ✅ Yes | Max unacknowledged batches |
| **SendQueue** | 16 batches | ✅ Yes | Max batches waiting to send |
| **autoFlushRows** | 500 rows | ✅ Yes | Rows per batch |

**Maximum rows in flight (with defaults):**
```
(8 in-flight + 16 queued + 2 buffers) × 500 rows = 13,000 rows
```

**Maximum memory usage (approximate):**
```
26 batches × 1MB max per batch ≈ 26 MB (worst case)
```

### Configuring Flow Control

All flow control parameters can be configured via the `connectAsync()` factory method:

```java
// Custom flow control: larger in-flight window and send queue
QwpWebSocketSender.connectAsync(host, port, false,
    10_000,         // autoFlushRows: 10,000 rows per batch
    1024 * 1024,    // autoFlushBytes: 1MB per batch
    100_000_000L,   // autoFlushInterval: 100ms
    16,             // inFlightWindowSize: 16 batches (default: 8)
    32);            // sendQueueCapacity: 32 batches (default: 16)
```

Or via command line with the allocation test client:
```bash
./run-alloc-test.sh client --protocol=qwp-websocket \
    --in-flight-window=16 --send-queue=32
```

## Threading Model

| Thread | Responsibility |
|--------|----------------|
| **User Thread** | Writes rows, triggers auto-flush, calls explicit flush() |
| **I/O Thread** | Dequeues batches, adds to in-flight window, sends over WebSocket |
| **Response Reader** | Reads server ACKs, removes from in-flight window |

### Thread Safety

- `MicrobatchBuffer.state` is `volatile` for visibility
- `MicrobatchBuffer.recycleLatch` is `volatile` for hand-off synchronization
- `InFlightWindow` uses `ReentrantLock` with conditions for blocking
- `SendQueue` uses `ArrayBlockingQueue` (thread-safe)

## Flush Behavior

### Implicit Flush (Auto-Flush)

Triggered automatically during `at()` / `atNow()` calls when thresholds are exceeded:

```java
sender.table("test")
      .longColumn("x", 1)
      .at(timestamp);  // ← May trigger auto-flush if thresholds exceeded
```

### Explicit Flush

```java
sender.flush();  // Blocks until ALL data is acknowledged by server
```

**What `flush()` does:**
1. Flushes any pending rows from TableBuffers to MicrobatchBuffer
2. Seals and enqueues the current MicrobatchBuffer (if it has data)
3. Waits for SendQueue to drain (all batches sent)
4. Waits for InFlightWindow to become empty (all ACKs received)

**Flush is required for durability** - data is not guaranteed persisted until flush completes.

## Sequence Numbers and ACKs

Each batch gets a **sequence number** assigned when dequeued by the I/O thread:

```
Client sends:     [batch seq=0] [batch seq=1] [batch seq=2]
Server responds:  [ACK seq=0]   [ACK seq=1]   [ACK seq=2]
```

The server processes batches **in order** and sends ACKs as each batch commits. The client's InFlightWindow removes batches as ACKs arrive.

### Error Handling

If the server returns an error instead of ACK:
1. `InFlightWindow.fail()` is called with the error
2. All waiting threads are notified
3. Subsequent operations throw `LineSenderException`

## Performance Characteristics

### Throughput Optimization

For maximum throughput:
- Use large `autoFlushRows` (e.g., 10,000+)
- Use async mode (default for `connectAsync()`)
- Pipeline is deep: 8 in-flight + 16 queued = 24 batches in parallel

### Latency Optimization

For minimum latency:
- Use small `autoFlushRows` (e.g., 1-100)
- Use shorter `autoFlushInterval`
- Trade-off: more overhead per row, lower throughput

### Memory Usage

Memory is bounded by:
```
(maxInFlight + queueCapacity + 2) × bufferSize
= (8 + 16 + 2) × 64KB = 1.6MB (default)
```

## Example: High-Volume Ingestion

```java
try (Sender sender = QwpWebSocketSender.connectAsync("localhost", 9000, false,
        10_000,  // 10K rows per batch
        0, 0)) {

    for (int i = 0; i < 100_000_000; i++) {
        sender.table("metrics")
              .symbol("host", "server-" + (i % 100))
              .longColumn("value", i)
              .at(baseTimestamp + i, ChronoUnit.MICROS);
        // Auto-flush happens every 10K rows
    }

    sender.flush();  // Wait for all ACKs before closing
}
```

**Data flow for 100M rows:**
- 10,000 batches created (100M / 10K rows each)
- At any time: up to 8 batches awaiting ACK, 16 in queue, 2 in buffers
- Backpressure automatically throttles if server is slow
