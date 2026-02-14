# QwpWebSocketSender Design Document

## Overview

This document describes the design for completing the `QwpWebSocketSender` implementation. The key goal is to enable **streaming to hide latency** through a pipeline of in-flight microbatches, achieving higher throughput than request-response patterns while maintaining lower latency than large batch HTTP sends.

## Current State

### What Exists

1. **QwpWebSocketSender** (`core/src/main/java/io/questdb/cutlass/line/websocket/QwpWebSocketSender.java`)
   - Implements `Sender` interface with fluent API
   - Encodes ILP v4 binary messages
   - Currently sends one row per message (no batching)
   - **Missing:** References non-existent `WebSocketChannel` class

2. **Server-side WebSocket Infrastructure** (in `core/src/main/java/io/questdb/cutlass/http/websocket/`)
   - `WebSocketFrameParser` - Zero-allocation RFC 6455 frame parser
   - `WebSocketFrameWriter` - Frame header/payload writing
   - `WebSocketHandshake` - RFC 6455 handshake validation
   - `WebSocketConnectionContext` - Connection state management
   - `WebSocketProcessor` - Event callback interface
   - `QwpWebSocketProcessor` - Adapter for binary message processing

3. **Test Infrastructure** (in `core/src/test/java/io/questdb/test/cutlass/http/websocket/`)
   - `TestWebSocketServer` - Test server for integration tests
   - `AbstractWebSocketIntegrationTest` - Base class with helper methods
   - 15 test files covering frame parsing, handshake, connection, etc.

### Key Problem with Current Design

The current `QwpWebSocketSender` sends each row immediately in `sendRow()`:

```java
private void sendRow() {
    // Encode the single row as ILP v4 message
    // ...
    channel.sendBinary(bufferPtr, bufferPos);  // Blocking send!
    currentTableBuffer.reset();
}
```

This **blocks on every row**, negating the benefit of WebSocket's persistent connection. To hide latency, we need:
1. A **dedicated sending thread** to handle I/O asynchronously
2. **Microbatch buffering** to accumulate rows between sends
3. **In-flight window tracking** to limit outstanding acknowledgments
4. **Response handling** to correlate acknowledgments with sent batches

## Concurrency Model

### Thread Safety Contract

**`Sender` instances are NOT thread-safe.** This is a fundamental contract of the `Sender` interface that users must respect. A single `Sender` instance must only be accessed from one thread at a time. This design choice simplifies the API and avoids synchronization overhead in the common single-threaded use case.

However, `QwpWebSocketSender` is unique among `Sender` implementations because it **internally spawns an I/O thread**. This creates a producer-consumer pattern that must be carefully designed:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         THREAD BOUNDARIES                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                      USER THREAD                                  │   │
│  │  (Single-threaded access - NOT thread-safe)                       │   │
│  │                                                                   │   │
│  │   sender.table("x")                                              │   │
│  │         .symbol("a", "b")                                        │   │
│  │         .at(timestamp);                                          │   │
│  │              │                                                    │   │
│  │              ▼                                                    │   │
│  │   ┌─────────────────────┐                                        │   │
│  │   │  Active Buffer      │◄── User writes here                    │   │
│  │   │  (owned by user     │                                        │   │
│  │   │   thread)           │                                        │   │
│  │   └──────────┬──────────┘                                        │   │
│  │              │                                                    │   │
│  │              │ HAND-OVER POINT (see below)                       │   │
│  │              │                                                    │   │
│  └──────────────┼───────────────────────────────────────────────────┘   │
│                 │                                                        │
│                 ▼                                                        │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                    SEND QUEUE                                    │   │
│   │  (Thread-safe bounded queue - synchronization point)             │   │
│   │  - ArrayBlockingQueue or similar                                 │   │
│   │  - Bounded to limit memory                                       │   │
│   └──────────────────────────────┬──────────────────────────────────┘   │
│                                  │                                       │
│   ┌──────────────────────────────┼──────────────────────────────────┐   │
│   │                              ▼        I/O THREAD                 │   │
│   │  (Dedicated thread - owns the socket)                            │   │
│   │                                                                  │   │
│   │   ┌─────────────────────┐                                       │   │
│   │   │  Pending Buffer     │◄── I/O thread sends this              │   │
│   │   │  (owned by I/O      │                                       │   │
│   │   │   thread)           │                                       │   │
│   │   └──────────┬──────────┘                                       │   │
│   │              │                                                   │   │
│   │              ▼                                                   │   │
│   │   ┌─────────────────────┐                                       │   │
│   │   │  WebSocket Channel  │───► Network I/O                       │   │
│   │   └─────────────────────┘                                       │   │
│   │                                                                  │   │
│   └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Buffer Hand-Over Mechanism

The critical concurrency challenge is **transferring ownership** of a filled buffer from the user thread to the I/O thread. We use a **double-buffering** scheme:

#### Double-Buffer Design

```java
class QwpWebSocketSender {
    // Buffer pool - two buffers that swap roles
    private MicrobatchBuffer buffer0;
    private MicrobatchBuffer buffer1;

    // Which buffer is currently "active" (user is writing to it)
    private MicrobatchBuffer activeBuffer;  // Only user thread touches this

    // Send queue holds sealed buffers waiting to be sent
    private final BlockingQueue<MicrobatchBuffer> sendQueue;
}
```

#### Hand-Over State Machine

```
Buffer States:
┌─────────────┐    seal()     ┌─────────────┐    dequeue()   ┌─────────────┐
│   FILLING   │──────────────►│   QUEUED    │───────────────►│   SENDING   │
│ (user owns) │               │ (in queue)  │                │ (I/O owns)  │
└─────────────┘               └─────────────┘                └──────┬──────┘
       ▲                                                            │
       │                         recycle()                          │
       └────────────────────────────────────────────────────────────┘
                            (after send complete)
```

#### Hand-Over Protocol

```java
// USER THREAD: When buffer is full or timeout triggers
private void sealAndEnqueueBuffer() {
    MicrobatchBuffer toSend = activeBuffer;

    // 1. Seal the buffer (mark as immutable)
    toSend.seal();

    // 2. Swap to the other buffer for continued writing
    activeBuffer = (activeBuffer == buffer0) ? buffer1 : buffer0;

    // 3. If the other buffer is still being sent, BLOCK here
    //    This provides backpressure to the user
    if (activeBuffer.isInUse()) {
        // Wait for I/O thread to recycle it
        activeBuffer.awaitRecycled();
    }

    // 4. Reset the new active buffer for fresh writes
    activeBuffer.reset();

    // 5. Enqueue the sealed buffer (non-blocking, queue is bounded)
    //    If queue is full, this blocks - more backpressure
    sendQueue.put(toSend);
}

// I/O THREAD: Consumes from queue
private void sendLoop() {
    while (running) {
        MicrobatchBuffer batch = sendQueue.take();  // Blocks if empty

        // Send the data
        channel.sendBinary(batch.getDataPtr(), batch.getDataLen());

        // Mark as available for recycling
        batch.markRecycled();
    }
}
```

#### Memory Ownership Rules

| State | Owner | User Thread Can | I/O Thread Can |
|-------|-------|-----------------|----------------|
| FILLING | User | Read/Write | Nothing |
| QUEUED | Queue | Nothing | Nothing |
| SENDING | I/O | Nothing | Read |
| RECYCLED | Pool | Reset & claim | Nothing |

### Critical Synchronization Points

#### 1. Send Queue (Primary Sync Point)

The `sendQueue` is the **only** synchronization point between threads:

```java
// Thread-safe bounded blocking queue
private final ArrayBlockingQueue<MicrobatchBuffer> sendQueue;

// User thread: non-blocking offer with backpressure
if (!sendQueue.offer(batch, timeout, TimeUnit.MILLISECONDS)) {
    // Queue full - apply backpressure or throw
}

// I/O thread: blocking take
MicrobatchBuffer batch = sendQueue.poll(timeout, TimeUnit.MILLISECONDS);
```

#### 2. Buffer State Transitions

Buffer state changes use **volatile** fields and **compare-and-swap**:

```java
class MicrobatchBuffer {
    private static final int STATE_FILLING = 0;
    private static final int STATE_SEALED = 1;
    private static final int STATE_SENDING = 2;
    private static final int STATE_RECYCLED = 3;

    private volatile int state = STATE_FILLING;

    public void seal() {
        // Only user thread calls this
        state = STATE_SEALED;
    }

    public void markSending() {
        // Only I/O thread calls this
        state = STATE_SENDING;
    }

    public void markRecycled() {
        // Only I/O thread calls this
        state = STATE_RECYCLED;
        recycledLatch.countDown();  // Wake up waiting user thread
    }

    public void awaitRecycled() {
        // Only user thread calls this
        recycledLatch.await();
    }
}
```

#### 3. Shutdown Coordination

Graceful shutdown requires careful coordination:

```java
public void close() {
    // 1. Signal I/O thread to stop accepting new work
    shuttingDown = true;

    // 2. Flush any remaining data in active buffer
    if (activeBuffer.hasData()) {
        sealAndEnqueueBuffer();
    }

    // 3. Send poison pill to wake I/O thread
    sendQueue.put(POISON_PILL);

    // 4. Wait for I/O thread to finish
    ioThread.join(shutdownTimeoutMs);

    // 5. Close the channel
    channel.close();

    // 6. Free native memory
    buffer0.close();
    buffer1.close();
}
```

### Backpressure Mechanism

Backpressure flows from server to user thread through multiple levels:

```
Server slow to process
        │
        ▼
In-flight window fills up (Phase 5)
        │
        ▼
I/O thread blocks waiting for acks
        │
        ▼
Send queue fills up
        │
        ▼
User thread blocks on enqueue
        │
        ▼
User's writes slow down
```

This ensures memory remains bounded even under slow server conditions.

### Concurrency Invariants

These invariants must **always** hold:

1. **Single Writer**: Only one thread writes to a buffer at a time
2. **Ownership Transfer**: A buffer has exactly one owner at any time
3. **Bounded Memory**: Total buffers = 2 (double-buffering) + queue capacity
4. **No Data Races**: All shared state accessed through queue or volatile/CAS
5. **Graceful Shutdown**: All pending data sent before close() returns

### Design Alternatives Considered

#### Alternative 1: Ring Buffer Instead of Double-Buffer

**Considered:** Use a lock-free ring buffer (LMAX Disruptor style) for buffer hand-over.

**Rejected because:**
- More complex implementation
- Ring buffer requires power-of-two sizing
- Double-buffer is simpler and sufficient for our needs
- We only need to overlap encoding with sending (not multiple consumers)

#### Alternative 2: Copy-on-Handover

**Considered:** Copy the buffer contents when handing over instead of ownership transfer.

**Rejected because:**
- Memory copy overhead for large batches
- Doubles memory usage (source + destination)
- Ownership transfer is zero-copy

#### Alternative 3: Single Buffer with Lock

**Considered:** Single buffer protected by mutex, I/O thread locks to read.

**Rejected because:**
- Lock contention between user and I/O thread
- User thread blocks during network I/O
- Defeats the purpose of async sending

#### Alternative 4: No Internal Thread (Caller Drives I/O)

**Considered:** User calls `poll()` or similar to drive I/O from their thread.

**Rejected because:**
- Changes the `Sender` API contract
- Complicates user code
- Inconsistent with other `Sender` implementations
- Harder to implement timeout-based auto-flush

#### Alternative 5: Thread Pool Instead of Single I/O Thread

**Considered:** Use a thread pool for sending multiple batches concurrently.

**Rejected because:**
- Out-of-order sends could cause issues
- More complex connection management
- Single persistent connection is WebSocket's strength
- No benefit since we have one TCP connection anyway

### Why Double-Buffering Works

The double-buffer design is optimal for our use case:

1. **Exactly two parties**: One producer (user), one consumer (I/O thread)
2. **Non-overlapping access**: User fills one buffer while I/O sends the other
3. **Natural backpressure**: When both buffers in use, user blocks
4. **Simple state machine**: Only 4 states per buffer
5. **Bounded memory**: Exactly 2 buffers regardless of queue depth

```
Time →
──────────────────────────────────────────────────────────────────
User:    [Fill B0] [Fill B1] [wait] [Fill B0] [Fill B1] ...
I/O:            [Send B0] [Send B1] [Send B0] [Send B1] ...
──────────────────────────────────────────────────────────────────
B0:      FILLING → QUEUED → SENDING → RECYCLED → FILLING → ...
B1:               FILLING → QUEUED → SENDING → RECYCLED → ...
```

### Concurrency Testing Strategy

#### Unit Tests for Concurrency

| Test | Purpose |
|------|---------|
| `MicrobatchBufferStateTransitionTest` | Verify state machine correctness |
| `SendQueueBoundednessTest` | Verify queue doesn't grow unbounded |
| `DoubleBufferSwapTest` | Verify buffer swap is safe |
| `ShutdownCoordinationTest` | Verify clean shutdown |

#### Stress Tests

| Test | Scenario |
|------|----------|
| `HighThroughputStressTest` | Max rows/sec for 60 seconds |
| `SlowServerBackpressureTest` | Server delays responses |
| `BurstTrafficTest` | Alternating idle and burst |
| `LongRunningStabilityTest` | 24-hour continuous operation |

#### Race Condition Tests

| Test | Technique |
|------|-----------|
| `ConcurrentCloseTest` | Close during active sending |
| `FlushDuringWriteTest` | Flush while adding rows |
| `ReconnectDuringWriteTest` | Connection drop mid-batch |
| `ThreadSanitizerTest` | Run with -fsanitize=thread |

#### Deterministic Concurrency Tests

Using controlled scheduling to test specific interleavings:

```java
@Test
public void testBufferSwapRace() {
    // Use latches to control exact ordering
    CountDownLatch userAtSwap = new CountDownLatch(1);
    CountDownLatch ioAtRecycle = new CountDownLatch(1);

    // User thread: pause just before swap
    userThread.pauseAt(BEFORE_SWAP, userAtSwap);

    // I/O thread: pause just before recycle
    ioThread.pauseAt(BEFORE_RECYCLE, ioAtRecycle);

    // Let both reach their pause points
    userAtSwap.await();
    ioAtRecycle.await();

    // Release I/O first, then user - test this ordering
    ioThread.resume();
    userThread.resume();

    // Verify no corruption
    assertDataIntegrity();
}
```

## Proposed Architecture

### Core Concept: Pipelined Streaming

```
┌─────────────────────────────────────────────────────────────────────┐
│                        User Thread                                   │
│                                                                      │
│   table("x").symbol("a","b").at(ts)                                 │
│           │                                                          │
│           ▼                                                          │
│   ┌───────────────────┐                                             │
│   │  Microbatch Buffer │◄── Rows accumulate here                    │
│   │  (per-table)       │                                             │
│   └─────────┬─────────┘                                             │
│             │ When full or timeout                                   │
│             ▼                                                        │
│   ┌───────────────────┐                                             │
│   │  Send Queue       │◄── Thread-safe queue                        │
│   │  (bounded)        │                                             │
│   └─────────┬─────────┘                                             │
└─────────────┼───────────────────────────────────────────────────────┘
              │
              │ Non-blocking enqueue
              │
┌─────────────┼───────────────────────────────────────────────────────┐
│             ▼                    I/O Thread                          │
│   ┌───────────────────┐                                             │
│   │  WebSocket Channel │───► Send frames to server                  │
│   │                    │◄─── Receive ack/error frames               │
│   └─────────┬─────────┘                                             │
│             │                                                        │
│             ▼                                                        │
│   ┌───────────────────┐                                             │
│   │  In-Flight Window │◄── Track unacknowledged batches             │
│   │  (bounded)        │                                             │
│   └───────────────────┘                                             │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

#### 1. Microbatch Size vs Latency Trade-off

- **Small batches (100-500 rows)**: Lower latency, more framing overhead
- **Large batches (1000+ rows)**: Higher throughput, higher latency
- **Configurable** with sensible defaults (e.g., 500 rows or 100ms timeout)

#### 2. In-Flight Window

- Limit the number of unacknowledged microbatches (e.g., 8)
- Provides backpressure when server is slow
- Prevents unbounded memory growth

#### 3. Acknowledgment Protocol

The server should send binary acknowledgment frames:

```
Acknowledgment Frame:
┌──────────────────────────────────────────┐
│ Header: "ACK" (3 bytes)                  │
│ Batch ID (8 bytes, big-endian)           │
│ Status (1 byte): 0=OK, 1+=error code     │
│ Optional: Error message (variable)       │
└──────────────────────────────────────────┘
```

#### 4. Error Handling Strategy

- **Retryable errors** (5xx, network): Retry with exponential backoff
- **Non-retryable errors** (4xx, protocol): Fail immediately
- **Batch-level errors**: Report to user, continue with next batch

## Implementation Plan

### Phase 1: WebSocketChannel (Foundation)

Create the missing `WebSocketChannel` class with synchronous I/O first.

**File:** `core/src/main/java/io/questdb/cutlass/line/websocket/WebSocketChannel.java`

```java
public class WebSocketChannel implements QuietCloseable {
    // Connection state
    private final String host;
    private final int port;
    private final String path;
    private final boolean tlsEnabled;
    private Socket socket;
    private InputStream in;
    private OutputStream out;

    // Frame parsing/writing
    private final WebSocketFrameParser parser;
    private long sendBuffer;
    private int sendBufferSize;

    // Configuration
    private int connectTimeoutMs = 10_000;
    private int readTimeoutMs = 30_000;

    public void connect();
    public void sendBinary(long data, int length);
    public void sendPing(byte[] payload);
    public void close();

    // For later: response handling
    public interface ResponseHandler {
        void onBinaryMessage(long payload, int length);
        void onClose(int code, String reason);
        void onError(Exception e);
    }
}
```

**Tests for Phase 1:**
- `WebSocketChannelTest` - Unit tests for frame encoding/decoding
- `WebSocketChannelHandshakeTest` - Handshake validation
- `WebSocketChannelIntegrationTest` - Connect to `TestWebSocketServer`

**Deliverables:**
1. `WebSocketChannel` class with connect, sendBinary, close
2. Client-side frame masking (RFC 6455 requirement)
3. Basic error handling
4. Unit and integration tests

### Phase 2: Make QwpWebSocketSender Work (Synchronous)

Wire `QwpWebSocketSender` to use `WebSocketChannel` for basic functionality.

**Changes to QwpWebSocketSender:**
- Create `WebSocketChannel` in `ensureConnected()`
- Implement `sendBinary()` calls
- Handle connection errors

**Tests for Phase 2:**
- `QwpWebSocketSenderBasicTest` - Basic send operations
- `QwpWebSocketSenderIntegrationTest` - Full round-trip with server

**Deliverables:**
1. Working synchronous sender (per-row send)
2. Basic test coverage
3. Verify data arrives at server correctly

### Phase 3: Microbatch Buffering

Add batching to reduce per-row overhead.

**New Class:** `MicrobatchBuffer`

```java
class MicrobatchBuffer {
    private final int maxRows;
    private final int maxBytes;
    private final long maxAgeNanos;

    private long bufferPtr;
    private int bufferPos;
    private int rowCount;
    private long firstRowTime;

    public boolean addRow(QwpTableBuffer table);
    public boolean shouldFlush();
    public void reset();
}
```

**Changes to QwpWebSocketSender:**
- Add `MicrobatchBuffer` instead of immediate send
- Add flush triggers: row count, byte size, time
- Keep `flush()` for explicit user control

**Tests for Phase 3:**
- `MicrobatchBufferTest` - Unit tests for buffering logic
- `QwpWebSocketSenderBatchingTest` - Verify batching behavior
- `QwpWebSocketSenderAutoFlushTest` - Row/time-based auto-flush

**Deliverables:**
1. Configurable microbatch buffering
2. Auto-flush on row count, byte size, or time
3. Tests verifying batch boundaries

### Phase 4: Asynchronous I/O Thread

Introduce dedicated sending thread for non-blocking operation. **This is the critical phase for concurrency correctness.**

**New Class:** `WebSocketSendQueue`

```java
class WebSocketSendQueue implements QuietCloseable {
    private final ArrayBlockingQueue<MicrobatchBuffer> sendQueue;
    private final WebSocketChannel channel;
    private final Thread sendThread;
    private volatile boolean running;

    public boolean enqueue(MicrobatchBuffer batch);  // Non-blocking with timeout
    public void flush();                              // Wait for queue drain
    public void close();                              // Graceful shutdown
}
```

**Double-Buffer Integration:**

```java
class QwpWebSocketSender {
    private MicrobatchBuffer buffer0;
    private MicrobatchBuffer buffer1;
    private MicrobatchBuffer activeBuffer;
    private final WebSocketSendQueue sendQueue;

    // Called when activeBuffer is full
    private void sealAndEnqueueBuffer() {
        MicrobatchBuffer toSend = activeBuffer;
        toSend.seal();

        // Swap to other buffer
        activeBuffer = (activeBuffer == buffer0) ? buffer1 : buffer0;

        // Block if other buffer still in use (backpressure)
        activeBuffer.awaitAvailable();
        activeBuffer.reset();

        // Enqueue sealed buffer
        sendQueue.enqueue(toSend);
    }
}
```

**Tests for Phase 4:**

*Unit Tests:*
- `WebSocketSendQueueTest` - Queue operations
- `MicrobatchBufferStateTest` - State transitions
- `DoubleBufferSwapTest` - Buffer ownership transfer

*Concurrency Tests:*
- `ConcurrentEnqueueTest` - Verify thread-safe enqueue
- `BackpressureTest` - Full queue blocks user
- `ShutdownDuringActiveTest` - Clean shutdown mid-send
- `BufferRecycleRaceTest` - No use-after-recycle

*Integration Tests:*
- `QwpWebSocketSenderAsyncTest` - Non-blocking sends work
- `QwpWebSocketSenderThroughputTest` - Throughput improvement

**Deliverables:**
1. Dedicated I/O thread with proper lifecycle
2. Double-buffering with safe hand-over
3. Backpressure when queue full
4. Graceful shutdown (drain queue, wait for I/O thread)
5. Comprehensive concurrency tests

### Phase 5: Response Handling and In-Flight Window

Add acknowledgment tracking for flow control.

**New Components:**

```java
class InFlightWindow {
    private final int maxInFlight;
    private final Deque<SentBatch> pendingAcks;
    private final Semaphore permits;

    public void recordSent(long batchId, int rowCount);
    public void recordAck(long batchId);
    public boolean hasCapacity();
    public void awaitCapacity(long timeoutMs);
}

class SentBatch {
    final long batchId;
    final int rowCount;
    final long sentTimeNanos;
}
```

**Response Processing:**
- Parse server acknowledgment frames
- Update `InFlightWindow` on ack received
- Handle error responses

**Tests for Phase 5:**
- `InFlightWindowTest` - Unit tests for window tracking
- `QwpWebSocketSenderBackpressureTest` - Verify backpressure works
- `QwpWebSocketSenderErrorHandlingTest` - Error recovery

**Deliverables:**
1. In-flight window with configurable size
2. Backpressure when window full
3. Acknowledgment parsing
4. Error handling with retry logic

### Phase 6: Authentication and TLS

Add security features.

**Authentication Methods:**
1. Bearer token in WebSocket upgrade request
2. Basic auth in upgrade request headers

**Changes:**
- Add auth headers to HTTP upgrade request
- TLS socket creation using `SSLContext`

**Tests for Phase 6:**
- `QwpWebSocketSenderAuthTest` - Token and basic auth
- `QwpWebSocketSenderTlsTest` - TLS connection

**Deliverables:**
1. Bearer token authentication
2. Basic authentication
3. TLS/WSS support
4. Tests for all auth combinations

### Phase 7: Resilience and Production Hardening

Final polish for production use.

**Features:**
- Automatic reconnection on connection loss
- Exponential backoff with jitter
- Metrics/logging for debugging
- Connection keepalive (ping/pong)

**Tests for Phase 7:**
- `QwpWebSocketSenderReconnectTest` - Auto-reconnect scenarios
- `QwpWebSocketSenderStressTest` - Long-running stability
- `QwpWebSocketSenderConcurrencyTest` - Multi-threaded access

**Deliverables:**
1. Robust reconnection logic
2. Comprehensive logging
3. Stress tests for stability

## API Design

### Builder Pattern

```java
QwpWebSocketSender sender = Sender.builder(Sender.Transport.WEBSOCKET)
    .address("localhost:9000")
    .autoFlushRows(500)
    .autoFlushInterval(Duration.ofMillis(100))
    .maxInFlight(8)
    .token("my-auth-token")
    .build();
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `autoFlushRows` | int | 500 | Rows per microbatch |
| `autoFlushBytes` | int | 1MB | Max bytes per microbatch |
| `autoFlushInterval` | Duration | 100ms | Max time before flush |
| `maxInFlight` | int | 8 | Max unacknowledged batches |
| `connectTimeout` | Duration | 10s | Connection timeout |
| `readTimeout` | Duration | 30s | Response timeout |
| `retryTimeout` | Duration | 10s | Max retry duration |

### Usage Example

```java
try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
        .address("localhost:9000")
        .autoFlushRows(100)
        .build()) {

    for (int i = 0; i < 100_000; i++) {
        sender.table("metrics")
              .symbol("host", "server-" + (i % 10))
              .doubleColumn("cpu", Math.random() * 100)
              .atNow();
        // Rows are batched and sent asynchronously!
    }
    // flush() waits for all pending batches to be acknowledged
    sender.flush();
}
```

## Protocol Specification

### WebSocket Upgrade Request

```http
GET /write/v4 HTTP/1.1
Host: localhost:9000
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
Sec-WebSocket-Version: 13
Authorization: Bearer <token>
```

### ILP v4 Binary Message Format

```
┌─────────────────────────────────────────────────────────┐
│ WebSocket Frame Header (RFC 6455)                       │
│ - FIN=1, Opcode=0x02 (binary)                          │
│ - MASK=1 (client-to-server)                            │
│ - Payload length                                        │
│ - Mask key (4 bytes)                                    │
├─────────────────────────────────────────────────────────┤
│ ILP v4 Message Header (12 bytes)                        │
│ - Magic: "ILP4"                                         │
│ - Version: 1                                            │
│ - Flags: 0x04 (Gorilla encoding)                        │
│ - Table count: N                                        │
│ - Payload length                                        │
├─────────────────────────────────────────────────────────┤
│ Table 1 Data                                            │
│ Table 2 Data                                            │
│ ...                                                     │
└─────────────────────────────────────────────────────────┘
```

### Server Acknowledgment Frame

```
┌─────────────────────────────────────────────────────────┐
│ WebSocket Frame Header (binary)                         │
├─────────────────────────────────────────────────────────┤
│ "ACK" (3 bytes)                                         │
│ Batch ID (8 bytes, big-endian)                          │
│ Status (1 byte): 0=OK                                   │
└─────────────────────────────────────────────────────────┘
```

### Server Error Frame

```
┌─────────────────────────────────────────────────────────┐
│ WebSocket Frame Header (binary)                         │
├─────────────────────────────────────────────────────────┤
│ "ERR" (3 bytes)                                         │
│ Batch ID (8 bytes, big-endian)                          │
│ Error code (4 bytes, big-endian)                        │
│ Error message (UTF-8, variable length)                  │
└─────────────────────────────────────────────────────────┘
```

## Testing Strategy

### Unit Tests (Per Class)

| Class | Test Class | Focus |
|-------|------------|-------|
| `WebSocketChannel` | `WebSocketChannelTest` | Frame encoding, masking |
| `MicrobatchBuffer` | `MicrobatchBufferTest` | Buffer management |
| `WebSocketSendQueue` | `WebSocketSendQueueTest` | Queue operations |
| `InFlightWindow` | `InFlightWindowTest` | Window tracking |

### Integration Tests

| Test Class | Scenario |
|------------|----------|
| `QwpWebSocketSenderBasicIntegrationTest` | Simple send/receive |
| `QwpWebSocketSenderBatchingIntegrationTest` | Batch accumulation |
| `QwpWebSocketSenderBackpressureIntegrationTest` | Window limiting |
| `QwpWebSocketSenderReconnectIntegrationTest` | Connection recovery |
| `QwpWebSocketSenderTlsIntegrationTest` | TLS connections |
| `QwpWebSocketSenderAuthIntegrationTest` | Authentication |

### Performance Tests

| Test | Metric |
|------|--------|
| Throughput | Rows/second at various batch sizes |
| Latency | p50/p99 time from send to ack |
| Memory | Buffer allocation under load |

## Success Criteria

1. **Correctness**: All data arrives at server intact
2. **Performance**: >100K rows/sec sustained throughput
3. **Latency**: <10ms p99 latency for row acknowledgment
4. **Resilience**: Automatic recovery from transient failures
5. **Test Coverage**: >90% line coverage with meaningful tests

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Server doesn't send acks | No flow control | Implement timeout-based window advance |
| Memory pressure under load | OOM | Bounded queues, backpressure |
| Thread safety bugs | Data corruption | Careful locking, concurrent tests |
| Protocol incompatibility | Connection failures | Extensive handshake tests |

## Implementation Schedule

Work proceeds phase-by-phase. Each phase must have:
1. All tests passing before moving forward
2. Code review completed
3. No known bugs or shortcuts

**Phase completion order:**
1. Phase 1: WebSocketChannel - Foundation
2. Phase 2: Basic QwpWebSocketSender - Verification
3. Phase 3: Microbatching - Batching logic
4. Phase 4: Async I/O - Non-blocking sends
5. Phase 5: In-Flight Window - Flow control
6. Phase 6: Auth/TLS - Security
7. Phase 7: Hardening - Production readiness

## File Locations Summary

**Main Source (`core/src/main/java/io/questdb/cutlass/line/websocket/`):**
- `QwpWebSocketSender.java` (exists, needs updates)
- `WebSocketChannel.java` (new)
- `MicrobatchBuffer.java` (new)
- `WebSocketSendQueue.java` (new)
- `InFlightWindow.java` (new)

**Test Source (`core/src/test/java/io/questdb/test/cutlass/line/websocket/`):**
- (new test directory)
- All test classes as listed above

**Shared Infrastructure (`core/src/main/java/io/questdb/cutlass/http/websocket/`):**
- Reuse existing frame parser/writer classes
