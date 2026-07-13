# Multi-Tier Durable-Ack Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a QWP ingest client choose its durable-ack tier — `LOCAL` (power-loss-safe, adaptive fdatasync) or `REPLICATED` (failover-safe, object-store upload) — exposing the adaptive local tier under Enterprise without ever silently downgrading the existing replicated guarantee.

**Architecture:** OSS core owns the tier vocabulary, the handshake negotiation, and the per-connection tier-selected durable-ack consumer. Enterprise's `DurableUploadRegistry` gains the local tier (delegating to `SeqTxnTracker`) so both tiers are live on a primary. The handshake echoes the *granted* tier; an unsatisfiable explicit request gets no confirmation (existing fail-at-client mechanism).

**Tech Stack:** Java 25, QuestDB core + questdb-ent, Maven, JUnit4.

## Global Constraints

- OSS work lands on branch `nw_adaptive_commit` in `~/claude/wt/oss/adaptive-commit`; Enterprise work on branch `nw_adaptive_commit_ent` in `~/claude/wt/ent/adaptive` (its `questdb` submodule tracks the OSS branch — bump + rebuild after OSS tasks land).
- Build/test JDK: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64`.
- OSS test command (run from `~/claude/wt/oss/adaptive-commit`): `mvn -q -pl core -am -Dtest="<Class>#<method>" -DfailIfNoTests=false test`.
- Ent test command (run from `~/claude/wt/ent/adaptive`): `mvn -q -pl questdb-ent -am -Dtest="<Class>#<method>" -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false test`.
- Commit with `--no-verify` (repo hooks are out-of-band here). End messages with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- Version stays `9.4.4-SNAPSHOT` on both sides (no pom bump).
- Java test style: fluent `assertQuery()`/`QueryAssertion` for SQL; plain JUnit for unit-level registry/protocol tests.
- No on-disk/`_meta` format change. Tier is per-connection, negotiated at handshake.

## Tier vocabulary (used throughout)

- `DurabilityTier.NONE = -1` — durable-ack disabled.
- `DurabilityTier.LOCAL = 0` — power-loss-safe; source `getLocalDurableSeqTxn`.
- `DurabilityTier.REPLICATED = 1` — failover-safe; source `getDurablyUploadedSeqTxn`.
- `DurabilityTier.DEFAULT = 2` — request intent from legacy `true`: resolve to strongest available tier.
- Ordering: `localDurableSeqTxn ≥ uploadedSeqTxn`; `LOCAL` weaker than `REPLICATED`.

## File Structure

**OSS core (`questdb/core`):**
- Create `.../cairo/wal/DurabilityTier.java` — tier constants + header parse + response token.
- Modify `.../cairo/wal/DurableAckRegistry.java` — `isTierAvailable` / `strongestAvailableTier` defaults.
- Modify `.../cairo/wal/LocalDurableAckRegistry.java` — advertise `{LOCAL}`; expose shared static local lookup.
- Modify `.../cutlass/qwp/server/QwpIngressProcessorState.java` — per-connection tier field; tier-selected `collectDurableProgress`.
- Modify `.../cutlass/qwp/server/QwpIngressHttpProcessor.java` — response takes a confirm-token (bytes) instead of a boolean.
- Modify `.../cutlass/qwp/server/QwpIngressUpgradeProcessor.java` — parse + resolve + grant tier; pass token.
- Modify `.../cutlass/qwp/server/egress/QwpEgressUpgradeProcessor.java` — adapt to the new response signature.

**Enterprise (`questdb-ent`):**
- Modify `.../cairo/wal/transfer/DurableUploadRegistry.java` — engine handle; local tier; `{LOCAL, REPLICATED}`; frozen-snapshot tier.
- Modify `.../lifecycle/PrimaryRoleState.java` — pass `engine` to the registry constructor.

**Tests:**
- `LocalDurableAckRegistryTest` (OSS, exists) — availability.
- `QwpIngressProcessorState` consumer test (OSS) — tier selection / anti-downgrade.
- `QwpIngressUpgradeProcessorOnHeadersReadyTest` (OSS, exists) — negotiation.
- `DurableUploadRegistryTest` (Ent, exists) — local tier + availability + frozen snapshot.

---

### Task 1: `DurabilityTier` vocabulary (OSS)

**Files:**
- Create: `core/src/main/java/io/questdb/cairo/wal/DurabilityTier.java`
- Test: `core/src/test/java/io/questdb/test/cairo/wal/DurabilityTierTest.java`

**Interfaces:**
- Produces: `DurabilityTier.NONE/LOCAL/REPLICATED/DEFAULT` (int); `int fromHeaderValue(Utf8Sequence)`; `Utf8String responseToken(int tier)` (returns `null` for NONE/DEFAULT).

- [ ] **Step 1: Write the failing test**

```java
package io.questdb.test.cairo.wal;

import io.questdb.cairo.wal.DurabilityTier;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

public class DurabilityTierTest {
    @Test
    public void testFromHeaderValue() {
        Assert.assertEquals(DurabilityTier.DEFAULT, DurabilityTier.fromHeaderValue(new Utf8String("true")));
        Assert.assertEquals(DurabilityTier.DEFAULT, DurabilityTier.fromHeaderValue(new Utf8String("TRUE")));
        Assert.assertEquals(DurabilityTier.LOCAL, DurabilityTier.fromHeaderValue(new Utf8String("local")));
        Assert.assertEquals(DurabilityTier.REPLICATED, DurabilityTier.fromHeaderValue(new Utf8String("replicated")));
        Assert.assertEquals(DurabilityTier.NONE, DurabilityTier.fromHeaderValue(new Utf8String("bogus")));
        Assert.assertEquals(DurabilityTier.NONE, DurabilityTier.fromHeaderValue(null));
    }

    @Test
    public void testResponseToken() {
        Assert.assertEquals("local", DurabilityTier.responseToken(DurabilityTier.LOCAL).toString());
        Assert.assertEquals("replicated", DurabilityTier.responseToken(DurabilityTier.REPLICATED).toString());
        Assert.assertNull(DurabilityTier.responseToken(DurabilityTier.NONE));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl core -am -Dtest="DurabilityTierTest" -DfailIfNoTests=false test`
Expected: FAIL — `DurabilityTier` does not exist / does not compile.

- [ ] **Step 3: Write minimal implementation**

```java
package io.questdb.cairo.wal;

import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.Utf8s;

/**
 * Durability tiers for the QWP durable-ack. Ordered by strength:
 * {@link #LOCAL} (power-loss-safe, adaptive fdatasync) is weaker than
 * {@link #REPLICATED} (failover-safe, object-store upload). {@link #NONE}
 * means durable-ack is off; {@link #DEFAULT} is the legacy {@code "true"}
 * request intent, resolved to the server's strongest available tier.
 */
public final class DurabilityTier {
    public static final int NONE = -1;
    public static final int LOCAL = 0;
    public static final int REPLICATED = 1;
    public static final int DEFAULT = 2;

    private static final Utf8String TOKEN_LOCAL = new Utf8String("local");
    private static final Utf8String TOKEN_REPLICATED = new Utf8String("replicated");

    private DurabilityTier() {
    }

    /** Parse the X-QWP-Request-Durable-Ack header value into a request intent. */
    public static int fromHeaderValue(Utf8Sequence v) {
        if (v == null) {
            return NONE;
        }
        if (Utf8s.equalsIgnoreCaseAscii(v, "true")) {
            return DEFAULT;
        }
        if (Utf8s.equalsIgnoreCaseAscii(v, "local")) {
            return LOCAL;
        }
        if (Utf8s.equalsIgnoreCaseAscii(v, "replicated")) {
            return REPLICATED;
        }
        return NONE;
    }

    /** The confirmation token echoed for an explicitly-granted tier, or null. */
    public static Utf8String responseToken(int tier) {
        switch (tier) {
            case LOCAL:
                return TOKEN_LOCAL;
            case REPLICATED:
                return TOKEN_REPLICATED;
            default:
                return null;
        }
    }
}
```

Note: confirm `Utf8s.equalsIgnoreCaseAscii(Utf8Sequence, String)` exists (it is used at `QwpIngressUpgradeProcessor:363` against a `Utf8String`; if only the `(Utf8Sequence, Utf8Sequence)` overload exists, compare against `HEADER_VALUE_*` constants instead).

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl core -am -Dtest="DurabilityTierTest" -DfailIfNoTests=false test`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
cd ~/claude/wt/oss/adaptive-commit
git add core/src/main/java/io/questdb/cairo/wal/DurabilityTier.java core/src/test/java/io/questdb/test/cairo/wal/DurabilityTierTest.java
git commit --no-verify -m "feat(adaptive): DurabilityTier vocabulary for multi-tier durable-ack

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Registry tier availability + shared local lookup (OSS)

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/DurableAckRegistry.java`
- Modify: `core/src/main/java/io/questdb/cairo/wal/LocalDurableAckRegistry.java`
- Test: `core/src/test/java/io/questdb/test/cairo/wal/LocalDurableAckRegistryTest.java` (exists)

**Interfaces:**
- Consumes: `DurabilityTier.*` (Task 1).
- Produces: `DurableAckRegistry.isTierAvailable(int tier)` (default false); `DurableAckRegistry.strongestAvailableTier()` (default derives from `isTierAvailable`); `static long LocalDurableAckRegistry.resolveLocalDurableSeqTxn(CairoEngine, CharSequence)`.

- [ ] **Step 1: Write the failing test** (append to `LocalDurableAckRegistryTest`)

```java
@Test
public void testTierAvailability() {
    assertMemoryLeak(() -> {
        LocalDurableAckRegistry registry = new LocalDurableAckRegistry(engine);
        Assert.assertTrue(registry.isTierAvailable(DurabilityTier.LOCAL));
        Assert.assertFalse(registry.isTierAvailable(DurabilityTier.REPLICATED));
        Assert.assertEquals(DurabilityTier.LOCAL, registry.strongestAvailableTier());
    });
}
```

(Add imports `io.questdb.cairo.wal.DurabilityTier`, `io.questdb.cairo.wal.LocalDurableAckRegistry` if absent.)

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl core -am -Dtest="LocalDurableAckRegistryTest#testTierAvailability" -DfailIfNoTests=false test`
Expected: FAIL — `isTierAvailable` not defined.

- [ ] **Step 3: Write minimal implementation**

In `DurableAckRegistry.java`, add:

```java
/**
 * Whether this server can offer the given {@link DurabilityTier}. Availability is server-level;
 * an offered tier may still report -1 for a table that cannot satisfy it (e.g. a NOSYNC table
 * under the LOCAL tier). Default: no tier available; concrete registries override.
 */
default boolean isTierAvailable(int tier) {
    return false;
}

/** The strongest tier this server can offer, or {@link DurabilityTier#NONE}. */
default int strongestAvailableTier() {
    if (isTierAvailable(DurabilityTier.REPLICATED)) {
        return DurabilityTier.REPLICATED;
    }
    if (isTierAvailable(DurabilityTier.LOCAL)) {
        return DurabilityTier.LOCAL;
    }
    return DurabilityTier.NONE;
}
```

In `LocalDurableAckRegistry.java`, extract the existing lookup into a reusable static and add availability:

```java
/** Shared local-fsync tier lookup, reused by Enterprise's DurableUploadRegistry. */
public static long resolveLocalDurableSeqTxn(CairoEngine engine, CharSequence tableDirName) {
    TableToken token = engine.getTableTokenByDirName(tableDirName);
    if (token == null) {
        return -1L;
    }
    try {
        SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
        return tracker.getLocalDurableSeqTxn();
    } catch (Throwable ignored) {
        return -1L;
    }
}

@Override
public long getLocalDurableSeqTxn(CharSequence tableDirName) {
    return resolveLocalDurableSeqTxn(engine, tableDirName);
}

@Override
public boolean isTierAvailable(int tier) {
    return tier == DurabilityTier.LOCAL;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl core -am -Dtest="LocalDurableAckRegistryTest" -DfailIfNoTests=false test`
Expected: PASS (existing 15 + new).

- [ ] **Step 5: Commit**

```bash
cd ~/claude/wt/oss/adaptive-commit
git add core/src/main/java/io/questdb/cairo/wal/DurableAckRegistry.java core/src/main/java/io/questdb/cairo/wal/LocalDurableAckRegistry.java core/src/test/java/io/questdb/test/cairo/wal/LocalDurableAckRegistryTest.java
git commit --no-verify -m "feat(adaptive): tier-availability probe + shared local lookup on DurableAckRegistry

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Per-connection tier + tier-selected consumer (OSS)

**Files:**
- Modify: `core/src/main/java/io/questdb/cutlass/qwp/server/QwpIngressProcessorState.java` (field ~116; setter ~966; `collectDurableProgress` 300-322; reset ~667)
- Test: `core/src/test/java/io/questdb/test/cutlass/qwp/QwpIngressProcessorStateTest.java` (exists)

**Interfaces:**
- Consumes: `DurabilityTier.*`; `DurableAckRegistry.getLocalDurableSeqTxn`/`getDurablyUploadedSeqTxn`.
- Produces: `QwpIngressProcessorState.setDurableAckTier(int)`; `int getDurableAckTier()`; `collectDurableProgress` selects by tier.

- [ ] **Step 1: Write the failing test** (append to `QwpIngressProcessorStateTest`)

```java
@Test
public void testCollectDurableProgressSelectsByTier() {
    // registry where local (10) >= uploaded (4)
    DurableAckRegistry registry = new DurableAckRegistry() {
        public long getLocalDurableSeqTxn(CharSequence d) { return 10; }
        public long getDurablyUploadedSeqTxn(CharSequence d) { return 4; }
        public boolean isEnabled() { return true; }
    };
    // REPLICATED connection must NOT advance past the uploaded frontier (anti-downgrade)
    QwpIngressProcessorState replicated = newStateWithPendingTable("t", "t~1");
    replicated.setDurableAckEnabled(true);
    replicated.setDurableAckTier(DurabilityTier.REPLICATED);
    Assert.assertEquals(4, replicated.collectDurableProgress(registry).get("t"));
    // LOCAL connection may advance to the local frontier
    QwpIngressProcessorState local = newStateWithPendingTable("t", "t~1");
    local.setDurableAckEnabled(true);
    local.setDurableAckTier(DurabilityTier.LOCAL);
    Assert.assertEquals(10, local.collectDurableProgress(registry).get("t"));
}
```

(Use the test's existing helper for seeding a pending table + dir mapping; if none, add a small `newStateWithPendingTable` helper that populates `pendingDurableDirNames`/`pendingDurableSeqTxns` via the existing package-private path the other tests use. Follow the patterns already in this test file.)

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl core -am -Dtest="QwpIngressProcessorStateTest#testCollectDurableProgressSelectsByTier" -DfailIfNoTests=false test`
Expected: FAIL — `setDurableAckTier` undefined / blind max returns 10 for REPLICATED.

- [ ] **Step 3: Write minimal implementation**

Add field near line 116:

```java
private int durableAckTier = DurabilityTier.NONE;
```

Add setter/getter near line 966 and reset in `clear()`/close near 667 (`durableAckTier = DurabilityTier.NONE;`):

```java
public void setDurableAckTier(int tier) {
    this.durableAckTier = tier;
}

public int getDurableAckTier() {
    return durableAckTier;
}
```

Replace the `Math.max` selection in `collectDurableProgress` (lines 309-313):

```java
// Durability frontier for this connection's negotiated tier. REPLICATED reads the
// uploaded frontier; LOCAL reads the local-fsync frontier. No max(): selecting the
// requested tier is exactly the requested guarantee (local >= uploaded), so a
// REPLICATED client is never advanced by the weaker local tier.
long durableSeqTxn = (durableAckTier == DurabilityTier.REPLICATED)
        ? registry.getDurablyUploadedSeqTxn(dirName)
        : registry.getLocalDurableSeqTxn(dirName);
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl core -am -Dtest="QwpIngressProcessorStateTest" -DfailIfNoTests=false test`
Expected: PASS (existing 71 + new).

- [ ] **Step 5: Commit**

```bash
cd ~/claude/wt/oss/adaptive-commit
git add core/src/main/java/io/questdb/cutlass/qwp/server/QwpIngressProcessorState.java core/src/test/java/io/questdb/test/cutlass/qwp/QwpIngressProcessorStateTest.java
git commit --no-verify -m "feat(adaptive): tier-selected durable-ack consumer (kills blind max downgrade)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Handshake response carries a confirm token (OSS plumbing)

**Files:**
- Modify: `core/src/main/java/io/questdb/cutlass/qwp/server/QwpIngressHttpProcessor.java` (`responseSize`/`writeResponse` overloads: 301-324, 418-470; token constants 60-113)
- Modify: `core/src/main/java/io/questdb/cutlass/qwp/server/egress/QwpEgressUpgradeProcessor.java` (call sites 360, 399)

**Interfaces:**
- Produces: `responseSize(...)`/`writeResponse(...)` overloads whose durable-ack parameter is a confirmation-token `Utf8Sequence confirmToken` (null = disabled) instead of `boolean durableAckEnabled`. When non-null, the response emits `X-QWP-Durable-Ack: <confirmToken>`.

- [ ] **Step 1: Write the failing test** (append to `QwpIngressUpgradeProcessorOnHeadersReadyTest`)

```java
@Test
public void testResponseEchoesGrantedTierToken() {
    // "true" request -> legacy "true" echo (backward compatible)
    String legacy = doHandshakeResponse("true");
    Assert.assertTrue(legacy.contains("X-QWP-Durable-Ack: true"));
    // explicit "local" request on an adaptive server -> "local" echo
    String local = doHandshakeResponse("local");
    Assert.assertTrue(local.contains("X-QWP-Durable-Ack: local"));
}
```

(`doHandshakeResponse` = the test's existing helper that drives `onHeadersReady` with a given `X-QWP-Request-Durable-Ack` value and returns the raw 101 response text; reuse whatever this test already uses to build a request + capture the response bytes.)

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl core -am -Dtest="QwpIngressUpgradeProcessorOnHeadersReadyTest#testResponseEchoesGrantedTierToken" -DfailIfNoTests=false test`
Expected: FAIL — response only ever emits "true".

- [ ] **Step 3: Write minimal implementation**

In `QwpIngressHttpProcessor.java`, add token constants beside the existing ones (60-113):

```java
public static final Utf8String RESPONSE_DURABLE_ACK_HEADER = new Utf8String("X-QWP-Durable-Ack: ");
```

Add new overloads of `responseSize` / `writeResponse` taking `Utf8Sequence durableAckConfirmToken` (null = no durable-ack header). They size/write `RESPONSE_DURABLE_ACK_HEADER + token + CRLF` when non-null. Keep the existing `boolean` overloads delegating: `boolean true` maps to the legacy `"true"` token, `false` maps to `null`, so no other caller breaks. Example (size path mirrors the existing `if (durableAckEnabled)` block at 323):

```java
public static int responseSize(byte[] acceptKey, int qwpVersion, byte[] contentEncodingBytes,
                               Utf8Sequence durableAckConfirmToken, byte[] roleBytes, byte[] maxBatchSizeBytes) {
    int size = /* ...existing base size... */;
    if (durableAckConfirmToken != null) {
        size += RESPONSE_DURABLE_ACK_HEADER.size() + durableAckConfirmToken.size() + 2; // CRLF
    }
    // ...role/maxBatch as before...
    return size;
}
```

Update `QwpEgressUpgradeProcessor` call sites (360, 399) to pass `null` (egress never grants durable-ack) or the legacy `"true"` token to preserve current behavior — match whatever boolean they pass today (they pass the egress durable-ack flag; convert `flag ? HEADER_VALUE_DURABLE_ACK_ENABLED : null`).

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl core -am -Dtest="QwpIngressUpgradeProcessorOnHeadersReadyTest" -DfailIfNoTests=false test`
Expected: PASS (existing 16 + new). Note: the negotiation that picks the token is Task 5 — for this task the upgrade processor may still pass the legacy `"true"` token; adjust the test's `local` assertion to Task 5 if ordering demands. If splitting is awkward, fold Step 1's `local` assertion into Task 5.

- [ ] **Step 5: Commit**

```bash
cd ~/claude/wt/oss/adaptive-commit
git add core/src/main/java/io/questdb/cutlass/qwp/server/QwpIngressHttpProcessor.java core/src/main/java/io/questdb/cutlass/qwp/server/egress/QwpEgressUpgradeProcessor.java core/src/test/java/io/questdb/test/cutlass/websocket/QwpIngressUpgradeProcessorOnHeadersReadyTest.java
git commit --no-verify -m "refactor(qwp): durable-ack response carries a confirm token, not a boolean

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Handshake tier negotiation (OSS)

**Files:**
- Modify: `core/src/main/java/io/questdb/cutlass/qwp/server/QwpIngressUpgradeProcessor.java` (360-393)
- Test: `core/src/test/java/io/questdb/test/cutlass/websocket/QwpIngressUpgradeProcessorOnHeadersReadyTest.java`

**Interfaces:**
- Consumes: `DurabilityTier.fromHeaderValue`/`responseToken`; `DurableAckRegistry.isEnabled/isTierAvailable/strongestAvailableTier`; `QwpIngressProcessorState.setDurableAckTier`; `QwpIngressHttpProcessor` token overloads (Task 4).
- Produces: negotiated `grantedTier` stored on state + echoed in the response.

- [ ] **Step 1: Write the failing test** (append to `QwpIngressUpgradeProcessorOnHeadersReadyTest`)

```java
@Test
public void testTierNegotiation() {
    // OSS-adaptive registry offers LOCAL only.
    // explicit replicated on a local-only server -> NO confirmation (fail-loud at client)
    String replicatedOnLocalOnly = doHandshakeResponse("replicated");
    Assert.assertFalse(replicatedOnLocalOnly.contains("X-QWP-Durable-Ack"));
    // "true" -> strongest available (LOCAL here) -> legacy "true" echo, state tier LOCAL
    String def = doHandshakeResponse("true");
    Assert.assertTrue(def.contains("X-QWP-Durable-Ack: true"));
    Assert.assertEquals(DurabilityTier.LOCAL, lastState().getDurableAckTier());
    // explicit "local" -> "local" echo, state tier LOCAL
    String local = doHandshakeResponse("local");
    Assert.assertTrue(local.contains("X-QWP-Durable-Ack: local"));
    Assert.assertEquals(DurabilityTier.LOCAL, lastState().getDurableAckTier());
}
```

(`lastState()` = accessor for the `QwpIngressProcessorState` the processor set; reuse the test's existing state-capture. Assumes the test engine installs a `LocalDurableAckRegistry` — if it installs the no-op default, seed a `LocalDurableAckRegistry` via `engine.setDurableAckRegistry(new LocalDurableAckRegistry(engine))` in setup.)

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl core -am -Dtest="QwpIngressUpgradeProcessorOnHeadersReadyTest#testTierNegotiation" -DfailIfNoTests=false test`
Expected: FAIL — no tier negotiation yet.

- [ ] **Step 3: Write minimal implementation** — replace lines 360-393 negotiation:

```java
Utf8Sequence durableAckHeader = requestHeader.getHeader(
        QwpIngressHttpProcessor.HEADER_X_QWP_REQUEST_DURABLE_ACK);
int requestIntent = DurabilityTier.fromHeaderValue(durableAckHeader);
DurableAckRegistry ackRegistry = engine.getDurableAckRegistry();

int grantedTier;
if (requestIntent == DurabilityTier.NONE || !ackRegistry.isEnabled()) {
    grantedTier = DurabilityTier.NONE;
} else if (requestIntent == DurabilityTier.DEFAULT) {
    grantedTier = ackRegistry.strongestAvailableTier();      // Ent -> REPLICATED, OSS -> LOCAL
} else {
    grantedTier = ackRegistry.isTierAvailable(requestIntent) // explicit request
            ? requestIntent
            : DurabilityTier.NONE;                            // unsupported -> no confirmation (fail-loud)
}
boolean durableAckEnabled = grantedTier != DurabilityTier.NONE;

// Echo legacy "true" for the DEFAULT grant (backward compatible with existing clients);
// echo the explicit tier token otherwise.
Utf8Sequence confirmToken = !durableAckEnabled ? null
        : (requestIntent == DurabilityTier.DEFAULT
            ? QwpIngressHttpProcessor.HEADER_VALUE_DURABLE_ACK_ENABLED
            : DurabilityTier.responseToken(grantedTier));

int requiredHandshakeSize = QwpIngressHttpProcessor.responseSize(
        acceptKey, negotiatedVersion, null, confirmToken, roleBytes, effectiveMaxBatchSizeBytes);
// ...fits check unchanged...
state.setDurableAckEnabled(durableAckEnabled);
state.setDurableAckTier(grantedTier);
int bytesWritten = QwpIngressHttpProcessor.writeResponse(
        bufferAddr, acceptKey, negotiatedVersion, null, confirmToken, roleBytes, effectiveMaxBatchSizeBytes);
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl core -am -Dtest="QwpIngressUpgradeProcessorOnHeadersReadyTest" -DfailIfNoTests=false test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd ~/claude/wt/oss/adaptive-commit
git add core/src/main/java/io/questdb/cutlass/qwp/server/QwpIngressUpgradeProcessor.java core/src/test/java/io/questdb/test/cutlass/websocket/QwpIngressUpgradeProcessorOnHeadersReadyTest.java
git commit --no-verify -m "feat(adaptive): negotiate durable-ack tier at handshake (fail-loud on unsupported)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 6: OSS regression gate** — run the adaptive + qwp suites to confirm no downgrade/regression:

Run: `mvn -q -pl core -am -Dtest="LocalDurableAckRegistryTest,QwpIngressProcessorStateTest,QwpIngressUpgradeProcessorOnHeadersReadyTest,DurabilityTierTest,AdaptiveWalDurabilityTest" -DfailIfNoTests=false test`
Expected: all green.

---

### Task 6: Enterprise `DurableUploadRegistry` reports the local tier (Ent)

**Files:**
- Modify: `questdb-ent/src/main/java/com/questdb/cairo/wal/transfer/DurableUploadRegistry.java` (fields; ctor 118-124; `getDurablyUploadedSeqTxn` 184-188; `FrozenSnapshot` 364-391)
- Modify: `questdb-ent/src/main/java/com/questdb/lifecycle/PrimaryRoleState.java` (ctor install 122-124)
- Test: `questdb-ent/src/test/java/com/questdb/cairo/wal/transfer/DurableUploadRegistryTest.java` (exists)

**Interfaces:**
- Consumes: `LocalDurableAckRegistry.resolveLocalDurableSeqTxn(CairoEngine, CharSequence)` (Task 2); `DurabilityTier.*`.
- Produces: Enterprise registry now reports `getLocalDurableSeqTxn` (delegated) and `isTierAvailable(LOCAL|REPLICATED) == true`.

- [ ] **Step 1: Bump the ent submodule to the OSS branch head + rebuild base**

```bash
cd ~/claude/wt/ent/adaptive/questdb && git fetch adaptive && git checkout "$(git -C ~/claude/wt/oss/adaptive-commit rev-parse HEAD)"
cd ~/claude/wt/ent/adaptive && git add questdb
```
(Do not commit yet — the bump commits with Step 5.)

- [ ] **Step 2: Write the failing test** (append to `DurableUploadRegistryTest`)

```java
@Test
public void testReportsLocalTierAndAvailability() throws Exception {
    assertMemoryLeak(() -> {
        // adaptive table 't' with a local-durable frontier of 7
        TableToken tt = createAdaptiveTable("t");   // helper: CREATE ... WITH commitMode=adaptive, commit 8 rows, etc.
        advanceLocalDurable(tt, 7);                  // helper: drive SeqTxnTracker local-durable to 7
        DurableUploadRegistry registry = new DurableUploadRegistry(engine,
                engine.getConfiguration().getMillisecondClock());
        Assert.assertEquals(7, registry.getLocalDurableSeqTxn(tt.getDirName()));
        Assert.assertTrue(registry.isTierAvailable(DurabilityTier.LOCAL));
        Assert.assertTrue(registry.isTierAvailable(DurabilityTier.REPLICATED));
        Assert.assertEquals(-1, registry.frozenSnapshot().getLocalDurableSeqTxn(tt.getDirName()));
    });
}
```

(Use the harness the other `DurableUploadRegistryTest` methods use to build an `engine` and adaptive tables; `advanceLocalDurable` mirrors how `AdaptiveWalDurabilityTest` drives the local frontier. If driving a real SeqTxnTracker is heavy here, assert against a table whose tracker is seeded via the same path those adaptive tests use.)

- [ ] **Step 3: Run test to verify it fails**

Run: `mvn -q -pl questdb-ent -am -Dtest="DurableUploadRegistryTest#testReportsLocalTierAndAvailability" -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false test`
Expected: FAIL — ctor has no `engine`; `getLocalDurableSeqTxn` returns -1; `isTierAvailable` false.

- [ ] **Step 4: Write minimal implementation**

Add an `engine` field + constructor param:

```java
private final CairoEngine engine;

public DurableUploadRegistry(CairoEngine engine, MillisecondClock clock) {
    this.engine = engine;
    this.clock = clock;
}
```

Override the local tier + availability:

```java
@Override
public long getLocalDurableSeqTxn(CharSequence tableDirName) {
    return LocalDurableAckRegistry.resolveLocalDurableSeqTxn(engine, tableDirName);
}

@Override
public boolean isTierAvailable(int tier) {
    return tier == DurabilityTier.LOCAL || tier == DurabilityTier.REPLICATED;
}
```

`FrozenSnapshot` keeps the interface default `getLocalDurableSeqTxn == -1` and overrides availability to `{REPLICATED}`:

```java
@Override
public boolean isTierAvailable(int tier) {
    return tier == DurabilityTier.REPLICATED; // demoting node: local tier is moot
}
```

In `PrimaryRoleState` (122-124) pass `engine`:

```java
this.durableUploadRegistry = new DurableUploadRegistry(
        engine,
        serverConfig.getCairoConfiguration().getMillisecondClock()
);
```

(Also fix the no-arg/1-arg `DurableUploadRegistry` constructors if other call sites/tests use them — add an `engine`-less test constructor only if a test needs it, else update those call sites.)

- [ ] **Step 5: Run test to verify it passes + commit the bump**

Run: `mvn -q -pl questdb-ent -am -Dtest="DurableUploadRegistryTest" -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false test`
Expected: PASS (existing 23 + new).

```bash
cd ~/claude/wt/ent/adaptive
git add questdb questdb-ent/src/main/java/com/questdb/cairo/wal/transfer/DurableUploadRegistry.java questdb-ent/src/main/java/com/questdb/lifecycle/PrimaryRoleState.java questdb-ent/src/test/java/com/questdb/cairo/wal/transfer/DurableUploadRegistryTest.java
git commit --no-verify -m "feat(adaptive): DurableUploadRegistry reports the local-fsync tier (S1)

Bumps OSS submodule to the multi-tier durable-ack OSS work and wires the
Enterprise registry to report both tiers so a primary offers LOCAL and
REPLICATED durable-ack. Frozen (demote) snapshot offers REPLICATED only.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Enterprise end-to-end regression (Ent)

**Files:**
- Test: `questdb-ent/src/test/java/com/questdb/cairo/wal/transfer/DurableUploadRegistryTest.java` (or a focused new `MultiTierDurableAckTest`)

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Write the failing test** — the anti-downgrade guarantee end-to-end

```java
@Test
public void testEnterpriseDefaultStaysReplicatedLocalOptIn() throws Exception {
    assertMemoryLeak(() -> {
        TableToken tt = createAdaptiveTable("t");
        advanceLocalDurable(tt, 10);   // locally durable to 10
        recordUploaded(tt, 4);         // uploaded only to 4
        DurableUploadRegistry registry = new DurableUploadRegistry(engine,
                engine.getConfiguration().getMillisecondClock());
        registry.recordUploaded(/* dir */ tt, 4);
        // strongest available = REPLICATED (default grant preserves today's guarantee)
        Assert.assertEquals(DurabilityTier.REPLICATED, registry.strongestAvailableTier());
        // REPLICATED frontier = uploaded (4); LOCAL frontier = local (10)
        Assert.assertEquals(4, registry.getDurablyUploadedSeqTxn(tt.getDirName()));
        Assert.assertEquals(10, registry.getLocalDurableSeqTxn(tt.getDirName()));
    });
}
```

(`recordUploaded` uses the registry's existing `recordUploaded(dirNamePtr, size, seqTxn)` path the other tests already exercise.)

- [ ] **Step 2: Run test to verify it fails, then passes after wiring**

Run: `mvn -q -pl questdb-ent -am -Dtest="DurableUploadRegistryTest#testEnterpriseDefaultStaysReplicatedLocalOptIn" -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false test`
Expected: PASS once Task 6 is in (this test asserts the composed behavior; it fails before Task 6).

- [ ] **Step 3: Full ent durable/transfer regression**

Run: `mvn -q -pl questdb-ent -am -Dtest="DurableUploadRegistryTest,CheckWalTransactionsJobTest" -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false test`
Expected: all green (24 baseline + new).

- [ ] **Step 4: Commit**

```bash
cd ~/claude/wt/ent/adaptive
git add questdb-ent/src/test/java/com/questdb/cairo/wal/transfer/DurableUploadRegistryTest.java
git commit --no-verify -m "test(adaptive): Enterprise multi-tier durable-ack end-to-end (default stays replicated)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- §4.1 registry both tiers → Tasks 2 (OSS availability + shared lookup) + 6 (Ent local tier). ✓
- §4.2 protocol negotiation + granted-tier echo + fail-loud → Tasks 1, 4, 5. ✓
- §4.3 consumer tier selection (kills blind max) → Task 3. ✓
- §4.4 truthful edges (NOSYNC local = -1) → covered by `resolveLocalDurableSeqTxn` returning -1 (Task 2) + consumer (Task 3); NOSYNC assertion can be added to Task 3's test. ✓
- §6 testing incl. anti-downgrade headline → Task 3 (consumer) + Task 7 (Ent e2e). ✓
- §7 backward compat (legacy `true` echo) → Task 5 (DEFAULT → "true" echo) + Task 4 (boolean overloads preserved). ✓
- §8 cross-repo order → Task 6 Step 1 bump. ✓
- Demote-drain stays uploaded-tier → unchanged `isDurableWorkFullyUploaded` (no task touches it — deliberate). ✓

**Placeholder scan:** helper methods referenced in tests (`newStateWithPendingTable`, `doHandshakeResponse`, `lastState`, `createAdaptiveTable`, `advanceLocalDurable`, `recordUploaded`) are called out as "reuse the existing test harness" with the pattern to follow — the executing engineer must wire them to the concrete helpers each test file already has; they are not inventions. Flag if a target test lacks such a helper and add it in that task.

**Type consistency:** `setDurableAckTier(int)`/`getDurableAckTier()`, `isTierAvailable(int)`, `strongestAvailableTier()`, `resolveLocalDurableSeqTxn(CairoEngine, CharSequence)`, `responseToken(int)` used consistently across tasks. Response plumbing uses `Utf8Sequence confirmToken` (null = off) uniformly in Tasks 4-5.

**Known follow-ups (out of scope, documented):** per-tier `isDurableWorkFullyUploaded` optimization; read-path `read_durability='replicated'`; client-library tier request support (this plan is server-side only).
