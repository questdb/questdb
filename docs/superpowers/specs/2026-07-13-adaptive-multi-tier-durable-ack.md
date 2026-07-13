# Adaptive Multi-Tier Durable-Ack (D4 · seams S1 + S2 write-path)

**Date:** 2026-07-13
**Branch (OSS):** `nw_adaptive_commit` · **Branch (Ent):** `nw_adaptive_commit_ent`
**Status:** design approved, pre-plan
**Depends on:** OSS adaptive D3 (`LocalDurableAckRegistry`, `SeqTxnTracker.localDurableSeqTxn`); Enterprise `DurableUploadRegistry` + upload pipeline.
**Relates to:** [adaptive-commit-mode design §7 (read-durability), §17 seams S1/S2](2026-06-25-adaptive-commit-mode-design.md).

## 1. Problem

Adaptive commit mode added a **local-fsync durability tier** (`SeqTxnTracker.getLocalDurableSeqTxn()`): a WAL commit made power-loss-safe by `fdatasync`, well before it is uploaded to object store. The QWP durable-ack path was generalized to a tier-ordered frontier — `applied ≥ localDurable ≥ uploaded` — and the OSS consumer already reads both tiers:

```java
// QwpIngressProcessorState.collectDurableProgress  (OSS core)
long localSeqTxn    = registry.getLocalDurableSeqTxn(dirName);
long uploadedSeqTxn = registry.getDurablyUploadedSeqTxn(dirName);
long durableSeqTxn  = Math.max(localSeqTxn, uploadedSeqTxn);   // <-- reported as THE durable frontier
```

Two defects follow under Enterprise:

1. **The local tier is invisible under Enterprise.** Enterprise installs `DurableUploadRegistry` (via `engine.setDurableAckRegistry`), which implements `getDurablyUploadedSeqTxn` but **does not override `getLocalDurableSeqTxn`** — so it inherits the interface default `-1`. An adaptive table's power-loss-safe frontier is never reported to durable-ack clients; they wait for the (slower) uploaded tier even when they only needed power-loss safety.

2. **The naive fix would silently downgrade Enterprise.** Because `localDurable ≥ uploaded` always, once `getLocalDurableSeqTxn` returns a real value the blind `Math.max(local, uploaded)` resolves to **local** — telling every Enterprise durable-ack client its commit is "durable" while it is only locally fsync'd, not yet replicated. That is a guarantee regression (replicated → power-loss-only) for clients who rely on durable-ack meaning "survives node failover."

## 2. Goals / Non-goals

**Goals**
- Expose the local-fsync tier from the Enterprise registry (S1), so both tiers are live simultaneously.
- Let an ingest client **choose** its durable-ack tier: `LOCAL` (power-loss-safe, lower latency) or `REPLICATED` (failover-safe). Default preserves today's guarantee.
- Never silently downgrade: an unsupported-tier request fails loud; the granted tier is echoed to the client.

**Non-goals (explicitly deferred)**
- Read-path `read_durability='replicated'` — SELECT visibility gating after failover (adaptive design §7/S2 read-path). Separate change.
- `read_durability='latest'` speculative reads.
- Per-tier optimization of the demote-drain gate (`isDurableWorkFullyUploaded`) — stays uploaded-tier (conservative) for now.

## 3. Tiers

A strength-ordered enum of durability tiers used by the ack path:

| Tier | Guarantee | Source frontier | Available when |
|------|-----------|-----------------|----------------|
| `LOCAL` | survives power loss | `getLocalDurableSeqTxn` (adaptive fdatasync) | server supports adaptive (per-table frontier is `-1` for non-ADAPTIVE tables) |
| `REPLICATED` | survives node/failover | `getDurablyUploadedSeqTxn` (object store) | Enterprise primary with upload pipeline enabled |

`applied` (visible-but-not-durable) is not an ack tier. Ordering: `LOCAL < REPLICATED` in strength; `localDurableSeqTxn ≥ uploadedSeqTxn` in seqTxn value. **Tier availability is server-level** (which tiers the node can offer); **frontier truthfulness is per-table** (an offered tier reports `-1` for a table that cannot satisfy it). §4.1 and §9 elaborate.

## 4. Design

### 4.1 Registry — expose both tiers (S1)

- `DurableUploadRegistry` (Enterprise) gains an engine handle (or a narrow `LocalDurableSource` functional interface) supplied at construction in `PrimaryRoleState` (the install site already holds `engine`). It **composes** the local-tier lookup that `LocalDurableAckRegistry` already implements — resolve `TableToken` from `dirName`, read `SeqTxnTracker.getLocalDurableSeqTxn()` — rather than duplicating it. Extract that lookup into one shared helper reused by both registries.
- `DurableAckRegistry` gains a capability probe: `boolean isTierAvailable(DurabilityTier tier)` (default: `LOCAL` available iff `getLocalDurableSeqTxn` can be non-`-1`; `REPLICATED` available iff the impl tracks uploads). OSS `LocalDurableAckRegistry` → `{LOCAL}`. Enterprise `DurableUploadRegistry` → `{LOCAL, REPLICATED}`.
- `FrozenSnapshot` (demote snapshot) reports `getLocalDurableSeqTxn = -1`: on a demoting node the local tier is moot, and deferred connections gate on uploaded coverage (existing behavior). Its available tiers = `{REPLICATED}` frozen.

### 4.2 Protocol — client selects a tier (S2 write-path), backward compatible

- Extend the accepted values of the existing handshake header `X-QWP-Request-Durable-Ack` (today: `true`, case-insensitive):
  - `true` → **strongest available tier** (Enterprise ⇒ `REPLICATED`, OSS ⇒ `LOCAL`). Preserves every existing client's current guarantee.
  - `local` → request `LOCAL`.
  - `replicated` → request `REPLICATED`.
- The handshake **response echoes the granted tier** (extend the existing `durableAckEnabled` response token to carry the tier), so a client can confirm what it received.
- **Unsupported-tier request fails the handshake loud** (reject with a clear status/error frame) — e.g. `replicated` on OSS, or when the primary's upload pipeline is not enabled. Never clamp to a weaker tier silently.
- Store `requestedDurabilityTier` on the ingress connection state (`QwpIngressProcessorState`), set at handshake.

### 4.3 Consumer — tier selection replaces blind max

In `collectDurableProgress`, select the frontier for the connection's tier instead of `Math.max`:

```java
long frontier = (tier == LOCAL)
        ? registry.getLocalDurableSeqTxn(dirName)
        : registry.getDurablyUploadedSeqTxn(dirName);
if (frontier >= 0 && frontier > lastSent) { ...advance... }
```

No `max`: since `local ≥ uploaded`, selecting the requested tier's frontier is exactly the requested guarantee. OSS default (`LOCAL`) reads local; Enterprise default (`REPLICATED`) reads uploaded — identical to today, no downgrade.

`isDurableWorkFullyUploaded` (demote-drain gate) is **unchanged** — stays uploaded-tier. Over-waiting for upload coverage on a role change is always safe; specializing it per tier is a later optimization.

### 4.4 Truthful edges

- A `LOCAL`-tier ack on a **NOSYNC** table stays `-1` (never acked): NOSYNC provides no local durability, so a local-durable ack cannot be honored — truthful, not a bug. Optionally logged once.
- Frontier is per-table; tier is per-connection. A connection writing to several tables gets, per table, that table's frontier at the connection's chosen tier.

## 5. Affected components

**OSS core (`questdb/core`)**
- `wal/DurableAckRegistry.java` — add `DurabilityTier` enum + `isTierAvailable`; keep both accessors.
- `wal/LocalDurableAckRegistry.java` — advertise `{LOCAL}`; factor the local lookup into a shared helper.
- `cutlass/qwp/server/QwpIngressHttpProcessor.java` — parse tier from header; echo granted tier; reject unsupported.
- `cutlass/qwp/server/QwpIngressProcessorState.java` — store `requestedDurabilityTier`; tier-select in `collectDurableProgress`.
- `cutlass/qwp/protocol/QwpConstants.java` — tier tokens/constants.

**Enterprise (`questdb-ent`)**
- `cairo/wal/transfer/DurableUploadRegistry.java` — engine handle; override `getLocalDurableSeqTxn`; advertise `{LOCAL, REPLICATED}`; `FrozenSnapshot` tier behavior.
- `lifecycle/PrimaryRoleState.java` — pass `engine`/local source when constructing the registry.

## 6. Testing (TDD — write the failing test first)

- **`DurableUploadRegistryTest` (Ent):** under adaptive, `getLocalDurableSeqTxn` reflects `SeqTxnTracker`; `local ≥ uploaded`; `isTierAvailable` correct; `FrozenSnapshot` reports local `-1`.
- **Handshake (OSS):** `true`/`local`/`replicated` parse to the right tier; response echoes granted tier; `replicated` on an OSS/no-upload server rejects loud.
- **`collectDurableProgress` selection (OSS):** LOCAL client's frontier advances at local fsync; **REPLICATED client's frontier does NOT advance until uploaded** (the headline anti-downgrade test); NOSYNC-table LOCAL ack stays `-1`.
- **Regression:** existing `DurableUploadRegistryTest` (23) + QWP ingress/durable-ack suites stay green; a default-`true` Enterprise client still acks at `REPLICATED` (byte-for-byte same as before this change).

## 7. Backward compatibility

- Existing clients send `X-QWP-Request-Durable-Ack: true` → strongest available tier → **unchanged** guarantee (Enterprise replicated, OSS local).
- Response gains a tier token; older clients ignore unknown response fields (they only checked for enablement). Confirm the response framing is additive.
- No `_meta`/on-disk format change. No new config knobs required (tier is per-connection, negotiated).

## 8. Cross-repo coordination

Two-branch change like the rest of adaptive: OSS core carries the interface + protocol + consumer; Enterprise carries the registry override + install. Land OSS first (or together); the Ent worktree submodule bump follows. Both must stay `9.4.4-SNAPSHOT`-coupled and compile green (validated in D4 Phase 0).

## 9. Open questions / risks

- **Header token surface:** reuse `X-QWP-Request-Durable-Ack` value (chosen) vs a new `X-QWP-Durable-Ack-Tier` header. Chosen: extend existing value (fewer moving parts, one negotiation point). Revisit if the response echo is awkward to frame additively.
- **Dynamic replication toggle:** if the upload pipeline can turn off mid-connection, a `REPLICATED` connection's acks stall (correctly). Decide whether to surface that as a connection error or let it stall until re-enabled. Default: stall (truthful), document it.
- **Capability probe semantics:** `isTierAvailable(LOCAL)` when a *server* is adaptive-capable but a given *table* is NOSYNC — availability is server-level (LOCAL is offerable), truthfulness is per-table (that table's frontier stays `-1`). Keep the probe server-level.
