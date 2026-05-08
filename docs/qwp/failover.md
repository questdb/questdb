# QWP Client Failover Spec

Normative behaviour for multi-host failover across QWP clients. This
document specifies the **shared primitives** — host-health model,
backoff function, role filter, error classification — that every
client uses. The per-context loops that consume these primitives live
in the wire docs:

| Context | Loop home |
|---|---|
| Ingress non-SF — `Sender.New("ws::...")` initial connect; one-shot walk, no run-time reconnect | [`wire-ingress.md`](wire-ingress.md) §15.5 |
| Ingress SF — the cursor-engine reconnect loop (`sf_dir=...`); long-running, recovers across outages | [`sf-client.md`](sf-client.md) §13.6 |
| Egress — per-`Execute()` attempt loop on the read-side `QueryClient`; bounded by attempt count + wall-clock budget | [`wire-egress.md`](wire-egress.md) §11.9 |

## 1. Connect-string keys (common)

These keys apply to every WS client. Per-context knobs (SF reconnect
budget, egress failover budget, etc.) are documented next to their
loops in the wire docs above.

| Key | Type | Default | Scope |
|---|---|---|---|
| `addr` | `host:port[,host:port…]` | required | Comma-separated multi-host failover list. |
| `auth_timeout_ms` | int | `15_000` | Per-host upper bound on the **HTTP upgrade response read** only. Does NOT cover TCP connect (OS default), TLS handshake, or the post-upgrade `SERVER_INFO` frame read (those use a separate hard-coded 5s timeout). Bounds the common "TCP accepts but the server never replies" blackhole; full-route blackholes that drop SYN-ACK still fall back to the OS connect timeout. |
| `zone` | string | unset | Client's zone identifier (opaque, case-insensitive; e.g. `eu-west-1a`, `dc-amsterdam`). Egress with `target=any\|replica` prefers endpoints whose server-advertised `zone_id` matches; see §2 for the priority lattice and `wire-egress.md` §11.8 for the `SERVER_INFO.zone_id` field. Ignored when `target=primary` (writers follow the master across zones). Ignored on ingress (zone-blind, pinned to v1) — ingress logs a one-time WARN if set so the no-op is visible. |

`addr` accepts comma syntax (`addr=h1:p1,h2:p2`) and repeated keys
(`addr=h1:p1;addr=h2:p2`). The two forms MUST accumulate; empty
entries (`,,` or trailing/leading commas) MUST be rejected. Multi-host
failover semantics are normative for WS/WSS and HTTP/HTTPS only.

## 2. Host health model

Each configured `addr` entry carries a `(state, zone_tier)` classification.

State lattice:

| State | Meaning | State priority |
|---|---|---|
| `Healthy` | Last connect to this host succeeded. | 1 (best) |
| `Unknown` | Never tried this round, or just reset. | 2 |
| `TransientReject` | Server returned `421 Misdirected Request` + `X-QuestDB-Role: PRIMARY_CATCHUP`. Likely to recover. | 3 |
| `TransportError` | TCP/TLS/handshake error during connect, or mid-stream send/recv failure (the latter only via `RecordMidStreamFailure`, see §2.1). | 4 |
| `TopologyReject` | Server returned `421 Misdirected Request` + `X-QuestDB-Role: REPLICA`. Will not become writable without topology change. | 5 (worst) |

Zone tier is assigned the first time the host's zone is observed
(`SERVER_INFO.zone_id` on the upgrade frame, or `X-QuestDB-Zone` on a
`421` reject — see §5):

| Zone tier | Meaning | Zone priority |
|---|---|---|
| `Same`    | Server zone equals client `zone=` (case-insensitive), OR client `zone=` is unset, OR `target=primary`. | 1 (best) |
| `Unknown` | Server did not advertise a zone (no `CAP_ZONE`, no `X-QuestDB-Zone` header, or v1-pinned client). | 2 |
| `Other`   | Server advertised a different zone. | 3 (worst) |

`target=primary` collapses every host's zone tier to `Same`: writers
must follow the master regardless of geography. Ingress (SF and non-SF)
is likewise zone-blind — it pins v1 and never reads zone information,
so every host is `Same` by default.

### Selection priority

`PickNext()` returns the host with the lowest `(state_priority,
zone_priority)` tuple — compared lexicographically — that has not been
tried in the current round. State outranks zone, so a known-good host
in another zone is picked before an untried local host (the alternative
risks wasted probes against unproven peers when a working endpoint is
already in hand). Within a tied `(state, zone_tier)` bucket, the order
is the address-list shuffle established at client construction.

Clients SHOULD shuffle the configured `addr=` list at construction
time. The shuffle has no operator-visible knob; it spreads first-connect
attempts across same-tier hosts when many clients start simultaneously.

A round-tracker bit per host records whether **this round** has tried
the host yet. A "round" is the time between two `BeginRound` calls.

### 2.1 Operations

```
PickNext()                       → highest-priority host with attempted-this-round=false; -1 if none
RecordSuccess(idx)               → state=Healthy,           attempted=true
RecordRoleReject(idx, transient) → state=TransientReject (transient=true) or TopologyReject (false), attempted=true
RecordTransportError(idx)        → state=TransportError,    attempted=true
RecordMidStreamFailure(idx)      → if state==Healthy, demote to TransportError; do NOT touch attempted
RecordZone(idx, zoneId)          → if zoneId is non-null/non-empty: zone_tier = Same when
                                   zoneId equals client zone= (case-insensitive) OR client zone=
                                   is unset OR target=primary; else zone_tier = Other.
                                   if zoneId is null/empty: no-op (existing zone_tier preserved,
                                   defaulting to Unknown if never set).
                                   Do NOT touch state or attempted.
                                   Caller invokes from the connect path: once after SERVER_INFO
                                   is read iff capabilities & CAP_ZONE, and once with the
                                   X-QuestDB-Zone header value (or null) on a 421 reject.
BeginRound(forgetClassifications)→ clear attempted flags. forgetClassifications=true also resets every
                                   non-Healthy state to Unknown and preserves the last-known same-zone
                                   Healthy entry only (cross-zone Healthy entries are reset to Unknown);
                                   see §2.2 for the sticky-Healthy rationale.
                                   zone_tier is NOT cleared by BeginRound — once observed, it persists
                                   for the host's lifetime in this client until re-observed differently.
IsRoundExhausted                 → true iff every host has attempted=true
```

`BeginRound(forgetClassifications=true)` is the **between-outages**
reset; it gives stale `TransientReject` / `TopologyReject` hosts
another chance on the next round while keeping the last-known good
host as the priority pick.

`BeginRound(forgetClassifications=false)` is the **within-outage**
reset; same round bits cleared, classifications preserved (so a host
that role-rejected this outage stays demoted).

### 2.2 Sticky-Healthy across rounds

`RecordSuccess(idx)` only mutates the target index; previously-Healthy
hosts are not implicitly demoted. The sticky effect emerges from
`BeginRound(forgetClassifications=true)`: when scanning for the entry
to preserve, the **last** `Healthy` index whose zone tier is `Same`
wins. Cross-zone (`Other`) `Healthy` entries are reset to `Unknown`
rather than preserved — a sticky pin in another zone would otherwise
lock the client out of probing local hosts after they recover. The
most recent same-zone success stays first in line on the next round;
older `Healthy` entries (regardless of zone tier) are reset to
`Unknown`.

When `target=primary` (or client `zone=` is unset), every zone tier
collapses to `Same`, so the rule degenerates to "preserve the last
`Healthy` host" — the original behavior.

### 2.3 Tracker ordering invariants

These invariants are normative; violation produces user-visible
behaviour drift across clients.

- **Mid-stream demote before round reset.** When a connection fails
  after a successful upgrade, the loop MUST call
  `RecordMidStreamFailure(previousIdx)` **before** the next
  `BeginRound(forgetClassifications=true)`. Reversing the order makes
  sticky-Healthy preserve the just-failed host as priority pick,
  which then receives the first reconnect attempt and fails again.
- **`previousIdx` is per-caller, not shared.** When multiple loops drive
  reconnects against the same tracker (e.g. the foreground SF I/O
  thread plus N background drainers), each loop MUST own its own
  `previousIdx` slot. Sharing one slot across loops corrupts the
  Healthy→TransportError demotion: drainer A's reconnect would demote
  foreground's just-bound endpoint, and vice versa. The Java client
  expresses this as a per-caller `ReconnectFactory` instance with a
  private `previousIdx` field.
- **Round reset on exhaustion.** When `PickNext()` returns `-1`, the
  loop MUST call `BeginRound(forgetClassifications=true)` before the
  next `PickNext()`. Calling `PickNext()` twice without an
  intervening reset is undefined.
- **Thread-safety.** All tracker operations MUST be observable as a
  total order across threads. Implementations MAY use a single
  internal lock; the public methods are not required to be
  reentrant.

## 3. Backoff function

Used by SF reconnect ([`sf-client.md`](sf-client.md) §13.6) and egress
failover ([`wire-egress.md`](wire-egress.md) §11.9). Non-SF ingress
walks the address list once with no inter-host backoff and never
reconnects, so it doesn't consume this function (see
[`wire-ingress.md`](wire-ingress.md) §15.5). The function is pure; the
loop owner provides elapsed wall-clock time.

```
ComputeBackoff(attempt):       # attempt is 0-based; backoff(0) returns InitialBackoff
    base = InitialBackoff
    repeat `attempt` times while base < MaxBackoff:
        if base > MaxBackoff/2: base = MaxBackoff; break    # saturate before doubling overflow
        base = base * 2
    if base > MaxBackoff: base = MaxBackoff
    return Jitter(base)            # see §3.1 — context-specific

NextBackoffOrGiveUp(attempt, elapsedSinceOutage):
    if elapsedSinceOutage > MaxOutageDuration: return GIVE_UP
    backoff = ComputeBackoff(attempt)
    remaining = MaxOutageDuration - elapsedSinceOutage
    if backoff > remaining:
        return remaining if remaining > 0 else GIVE_UP
    return backoff
```

The post-jitter sleep MUST be clamped to `remaining` so a single
sleep cannot overshoot `MaxOutageDuration`. SF role-rejects use
`ComputeBackoff(0)` (i.e. always the initial value) but still pass
through the deadline check above; see §3.2.

### 3.1 Jitter

Two jitter shapes are normative, picked by context:

- **SF (ingress) — equal-jitter** `[base, 2·base)`. Multiple producers
  may share a cluster; equal-jitter has a non-zero lower bound that
  damps reconnect storms. The post-jitter sleep is **not** clamped to
  `MaxBackoff` — once `base` saturates the cap, the actual sleep
  lands in `[max, 2·max)`.
- **Egress — full-jitter** `[0, base)`. A query client is single-user
  and benefits from the lowest expected recovery time; thundering
  herd is not a concern at one client per workload.

Reference formulas:

```
EqualJitter(base):  return base + uniform_long(base)        # [base, 2·base)
FullJitter(base):   return uniform_long(base)               # [0, base)
```

### 3.2 Outage state

The pair `(attempt, elapsedSinceOutage)` is the **outage state**, owned
by the loop:

- `attempt` increments on every consumed backoff and resets to `0`
  on a successful reconnect.
- `elapsedSinceOutage` starts at `0` when the first failure is
  observed and resets to `0` on a successful reconnect. A reconnect
  that immediately fails again starts a fresh outage clock.

In the SF reconnect loop ([`sf-client.md`](sf-client.md) §13.6),
role-rejects do not advance the per-attempt backoff (when a round
exhausts with role-reject as `lastError`, the round-boundary sleep
is the configured `InitialBackoff` and `backoff.attempt` is reset,
so the client doesn't accumulate exponential delay against a
known-but-not-writable endpoint). They DO consume the wall-clock
outage budget (`reconnect_max_duration_millis`) — without that
bound a long-lived PRIMARY_CATCHUP cluster would let an unattended
SF producer block forever. The intended usage pattern is to set
`reconnect_max_duration_millis` large enough (default 5 min) to
absorb a normal failover window and short enough to surface
permanent topology issues. The egress per-Execute loop
([`wire-egress.md`](wire-egress.md) §11.9) handles role-rejects only
during `WalkTracker` (initial connect / reconnect) and so doesn't
share the outage clock with the per-Execute loop.

## 4. Loop semantics by context

The three failover loops live in their respective wire docs. Each
consumes the primitives in §1.1 / §2 / §3 / §5 / §6 and adds the
context-specific knobs, pseudocode, and key properties.

| Context | Wire-doc home |
|---|---|
| Ingress non-SF — one-shot walk over `addr=` at construction; AuthError / ProtocolVersionError terminal; role rejects walk to next host | [`wire-ingress.md`](wire-ingress.md) §15.5 |
| Ingress SF — long-running reconnect loop with mid-stream demote, equal-jitter backoff, `initial_connect_retry` policy, `reconnect_max_duration_millis` outage budget | [`sf-client.md`](sf-client.md) §13.6 |
| Egress — per-`Execute()` loop with `failover=on/off` master switch, `failover_max_attempts` cap, full-jitter backoff, `OnFailoverReset` callback; `WalkTracker` helper for initial connect / reconnect | [`wire-egress.md`](wire-egress.md) §11.9 |

## 5. Role filter (`target=`)

Per-endpoint check sequence:

1. Open TCP/TLS connection within `auth_timeout_ms`.
2. Issue HTTP upgrade; observe response.
3. If response is `421 + X-QuestDB-Role: <role>`: extract `X-QuestDB-Zone`
   if present and record the host's zone tier (§2); then `RecordRoleReject`
   and walk to next host (no `SERVER_INFO` frame is read).
4. Otherwise upgrade succeeds; if negotiated version ≥ 2, read the
   `SERVER_INFO` frame. If `capabilities & CAP_ZONE`, parse `zone_id`
   and record the host's zone tier. Apply the role table below.
5. v1 negotiation skips the `SERVER_INFO` read entirely; the host's
   zone tier remains `Unknown`.

`SERVER_INFO.Role` byte values:

| Role byte | Name | `target=any` | `target=primary` | `target=replica` |
|---|---|---|---|---|
| `0x00` | STANDALONE | ✓ | ✓ | ✗ |
| `0x01` | PRIMARY | ✓ | ✓ | ✗ |
| `0x02` | REPLICA | ✓ | ✗ | ✓ |
| `0x03` | PRIMARY_CATCHUP | ✓ | ✓ | ✗ |

When v1 is negotiated (either v1 server, or v1-pinned client):
`target=any` matches; `target=primary` and `target=replica` produce
`TopologyReject`. The filter is wire-supported only when both sides
reach v2+.

The `X-QuestDB-Role` HTTP response header on a `421` upgrade reject
SHOULD be one of `STANDALONE` / `PRIMARY` / `REPLICA` /
`PRIMARY_CATCHUP` (ASCII uppercase). Classification:

- `PRIMARY_CATCHUP` → `TransientReject`.
- Any other non-empty role value (including `STANDALONE`, `PRIMARY`,
  `REPLICA`, and unrecognised tokens) → `TopologyReject`. The
  rationale is conservative: an unknown role is treated as
  structurally unwritable until the operator confirms the cluster is
  healthy; demoting to last priority lets the client still discover
  it if every other endpoint is also unreachable. The client uses
  case-insensitive matching for `PRIMARY_CATCHUP` and `REPLICA`
  predicates.
- `421` **without** an `X-QuestDB-Role` header (or with an empty
  value, after trimming) degrades to a generic transport error
  (per-endpoint, walk continues).

The `X-QuestDB-Zone` HTTP response header is the upgrade-time companion
to `SERVER_INFO.zone_id` (`wire-egress.md` §11.8). The value is an
opaque, case-insensitive identifier (`eu-west-1a`, `dc-amsterdam`,
etc.) compared as bytes against the client's configured `zone=`. Servers
that have a zone configured SHOULD emit it on every `421` reject so the
zone tier is observable without a successful upgrade. Absence (or empty
value, after trimming) leaves the host's zone tier as `Unknown`. The
header has no effect on ingress (zone-blind, pinned to v1) and no effect
on any client whose `zone=` is unset (every host collapses to `Same`).

## 6. Error classification

Errors are classified into three buckets that drive the loop:

### Terminal (bypass failover)

These categories MUST NOT trigger reconnect-and-retry. The loop
records the host outcome (where applicable) and surfaces the error
to the caller.

| Category | Trigger | Why terminal |
|---|---|---|
| `AuthError` | Upgrade HTTP `401` or `403` | Credentials are cluster-wide; retrying every host floods server logs without recovery. |
| `ProtocolVersionError` | Server-emitted frame carries QWP version outside `[1, ClientMaxVersion]` | Version negotiation is cluster-wide; failover masks the disagreement. The egress client surfaces this via `KIND_PROTOCOL_ERROR` and latches it on the connection's terminal-failure listener so subsequent `Execute()` calls fail fast through `handler.onError` (no failover). |
| Server status reject (SF) | Receive pump decoded a non-OK status frame | Application-layer reject won't change on replay. |

### Topology (handled in-round; do not advance the doubling counter)

| Category | Trigger | Treatment |
|---|---|---|
| Role reject | Upgrade `421` + non-empty `X-QuestDB-Role` | `RecordRoleReject(idx, transient = role == PRIMARY_CATCHUP)`, then walk to the next host within the round. SF additionally pays a fixed `InitialBackoff` sleep at round exhaustion (no exponential doubling) and the wall-clock outage budget still applies; see §3.2. Non-SF / egress fail the round if every host role-rejects. |
| Target mismatch | `SERVER_INFO.Role` does not match `target=` filter | Same as role reject. |

### Transient (enter backoff)

Everything not terminal and not topology:

- TCP/TLS errors, generic upgrade failures, `auth_timeout_ms`
  expiry, mid-stream send/receive failures.
- `404 Not Found` on upgrade (per-endpoint path mismatch — a single
  mid-deploy node can return 404 while peers are healthy).
- `503 Service Unavailable` on upgrade (server is reachable but
  currently unable to serve — distinct from `421`).
- `421` without an `X-QuestDB-Role` header (or with an empty value).
- `426 Upgrade Required` and any other 4xx/5xx that is not `401`,
  `403`, or `421`.
- Generic frame-decode errors (truncated payload, unknown
  `msg_kind`, varint overflow). Clients SHOULD log these at
  WARNING; if the same decode error repeats across every host the
  failover-exhausted error surfaces the underlying bug.

Transient errors enter the appropriate backoff / round logic per the
context's loop spec ([`wire-ingress.md`](wire-ingress.md) §15.5,
[`sf-client.md`](sf-client.md) §13.6, or
[`wire-egress.md`](wire-egress.md) §11.9).

## 7. Defaults at a glance

| Knob | Ingress non-SF | Ingress SF | Egress |
|---|---|---|---|
| Per-host connect timeout | `auth_timeout_ms` (15s) | `auth_timeout_ms` (15s) | `auth_timeout_ms` (15s) |
| Inter-host backoff within a round | none (single-pass walk) | none (skip-backoff-within-round; sleep paid at round boundary) | none (`WalkTracker` walks the full round in one factory call) |
| Backoff initial | n/a | `100ms` (`reconnect_initial_backoff_millis`) | `50ms` (`failover_backoff_initial_ms`) |
| Backoff max | n/a | `5_000ms` | `1_000ms` |
| Total budget | n/a | `300_000ms` (`reconnect_max_duration_millis`) | `30_000ms` (`failover_max_duration_ms`) |
| Attempt cap | hosts × 1 | unbounded (budget-only) | `8` (`failover_max_attempts`) |
| Jitter | n/a | equal `[base, 2·base)` | full `[0, base]` |
| Initial-connect retry | no | gated by `initial_connect_retry=off\|on\|async` | one-shot `WalkTracker` round (no retry beyond round) |
| Auth-error policy | terminal | terminal | terminal |
| Role-reject (`421+role`) | skip host within round; fail round if all rejected | retry with backoff reset to `InitialBackoff`; bounded by `reconnect_max_duration_millis` | skip host within round; fail Execute if all rejected |
| Zone-locality (`zone=`) | n/a (zone-blind: every host treated as `Same`) | n/a (zone-blind: every host treated as `Same`) | same-zone preferred via `(state, zone)` priority lattice when `zone=` set; `target=primary` collapses to zone-blind |

The split (5min budget for SF vs 30s for egress) reflects intent:
SF is unattended background ingest with on-disk durability — long
outages are recoverable. Egress is interactive — failing fast keeps
the user in the loop.

Knob homes: SF connect-string keys live in [`sf-client.md`](sf-client.md)
§4.2; egress connect-string keys live in [`wire-egress.md`](wire-egress.md)
§11.9. The values above are derived; this table is a summary, not
the canonical defaults.

## Document history

| Date | Change |
|---|---|
| 2026-05-06 | Initial spec. |
| 2026-05-08 | §4.2: capture skip-backoff-within-round (the SF reconnect loop walks the full address list with no inter-host sleep, then pays one backoff at round exhaustion). Pseudocode rewritten to make `PickNext == -1` the explicit sleep point; new key-property bullet; mid-stream / role-reject bullets reconciled. §7: new "Inter-host backoff within a round" row covering all three contexts. |
| 2026-05-08 | Server-advertised zone preference. §1.1 adds `zone=` connect-string knob (egress-only effective; ingress logs WARN). §2 extends host classification with a `zone_tier` dimension (`Same`/`Unknown`/`Other`); selection priority becomes `(state, zone)` lexicographic; sticky-Healthy preserves only same-zone entries. §5 adds `X-QuestDB-Zone` header on `421` rejects and references the `SERVER_INFO.zone_id` field gated by `CAP_ZONE` (`wire-egress.md` §11.8). §7 adds a Zone-locality row. `lb_strategy` removed from §1.3 — intra-tier order is now implementation-defined; clients SHOULD shuffle the address list at construction. |
| 2026-05-08 | Consistency follow-up to the zone feature. §2.1 adds the `RecordZone(idx, zoneId)` operation explicitly (zone tier was previously assigned implicitly); zone tier survives `BeginRound` so observations don't have to be re-issued each round. §4.4 WalkTracker pseudocode now invokes `RecordZone` from both the post-`SERVER_INFO` path (gated by `CAP_ZONE`) and the `421` upgrade-reject path (using the optional `X-QuestDB-Zone` header). |
| 2026-05-08 | Clarifications. §4.3 new key-property bullet: `failover_max_duration_ms` bounds failover eligibility, not Execute wall-clock — `WalkTracker` is bounded only by `hostCount × auth_timeout_ms`, so total Execute time can be `failover_max_duration_ms + (last walk)`. §4.4 trailing paragraph: under `target=primary`, `RecordZone` is still called but zone tier collapses to `Same` for selection. |
| 2026-05-08 | Structural split. The per-context loops (formerly §4.1 / §4.2 / §4.3 / §4.4) and their connect-string knobs (formerly §1.2 / §1.3) move out of this doc into the corresponding wire docs: non-SF ingress to `wire-ingress.md` §15.5, SF reconnect to `sf-client.md` §13.6, egress per-Execute and `WalkTracker` to `wire-egress.md` §11.9. `failover.md` now hosts only the shared primitives — common keys (§1.1), host-health model (§2), backoff function (§3), role filter (§5), error classification (§6), defaults cheat sheet (§7). §3 / §3.2 / §6 cross-references rewritten to point at the new loop homes. |
