# QWP Client Failover Spec

Normative behaviour for multi-host failover across QWP clients. Three
contexts share one host-health model and one backoff function; they
differ in (a) when they enter the loop, (b) how they bound it, and
(c) how they react to terminal errors.

The three contexts:

1. **Ingress non-SF** — `Sender.New("ws::...")` initial connect for the
   non-store-and-forward producer. One-shot walk over the configured
   address list at construction time; no run-time reconnect.
2. **Ingress SF** — the cursor-engine reconnect loop driving the
   store-and-forward replay (`sf_dir=...`). Long-running, recovers
   silently across transient outages.
3. **Egress** — per-`Execute()` attempt loop on the read-side
   `QueryClient`. Bounded by attempt count + wall-clock budget;
   replays the in-flight `QUERY_REQUEST` after a successful reconnect.

## 1. Connect-string keys

### 1.1 Common (every WS client)

| Key | Type | Default | Scope |
|---|---|---|---|
| `addr` | `host:port[,host:port…]` | required | Comma-separated multi-host failover list. |
| `auth_timeout_ms` | int | `15_000` | Per-host upper bound on the WebSocket upgrade handshake (TCP + TLS + HTTP + `SERVER_INFO` read). |

`addr` accepts comma syntax (`addr=h1:p1,h2:p2`) and repeated keys
(`addr=h1:p1;addr=h2:p2`). The two forms MUST accumulate; empty
entries (`,,` or trailing/leading commas) MUST be rejected. Multi-host
failover semantics are normative for WS/WSS and HTTP/HTTPS only.

### 1.2 Ingress SF (`sf_dir=...` set)

| Key | Type | Default | Notes |
|---|---|---|---|
| `reconnect_initial_backoff_millis` | int | `100` | First post-failure sleep before the second connect attempt. |
| `reconnect_max_backoff_millis` | int | `5_000` | Cap on per-attempt sleep after exponential growth. |
| `reconnect_max_duration_millis` | int | `300_000` | Total wall-clock budget across one outage; exceeded → engine becomes terminal. |
| `initial_connect_retry` | `off \| on \| async` | `off` | First-connect retry policy. Semantics defined in §4.2. |

The non-SF ingress sender does **not** expose backoff knobs because
it never reconnects.

SF subsystem tuning (`sf_append_deadline_millis`, `drain_orphans`,
`max_background_drainers`, etc.) is orthogonal to failover and lives
in [`sf-client.md`](sf-client.md).

### 1.3 Egress (`QueryClient.New("ws::...")`)

| Key | Type | Default | Notes |
|---|---|---|---|
| `target` | `any \| primary \| replica` | `any` | Server-role filter applied per-endpoint after the upgrade reads `SERVER_INFO`. |
| `lb_strategy` | `random \| first` | `random` | Initial address-list ordering at client construction. |
| `failover` | `on \| off` | `on` | Master switch for the per-Execute reconnect loop. `off` surfaces transport errors directly. |
| `failover_max_attempts` | int | `8` | Cap on reconnects per `Execute()` (initial attempt + `N-1` failovers). |
| `failover_backoff_initial_ms` | int | `50` | First post-failure sleep. |
| `failover_backoff_max_ms` | int | `1_000` | Cap on per-attempt sleep. |
| `failover_max_duration_ms` | int | `30_000` | Total wall-clock budget per `Execute()`; `0` = unbounded. |

## 2. Host health model

Each configured `addr` entry has a single state from this lattice:

| State | Meaning | Selection priority |
|---|---|---|
| `Healthy` | Last connect to this host succeeded. | 1 (best) |
| `Unknown` | Never tried this round, or just reset. | 2 |
| `TransientReject` | Server returned `421 Misdirected Request` + `X-QuestDB-Role: PRIMARY_CATCHUP`. Likely to recover. | 3 |
| `TransportError` | TCP/TLS/handshake error or mid-stream send/recv failure. | 4 |
| `TopologyReject` | Server returned `421 Misdirected Request` + `X-QuestDB-Role: REPLICA`. Will not become writable without topology change. | 5 (worst) |

A round-tracker bit per host records whether **this round** has tried
the host yet. A "round" is the time between two `BeginRound` calls.

### 2.1 Operations

```
PickNext()                       → highest-priority host with attempted-this-round=false; -1 if none
RecordSuccess(idx)               → state=Healthy,           attempted=true
RecordRoleReject(idx, transient) → state=TransientReject (transient=true) or TopologyReject (false), attempted=true
RecordTransportError(idx)        → state=TransportError,    attempted=true
RecordMidStreamFailure(idx)      → if state==Healthy, demote to TransportError; do NOT touch attempted
BeginRound(forgetClassifications)→ clear attempted flags. forgetClassifications=true also resets every
                                   non-Healthy state to Unknown but preserves the last-known Healthy entry
                                   (sticky-Healthy: the previously-good host stays first in line on the next round)
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
to preserve, the **last** `Healthy` index wins. The most recent
successful connection stays first in line on the next round; older
`Healthy` entries are reset to `Unknown`.

### 2.3 Tracker ordering invariants

These invariants are normative; violation produces user-visible
behaviour drift across clients.

- **Mid-stream demote before round reset.** When a connection fails
  after a successful upgrade, the loop MUST call
  `RecordMidStreamFailure(previousIdx)` **before** the next
  `BeginRound(forgetClassifications=true)`. Reversing the order makes
  sticky-Healthy preserve the just-failed host as priority pick,
  which then receives the first reconnect attempt and fails again.
- **Round reset on exhaustion.** When `PickNext()` returns `-1`, the
  loop MUST call `BeginRound(forgetClassifications=true)` before the
  next `PickNext()`. Calling `PickNext()` twice without an
  intervening reset is undefined.
- **Thread-safety.** All tracker operations MUST be observable as a
  total order across threads. Implementations MAY use a single
  internal lock; the public methods are not required to be
  reentrant.

## 3. Backoff function

Shared by ingress SF and egress (the non-SF initial connect doesn't
back off — see §4.1). The function is pure; the loop owner provides
elapsed wall-clock time.

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
sleep cannot overshoot `MaxOutageDuration`.

### 3.1 Jitter

Two jitter shapes are normative, picked by context:

- **SF (ingress) — equal-jitter** `[base, 2·base)`. Multiple producers
  may share a cluster; equal-jitter has a non-zero lower bound that
  damps reconnect storms. The post-jitter sleep is **not** clamped to
  `MaxBackoff` — once `base` saturates the cap, the actual sleep
  lands in `[max, 2·max)`.
- **Egress — full-jitter** `[0, base]`. A query client is single-user
  and benefits from the lowest expected recovery time; thundering
  herd is not a concern at one client per workload.

Reference formulas:

```
EqualJitter(base):  return base + uniform_long(base)        # [base, 2·base)
FullJitter(base):   return uniform_long(base + 1)           # [0, base]
```

### 3.2 Outage state

The pair `(attempt, elapsedSinceOutage)` is the **outage state**, owned
by the loop:

- `attempt` increments on every consumed backoff and resets to `0`
  on a successful reconnect.
- `elapsedSinceOutage` starts at `0` when the first failure is
  observed and resets to `0` on a successful reconnect. A reconnect
  that immediately fails again starts a fresh outage clock.

In SF (§4.2), role-rejects MUST NOT advance either field: a topology
signal is not an outage. Egress (§4.3) handles role-rejects only
during `WalkTracker` (initial connect / reconnect) and so doesn't
share the outage clock with the per-Execute loop.

## 4. Loop semantics by context

### 4.1 Ingress non-SF — initial connect

```
hosts = configured addr list
tracker = HostTracker(hosts)
for attempt in 0 .. hosts.length - 1:
    idx = tracker.PickNext()                # priority order
    if idx < 0: break                       # round exhausted
    try connect host[idx] with auth_timeout_ms budget
        on success:        RecordSuccess(idx); return transport
        on AuthError:      rethrow (terminal, do NOT continue)
        on RoleReject(t):  RecordRoleReject(idx, t); continue
        on other errors:   RecordTransportError(idx); continue
throw "ingress failed against all <count> endpoints" with last error attached
```

Key properties:

- **No backoff between hosts.** The walk is bounded by
  `auth_timeout_ms × hostCount` worst case.
- **One round only.** If every host is in a non-Healthy state, the
  client throws — no retry loop.
- **AuthError is structurally terminal** at any host: `401/403/404`
  on the upgrade aborts immediately. Re-running an unsupported
  credential against every host wastes time and floods server logs.
- **`ProtocolVersionError` is also terminal** in the same way: the
  server negotiated an unsupported QWP version, and frame-decode
  corruption is a permanent disagreement.
- **Role rejects are recoverable** within this single round: the
  client moves on to the next configured host. Once all hosts are
  exhausted, the role-mismatch detail is surfaced.

### 4.2 Ingress SF — reconnect loop

```
backoff = BackoffState()
seenFirstConnect = false
loop forever:
    if previousIdx >= 0:                # mid-stream failure path
        tracker.RecordMidStreamFailure(previousIdx)
        previousIdx = -1
    tracker.PickNext() and walk through hosts (one factory call yields one tracker pick)
    try connect:
        on AuthError or ProtocolVersionError: SET TERMINAL; exit loop
        on RoleReject:
            backoff.Reset()
            sleep InitialBackoff (do NOT count against MaxOutageDuration)
            continue
        on other error:
            if !seenFirstConnect && !initial_connect_retry:
                SET TERMINAL; exit loop
            sleep = NextBackoffOrGiveUp(backoff.attempt, backoff.elapsedSinceOutage)
            if sleep == GIVE_UP: SET TERMINAL; exit loop
            sleep; backoff.attempt++
            continue
    seenFirstConnect = true
    backoff.Reset()
    rewind cursor to (ackedFsn + 1)         # replay first un-acked frame
    run send/recv pumps until either pump throws
        on server status reject / AuthError / ProtocolVersionError: SET TERMINAL
        on transient error (TCP/TLS, mid-stream send/recv, etc.):
            sleep = NextBackoffOrGiveUp(...)
            if GIVE_UP: SET TERMINAL
            else: continue (next iter resets attempt after the next successful connect)
```

Key properties:

- **Single-round walk per attempt.** When the tracker exhausts
  (`PickNext == -1`), the next factory call **MUST** invoke
  `BeginRound(forgetClassifications=true)` and pick again. The
  current-round flags reset; classifications fade except for the
  sticky-Healthy entry.
- **`initial_connect_retry`** (default `off`):
  - `off` — first connect failure is terminal (matches non-SF §4.1).
  - `on` (alias `sync`) — block the constructor; enter the standard
    reconnect loop until success or `MaxOutageDuration` exhaustion.
  - `async` — return from the constructor immediately; the
    background I/O thread drives the reconnect loop. `append` writes
    accumulate in the SF dir without blocking on connect. Intended
    for unattended producers where the SF dir may already carry
    segments queued from a prior process and the server may come up
    later.
- **Role-rejects do not consume the give-up budget.** A `421 + role`
  reply is interpreted as a topology hint, not an outage; the
  client sleeps `InitialBackoff` and retries. This avoids
  permanently terminating an unattended SF producer when only a
  PRIMARY_CATCHUP node responded.
- **Mid-stream failure** (the send or receive pump throws after a
  successful connect): the failed host index is captured as
  `previousIdx`. The next loop iteration calls
  `tracker.RecordMidStreamFailure(previousIdx)` **before** the next
  `PickNext`, so Healthy→TransportError demotion happens before host
  selection runs. Backoff state is **not** reset before the
  next backoff sleep, so the second consecutive failure waits
  longer.

### 4.3 Egress — per-Execute loop

```
on QueryClient.New(): connect once via WalkTracker (see §4.4); fail loud if no endpoint matches
on Execute(sql, handler):
    if execute already in flight on this client: throw "one query at a time"
    tracker.BeginRound(forgetClassifications=true)
    attempt = 0
    backoffMs = failover_backoff_initial_ms
    deadline = now + failover_max_duration_ms (∞ if 0)
    loop:
        send QUERY_REQUEST(rid, sql, binds)
        drive receive loop until terminator
        on success: return
        on transport-error AND failover=on AND attempt+1 < failover_max_attempts AND now < deadline:
            tracker.RecordMidStreamFailure(activeIdx)
            sleep clamp(FullJitter(backoffMs), deadline - now)
            backoffMs = min(2 × backoffMs, failover_backoff_max_ms)
            attempt++
            ReconnectAsync(attempt)         # see §4.4; uses BeginRound(forget=false)
            handler.OnFailoverReset(serverInfo)
            continue
        else: rethrow (failover not eligible or budget exhausted)
```

Key properties:

- **Per-Execute round reset.** Every `Execute()` enters with
  `BeginRound(forgetClassifications=true)` so stale topology rejects
  from a prior query don't permanently exclude an endpoint.
- **Reconnect inside the loop uses `forgetClassifications=false`**: the
  topology classifications observed during this `Execute()`'s
  reconnects accumulate. Once the round exhausts within a single
  `Execute()`, fail with a role-mismatch error (carries the most
  recent observed `SERVER_INFO`).
- **`OnFailoverReset` callback** fires after a successful reconnect
  but **before** any replayed batches arrive. Handlers reset their
  per-batch_seq state here (the new node restarts `batch_seq` at 0).
  Clients that omit the callback MUST instead surface the failure
  and have the user re-issue `Execute` from a clean accumulator.
- **AuthError is terminal** at any host: same reasoning as ingress.
- **`ProtocolVersionError` is terminal** and surfaces directly to the
  caller; failover masks the cluster-wide disagreement.
- **Cooperative cancel** (`Cancel()` API, out of scope for this spec)
  sends a `CANCEL` frame; the server replies with a `QUERY_ERROR`
  carrying `STATUS_CANCELLED`. That reply routes through the normal
  error path, not the transport-error path, so it never triggers
  failover.

### 4.4 Egress connect / reconnect helper (`WalkTracker`)

Shared between initial connect and `ReconnectAsync`:

```
WalkTracker():
    while idx = tracker.PickNext() >= 0:
        candidate = build transport for hosts[idx]
        try connect with auth_timeout_ms budget:
            ServerInfo = (negotiatedVersion >= 2) ? read SERVER_INFO frame : null
        on success:
            if EndpointMatchesTarget(ServerInfo):
                RecordSuccess(idx); install transport; return (info, null, false)
            else:
                RecordRoleReject(idx, transient = (ServerInfo.Role == PRIMARY_CATCHUP))
                lastInfo = ServerInfo; anyRoleMismatch = true
                continue
        on AuthError: rethrow (do NOT continue past this host)
        on 421 + X-QuestDB-Role:
            RecordRoleReject(idx, transient = (role == PRIMARY_CATCHUP))
            anyRoleMismatch = true; continue
        on other error:
            RecordTransportError(idx); continue
    return (lastInfo, lastError, anyRoleMismatch)
```

When `WalkTracker` returns no transport: if any role mismatch was
observed, raise a role-mismatch error with the last `SERVER_INFO`
attached so callers can distinguish "no endpoint matched target=" from
"all endpoints unreachable". Otherwise raise a transport error
summarising attempts × endpoints. The reconnect path uses the same
helper and wraps the outcome with "failover exhausted after N attempts
across M endpoints".

## 5. Role filter (`target=`)

Per-endpoint check sequence:

1. Open TCP/TLS connection within `auth_timeout_ms`.
2. Issue HTTP upgrade; observe response.
3. If response is `421 + X-QuestDB-Role: <role>` → `RecordRoleReject`,
   walk to next host (no `SERVER_INFO` frame is read).
4. Otherwise upgrade succeeds; if negotiated version ≥ 2, read the
   `SERVER_INFO` frame and apply the table below.
5. v1 negotiation skips the `SERVER_INFO` read entirely.

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
MUST be one of `STANDALONE` / `PRIMARY` / `REPLICA` / `PRIMARY_CATCHUP`
(ASCII uppercase). `PRIMARY_CATCHUP` is classified `TransientReject`;
`REPLICA` and any other recognised name is `TopologyReject`. An
unrecognised role header degrades to a generic transport error
(`421` without role headers is identical).

## 6. Error classification

Errors are classified into three buckets that drive the loop:

### Terminal (bypass failover)

These categories MUST NOT trigger reconnect-and-retry. The loop
records the host outcome (where applicable) and surfaces the error
to the caller.

| Category | Trigger | Why terminal |
|---|---|---|
| `AuthError` | Upgrade HTTP `401`, `403`, or `404` | Credentials are cluster-wide; retrying every host floods server logs without recovery. |
| `ProtocolVersionError` | Server-emitted frame carries QWP version outside `[1, ClientMaxVersion]` | Version negotiation is cluster-wide; failover masks the disagreement. |
| Server status reject (SF) | Receive pump decoded a non-OK status frame | Application-layer reject won't change on replay. |

### Topology (handled in-round; do not consume outage budget)

| Category | Trigger | Treatment |
|---|---|---|
| Role reject | Upgrade `421` + `X-QuestDB-Role: <REPLICA \| PRIMARY_CATCHUP>` | `RecordRoleReject(idx, transient=…)`; SF sleeps `InitialBackoff` and retries without advancing outage state; non-SF / egress walks to next host within the round. |
| Target mismatch | `SERVER_INFO.Role` does not match `target=` filter | Same as role reject. |

### Transient (enter backoff)

Everything not terminal and not topology:

- TCP/TLS errors, generic upgrade failures, `auth_timeout_ms`
  expiry, mid-stream send/receive failures.
- `503 Service Unavailable` on upgrade (server is reachable but
  currently unable to serve — distinct from `421`).
- `421` without a recognised role header.
- Generic frame-decode errors (truncated payload, unknown
  `msg_kind`, varint overflow). Clients SHOULD log these at
  WARNING; if the same decode error repeats across every host the
  failover-exhausted error surfaces the underlying bug.

Transient errors enter the appropriate backoff/round logic per §4.

## 7. Defaults at a glance

| Knob | Ingress non-SF | Ingress SF | Egress |
|---|---|---|---|
| Per-host connect timeout | `auth_timeout_ms` (15s) | `auth_timeout_ms` (15s) | `auth_timeout_ms` (15s) |
| Backoff initial | n/a | `100ms` (`reconnect_initial_backoff_millis`) | `50ms` (`failover_backoff_initial_ms`) |
| Backoff max | n/a | `5_000ms` | `1_000ms` |
| Total budget | n/a | `300_000ms` (`reconnect_max_duration_millis`) | `30_000ms` (`failover_max_duration_ms`) |
| Attempt cap | hosts × 1 | unbounded (budget-only) | `8` (`failover_max_attempts`) |
| Jitter | n/a | equal `[base, 2·base)` | full `[0, base]` |
| Initial-connect retry | no | gated by `initial_connect_retry=off\|on\|async` | one-shot `WalkTracker` round (no retry beyond round) |
| Auth-error policy | terminal | terminal | terminal |
| Role-reject (`421+role`) | skip host within round; fail round if all rejected | infinite-retry without budget consumption | skip host within round; fail Execute if all rejected |

The split (5min budget for SF vs 30s for egress) reflects intent:
SF is unattended background ingest with on-disk durability — long
outages are recoverable. Egress is interactive — failing fast keeps
the user in the loop.

## Document history

| Date | Change |
|---|---|
| 2026-05-06 | Initial spec. |
