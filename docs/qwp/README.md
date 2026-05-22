# QuestWire Protocol (QWP) — Documentation Index

QuestWire Protocol is QuestDB's columnar binary protocol for high-throughput
data ingestion (`/write/v4`) and query result streaming (`/read/v1`) over
WebSocket and UDP. This directory holds the cross-language specifications that
every conformant client and server must implement to.

## Layout

```
docs/qwp/
├── README.md                          this index
├── wire-ingress.md                    QWP1 ingest wire format (WebSocket + UDP shared core); §15.5 points at the cursor-engine reconnect loop that applies to both Memory and SF modes
├── wire-udp.md                        UDP-specific deviations from wire-ingress
├── wire-egress.md                     egress (query result streaming) wire format; §11.9 owns the per-Execute failover loop and WalkTracker
├── sf-client.md                       client-side cursor-engine substrate spec (Memory mode and Store-and-Forward); §13.6 owns the ingress reconnect loop
├── failover.md                        shared failover primitives (host-health model, backoff, role filter, error classification, cross-context defaults table)
└── design/                            non-normative working notes (decision logs, backlogs)
    └── egress-phase2-backlog.md
```

## Audience matrix

| You are writing… | Read |
|------------------|------|
| A new ingest client (any language), no on-disk durability | `wire-ingress.md` (incl. §15.5 pointing at the reconnect loop) + `sf-client.md` §13.6 (the loop, shared with SF mode) + `failover.md` §1.1 / §2 / §5 / §6 (primitives) |
| A new ingest client with durability across restarts | `wire-ingress.md` + `sf-client.md` (incl. §13.6 reconnect loop and §5–6 / §18 for the on-disk slot layout) + `failover.md` (primitives) |
| A UDP-only ingest client (e.g. metrics collector) | `wire-ingress.md` + `wire-udp.md` |
| A new query client (SELECT, DDL, EXEC) | `wire-egress.md` (incl. §11.9 failover loop + WalkTracker) + `failover.md` (primitives); `wire-egress.md` references `wire-ingress.md` for the shared header / type system |
| A server-side change to ingest framing | `wire-ingress.md` (+ `wire-udp.md` if UDP, + `sf-client.md` if it touches ACKs, durable-ack, or close-codes) |
| A server-side change to egress framing | `wire-egress.md` |
| A server-side change to topology / role headers | `failover.md` §5–6 + `wire-egress.md` §11.8 / §11.9 |
| Tuning the cross-context defaults (knob names, budgets) | `failover.md` §7 (cheat sheet) → drill into `sf-client.md` §4.2 or `wire-egress.md` §11.9.1 for the canonical knob homes |
| A bug fix that doesn't change interop | none of the above; just the code |

## Spec layers

**Layer 1 — Specs (normative).** Everything in this directory's top level. Any
implementation that calls itself a QWP client or server must match these byte
layouts and semantics. Drift between code and spec is a bug; the fix is to
update whichever is wrong, not silently diverge.

**Layer 2 — Design (non-normative).** Files under `design/`. Decision logs,
phase backlogs, evaluation notes. Useful as historical context, not as a
contract.

**Layer 3 — Implementation notes.** Live next to the code (Javadoc, rustdoc,
inline comments). Out of scope for this directory.

The Java reference client also keeps in-flight design notes at
`questdb/java-questdb-client/design/qwp-cursor-*.md`. Those are decision logs
authored alongside the client implementation; for cross-language interop,
`sf-client.md` here is authoritative.

## Drift control

Each spec has a **Reference Implementation** footer with a commit hash. When
that hash drifts far from `main`, the spec is presumptively stale: re-extract
from the implementation and bump the hash. PRs that change interop semantics
(wire bytes, status codes, handshake headers, on-disk layout, ACK rules,
close-code routing) MUST update the relevant spec in the same PR.

A short checklist for spec-affecting PRs:

- [ ] Did the wire bytes change? Update `wire-ingress.md` / `wire-egress.md`.
- [ ] Did the on-disk segment format, slot layout, or recovery contract change? Update `sf-client.md` §5–6, §18.
- [ ] Did handshake headers, ACK rules, durable-ack, keepalive, close-code routing, error categories, or connect-string keys change? Update `sf-client.md` §4, §8–17.
- [ ] Did the ingress reconnect loop (Memory mode and SF), mid-stream demote, or `initial_connect_retry` semantics change? Update `sf-client.md` §13.6 and the §15.5 pointer in `wire-ingress.md` if the storage-mode framing shifts.
- [ ] Did the egress per-`Execute` loop, `WalkTracker`, or `OnFailoverReset` contract change? Update `wire-egress.md` §11.9.
- [ ] Did the shared host-health model, backoff math, role filter, or error classification change? Update `failover.md` §2 / §3 / §5 / §6 — and check that the three loop docs above still match.
- [ ] Did the relevant Reference Implementation commit hash advance? Bump the footer.

## See also

- Wire protocol: [`wire-ingress.md`](wire-ingress.md) (incl. §15.5 pointer to the cursor-engine reconnect loop)
- UDP variant: [`wire-udp.md`](wire-udp.md)
- Egress (query results): [`wire-egress.md`](wire-egress.md) (incl. §11.9 per-Execute failover loop and WalkTracker)
- Cursor-engine client (Memory + SF storage, FSN, ACKs, errors, connect-string, §13.6 reconnect loop): [`sf-client.md`](sf-client.md)
- Failover primitives (host tracker, backoff math, role filter, error classes, cross-context defaults): [`failover.md`](failover.md)
- Egress Phase 2 backlog: [`design/egress-phase2-backlog.md`](design/egress-phase2-backlog.md)
