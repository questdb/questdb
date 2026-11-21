# Store-and-forward rules for QWP clients

`review-pr` applies these rules when a change touches QWP ingestion (connection
upgrade and role checks, in-place demotion or switch, connection handling on a
role change), replication failover, a `java-questdb-client` pointer that brings
client or ingestion changes, or tests that drive a producer through a failover
or switch, such as the enterprise end-to-end failover suites.

They are promises the client makes to users, and server changes and tests in
this repository must keep them. A change that breaks one is a Critical finding
and blocks the PR by rule: store-and-forward exists so that a running producer
never loses data and never fails because of a temporary outage.

The client documents the details, which change faster than this file: the
design note `java-questdb-client/design/qwp-nack-policy-v2.md`, the class
Javadoc of `BackgroundDrainer`, and the "INVARIANT B" comments in
`CursorWebSocketSendLoop` and `BackgroundDrainer`. Check the details there.
When they disagree with this file, the client's documentation wins; say so in
the report.

## A running sender

Once a sender is running, a background drainer thread ships the rows it has
stored to the server.

- **Producers never see transport or server errors.** The drainer doesn't pass
  them to `Sender` calls, `flush()` or a pooled handle. The only error a running
  sender may show a producer is that its store-and-forward space is full. A
  refused connection, a DNS failure, an unreachable host, a TLS or certificate
  error, an authentication failure, a role rejection, an upgrade or protocol
  timeout, a reset: none of these may reach a producer call.
- **Reconnecting has no time limit.** The drainer reconnects on its own thread,
  with no budget, deadline or "give up after N milliseconds" that stops the
  sender. `reconnect_max_duration_millis` must not bound this loop; it applies
  only to a blocking first connect and to the orphan drainer's settle budget
  (below). The delay between attempts grows exponentially and may be capped;
  the number of attempts and their total time may not. Flag a loop that stops
  on a deadline or after a number of attempts, or a terminal `SenderError`
  that the running drainer's reconnect path can reach.
- **Rejected batches are retried, never dropped.** A rejection (NACK) that a
  temporary cluster state can cause, such as `WRITE_ERROR`, `INTERNAL_ERROR`, a
  node that can't take writes, or `UNKNOWN` (any status a newer server may
  send), is retried: the client reconnects and replays everything after the
  last acknowledged frame. It never drops the batch and never stops the sender
  on first sight. Only a rejection that the same bytes would get again may stop
  it: `SCHEMA_MISMATCH`, `PARSE_ERROR`, and `SECURITY_ERROR` from a node that
  accepts writes. A client that moves its acknowledged position past a rejected
  frame loses data silently.
- **A frame rejected over and over escalates through the poison-frame
  detector,** which counts consecutive rejections of the same frame up to a
  limit, never through a list of WebSocket close codes: close codes carry no
  policy.
- **The orphan drainer may give up only by design.** `BackgroundDrainer` ships
  the rows a closed sender left behind. It may quarantine its slot (a `.failed`
  file that an operator clears) on conditions its Javadoc lists as terminal,
  such as an authentication failure that outlasts the ride-out for rotating
  credentials, or a cluster-wide lack of durable acknowledgement that outlasts
  its settle budget. Temporary conditions, such as a role rejection or a
  transport error, must never count toward such a budget or start its clock:
  a temporary state that uses up the budget, through a shared attempt counter
  or a deadline started on entry, breaks this rule.

## Startup of a `QuestDB` pool

- **`lazy_connect=true`:** `build()` succeeds with no server running. A
  borrowed sender can write at once, into store-and-forward, and the read side
  connects once the server is up: reads are deferred, not disabled.
- **`lazy_connect=false`, the default:** `build()` and the first connect report
  connection problems to the caller. DNS errors, refused or unreachable
  connections, TLS and certificate errors, authentication and authorization
  failures, and connect and upgrade timeouts all throw at startup; none is
  swallowed.
- **In both modes, connection errors are the caller's problem only during
  startup.** After it, the running sender follows the rules above.

## The server and tests in this repository

- **The server sends producers no role errors.** An in-place demotion closes
  QWP ingestion connections instead of rejecting each write with
  `SECURITY_ERROR`. A server change that brings back per-write role errors on
  QWP ingestion breaks the rules above.
- **A test can't use a producer's role error as evidence that a replica rejects
  writes:** under these rules, the producer sees nothing by design. Show it
  with PostgreSQL wire probes, commit counts that stay frozen on the settled
  replica, and the stored rows draining after promotion (wait for durable
  acknowledgement, then check every row).
- **Row checks must allow for replay.** Replay can deliver a row more than
  once, so a check that counts the rows sent through store-and-forward must
  wait for durable acknowledgement of the seed rows before the disturbance, or
  use a DEDUP table. Otherwise it reports replayed rows as corruption.
