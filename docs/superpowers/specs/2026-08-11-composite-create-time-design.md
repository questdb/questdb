# Composite Partitioning — Create-Time Surface (Sub-project 5) — Design

**Status:** drafted 2026-08-11, awaiting review. Sub-project 5 of 8. The smallest of the eight.

## 1. Scope

Two create-time restrictions:

- `composite partitioning is not yet supported with CREATE TABLE AS SELECT`
- `composite partitioning requires a WAL table`

They look alike but are opposites: one is a gap to close, the other is an architectural boundary to
document and keep.

## 2. CREATE TABLE AS SELECT — a gap to close

**Decision (D1): CTAS with a composite partition spec is supported.**

Nothing about CTAS conflicts with composite routing. CTAS creates the table and then inserts the
select's rows through the ordinary write path, which is already cell-aware and is the most
thoroughly tested part of the feature. The gate exists because CTAS was not part of Plan 1's grammar
work, not because the combination is unsound.

Two details make it more than pure plumbing:

- **Dimension source columns must exist in the select's projection.** `CREATE TABLE t AS
  (SELECT ts, price FROM s) TIMESTAMP(ts) PARTITION BY DAY, exchange` must fail at compile time with
  a clear message, not at first insert. The spec is resolved against the select's output metadata,
  exactly as `TIMESTAMP(ts)` already is.
- **The dimension column's type must be SYMBOL** (or, for an expression dimension, string-coercible),
  applying the same validation `CREATE TABLE` performs today. Resolution runs against the projection,
  so an aliased or computed column is resolvable by its output name.

Bulk CTAS insert benefits from the composite fast-append path automatically when its rows arrive
ordered per cell; no special casing.

## 3. Non-WAL — an architectural boundary to keep

**Decision (D2): composite partitioning remains WAL-only. The restriction is permanent, and is
documented as a design boundary rather than tracked as a gap.**

This is not a missing feature. Cell routing lives in `TableWriter.processO3Block` →
`processO3BlockComposite`, which is reached through WAL apply. The legacy non-WAL in-order append
path was declared out of scope in the original design (`2026-07-15-…-design.md` §3) and never
acquired routing. Supporting it would mean building a second, parallel routing implementation on a
path QuestDB is moving away from.

The work here is therefore to make the boundary *good*, not to remove it:

- The `CREATE TABLE` error is already clear and stays.
- `ALTER TABLE … BYPASS WAL` (or any path that could convert a composite table to non-WAL) must be
  refused with the same message, so a composite table cannot be moved onto a path that does not
  route.
- The restriction is stated in user-facing documentation as a property of the feature, not a
  limitation awaiting a fix.

## 4. Semantics summary

| Statement | Behaviour |
|---|---|
| `CREATE TABLE … AS (SELECT …) … PARTITION BY DAY, exch WAL` | supported (D1) |
| CTAS whose projection lacks a dimension source column | refused at compile time, naming the column |
| CTAS whose dimension column is not SYMBOL / not string-coercible | refused at compile time |
| `CREATE TABLE … PARTITION BY DAY, exch` without `WAL` | refused, permanent (D2) |
| `ALTER TABLE <composite> BYPASS WAL` | refused, permanent (D2) |

## 5. Implementation surfaces

| File | Change |
|---|---|
| `griffin/SqlCompilerImpl.java` CTAS path | resolve and validate the partition spec against the select's output metadata; drop the CTAS gate |
| `griffin/engine/ops/CreateTableOperationImpl.java` | carry the spec through the CTAS construction path (the non-CTAS path already does) |
| `griffin/SqlCompilerImpl.java` BYPASS WAL path | refuse for a composite table (D2) |
| documentation | state the WAL-only property |

Note for Enterprise: `EntCreateTableOperationImpl` must delegate `getPartitionSpec()` on the CTAS
path too. It failed to delegate it at all until the 2026-08-10 merge audit, which silently produced
plain tables from composite DDL; the CTAS path must not reintroduce that shape. Sub-project 6 owns
the ent-side verification.

## 6. Testing

- **CTAS end-to-end** for every dimension kind, differential against a plain twin built by the same
  select — via the sub-project 8 harness, where `FuzzDropCreateTableOperation` already exercises
  create/drop cycles.
- **CTAS validation failures:** missing dimension source column in the projection; non-SYMBOL
  dimension column; each asserted to fail at compile time with a message naming the column, not at
  first insert.
- **CTAS with an aliased/computed dimension column**, proving resolution runs against the projection.
- **Non-WAL rejection** at CREATE and via BYPASS WAL, on a routed and a never-routed composite table.
- **Enterprise CTAS** produces a genuinely composite table — the direct regression test for the
  `getPartitionSpec()` delegation bug.
- **Plain byte-identity** for CTAS of a plain table.

## 7. Out of scope

- Non-WAL composite support (D2) — permanently excluded by design.
- `CREATE TABLE LIKE` with composite specs, if it does not already round-trip: it inherits the spec
  through the same metadata path and is covered by the SHOW CREATE round-trip tests; any gap found
  is folded in here rather than deferred.
