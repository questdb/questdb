# The add-a-type tool

## 1. What this is

Adding a column type to QuestDB means declaring its facts and then making a decision at every
place the engine handles a type in its own way. This tool lists those places for one new type: it
registers the type from a facts file, writes its type driver, builds Java, Rust and C, runs the
conformance kit and the coverage tests with the type declared, and writes every place that needs a
decision into one file, `worklist.md`. It lists where, not what: how the type prints, parses,
compares and widens is still the author's to write, at the places the worklist names.

## 2. Quick start

Work on a throwaway branch: the tool edits the tree in place.

```bash
python3 utils/type-probe/type_probe.py init NN_INT --like INT --tag 41 > NN_INT.toml
# edit NN_INT.toml: every field, and the two left as CHANGE-ME (wire_kind, signature_char)
python3 utils/type-probe/type_probe.py run NN_INT.toml
# work the items of utils/target/type-probe/NN_INT/worklist.md, then run again, until it exits 0
```

The tag of a new type is the next free one: NULL's tag, which the tool moves up by one. Two new
types are registered in the order of their tags.

Working an item means one of six decisions (section 7): naming the type in a switch, implementing
a pair the type's relations admit, adding a writer arm, writing a type driver answer, declaring or
admitting a guarded-site refusal, or a manual check. A guarded-site refusal is declared by adding
the site's label to `refused_sites` in the facts file, which costs no edit; it is admitted by
adding the type's arm at the site.

Prerequisites: Python 3.11 or newer, a JDK, Maven with the offline cache of the project, cargo,
CMake with a C++ compiler, and the Java client the kit drives, built and installed as the
repository's `CLAUDE.md` describes (the tool runs Maven with `-P local-client`).

Options: `--out DIR` (default `utils/target/type-probe/<NAME>/`, ignored by git), `--skip-native`
(no cargo, no CMake), `--skip-kit` (no kit and no coverage tests), `--manual-done FILE` (a copy of
the manual list with the done entries ticked, section 6). A run with a skipped step never exits 0.

## 3. The facts file

TOML, one file per type, every field required. The facts of INT:

```toml
[type]
name = "INT"                # ColumnTypeTag constant; the type driver is <Name>TypeDriver
tag = 5                     # the tag number; for a new type, NULL's tag
sql_names = ["int"]         # the DDL keywords that declare the type; the first is canonical
storage = "fixed"           # "fixed" or "var"

[physical]
movement = "W4"             # PhysicalDescriptor.Movement: the width, VAR for a var-size type
arithmetic = "I32"          # PhysicalDescriptor.Arithmetic: the tier values compare and widen by
accessor = "INT"            # PhysicalDescriptor.Accessor: the family it is read and written through
null_policy = "SENTINEL"    # NullPolicy: how the type stores NULL
null_word = "Numbers.encodeLowHighInts(Numbers.INT_NULL, Numbers.INT_NULL)"   # the NULL bits, a Java long expression
wire_kind = "INT"           # a WireKind, or "new" for a kind of the type's own

[relations]
relation_kind = "INT"       # RelationKind: the kind the relation rules read
relation_bits = 32          # the width the relation rules read
implicit_casts = ["INT", "LONG", "FLOAT", "DOUBLE", "TIMESTAMP", "DATE", "DECIMAL"]   # tags, best match first
cast_target = "ALWAYS"      # CastTarget: ALWAYS, FROM_NULL_ONLY or NEVER

[protocols]
pg_oid = "PG_INT4"          # a PgTypeOids constant, or "0"
pg_array_oid = "0"          # a PgTypeOids constant, or "0"

[functions]
signature_char = "i"        # the function signature character, unused by every other type; "-" for none

[kit]
paths = ["storage.*", "sql.*", "ingest.*", "http.*", "pg.*", "lv.*"]   # the kit paths the type runs; "-" for none
refused_sites = []          # guarded sites the type is refused at on purpose (section 5)
```

INT runs every kit path because the kit holds a recording of each for it. A new type has no
recording, so it runs the paths that hold an invariant, and `init` writes those:

```toml
paths = ["storage.*", "sql.filter_null", "sql.filter_not_null", "sql.order_*", "sql.union_all",
         "sql.case_*", "sql.cast", "sql.fill_*", "sql.memoized", "sql.subsample_*", "sql.where_*",
         "sql.latest_by_key", "sql.copy_bind", "sql.between_timestamp", "sql.eq_null_double",
         "sql.bind_value", "ingest.*", "http.*", "pg.*", "lv.*"]
```

The SQL queries that need a literal of the type or introduce NULL (a filter by value, a join, lag,
GROUP BY) have no invariant yet; a type that names them fails with "no invariant for this query".

`init` copies every field from an existing type's driver and leaves `wire_kind` and
`signature_char` as `CHANGE-ME`, which the run refuses, so the author decides both. The run checks
the file against the tree before it writes anything and exits 2 with one line per problem,
`facts: <field>: <problem>`: a missing or unknown field, a value that names no constant, a tag that
is taken or is not the next free one, a name or a signature character already taken, a movement that
disagrees with the storage, an implicit cast that names no tag, a refused site that is no guarded
site of `sites.tsv`, a `pg_oid` that names no constant, or a new wire kind whose constant name
`WireKind` already has.

`refused_sites` takes the labels of the guarded sites (section 5, "Refused at setup"):
`memoized virtual column`, `SAMPLE BY FILL(PREV)`, `SAMPLE BY FILL(LINEAR)`,
`SAMPLE BY FILL(value)`, `COPY bind snapshot`, `ILP column kind`, `WAL columnar append`,
`QWP WAL append`, `Parquet conversion`, `between`, `= NULL`.

## 4. Predefined places, by kind

<!-- counts: start -->
The instrument table lists 555 sites where a type's behaviour could differ from its family's, by kind and by the instrument that names each when a type is added:

| kind | build | test | refused at setup | manual | not type dependent | all |
|---|---|---|---|---|---|---|
| c-switch | 3 | 0 | 0 | 0 | 0 | 3 |
| compare-arm | 0 | 0 | 1 | 0 | 0 | 1 |
| family-arm | 0 | 6 | 18 | 12 | 43 | 79 |
| family-opcode | 0 | 3 | 38 | 1 | 15 | 57 |
| kind-predicate | 0 | 3 | 0 | 0 | 3 | 6 |
| pair-switch | 5 | 0 | 0 | 0 | 0 | 5 |
| policy-switch | 18 | 0 | 0 | 0 | 0 | 18 |
| registration | 17 | 4 | 0 | 0 | 0 | 21 |
| rust-match | 12 | 0 | 1 | 18 | 13 | 44 |
| tag-switch-default | 0 | 114 | 4 | 19 | 137 | 274 |
| wire-kind-switch | 14 | 0 | 0 | 3 | 0 | 17 |
| writer-arm | 0 | 0 | 0 | 0 | 30 | 30 |
| all | 69 | 130 | 62 | 53 | 241 | 555 |
<!-- counts: end -->

- **Registration.** Six places, each marked by a comment `type-registration: <anchor>`: the tag
  list (`ColumnTypeTag`), the column type constant and its name entry (`ColumnType`, one anchor in
  two places), the type driver lookup (`TypeDrivers.find`), the Rust tag enum (`col_type.rs` of
  qdb-core), the native tag enum (`column_type.h`) and, for a type with a wire kind of its own, the
  end of `WireKind`. The tool inserts the type's lines above each anchor and moves NULL, the last
  tag, up by one. A second run on the same facts changes nothing.
- **The type driver.** `TypeDriver` asks 22 questions and answers one more by default
  (`getTypeName`). A fixed-size type driver is a facts instance: the tool writes its `TypeFacts`
  from the facts file, and the six answers that are code (defining a bind variable, the NULL
  constant, the type constant, the column function, the NULL appender and the NULL fill) are names
  javac reports until the author writes them. Every run rewrites the facts instance from the facts
  file and keeps the answers the author wrote. A var-size type driver gets its facts as methods and
  a stub that javac reports for every other answer.
- **Pair switches.** The exhaustive switches over a pair of types (ALTER COLUMN TYPE from fixed to
  fixed, from fixed to var-size and from var-size to fixed, the UNION cast, the CASE cast): the
  build lists each; the type names itself in a refusal group, or the pair is implemented.
- **Policy switches.** The exhaustive switches over `NullPolicy`: a type with an existing policy
  takes its arm; a new policy extends each once.
- **Wire-kind switches and writer arms.** The switches over `WireKind` (the PostgreSQL wire, JSON,
  CSV, QWP egress and printer opcodes, the import adapter): a type that shares a kind takes that
  kind's arm; for a new kind the build lists the exhaustive ones, and the three of the CSV import,
  which keep a default arm, are on the manual list. The writer arms run per row on an opcode one of
  these switches chose at setup, so they do not depend on the type.
- **The compare arm.** `compareOpcode` refuses a type that does not order as its family
  (`no compare arm for <type> at ORDER BY`): the comparator needs an arm for the type's order.
- **The guarded sites.** The family-arm guard refuses a type unlike its family's namesake where
  the family's code would read the namesake's NULL or order: `no family arm for <type> at <site>:
  add the arm or declare the type like its namesake`, raised at setup, before anything is
  allocated or copied. It is the per-site NULL gate a NOT NULL or bitmap type passes through. A type
  declares the refusal in `refused_sites`, which costs no edit and which the kit then checks, or is
  admitted at the site by its own arm. `WHERE key column` answers neutrally instead (the column
  stays a filter, which gives a correct result), and `ILP column kind` keeps the cast error ILP
  already raises (`cast error from protocol type`).
- **The kit declaration.** The type's line of `later-types.txt`, which the tool writes:
  `NAME | DDL | NULL policy | paths | tier | refused sites`. The kit checks a type it has no
  recording for by invariants: values read back as written, NULL behaves as the policy says, rows
  order by the tier, a query compiles or fails naming the type, and a declared site refuses it.
- **The function steps.** The per-type function bodies (the arithmetic of each function factory
  for each type) are written by the author; the coverage tests report an admitted pair, opcode or
  function without an implementation.
- **Native.** The Rust matches over the tag enum: rustc lists the exhaustive ones, a match with a
  wildcard arm is on the manual list, and the decode of an unknown tag code refuses the type at the
  Parquet boundary until the type joins the Rust enum. The C++ switches over the native enum: the
  compiler lists them under `-Wswitch`. The committed native libraries: a CI workflow rebuilds them
  on request.

## 5. What the build lists, what the tests report, what fails at setup, and what does not depend on the type

Every site of the instrument table has exactly one instrument, and none has "none": the build
lists it, a test or a kit path reports it, a setup refusal names it, it is on the manual list
(section 6), or it does not depend on the type, for the reason given. The sites the type drivers
made type-dependent fall into these groups: the eleven guarded sites are refused at setup, and
`WHERE key column`, which answers neutrally, is reported by the kit path `sql.where_key`; the
predicates that read a value by the type's own tier and NULL policy are reported by their kit
paths (SUBSAMPLE's stride and target, the WHERE bounds) or by `TypeDriverTest` (the cadence seed),
and the predicates that test the kind alone do not depend on the type; the pair switches and the
switches over the NULL policy and the wire kind are listed by the build; the Rust answers of a
type's tier and policy are listed by rustc. This is no claim that no site is silent: a site on the
manual list is one nothing reports.

The columns: the site's label, its file and method, and how the site reaches the worklist (the
build, the test or kit path, the refusal text) or why it does not depend on the type.

<!-- sites: start -->
### Listed by the build (69)

The compiler lists the site when a type is added: an exhaustive switch or match with no default arm.

| site | file | method | how |
|---|---|---|---|
| ColumnConversionSoundnessTest.ddlTypesOf test tag switch | `core/src/test/java/io/questdb/test/griffin/ColumnConversionSoundnessTest.java` | `ddlTypesOf` | a test's exhaustive switch over the tag, which pins an answer per tag: javac lists it, and the type takes the arm of its answer |
| OverloadSoundnessTest.callGetter test tag switch | `core/src/test/java/io/questdb/test/griffin/OverloadSoundnessTest.java` | `callGetter` | a test's exhaustive switch over the tag, which pins an answer per tag: javac lists it, and the type takes the arm of its answer |
| OverloadSoundnessTest.columnTypeOf test tag switch | `core/src/test/java/io/questdb/test/griffin/OverloadSoundnessTest.java` | `columnTypeOf` | a test's exhaustive switch over the tag, which pins an answer per tag: javac lists it, and the type takes the arm of its answer |
| OverloadSoundnessTest.isExplicitCast test tag switch | `core/src/test/java/io/questdb/test/griffin/OverloadSoundnessTest.java` | `isExplicitCast` | a test's exhaustive switch over the tag, which pins an answer per tag: javac lists it, and the type takes the arm of its answer |
| OverloadSoundnessTest.isFamilySignature test tag switch | `core/src/test/java/io/questdb/test/griffin/OverloadSoundnessTest.java` | `isFamilySignature` | a test's exhaustive switch over the tag, which pins an answer per tag: javac lists it, and the type takes the arm of its answer |
| OverloadSoundnessTest.ownSignatureTag test tag switch | `core/src/test/java/io/questdb/test/griffin/OverloadSoundnessTest.java` | `ownSignatureTag` | a test's exhaustive switch over the tag, which pins an answer per tag: javac lists it, and the type takes the arm of its answer |
| ColumnTypeConverter.convertColumn policy switch #1 | `core/src/main/java/io/questdb/cairo/ColumnTypeConverter.java` | `convertColumn` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| ColumnTypeConverter.convertColumn policy switch #2 | `core/src/main/java/io/questdb/cairo/ColumnTypeConverter.java` | `convertColumn` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| ALTER COLUMN TYPE var-size to fixed pair switch | `core/src/main/java/io/questdb/cairo/ColumnTypeConverter.java` | `getConverterFromVarToFixed` | an exhaustive switch over the tag: javac lists it for a new tag, which names itself in a refusal group or implements the pair |
| ALTER COLUMN TYPE fixed to var-size pair switch | `core/src/main/java/io/questdb/cairo/ColumnTypeConverter.java` | `getFixedToVarConverter` | an exhaustive switch over the tag: javac lists it for a new tag, which names itself in a refusal group or implements the pair |
| ALTER COLUMN TYPE fixed to fixed pair switch | `core/src/main/java/io/questdb/cairo/ColumnTypeConverter.java` | `convertValues` | an exhaustive switch over the tag: javac lists it for a new tag, which names itself in a refusal group or implements the pair |
| CursorPrinter.printOpcode wire-kind switch | `core/src/main/java/io/questdb/cairo/CursorPrinter.java` | `printOpcode` | an exhaustive switch over WireKind: javac lists it for a new kind |
| DedupColumnCommitAddresses.setColValues policy switch | `core/src/main/java/io/questdb/cairo/DedupColumnCommitAddresses.java` | `setColValues` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| O3PartitionJob.hasLegacyRequiredNoSentinelColumn policy switch | `core/src/main/java/io/questdb/cairo/O3PartitionJob.java` | `hasLegacyRequiredNoSentinelColumn` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| ParquetColumnTypeConverter.leadingNullCount policy switch | `core/src/main/java/io/questdb/cairo/ParquetColumnTypeConverter.java` | `leadingNullCount` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| ParquetColumnTypeConverter.maxCharsPerRow wire-kind switch | `core/src/main/java/io/questdb/cairo/ParquetColumnTypeConverter.java` | `maxCharsPerRow` | an exhaustive switch over WireKind: javac lists it for a new kind |
| ParquetColumnTypeConverter.maxUtf8BytesPerRow wire-kind switch | `core/src/main/java/io/questdb/cairo/ParquetColumnTypeConverter.java` | `maxUtf8BytesPerRow` | an exhaustive switch over WireKind: javac lists it for a new kind |
| ParquetRowGroupMaterializer.requiresMaterialization policy switch | `core/src/main/java/io/questdb/cairo/ParquetRowGroupMaterializer.java` | `requiresMaterialization` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| RelationRules.alter relation kind switch | `core/src/main/java/io/questdb/cairo/RelationRules.java` | `alter` | an exhaustive switch over RelationKind: javac lists it for a new kind; a type of an existing kind takes its kind's rule |
| RelationRules.caseEscalation relation kind switch | `core/src/main/java/io/questdb/cairo/RelationRules.java` | `caseEscalation` | an exhaustive switch over RelationKind: javac lists it for a new kind; a type of an existing kind takes its kind's rule |
| RelationRules.copier relation kind switch | `core/src/main/java/io/questdb/cairo/RelationRules.java` | `copier` | an exhaustive switch over RelationKind: javac lists it for a new kind; a type of an existing kind takes its kind's rule |
| RelationRules.ctasCastGroup relation kind switch | `core/src/main/java/io/questdb/cairo/RelationRules.java` | `ctasCastGroup` | an exhaustive switch over RelationKind: javac lists it for a new kind; a type of an existing kind takes its kind's rule |
| RelationRules.holdsNullOf policy switch #1 | `core/src/main/java/io/questdb/cairo/RelationRules.java` | `holdsNullOf` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| RelationRules.holdsNullOf policy switch #2 | `core/src/main/java/io/questdb/cairo/RelationRules.java` | `holdsNullOf` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| TableWriter.configureNullSetters policy switch | `core/src/main/java/io/questdb/cairo/TableWriter.java` | `configureNullSetters` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| TableWriter.hasNullsInValues policy switch | `core/src/main/java/io/questdb/cairo/TableWriter.java` | `hasNullsInValues` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| TableWriter.produceNativeFromParquet policy switch | `core/src/main/java/io/questdb/cairo/TableWriter.java` | `produceNativeFromParquet` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| TypeDrivers.find tag enum switch | `core/src/main/java/io/questdb/cairo/TypeDrivers.java` | `find` | where a new type registers itself: javac (or rustc, the C++ compiler) lists the exhaustive switch or the enum |
| WalWriter.configureNullSetters policy switch | `core/src/main/java/io/questdb/cairo/wal/WalWriter.java` | `configureNullSetters` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| CompiledFilterIRSerializer.serializeColumn policy switch | `core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java` | `serializeColumn` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| DecimalUtil.getTypePrecisionScale relation kind switch | `core/src/main/java/io/questdb/griffin/DecimalUtil.java` | `getTypePrecisionScale` | an exhaustive switch over RelationKind: javac lists it for a new kind; a type of an existing kind takes its kind's rule |
| RecordToRowCopierUtils.sameTypeOpcode policy switch | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `sameTypeOpcode` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| UNION cast pair switch | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateCastFunction` | an exhaustive switch over the tag: javac lists it for a new tag, which names itself in a refusal group or implements the pair |
| SqlOptimiser.printRecordColumnOrNull wire-kind switch | `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` | `printRecordColumnOrNull` | an exhaustive switch over WireKind: javac lists it for a new kind |
| SqlOptimiser.preparePivotForSelectSubquery wire-kind switch | `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` | `preparePivotForSelectSubquery` | an exhaustive switch over WireKind: javac lists it for a new kind |
| SqlOptimiser.pushOperationOutsideAgg policy switch | `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` | `pushOperationOutsideAgg` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| GroupByRecordCursorFactory.GroupByRecordCursorFactory policy switch | `core/src/main/java/io/questdb/griffin/engine/groupby/vect/GroupByRecordCursorFactory.java` | `GroupByRecordCursorFactory` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| PushdownFilterExtractor.isNullOpPushable policy switch | `core/src/main/java/io/questdb/griffin/engine/table/PushdownFilterExtractor.java` | `isNullOpPushable` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| PartitionEncoder.populateFromTableReader policy switch | `core/src/main/java/io/questdb/griffin/engine/table/parquet/PartitionEncoder.java` | `populateFromTableReader` | an exhaustive switch over NullPolicy: javac lists it for a new policy; a type with an existing policy takes its arm |
| CASE cast pair switch | `core/src/main/java/io/questdb/griffin/engine/functions/conditional/CaseCommon.java` | `castRow` | an exhaustive switch over the tag: javac lists it for a new tag, which names itself in a refusal group or implements the pair |
| PGPipelineEntry.outColumnOpcode wire-kind switch | `core/src/main/java/io/questdb/cutlass/pgwire/PGPipelineEntry.java` | `outColumnOpcode` | an exhaustive switch over WireKind: javac lists it for a new kind |
| PGPipelineEntry.txtAndBinSizesCanBeDifferent wire-kind switch | `core/src/main/java/io/questdb/cutlass/pgwire/PGPipelineEntry.java` | `txtAndBinSizesCanBeDifferent` | an exhaustive switch over WireKind: javac lists it for a new kind |
| PGUtils.calculateColumnBinSize wire-kind switch | `core/src/main/java/io/questdb/cutlass/pgwire/PGUtils.java` | `calculateColumnBinSize` | an exhaustive switch over WireKind: javac lists it for a new kind |
| PGUtils.estimateColumnTxtSize wire-kind switch | `core/src/main/java/io/questdb/cutlass/pgwire/PGUtils.java` | `estimateColumnTxtSize` | an exhaustive switch over WireKind: javac lists it for a new kind |
| ExportQueryProcessor.csvOpcode wire-kind switch | `core/src/main/java/io/questdb/cutlass/http/processors/ExportQueryProcessor.java` | `csvOpcode` | an exhaustive switch over WireKind: javac lists it for a new kind |
| JsonQueryProcessorState.jsonOpcode wire-kind switch | `core/src/main/java/io/questdb/cutlass/http/processors/JsonQueryProcessorState.java` | `jsonOpcode` | an exhaustive switch over WireKind: javac lists it for a new kind |
| QwpColumnTypeMapper.toWireType wire-kind switch | `core/src/main/java/io/questdb/cutlass/qwp/codec/QwpColumnTypeMapper.java` | `toWireType` | an exhaustive switch over WireKind: javac lists it for a new kind |
| QwpResultBatchBuffer.appendOpcode wire-kind switch | `core/src/main/java/io/questdb/cutlass/qwp/codec/QwpResultBatchBuffer.java` | `appendOpcode` | an exhaustive switch over WireKind: javac lists it for a new kind |
| TypeManager.getTypeAdapter wire-kind switch | `core/src/main/java/io/questdb/cutlass/text/types/TypeManager.java` | `getTypeAdapter` | an exhaustive switch over WireKind: javac lists it for a new kind |
| col_type::movement match | `core/rust/qdb-core/src/col_type.rs` | `movement` | an exhaustive match: rustc lists it for a tag added to the Rust enum |
| col_type::null_policy match | `core/rust/qdb-core/src/col_type.rs` | `null_policy` | an exhaustive match: rustc lists it for a tag added to the Rust enum |
| col_type::arithmetic match | `core/rust/qdb-core/src/col_type.rs` | `arithmetic` | an exhaustive match: rustc lists it for a tag added to the Rust enum |
| col_type::name match | `core/rust/qdb-core/src/col_type.rs` | `name` | an exhaustive match: rustc lists it for a tag added to the Rust enum |
| mod::try_lookup_driver match | `core/rust/qdb-core/src/col_driver/mod.rs` | `try_lookup_driver` | a match on the tag and the accessor with no wildcard arm: rustc lists it for a tag added to the Rust enum (non-exhaustive patterns) |
| decode::decode_byte_array_dispatch match #1 | `core/rust/qdbr/src/parquet_read/decode.rs` | `decode_byte_array_dispatch` | an exhaustive match over the tag: rustc lists it for a tag added to the Rust enum |
| decode::sliced_page_row_count match | `core/rust/qdbr/src/parquet_read/decode.rs` | `sliced_page_row_count` | an exhaustive match over the tag: rustc lists it for a tag added to the Rust enum |
| decode::page_row_count match | `core/rust/qdbr/src/parquet_read/decode.rs` | `page_row_count` | an exhaustive match over the tag: rustc lists it for a tag added to the Rust enum |
| row_groups::is_int_null match | `core/rust/qdbr/src/parquet_read/row_groups.rs` | `is_int_null` | an exhaustive match: rustc lists it for a tag added to the Rust enum |
| schema::column_type_to_parquet_type match #1 | `core/rust/qdbr/src/parquet_write/schema.rs` | `column_type_to_parquet_type` | an exhaustive match: rustc lists it for a tag added to the Rust enum |
| schema::encoding_map match | `core/rust/qdbr/src/parquet_write/schema.rs` | `encoding_map` | an exhaustive match: rustc lists it for a tag added to the Rust enum |
| update::generate_required_zero_page match | `core/rust/qdbr/src/parquet_write/update.rs` | `generate_required_zero_page` | an exhaustive match over the tag: rustc lists it for a tag added to the Rust enum |
| column_type.h::var_layout switch | `core/src/main/c/share/column_type.h` | `var_layout` | a switch over the native ColumnType enum under -Wswitch as an error: the C++ build lists it for a new constant |
| converters.cpp::Java_io_questdb_griffin_ConvertersNative_fixedToFixed switch | `core/src/main/c/share/converters.cpp` | `Java_io_questdb_griffin_ConvertersNative_fixedToFixed` | a switch over the native ColumnType enum under -Wswitch as an error: the C++ build lists it for a new constant |
| converters.h::is_fixed_convertible switch | `core/src/main/c/share/converters.h` | `is_fixed_convertible` | a switch over the native ColumnType enum under -Wswitch as an error: the C++ build lists it for a new constant |
| ColumnType tag constants | `core/src/main/java/io/questdb/cairo/ColumnType.java` | `` | where a new type registers itself: javac (or rustc, the C++ compiler) lists the exhaustive switch or the enum |
| ColumnTypeTag enum | `core/src/main/java/io/questdb/cairo/ColumnTypeTag.java` | `` | where a new type registers itself: javac (or rustc, the C++ compiler) lists the exhaustive switch or the enum |
| WireKind enum | `core/src/main/java/io/questdb/cairo/WireKind.java` | `` | where a new type registers itself: javac (or rustc, the C++ compiler) lists the exhaustive switch or the enum |
| Rust ColumnTypeTag enum | `core/rust/qdb-core/src/col_type.rs` | `` | where a new type registers itself: javac (or rustc, the C++ compiler) lists the exhaustive switch or the enum |
| native ColumnType enum | `core/src/main/c/share/column_type.h` | `` | where a new type registers itself: javac (or rustc, the C++ compiler) lists the exhaustive switch or the enum |

### Reported by a test (130)

A coverage test, `TypeDriverTest` or a kit path runs the type through the site and reports what is missing.

| site | file | method | how |
|---|---|---|---|
| ColumnType.pow2SizeOf tag table POW2_SIZE | `core/src/main/java/io/questdb/cairo/ColumnType.java` | `pow2SizeOf` | `TypeDriverTest`: the size table by tag, filled from the type drivers; testFixedSizeDriverWidthsMatchColumnType checks it |
| LiveViewWindow.isAnchorType family switch | `core/src/main/java/io/questdb/cairo/lv/LiveViewWindow.java` | `isAnchorType` | `lv.window_anchor`: the kit makes the column the ANCHOR EXPRESSION of a live view window: a type outside TIMESTAMP, LONG and INT, or unlike its family's order, is refused at CREATE, the others anchor the window and the view holds every row |
| LiveViewWindow.LiveViewWindow accessorOpcodeOf | `core/src/main/java/io/questdb/cairo/lv/LiveViewWindow.java` | `LiveViewWindow` | `lv.window_anchor`: the anchor value is read through the opcode of its family when the view refreshes; the kit refreshes an anchored view of every anchor type and checks it holds every row |
| LoopingRecordToRowCopier.copyColumn tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyColumn` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| LoopingRecordToRowCopier.copyFromByte tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromByte` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| LoopingRecordToRowCopier.copyFromChar tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromChar` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| LoopingRecordToRowCopier.copyFromDate tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromDate` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| LoopingRecordToRowCopier.copyFromDouble tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromDouble` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| LoopingRecordToRowCopier.copyFromFloat tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromFloat` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| LoopingRecordToRowCopier.copyFromGeoInt tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromGeoInt` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| LoopingRecordToRowCopier.copyFromGeoLong tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromGeoLong` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| LoopingRecordToRowCopier.copyFromInt tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromInt` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| LoopingRecordToRowCopier.copyFromLong tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromLong` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| LoopingRecordToRowCopier.copyFromShort tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromShort` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| LoopingRecordToRowCopier.copyFromString tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromString` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| LoopingRecordToRowCopier.copyFromSymbol tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromSymbol` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| LoopingRecordToRowCopier.copyFromTimestamp tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromTimestamp` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| LoopingRecordToRowCopier.copyFromUuid tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromUuid` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| LoopingRecordToRowCopier.copyFromVarchar tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromVarchar` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #1 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #2 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #3 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #4 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #5 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #6 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #7 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #8 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #9 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #10 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #11 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #12 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #13 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #14 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #15 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateChunkedCopier tag switch #16 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #1 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #2 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #3 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #4 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #5 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #6 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #7 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #8 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #9 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #10 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #11 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #12 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #13 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #14 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #15 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #16 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.generateSingleMethodCopier tag switch #17 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateSingleMethodCopier` | `RelationCoverageTest`: a copier arm; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.sameTypeOpcode family switch | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `sameTypeOpcode` | `RelationCoverageTest`: the copier's pair opcode; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.copyOpcode accessorOpcodeOf #1 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `copyOpcode` | `RelationCoverageTest`: the copier's pair opcode; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| RecordToRowCopierUtils.copyOpcode accessorOpcodeOf #2 | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `copyOpcode` | `RelationCoverageTest`: the copier's pair opcode; INSERT admits a pair only with a copier arm (rule K, bug y), checked for every kit type by testCopierHasAnArmForEveryAdmittedPair |
| SqlCodeGenerator.isLatestOnKeyType family switch | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `isLatestOnKeyType` | `sql.latest_by_key`: LATEST ON ... PARTITION BY the column on every table mode: one row per key, the latest, reading back as written; a type the switch refuses fails with the column and its type |
| SqlCodeGenerator.generateCastFunction tag switch #1 | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateCastFunction` | `RelationCoverageTest`: a UNION cast cell; testUnionHasACastForEveryAdmittedPair builds every cell the union matrix admits, for every kit type |
| SqlCodeGenerator.generateCastFunction tag switch #2 | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateCastFunction` | `RelationCoverageTest`: a UNION cast cell; testUnionHasACastForEveryAdmittedPair builds every cell the union matrix admits, for every kit type |
| SqlCodeGenerator.generateCastFunction tag switch #3 | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateCastFunction` | `RelationCoverageTest`: a UNION cast cell; testUnionHasACastForEveryAdmittedPair builds every cell the union matrix admits, for every kit type |
| SqlCodeGenerator.generateCastFunction tag switch #4 | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateCastFunction` | `RelationCoverageTest`: a UNION cast cell; testUnionHasACastForEveryAdmittedPair builds every cell the union matrix admits, for every kit type |
| SqlCodeGenerator.generateCastFunction tag switch #5 | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateCastFunction` | `RelationCoverageTest`: a UNION cast cell; testUnionHasACastForEveryAdmittedPair builds every cell the union matrix admits, for every kit type |
| SqlCodeGenerator.generateCastFunction tag switch #6 | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateCastFunction` | `RelationCoverageTest`: a UNION cast cell; testUnionHasACastForEveryAdmittedPair builds every cell the union matrix admits, for every kit type |
| SqlCodeGenerator.generateCastFunction tag switch #7 | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateCastFunction` | `RelationCoverageTest`: a UNION cast cell; testUnionHasACastForEveryAdmittedPair builds every cell the union matrix admits, for every kit type |
| SqlCodeGenerator.generateCastFunction tag switch #8 | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateCastFunction` | `RelationCoverageTest`: a UNION cast cell; testUnionHasACastForEveryAdmittedPair builds every cell the union matrix admits, for every kit type |
| SqlCodeGenerator.generateCastFunction tag switch #9 | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateCastFunction` | `RelationCoverageTest`: a UNION cast cell; testUnionHasACastForEveryAdmittedPair builds every cell the union matrix admits, for every kit type |
| SqlCodeGenerator.generateCastFunction tag switch #10 | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateCastFunction` | `RelationCoverageTest`: a UNION cast cell; testUnionHasACastForEveryAdmittedPair builds every cell the union matrix admits, for every kit type |
| SqlCodeGenerator.generateCastFunction tag switch #11 | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateCastFunction` | `RelationCoverageTest`: a UNION cast cell; testUnionHasACastForEveryAdmittedPair builds every cell the union matrix admits, for every kit type |
| SqlCodeGenerator.generateCastFunction tag switch #12 | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateCastFunction` | `RelationCoverageTest`: a UNION cast cell; testUnionHasACastForEveryAdmittedPair builds every cell the union matrix admits, for every kit type |
| SqlCodeGenerator.generateCastFunction tag switch #13 | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateCastFunction` | `RelationCoverageTest`: a UNION cast cell; testUnionHasACastForEveryAdmittedPair builds every cell the union matrix admits, for every kit type |
| SqlCompilerImpl.isCompatibleColumnTypeChange tag table columnConversionSupport | `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java` | `isCompatibleColumnTypeChange` | `TypeRelationGoldenTest`: the ALTER COLUMN TYPE support table by tag pair; the golden table pins it and a new tag changes its shape |
| SqlOptimiser.validateCadenceSeedOrThrow isIntegral | `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` | `validateCadenceSeedOrThrow` | `TypeDriverTest`: the seed is read by tier and NULL policy (CadenceFunctionFactory), pinned by TypeDriverTest.testSentinelRead* |
| SubsampleValidator.validatePositionTargetOrThrow isIntegral | `core/src/main/java/io/questdb/griffin/SubsampleValidator.java` | `validatePositionTargetOrThrow` | `sql.subsample_stride`: SUBSAMPLE takes a value of the type as the stride and the target point count (sql.subsample_target too): an integral type is read at its tier and policy, any other refused as no integer |
| WhereClauseParser.isKeyColumnType family switch | `core/src/main/java/io/questdb/griffin/WhereClauseParser.java` | `isKeyColumnType` | `sql.where_key`: answers neutrally: an unlike type is not a key column and stays a filter (plan.md); no kit path runs a later type as a WHERE key yet |
| WhereClauseParser.resolveScalarBound isIntegral | `core/src/main/java/io/questdb/griffin/WhereClauseParser.java` | `resolveScalarBound` | `sql.where_bound_const`: a WHERE bound over the timestamp cast to LONG takes a constant of the type (sql.where_bound_bind a bind variable): an integral type is read at its tier, only its own NULL empties the scan |
| SampleByFillNullRecordCursorFactory.createPlaceHolderFunction family switch | `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillNullRecordCursorFactory.java` | `createPlaceHolderFunction` | `sql.fill_null`: FILL(NULL) fills the gaps with the NULL of the column's family: the kit checks each gap reads as the type's NULL row |
| RuntimeConstFunction.isFoldableType tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/RuntimeConstFunction.java` | `isFoldableType` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| RuntimeConstFunction.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/RuntimeConstFunction.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| ScalarSubQueryUtils.readDoubleValue tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/ScalarSubQueryUtils.java` | `readDoubleValue` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| ScalarSubQueryUtils.readIntValue tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/ScalarSubQueryUtils.java` | `readIntValue` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| ScalarSubQueryUtils.readLongValue tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/ScalarSubQueryUtils.java` | `readLongValue` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InDoubleFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InDoubleFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InDoubleFunctionFactory.parseValue tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InDoubleFunctionFactory.java` | `parseValue` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InIPv4FunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InIPv4FunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InIPv4FunctionFactory.addIPv4ToSet tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InIPv4FunctionFactory.java` | `addIPv4ToSet` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InLongFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InLongFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InLongFunctionFactory.parseValue tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InLongFunctionFactory.java` | `parseValue` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InLongFunctionFactory.constElementValue tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InLongFunctionFactory.java` | `constElementValue` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InLongFunctionFactory.dynamicElementKind tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InLongFunctionFactory.java` | `dynamicElementKind` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InStrFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InStrFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InStrFunctionFactory.parseToString tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InStrFunctionFactory.java` | `parseToString` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InSymbolCursorFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InSymbolCursorFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InSymbolFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InSymbolFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InSymbolFunctionFactory.init tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InSymbolFunctionFactory.java` | `init` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InUuidFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InUuidFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InUuidFunctionFactory.addUUIDToSet tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InUuidFunctionFactory.java` | `addUUIDToSet` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InVarcharFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InVarcharFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| InVarcharFunctionFactory.parseToVarchar tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InVarcharFunctionFactory.java` | `parseToVarchar` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| CaseCommon.getCaseFunctionConstructor tag table constructors | `core/src/main/java/io/questdb/griffin/engine/functions/conditional/CaseCommon.java` | `getCaseFunctionConstructor` | `RelationCoverageTest`: the CASE function table by tag: a type without an entry is refused ("unsupported CASE value type"); testCaseEscalationHasAnImplementation checks every pair rule E admits, later types included |
| CoalesceFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/conditional/CoalesceFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| SwitchFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/conditional/SwitchFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| AbstractDoubleCursorFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/lt/AbstractDoubleCursorFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| AbstractIntCursorFunctionFactory.newInstance tag switch #1 | `core/src/main/java/io/questdb/griffin/engine/functions/lt/AbstractIntCursorFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| AbstractIntCursorFunctionFactory.newInstance tag switch #2 | `core/src/main/java/io/questdb/griffin/engine/functions/lt/AbstractIntCursorFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| AbstractLongCursorFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/lt/AbstractLongCursorFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| JsonExtractFunction.JsonExtractFunction tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/json/JsonExtractFunction.java` | `JsonExtractFunction` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| JsonExtractTypedFunctionFactory.isIntrusivelyOptimized tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/json/JsonExtractTypedFunctionFactory.java` | `isIntrusivelyOptimized` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| JsonExtractTypedFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/json/JsonExtractTypedFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| BucketSelectWindowFunction.readLongValue tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/window/BucketSelectWindowFunction.java` | `readLongValue` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| RndSymbolZipfFunctionFactory.newInstance tag switch #1 | `core/src/main/java/io/questdb/griffin/engine/functions/rnd/RndSymbolZipfFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| RndSymbolZipfFunctionFactory.newInstance tag switch #2 | `core/src/main/java/io/questdb/griffin/engine/functions/rnd/RndSymbolZipfFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| LevelTwoPriceFunctionFactory.allowedColumnType tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/finance/LevelTwoPriceFunctionFactory.java` | `allowedColumnType` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| BindVariableServiceImpl.setArray0 tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setArray0` | `sql.bind_value`: the kit defines a bind variable of the type and sets it through each value setter: the default arm refuses a tag it does not list with an error naming the type, which the kit checks |
| BindVariableServiceImpl.setBoolean0 tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setBoolean0` | `sql.bind_value`: the kit defines a bind variable of the type and sets it through each value setter: the default arm refuses a tag it does not list with an error naming the type, which the kit checks |
| BindVariableServiceImpl.setByte0 tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setByte0` | `sql.bind_value`: the kit defines a bind variable of the type and sets it through each value setter: the default arm refuses a tag it does not list with an error naming the type, which the kit checks |
| BindVariableServiceImpl.setChar0 tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setChar0` | `sql.bind_value`: the kit defines a bind variable of the type and sets it through each value setter: the default arm refuses a tag it does not list with an error naming the type, which the kit checks |
| BindVariableServiceImpl.setDate0 tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setDate0` | `sql.bind_value`: the kit defines a bind variable of the type and sets it through each value setter: the default arm refuses a tag it does not list with an error naming the type, which the kit checks |
| BindVariableServiceImpl.setDouble0 tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setDouble0` | `sql.bind_value`: the kit defines a bind variable of the type and sets it through each value setter: the default arm refuses a tag it does not list with an error naming the type, which the kit checks |
| BindVariableServiceImpl.setFloat0 tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setFloat0` | `sql.bind_value`: the kit defines a bind variable of the type and sets it through each value setter: the default arm refuses a tag it does not list with an error naming the type, which the kit checks |
| BindVariableServiceImpl.setInt0 tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setInt0` | `sql.bind_value`: the kit defines a bind variable of the type and sets it through each value setter: the default arm refuses a tag it does not list with an error naming the type, which the kit checks |
| BindVariableServiceImpl.setLong0 tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setLong0` | `sql.bind_value`: the kit defines a bind variable of the type and sets it through each value setter: the default arm refuses a tag it does not list with an error naming the type, which the kit checks |
| BindVariableServiceImpl.setLong2560 tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setLong2560` | `sql.bind_value`: the kit defines a bind variable of the type and sets it through each value setter: the default arm refuses a tag it does not list with an error naming the type, which the kit checks |
| BindVariableServiceImpl.setShort0 tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setShort0` | `sql.bind_value`: the kit defines a bind variable of the type and sets it through each value setter: the default arm refuses a tag it does not list with an error naming the type, which the kit checks |
| BindVariableServiceImpl.setStr0 tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setStr0` | `sql.bind_value`: the kit defines a bind variable of the type and sets it through each value setter: the default arm refuses a tag it does not list with an error naming the type, which the kit checks |
| BindVariableServiceImpl.setTimestamp0 tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setTimestamp0` | `sql.bind_value`: the kit defines a bind variable of the type and sets it through each value setter: the default arm refuses a tag it does not list with an error naming the type, which the kit checks |
| BindVariableServiceImpl.setUuid tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setUuid` | `sql.bind_value`: the kit defines a bind variable of the type and sets it through each value setter: the default arm refuses a tag it does not list with an error naming the type, which the kit checks |
| BindVariableServiceImpl.setVarchar0 tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setVarchar0` | `sql.bind_value`: the kit defines a bind variable of the type and sets it through each value setter: the default arm refuses a tag it does not list with an error naming the type, which the kit checks |
| GreatestNumericFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/math/GreatestNumericFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| LeastNumericFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/math/LeastNumericFunctionFactory.java` | `newInstance` | `FunctionReachTest`: inside a function factory: a later type reaches it only through overload matching, which FunctionReachTest lists for every later type |
| SortKeyEncoder.keyKind family switch | `core/src/main/java/io/questdb/griffin/engine/orderby/SortKeyEncoder.java` | `keyKind` | `GeneratedAccessorCoverageTest`: the sort-key kind by accessor; GeneratedAccessorCoverageTest reports a type keyKind does not handle (an unsigned INT look-alike, for example) |
| PGOids.getTypeOid tag table TYPE_OIDS | `core/src/main/java/io/questdb/cutlass/pgwire/PGOids.java` | `getTypeOid` | `TypeDriverTest`: the PG type OID table by tag, filled from the type drivers; testProtocolAnswers pins every tag's OID |

### Refused at setup (62)

A kit path reaches the site, which refuses the type before it allocates or copies anything; the refusal text maps the failure to the site.

| site | file | method | how |
|---|---|---|---|
| ColumnTypeConverter.convertFromFixedSize tag switch #1 | `core/src/main/java/io/questdb/cairo/ColumnTypeConverter.java` | `convertFromFixedSize` | `storage.alter`, "unsupported conversion": ALTER COLUMN TYPE refuses a conversion it has no arm for |
| ColumnTypeConverter.convertFromFixedSize tag switch #2 | `core/src/main/java/io/questdb/cairo/ColumnTypeConverter.java` | `convertFromFixedSize` | `storage.alter`, "unsupported conversion": ALTER COLUMN TYPE refuses a conversion it has no arm for |
| ParquetColumnTypeConverter.fixedTargetOpcode family switch | `core/src/main/java/io/questdb/cairo/ParquetColumnTypeConverter.java` | `fixedTargetOpcode` | `storage.parquet_convert`, "no family arm for <type> at Parquet conversion": switches on the accessor familyArmOf answered; the conversion refuses an unlike target before its row loop |
| Parquet conversion | `core/src/main/java/io/questdb/cairo/ParquetColumnTypeConverter.java` | `fixedTargetOpcode` | `storage.parquet_convert`, "no family arm for <type> at Parquet conversion": the family-arm guard refuses a type unlike its family's namesake before the switch |
| WAL columnar append #1 | `core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java` | `putBooleanToNumericColumn` | `ingest.qwp`, "no family arm for <type> at WAL columnar append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error (unsupportedColumnType) |
| WAL columnar append #2 | `core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java` | `putFixedColumn` | `ingest.qwp`, "no family arm for <type> at WAL columnar append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error (unsupportedColumnType) |
| WAL columnar append #3 | `core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java` | `putFixedToSmallDecimalColumn` | `ingest.qwp`, "no family arm for <type> at WAL columnar append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error (unsupportedColumnType) |
| WAL columnar append #4 | `core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java` | `putFloatToDecimalColumn` | `ingest.qwp`, "no family arm for <type> at WAL columnar append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error (unsupportedColumnType) |
| WAL columnar append #5 | `core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java` | `putFloatToNumericColumn` | `ingest.qwp`, "no family arm for <type> at WAL columnar append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error (unsupportedColumnType) |
| WAL columnar append #6 | `core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java` | `putGeoHashColumn` | `ingest.qwp`, "no family arm for <type> at WAL columnar append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error (unsupportedColumnType) |
| WAL columnar append #7 | `core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java` | `putIntegerToNumericColumn` | `ingest.qwp`, "no family arm for <type> at WAL columnar append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error (unsupportedColumnType) |
| WAL columnar append #8 | `core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java` | `putStringToDecimalColumn` | `ingest.qwp`, "no family arm for <type> at WAL columnar append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error (unsupportedColumnType) |
| WAL columnar append #9 | `core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java` | `putStringToGeoHashColumn` | `ingest.qwp`, "no family arm for <type> at WAL columnar append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error (unsupportedColumnType) |
| WAL columnar append #10 | `core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java` | `putStringToNumericColumn` | `ingest.qwp`, "no family arm for <type> at WAL columnar append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error (unsupportedColumnType) |
| WAL columnar append #11 | `core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java` | `writeDecimalNullSentinel` | `ingest.qwp`, "no family arm for <type> at WAL columnar append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error (unsupportedColumnType) |
| WAL columnar append #12 | `core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java` | `writeDecimalValue` | `ingest.qwp`, "no family arm for <type> at WAL columnar append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error (unsupportedColumnType) |
| WAL columnar append #13 | `core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java` | `unsupportedColumnType` | `ingest.qwp`, "no family arm for <type> at WAL columnar append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error (unsupportedColumnType) |
| memoized virtual column | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `memoized` | `sql.memoized`, "no family arm for <type> at memoized virtual column": the family-arm guard refuses a type unlike its family's namesake before the switch |
| SAMPLE BY FILL(PREV) #1 | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `isFixedSizePrevSlotEligible` | `sql.fill_prev`, "no family arm for <type> at SAMPLE BY FILL(PREV)": the family-arm guard refuses a type unlike its family's namesake before the switch |
| SqlCodeGenerator.generateFill familyArmOpcodeOf | `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateFill` | `sql.fill_prev`, "no family arm for <type> at SAMPLE BY FILL(PREV)": the FILL(PREV) slot key is the family arm opcode; SAMPLE BY FILL(PREV) refuses an unlike type first (isFixedSizePrevSlotEligible) |
| SampleByFillRecordCursorFactory.initPrevCacheSlots tag switch | `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` | `initPrevCacheSlots` | `sql.fill_prev`, "no family arm for <type> at SAMPLE BY FILL(PREV)": keyed on the family arm opcode; an unlike type was refused at the FILL(PREV) slot choice, and the default raises the same error |
| SAMPLE BY FILL(PREV) #2 | `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` | `initPrevCacheSlots` | `sql.fill_prev`, "no family arm for <type> at SAMPLE BY FILL(PREV)": the family-arm guard refuses a type unlike its family's namesake before the switch |
| SampleByFillValueRecordCursorFactory.createPlaceHolderFunction family switch | `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillValueRecordCursorFactory.java` | `createPlaceHolderFunction` | `sql.fill_value`, "no family arm for <type> at SAMPLE BY FILL(value)": switches on the accessor familyArmOf answered, so an unlike type was refused one line before |
| SAMPLE BY FILL(value) | `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillValueRecordCursorFactory.java` | `createPlaceHolderFunction` | `sql.fill_value`, "no family arm for <type> at SAMPLE BY FILL(value)": the family-arm guard refuses a type unlike its family's namesake before the switch |
| SampleByInterpolateRecordCursorFactory.SampleByInterpolateRecordCursorFactory family switch | `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByInterpolateRecordCursorFactory.java` | `SampleByInterpolateRecordCursorFactory` | `sql.fill_linear`, "no family arm for <type> at SAMPLE BY FILL(LINEAR)": the interpolation steps by accessor, after the FILL(LINEAR) guard refused an unlike type |
| SAMPLE BY FILL(LINEAR) | `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByInterpolateRecordCursorFactory.java` | `SampleByInterpolateRecordCursorFactory` | `sql.fill_linear`, "no family arm for <type> at SAMPLE BY FILL(LINEAR)": the family-arm guard refuses a type unlike its family's namesake before the switch |
| between | `core/src/main/java/io/questdb/griffin/engine/functions/bool/BetweenTimestampFunctionFactory.java` | `newInstance` | `sql.between_timestamp`, "no family arm for <type> at between": the family-arm guard refuses a type unlike its family's namesake before the switch |
| = NULL | `core/src/main/java/io/questdb/griffin/engine/functions/eq/EqDoubleFunctionFactory.java` | `dispatchUnaryFunc` | `sql.eq_null_double`, "no family arm for <type> at = NULL": the family-arm guard refuses a type unlike its family's namesake before the switch |
| BindVariableServiceImpl.snapshotIndexedFunction family switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `snapshotIndexedFunction` | `sql.copy_bind`, "no family arm for <type> at COPY bind snapshot": copies by accessor after checkSnapshotFamilyArm refused an unlike type for every variable |
| BindVariableServiceImpl.snapshotNamedFunction family switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `snapshotNamedFunction` | `sql.copy_bind`, "no family arm for <type> at COPY bind snapshot": copies by accessor after checkSnapshotFamilyArm refused an unlike type for every variable |
| COPY bind snapshot | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `checkSnapshotFamilyArm` | `sql.copy_bind`, "no family arm for <type> at COPY bind snapshot": the snapshot checks every bound variable against the guard before it copies any, so nothing is half copied when it refuses |
| RecordComparatorCompiler.comparatorOpcode family switch | `core/src/main/java/io/questdb/griffin/engine/orderby/RecordComparatorCompiler.java` | `comparatorOpcode` | `sql.order_asc`, "no compare arm for <type> at ORDER BY": compareOpcode refuses a type that does not order like its family |
| RecordComparatorCompiler.comparatorOpcode compare arm | `core/src/main/java/io/questdb/griffin/engine/orderby/RecordComparatorCompiler.java` | `comparatorOpcode` | `sql.order_asc`, "no compare arm for <type> at ORDER BY": compareOpcode refuses a type that does not order like its family |
| ILP column kind | `core/src/main/java/io/questdb/cutlass/line/LineUtils.java` | `columnKind` | `ingest.ilp-tcp`, "cast error from protocol type": columnKind answers UNDEFINED for a type unlike its namesake; the ILP switches then raise their kept cast error |
| LineUdpParserImpl.parseValue tag switch | `core/src/main/java/io/questdb/cutlass/line/udp/LineUdpParserImpl.java` | `parseValue` | `ingest.ilp-udp`, "cast error from protocol type": ILP keys on the column kind; an unlisted kind takes the kept refusal |
| LineUdpParserImpl.parseValue columnKind | `core/src/main/java/io/questdb/cutlass/line/udp/LineUdpParserImpl.java` | `parseValue` | `ingest.ilp-udp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default refuses the field |
| LineUdpParserSupport.putValue family opcode switch | `core/src/main/java/io/questdb/cutlass/line/udp/LineUdpParserSupport.java` | `putValue` | `ingest.ilp-udp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default refuses the field |
| LineUdpParserSupport.putNullValue family opcode switch | `core/src/main/java/io/questdb/cutlass/line/udp/LineUdpParserSupport.java` | `putNullValue` | `ingest.ilp-udp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default refuses the field |
| LineTcpMeasurementEvent.createMeasurementEvent family opcode switch #1 | `core/src/main/java/io/questdb/cutlass/line/tcp/LineTcpMeasurementEvent.java` | `createMeasurementEvent` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| LineTcpMeasurementEvent.createMeasurementEvent family opcode switch #2 | `core/src/main/java/io/questdb/cutlass/line/tcp/LineTcpMeasurementEvent.java` | `createMeasurementEvent` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| LineTcpMeasurementEvent.createMeasurementEvent family opcode switch #3 | `core/src/main/java/io/questdb/cutlass/line/tcp/LineTcpMeasurementEvent.java` | `createMeasurementEvent` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| LineTcpMeasurementEvent.createMeasurementEvent family opcode switch #4 | `core/src/main/java/io/questdb/cutlass/line/tcp/LineTcpMeasurementEvent.java` | `createMeasurementEvent` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| LineTcpMeasurementEvent.createMeasurementEvent family opcode switch #5 | `core/src/main/java/io/questdb/cutlass/line/tcp/LineTcpMeasurementEvent.java` | `createMeasurementEvent` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| LineTcpMeasurementEvent.createMeasurementEvent family opcode switch #6 | `core/src/main/java/io/questdb/cutlass/line/tcp/LineTcpMeasurementEvent.java` | `createMeasurementEvent` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| LineTcpMeasurementEvent.createMeasurementEvent family opcode switch #7 | `core/src/main/java/io/questdb/cutlass/line/tcp/LineTcpMeasurementEvent.java` | `createMeasurementEvent` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| LineTcpMeasurementEvent.createMeasurementEvent columnKind | `core/src/main/java/io/questdb/cutlass/line/tcp/LineTcpMeasurementEvent.java` | `createMeasurementEvent` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| LineWalAppender.appendToWal0 family opcode switch #1 | `core/src/main/java/io/questdb/cutlass/line/tcp/LineWalAppender.java` | `appendToWal0` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| LineWalAppender.appendToWal0 family opcode switch #2 | `core/src/main/java/io/questdb/cutlass/line/tcp/LineWalAppender.java` | `appendToWal0` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| LineWalAppender.appendToWal0 family opcode switch #3 | `core/src/main/java/io/questdb/cutlass/line/tcp/LineWalAppender.java` | `appendToWal0` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| LineWalAppender.appendToWal0 family opcode switch #4 | `core/src/main/java/io/questdb/cutlass/line/tcp/LineWalAppender.java` | `appendToWal0` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| LineWalAppender.appendToWal0 family opcode switch #5 | `core/src/main/java/io/questdb/cutlass/line/tcp/LineWalAppender.java` | `appendToWal0` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| LineWalAppender.appendToWal0 family opcode switch #6 | `core/src/main/java/io/questdb/cutlass/line/tcp/LineWalAppender.java` | `appendToWal0` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| LineWalAppender.appendToWal0 family opcode switch #7 | `core/src/main/java/io/questdb/cutlass/line/tcp/LineWalAppender.java` | `appendToWal0` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| LineWalAppender.appendToWal0 columnKind | `core/src/main/java/io/questdb/cutlass/line/tcp/LineWalAppender.java` | `appendToWal0` | `ingest.ilp-tcp`, "cast error from protocol type": keys on LineUtils.columnKind, which answers UNDEFINED for an unlike type; the default raises the kept cast error |
| QWP WAL append #1 | `core/src/main/java/io/questdb/cutlass/line/tcp/QwpWalAppender.java` | `appendToWalColumnar` | `ingest.qwp`, "no family arm for <type> at QWP WAL append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error |
| QwpWalAppender.appendToWalColumnar family opcode switch | `core/src/main/java/io/questdb/cutlass/line/tcp/QwpWalAppender.java` | `appendToWalColumnar` | `ingest.qwp`, "no family arm for <type> at QWP WAL append": keys on the familyArmOpcodeOf local assigned one line before; an unlike type reaches the guard error |
| QWP WAL append #2 | `core/src/main/java/io/questdb/cutlass/line/tcp/QwpWalAppender.java` | `isFixedTypeCoercionAllowed` | `ingest.qwp`, "no family arm for <type> at QWP WAL append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error |
| QWP WAL append #3 | `core/src/main/java/io/questdb/cutlass/line/tcp/QwpWalAppender.java` | `appendToWalColumnar` | `ingest.qwp`, "no family arm for <type> at QWP WAL append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error |
| QWP WAL append #4 | `core/src/main/java/io/questdb/cutlass/line/tcp/QwpWalAppender.java` | `appendToWalColumnar` | `ingest.qwp`, "no family arm for <type> at QWP WAL append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error |
| QWP WAL append #5 | `core/src/main/java/io/questdb/cutlass/line/tcp/QwpWalAppender.java` | `appendToWalColumnar` | `ingest.qwp`, "no family arm for <type> at QWP WAL append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error |
| QWP WAL append #6 | `core/src/main/java/io/questdb/cutlass/line/tcp/QwpWalAppender.java` | `appendToWalColumnar` | `ingest.qwp`, "no family arm for <type> at QWP WAL append": the switch keys on familyArmOpcodeOf; an unlike type reaches the default, which raises the guard error |
| col_type::try_from match | `core/rust/qdb-core/src/col_type.rs` | `try_from` | `storage.parquet`, "unknown QuestDB column tag code": decodes the tag number at the JNI boundary; a Java tag Rust does not know is refused |

### Not type-dependent (241)

The site moves bytes by width, orders by tier or is reached by one family only, so it cannot treat a new type wrongly; the reason says which.

| site | file | method | how |
|---|---|---|---|
| ColumnType.getTimestampType tag switch | `core/src/main/java/io/questdb/cairo/ColumnType.java` | `getTimestampType` | tells the timestamp or interval units apart; reached only for that family |
| CursorPrinter.printColumn writer arm | `core/src/main/java/io/questdb/cairo/CursorPrinter.java` | `printColumn` | the opcode comes from a closed switch at setup (printOpcode, csvOpcode, jsonOpcode, outColumnOpcode, exportOpcode), which refuses an unhandled type; ProtocolOpcodeCoverageTest covers those |
| DecimalColumnTypeConverter.convertToDecimal tag switch | `core/src/main/java/io/questdb/cairo/DecimalColumnTypeConverter.java` | `convertToDecimal` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| DecimalColumnTypeConverter.getLoader tag switch | `core/src/main/java/io/questdb/cairo/DecimalColumnTypeConverter.java` | `getLoader` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| GeoHashes.getGeoLong tag switch | `core/src/main/java/io/questdb/cairo/GeoHashes.java` | `getGeoLong` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| IntervalTypeDriver.IntervalTypeDriver tag switch | `core/src/main/java/io/questdb/cairo/IntervalTypeDriver.java` | `IntervalTypeDriver` | tells the timestamp or interval units apart; reached only for that family |
| IntervalTypeDriver.getName tag switch | `core/src/main/java/io/questdb/cairo/IntervalTypeDriver.java` | `getName` | tells the timestamp or interval units apart; reached only for that family |
| LoopingRecordSink.copyFunction writer arm | `core/src/main/java/io/questdb/cairo/LoopingRecordSink.java` | `copyFunction` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| ParquetColumnTypeConverter.writeFixedNull writer arm | `core/src/main/java/io/questdb/cairo/ParquetColumnTypeConverter.java` | `writeFixedNull` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| ParquetColumnTypeConverter.writeFixedParsedValue writer arm | `core/src/main/java/io/questdb/cairo/ParquetColumnTypeConverter.java` | `writeFixedParsedValue` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| RecordSinkFactory.estimateColumnBytecodeSize family switch | `core/src/main/java/io/questdb/cairo/RecordSinkFactory.java` | `estimateColumnBytecodeSize` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| RecordSinkFactory.getChunkedInstanceClass writer arm #1 | `core/src/main/java/io/questdb/cairo/RecordSinkFactory.java` | `getChunkedInstanceClass` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| RecordSinkFactory.getChunkedInstanceClass writer arm #2 | `core/src/main/java/io/questdb/cairo/RecordSinkFactory.java` | `getChunkedInstanceClass` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| RecordSinkFactory.getSingleInstanceClass writer arm #1 | `core/src/main/java/io/questdb/cairo/RecordSinkFactory.java` | `getSingleInstanceClass` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| RecordSinkFactory.getSingleInstanceClass writer arm #2 | `core/src/main/java/io/questdb/cairo/RecordSinkFactory.java` | `getSingleInstanceClass` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| RecordSinkFactory.estimateColumnBytecodeSize accessorOf | `core/src/main/java/io/questdb/cairo/RecordSinkFactory.java` | `estimateColumnBytecodeSize` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| RecordSinkFactory.sinkOpcode accessorOf | `core/src/main/java/io/questdb/cairo/RecordSinkFactory.java` | `sinkOpcode` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| TimestampTypeDriver.getName tag switch | `core/src/main/java/io/questdb/cairo/TimestampTypeDriver.java` | `getName` | tells the timestamp or interval units apart; reached only for that family |
| VarcharTypeDriver.getName tag switch | `core/src/main/java/io/questdb/cairo/VarcharTypeDriver.java` | `getName` | names VARCHAR and VARCHAR_SLICE, the two tags this type driver serves |
| CoveredColumnDecoder.coveredLayout family switch | `core/src/main/java/io/questdb/cairo/sql/CoveredColumnDecoder.java` | `coveredLayout` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| CoveredColumnDecoder.coveredOpcode family switch | `core/src/main/java/io/questdb/cairo/sql/CoveredColumnDecoder.java` | `coveredOpcode` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| CoveredColumnDecoder.coveredLayout accessorOf | `core/src/main/java/io/questdb/cairo/sql/CoveredColumnDecoder.java` | `coveredLayout` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| CoveredColumnDecoder.coveredOpcode accessorOf | `core/src/main/java/io/questdb/cairo/sql/CoveredColumnDecoder.java` | `coveredOpcode` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| OrderedMapFixedSizeRecord.keyHolderOpcode family switch | `core/src/main/java/io/questdb/cairo/map/OrderedMapFixedSizeRecord.java` | `keyHolderOpcode` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| OrderedMapFixedSizeRecord.OrderedMapFixedSizeRecord accessorOf | `core/src/main/java/io/questdb/cairo/map/OrderedMapFixedSizeRecord.java` | `OrderedMapFixedSizeRecord` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| OrderedMapFixedSizeRecord.keyHolderOpcode accessorOf | `core/src/main/java/io/questdb/cairo/map/OrderedMapFixedSizeRecord.java` | `keyHolderOpcode` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| OrderedMapVarSizeRecord.OrderedMapVarSizeRecord family switch | `core/src/main/java/io/questdb/cairo/map/OrderedMapVarSizeRecord.java` | `OrderedMapVarSizeRecord` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| OrderedMapVarSizeRecord.OrderedMapVarSizeRecord accessorOf #1 | `core/src/main/java/io/questdb/cairo/map/OrderedMapVarSizeRecord.java` | `OrderedMapVarSizeRecord` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| OrderedMapVarSizeRecord.OrderedMapVarSizeRecord accessorOf #2 | `core/src/main/java/io/questdb/cairo/map/OrderedMapVarSizeRecord.java` | `OrderedMapVarSizeRecord` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| RecordValueSinkFactory.getInstance family opcode switch | `core/src/main/java/io/questdb/cairo/map/RecordValueSinkFactory.java` | `getInstance` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| RecordValueSinkFactory.isSupportedColumnType family switch | `core/src/main/java/io/questdb/cairo/map/RecordValueSinkFactory.java` | `isSupportedColumnType` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| RecordValueSinkFactory.isSupportedColumnType accessorOf | `core/src/main/java/io/questdb/cairo/map/RecordValueSinkFactory.java` | `isSupportedColumnType` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| Unordered4Map.isSupportedKeyType family switch | `core/src/main/java/io/questdb/cairo/map/Unordered4Map.java` | `isSupportedKeyType` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| Unordered4Map.isSupportedKeyType accessorOf | `core/src/main/java/io/questdb/cairo/map/Unordered4Map.java` | `isSupportedKeyType` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| Unordered4MapRecord.Unordered4MapRecord accessorOf | `core/src/main/java/io/questdb/cairo/map/Unordered4MapRecord.java` | `Unordered4MapRecord` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| Unordered8Map.isSupportedKeyType family switch | `core/src/main/java/io/questdb/cairo/map/Unordered8Map.java` | `isSupportedKeyType` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| Unordered8Map.isSupportedKeyType accessorOf | `core/src/main/java/io/questdb/cairo/map/Unordered8Map.java` | `isSupportedKeyType` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| Unordered8MapRecord.Unordered8MapRecord accessorOf | `core/src/main/java/io/questdb/cairo/map/Unordered8MapRecord.java` | `Unordered8MapRecord` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| UnorderedVarcharMapRecord.UnorderedVarcharMapRecord accessorOf | `core/src/main/java/io/questdb/cairo/map/UnorderedVarcharMapRecord.java` | `UnorderedVarcharMapRecord` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| CoveringCompressor.codecKind family switch | `core/src/main/java/io/questdb/cairo/idx/CoveringCompressor.java` | `codecKind` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| ArrayTypeDriver.resolveAppender tag switch | `core/src/main/java/io/questdb/cairo/arr/ArrayTypeDriver.java` | `resolveAppender` | reached only for an array: the switch tells the element types apart |
| ArrayView.arrayEquals tag switch | `core/src/main/java/io/questdb/cairo/arr/ArrayView.java` | `arrayEquals` | reached only for an array: the switch tells the element types apart |
| FunctionArray.appendToMemFlat tag switch | `core/src/main/java/io/questdb/cairo/arr/FunctionArray.java` | `appendToMemFlat` | reached only for an array: the switch tells the element types apart |
| WalEventCursor.readDecimal writer arm | `core/src/main/java/io/questdb/cairo/wal/WalEventCursor.java` | `readDecimal` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| WalEventCursor.populateIndexedVariables writer arm | `core/src/main/java/io/questdb/cairo/wal/WalEventCursor.java` | `populateIndexedVariables` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| WalEventCursor.populateNamedVariables writer arm | `core/src/main/java/io/questdb/cairo/wal/WalEventCursor.java` | `populateNamedVariables` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| WalEventWriter.bindValueOpcode family switch | `core/src/main/java/io/questdb/cairo/wal/WalEventWriter.java` | `bindValueOpcode` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| WalEventWriter.appendFunctionValue writer arm | `core/src/main/java/io/questdb/cairo/wal/WalEventWriter.java` | `appendFunctionValue` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| WalEventWriter.bindValueOpcode accessorOf | `core/src/main/java/io/questdb/cairo/wal/WalEventWriter.java` | `bindValueOpcode` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| WriterRowUtils.putGeoHash tag switch | `core/src/main/java/io/questdb/cairo/wal/WriterRowUtils.java` | `putGeoHash` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| WriterRowUtils.putNullDecimal tag switch | `core/src/main/java/io/questdb/cairo/wal/WriterRowUtils.java` | `putNullDecimal` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| WriterRowUtils.putDecimal0 tag switch | `core/src/main/java/io/questdb/cairo/wal/WriterRowUtils.java` | `putDecimal0` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| LiveViewInMemoryBuffer.isTierSupported family switch | `core/src/main/java/io/questdb/cairo/lv/LiveViewInMemoryBuffer.java` | `isTierSupported` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| LiveViewInMemoryBuffer.copyRowFrom writer arm | `core/src/main/java/io/questdb/cairo/lv/LiveViewInMemoryBuffer.java` | `copyRowFrom` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| LiveViewInMemoryBuffer.copyRowFromRecord writer arm | `core/src/main/java/io/questdb/cairo/lv/LiveViewInMemoryBuffer.java` | `copyRowFromRecord` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| LiveViewInMemoryBuffer.copyRowsFrom writer arm | `core/src/main/java/io/questdb/cairo/lv/LiveViewInMemoryBuffer.java` | `copyRowsFrom` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| LiveViewInMemoryBuffer.LiveViewInMemoryBuffer accessorOpcodeOf | `core/src/main/java/io/questdb/cairo/lv/LiveViewInMemoryBuffer.java` | `LiveViewInMemoryBuffer` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| LiveViewInMemoryBuffer.isTierSupported accessorOf | `core/src/main/java/io/questdb/cairo/lv/LiveViewInMemoryBuffer.java` | `isTierSupported` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| LiveViewRefreshJob.copyReaderRowsToStaging family opcode switch | `core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java` | `copyReaderRowsToStaging` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| LiveViewRefreshJob.copyReaderRowsToStaging accessorOpcodeOf | `core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java` | `copyReaderRowsToStaging` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| LiveViewSnapshotKeyCodec.readKey family opcode switch #1 | `core/src/main/java/io/questdb/cairo/lv/LiveViewSnapshotKeyCodec.java` | `readKey` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| LiveViewSnapshotKeyCodec.readKey family opcode switch #2 | `core/src/main/java/io/questdb/cairo/lv/LiveViewSnapshotKeyCodec.java` | `readKey` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| LiveViewSnapshotKeyCodec.validateKey family opcode switch | `core/src/main/java/io/questdb/cairo/lv/LiveViewSnapshotKeyCodec.java` | `validateKey` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| LiveViewSnapshotKeyCodec.readValueSlots family opcode switch #1 | `core/src/main/java/io/questdb/cairo/lv/LiveViewSnapshotKeyCodec.java` | `readValueSlots` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| LiveViewSnapshotKeyCodec.writeKey family opcode switch #1 | `core/src/main/java/io/questdb/cairo/lv/LiveViewSnapshotKeyCodec.java` | `writeKey` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| LiveViewSnapshotKeyCodec.readValueSlots family opcode switch #2 | `core/src/main/java/io/questdb/cairo/lv/LiveViewSnapshotKeyCodec.java` | `readValueSlots` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| LiveViewSnapshotKeyCodec.writeKey family opcode switch #2 | `core/src/main/java/io/questdb/cairo/lv/LiveViewSnapshotKeyCodec.java` | `writeKey` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| LiveViewSnapshotKeyCodec.byteSizeOfType family switch | `core/src/main/java/io/questdb/cairo/lv/LiveViewSnapshotKeyCodec.java` | `byteSizeOfType` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| LiveViewSnapshotKeyCodec.byteSizeOfType accessorOf | `core/src/main/java/io/questdb/cairo/lv/LiveViewSnapshotKeyCodec.java` | `byteSizeOfType` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| LiveViewSnapshotKeyCodec.isSupportedKeyType accessorOpcodeOf | `core/src/main/java/io/questdb/cairo/lv/LiveViewSnapshotKeyCodec.java` | `isSupportedKeyType` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| LiveViewWindow.readAnchorValue writer arm | `core/src/main/java/io/questdb/cairo/lv/LiveViewWindow.java` | `readAnchorValue` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| CompiledFilterIRSerializer.isGeoHash tag switch | `core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java` | `isGeoHash` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| DecimalUtil.createDecimalConstant tag switch | `core/src/main/java/io/questdb/griffin/DecimalUtil.java` | `createDecimalConstant` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| DecimalUtil.createNullDecimalConstant tag switch | `core/src/main/java/io/questdb/griffin/DecimalUtil.java` | `createNullDecimalConstant` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| DecimalUtil.getImplicitCastFunction tag switch | `core/src/main/java/io/questdb/griffin/DecimalUtil.java` | `getImplicitCastFunction` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| DecimalUtil.load tag switch | `core/src/main/java/io/questdb/griffin/DecimalUtil.java` | `load` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| DecimalUtil.store tag switch #1 | `core/src/main/java/io/questdb/griffin/DecimalUtil.java` | `store` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| DecimalUtil.store tag switch #2 | `core/src/main/java/io/questdb/griffin/DecimalUtil.java` | `store` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| DecimalUtil.storeNonNull tag switch | `core/src/main/java/io/questdb/griffin/DecimalUtil.java` | `storeNonNull` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| DecimalUtil.storeNull tag switch | `core/src/main/java/io/questdb/griffin/DecimalUtil.java` | `storeNull` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| LoopingRecordToRowCopier.copyFromDecimal tag switch | `core/src/main/java/io/questdb/griffin/LoopingRecordToRowCopier.java` | `copyFromDecimal` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| RecordToRowCopierUtils.generateChunkedCopier writer arm | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `generateChunkedCopier` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| RecordToRowCopierUtils.hasComplexArm family switch | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `hasComplexArm` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| RecordToRowCopierUtils.transferDecimal tag switch | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `transferDecimal` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| RecordToRowCopierUtils.hasComplexArm accessorOf | `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java` | `hasComplexArm` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| SqlExecutionContextImpl.getNow tag switch | `core/src/main/java/io/questdb/griffin/SqlExecutionContextImpl.java` | `getNow` | tells the timestamp or interval units apart; reached only for that family |
| SqlOptimiser.checkSimpleIntegerColumn isIntegral | `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` | `checkSimpleIntegerColumn` | a relation-kind test; the rewrite it gates reads the NULL policy, not a value |
| SqlOptimiser.isConstantSdtCompdev isIntegralOrFloat | `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` | `isConstantSdtCompdev` | reads the constant through getDouble and rejects NaN and negatives, which covers every NULL form |
| SubsampleValidator.validateNumericType isIntegralOrFloat | `core/src/main/java/io/questdb/griffin/SubsampleValidator.java` | `validateNumericType` | a kind test only; the value column is read later by the function's own getter |
| UpdateOperatorImpl.updateOpcode family switch | `core/src/main/java/io/questdb/griffin/UpdateOperatorImpl.java` | `updateOpcode` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| UpdateOperatorImpl.appendRowUpdate writer arm | `core/src/main/java/io/questdb/griffin/UpdateOperatorImpl.java` | `appendRowUpdate` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| UpdateOperatorImpl.updateOpcode accessorOf | `core/src/main/java/io/questdb/griffin/UpdateOperatorImpl.java` | `updateOpcode` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| IntervalUtils.getIntervalType tag switch | `core/src/main/java/io/questdb/griffin/model/IntervalUtils.java` | `getIntervalType` | tells the timestamp or interval units apart; reached only for that family |
| IntervalUtils.getTimestampTypeByIntervalType tag switch | `core/src/main/java/io/questdb/griffin/model/IntervalUtils.java` | `getTimestampTypeByIntervalType` | tells the timestamp or interval units apart; reached only for that family |
| GroupByColumnSink.argTag accessorOf | `core/src/main/java/io/questdb/griffin/engine/groupby/GroupByColumnSink.java` | `argTag` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| SampleByFillRecordCursorFactory.writePrevCacheSlots tag switch | `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` | `writePrevCacheSlots` | keyed on the family arm opcode the setup guard chose (initPrevCacheSlots) |
| AsyncFilterAtom.preTouchOpcode family switch | `core/src/main/java/io/questdb/griffin/engine/table/AsyncFilterAtom.java` | `preTouchOpcode` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| AsyncFilterAtom.preTouchColumns writer arm | `core/src/main/java/io/questdb/griffin/engine/table/AsyncFilterAtom.java` | `preTouchColumns` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| AsyncFilterUtils.writeBindVarFunction family switch | `core/src/main/java/io/questdb/griffin/engine/table/AsyncFilterUtils.java` | `writeBindVarFunction` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| PushdownFilterExtractor.isNullOpPushable family switch | `core/src/main/java/io/questdb/griffin/engine/table/PushdownFilterExtractor.java` | `isNullOpPushable` | inside the SENTINEL arm of the policy switch; the accessor picks the sentinel's width |
| WindowAccumulatorDescriptor.decimalNullPayload tag switch | `core/src/main/java/io/questdb/griffin/engine/window/WindowAccumulatorDescriptor.java` | `decimalNullPayload` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| WindowAccumulatorDescriptor.decimalStateColumnType tag switch | `core/src/main/java/io/questdb/griffin/engine/window/WindowAccumulatorDescriptor.java` | `decimalStateColumnType` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| WindowAccumulatorDescriptor.isDecimalPayload tag switch | `core/src/main/java/io/questdb/griffin/engine/window/WindowAccumulatorDescriptor.java` | `isDecimalPayload` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| BetweenTimestampCursorFunctionFactory.assertComparableCursorColumn tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/BetweenTimestampCursorFunctionFactory.java` | `assertComparableCursorColumn` | tells the timestamp or interval units apart; reached only for that family |
| BetweenTimestampCursorFunctionFactory.readCursorBound tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/BetweenTimestampCursorFunctionFactory.java` | `readCursorBound` | tells the timestamp or interval units apart; reached only for that family |
| BetweenTimestampCursorFunctionFactory.resolveLeftTimestampType tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/BetweenTimestampCursorFunctionFactory.java` | `resolveLeftTimestampType` | tells the timestamp or interval units apart; reached only for that family |
| InTimestampTimestampFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InTimestampTimestampFunctionFactory.java` | `newInstance` | tells the timestamp or interval units apart; reached only for that family |
| InTimestampTimestampFunctionFactory.parseDiscreteTimestampValues tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InTimestampTimestampFunctionFactory.java` | `parseDiscreteTimestampValues` | tells the timestamp or interval units apart; reached only for that family |
| InTimestampTimestampFunctionFactory.init tag switch #1 | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InTimestampTimestampFunctionFactory.java` | `init` | tells the timestamp or interval units apart; reached only for that family |
| InTimestampTimestampFunctionFactory.init tag switch #2 | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InTimestampTimestampFunctionFactory.java` | `init` | tells the timestamp or interval units apart; reached only for that family |
| InTimestampTimestampFunctionFactory.getBool tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/InTimestampTimestampFunctionFactory.java` | `getBool` | tells the timestamp or interval units apart; reached only for that family |
| WithinGeohashFunctionFactory.getGeoHashAsLong tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bool/WithinGeohashFunctionFactory.java` | `getGeoHashAsLong` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| AvgDecimalGroupByFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/groupby/AvgDecimalGroupByFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| AvgDecimalRescaleGroupByFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/groupby/AvgDecimalRescaleGroupByFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CountDecimalGroupByFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/groupby/CountDecimalGroupByFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CountGeoHashGroupByFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/groupby/CountGeoHashGroupByFunctionFactory.java` | `newInstance` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| FirstDecimalGroupByFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/groupby/FirstDecimalGroupByFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| FirstGeoHashGroupByFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/groupby/FirstGeoHashGroupByFunctionFactory.java` | `newInstance` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| FirstNotNullDecimalGroupByFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/groupby/FirstNotNullDecimalGroupByFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| FirstNotNullGeoHashGroupByFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/groupby/FirstNotNullGeoHashGroupByFunctionFactory.java` | `newInstance` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| LastDecimalGroupByFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/groupby/LastDecimalGroupByFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| LastGeoHashGroupByFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/groupby/LastGeoHashGroupByFunctionFactory.java` | `newInstance` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| LastNotNullDecimalGroupByFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/groupby/LastNotNullDecimalGroupByFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| LastNotNullGeoHashGroupByFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/groupby/LastNotNullGeoHashGroupByFunctionFactory.java` | `newInstance` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| MaxDecimalGroupByFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MaxDecimalGroupByFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| MinDecimalGroupByFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MinDecimalGroupByFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| SumDecimalGroupByFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/groupby/SumDecimalGroupByFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| GenerateSeriesTimestampStringRecordCursorFactory.getMetadata tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/date/GenerateSeriesTimestampStringRecordCursorFactory.java` | `getMetadata` | tells the timestamp or interval units apart; reached only for that family |
| EqDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/eq/EqDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| EqGeoHashGeoHashFunctionFactory.createBinaryFunc tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/eq/EqGeoHashGeoHashFunctionFactory.java` | `createBinaryFunc` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| EqGeoHashGeoHashFunctionFactory.createConstCheckFunc tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/eq/EqGeoHashGeoHashFunctionFactory.java` | `createConstCheckFunc` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| EqNullCursorFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/eq/EqNullCursorFunctionFactory.java` | `newInstance` | tells the timestamp or interval units apart; reached only for that family |
| EqTimestampCursorFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/eq/EqTimestampCursorFunctionFactory.java` | `newInstance` | tells the timestamp or interval units apart; reached only for that family |
| CaseCommon.getCaseFunction tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/conditional/CaseCommon.java` | `getCaseFunction` | inside isGeoHash: tells the geohash widths apart; other types take the CASE function table |
| NullIfDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/conditional/NullIfDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| Constants.getGeoHashConstantWithType tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/constants/Constants.java` | `getGeoHashConstantWithType` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| IntervalConstant.getIntervalNull tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/constants/IntervalConstant.java` | `getIntervalNull` | tells the timestamp or interval units apart; reached only for that family |
| GtTimestampCursorFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/lt/GtTimestampCursorFunctionFactory.java` | `newInstance` | tells the timestamp or interval units apart; reached only for that family |
| LtDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/lt/LtDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| LtTimestampCursorFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/lt/LtTimestampCursorFunctionFactory.java` | `newInstance` | tells the timestamp or interval units apart; reached only for that family |
| Decimal128LoaderFunctionFactory.getInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/decimal/Decimal128LoaderFunctionFactory.java` | `getInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| Decimal256LoaderFunctionFactory.getInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/decimal/Decimal256LoaderFunctionFactory.java` | `getInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| Decimal64LoaderFunctionFactory.getInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/decimal/Decimal64LoaderFunctionFactory.java` | `getInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| AvgDecimalRescaleWindowFunctionFactory.newInstance tag switch #1 | `core/src/main/java/io/questdb/griffin/engine/functions/window/AvgDecimalRescaleWindowFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| AvgDecimalRescaleWindowFunctionFactory.newInstance tag switch #2 | `core/src/main/java/io/questdb/griffin/engine/functions/window/AvgDecimalRescaleWindowFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| AvgDecimalRescaleWindowFunctionFactory.writeSink tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/window/AvgDecimalRescaleWindowFunctionFactory.java` | `writeSink` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| AvgDecimalRescaleWindowFunctionFactory.writeNull tag switch #1 | `core/src/main/java/io/questdb/griffin/engine/functions/window/AvgDecimalRescaleWindowFunctionFactory.java` | `writeNull` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| AvgDecimalRescaleWindowFunctionFactory.writeNull tag switch #2 | `core/src/main/java/io/questdb/griffin/engine/functions/window/AvgDecimalRescaleWindowFunctionFactory.java` | `writeNull` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| AvgDecimalRescaleWindowFunctionFactory.writeNull tag switch #3 | `core/src/main/java/io/questdb/griffin/engine/functions/window/AvgDecimalRescaleWindowFunctionFactory.java` | `writeNull` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| AvgDecimalRescaleWindowFunctionFactory.writeNull tag switch #4 | `core/src/main/java/io/questdb/griffin/engine/functions/window/AvgDecimalRescaleWindowFunctionFactory.java` | `writeNull` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| AvgDecimalRescaleWindowFunctionFactory.writeNull tag switch #5 | `core/src/main/java/io/questdb/griffin/engine/functions/window/AvgDecimalRescaleWindowFunctionFactory.java` | `writeNull` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| AvgDecimalRescaleWindowFunctionFactory.writeNull tag switch #6 | `core/src/main/java/io/questdb/griffin/engine/functions/window/AvgDecimalRescaleWindowFunctionFactory.java` | `writeNull` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| AvgDecimalWindowFunctionFactory.newInstance tag switch #1 | `core/src/main/java/io/questdb/griffin/engine/functions/window/AvgDecimalWindowFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| AvgDecimalWindowFunctionFactory.newInstance tag switch #2 | `core/src/main/java/io/questdb/griffin/engine/functions/window/AvgDecimalWindowFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CountDecimalWindowFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/window/CountDecimalWindowFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| FirstValueDecimalWindowFunctionFactory.newInstance tag switch #1 | `core/src/main/java/io/questdb/griffin/engine/functions/window/FirstValueDecimalWindowFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| FirstValueDecimalWindowFunctionFactory.newInstance tag switch #2 | `core/src/main/java/io/questdb/griffin/engine/functions/window/FirstValueDecimalWindowFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| LagDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/window/LagDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| LastValueDecimalWindowFunctionFactory.newInstance tag switch #1 | `core/src/main/java/io/questdb/griffin/engine/functions/window/LastValueDecimalWindowFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| LastValueDecimalWindowFunctionFactory.newInstance tag switch #2 | `core/src/main/java/io/questdb/griffin/engine/functions/window/LastValueDecimalWindowFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| LeadDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/window/LeadDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| MaxDecimalWindowFunctionFactory.newMaxMinInstance tag switch #1 | `core/src/main/java/io/questdb/griffin/engine/functions/window/MaxDecimalWindowFunctionFactory.java` | `newMaxMinInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| MaxDecimalWindowFunctionFactory.newMaxMinInstance tag switch #2 | `core/src/main/java/io/questdb/griffin/engine/functions/window/MaxDecimalWindowFunctionFactory.java` | `newMaxMinInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| NthValueDecimalWindowFunctionFactory.newInstance tag switch #1 | `core/src/main/java/io/questdb/griffin/engine/functions/window/NthValueDecimalWindowFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| NthValueDecimalWindowFunctionFactory.newInstance tag switch #2 | `core/src/main/java/io/questdb/griffin/engine/functions/window/NthValueDecimalWindowFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| SumDecimalWindowFunctionFactory.newInstance tag switch #1 | `core/src/main/java/io/questdb/griffin/engine/functions/window/SumDecimalWindowFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| SumDecimalWindowFunctionFactory.newInstance tag switch #2 | `core/src/main/java/io/questdb/griffin/engine/functions/window/SumDecimalWindowFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| SumDecimalWindowFunctionFactory.newInstance tag switch #3 | `core/src/main/java/io/questdb/griffin/engine/functions/window/SumDecimalWindowFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| RndDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/rnd/RndDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| RndGeoHashFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/rnd/RndGeoHashFunctionFactory.java` | `newInstance` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| CastByteToDecimalFunctionFactory.newUnscaledInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastByteToDecimalFunctionFactory.java` | `newUnscaledInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDecimalToByteFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDecimalToByteFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDecimalToByteFunctionFactory.newUnscaledInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDecimalToByteFunctionFactory.java` | `newUnscaledInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDecimalToDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDecimalToDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDecimalToDecimalFunctionFactory.newUnscaledInstance tag switch #1 | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDecimalToDecimalFunctionFactory.java` | `newUnscaledInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDecimalToDecimalFunctionFactory.newUnscaledInstance tag switch #2 | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDecimalToDecimalFunctionFactory.java` | `newUnscaledInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDecimalToDoubleFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDecimalToDoubleFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDecimalToFloatFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDecimalToFloatFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDecimalToIntFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDecimalToIntFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDecimalToIntFunctionFactory.newUnscaledInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDecimalToIntFunctionFactory.java` | `newUnscaledInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDecimalToLongFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDecimalToLongFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDecimalToLongFunctionFactory.newUnscaledInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDecimalToLongFunctionFactory.java` | `newUnscaledInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDecimalToShortFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDecimalToShortFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDecimalToShortFunctionFactory.newUnscaledInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDecimalToShortFunctionFactory.java` | `newUnscaledInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDecimalToStrFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDecimalToStrFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDecimalToVarcharFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDecimalToVarcharFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastDoubleToDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastDoubleToDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastFloatToDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastFloatToDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastGeoHashToGeoHashFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastGeoHashToGeoHashFunctionFactory.java` | `newInstance` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| CastGeoHashToGeoHashFunctionFactory.getCastGeoHashToGeoHashFunction tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastGeoHashToGeoHashFunctionFactory.java` | `getCastGeoHashToGeoHashFunction` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| CastIntToDecimalFunctionFactory.newUnscaledInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastIntToDecimalFunctionFactory.java` | `newUnscaledInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastLongToDecimalFunctionFactory.newUnscaledInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastLongToDecimalFunctionFactory.java` | `newUnscaledInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastLongToGeoHashFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastLongToGeoHashFunctionFactory.java` | `newInstance` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| CastShortToDecimalFunctionFactory.newUnscaledInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastShortToDecimalFunctionFactory.java` | `newUnscaledInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastStrToDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastStrToDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| CastVarcharToDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/cast/CastVarcharToDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| BindVariableServiceImpl.setGeoHash0 tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/bind/BindVariableServiceImpl.java` | `setGeoHash0` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| AbsDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/math/AbsDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| AddDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/math/AddDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| GreatestNumericFunctionFactory.getDecimalGreatestFunction tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/math/GreatestNumericFunctionFactory.java` | `getDecimalGreatestFunction` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| LeastNumericFunctionFactory.getDecimalLeastFunction tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/math/LeastNumericFunctionFactory.java` | `getDecimalLeastFunction` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| NegDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/math/NegDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| RemDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/math/RemDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| SignDecimalFunctionFactory.newInstance tag switch | `core/src/main/java/io/questdb/griffin/engine/functions/math/SignDecimalFunctionFactory.java` | `newInstance` | reached only for a DECIMAL type: the switch tells the decimal widths apart |
| RecordComparatorCompiler.poolFieldArtifacts writer arm | `core/src/main/java/io/questdb/griffin/engine/orderby/RecordComparatorCompiler.java` | `poolFieldArtifacts` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| SortKeyEncoder.appendKeyBytes writer arm | `core/src/main/java/io/questdb/griffin/engine/orderby/SortKeyEncoder.java` | `appendKeyBytes` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| SortKeyEncoder.encodeFixed8 writer arm | `core/src/main/java/io/questdb/griffin/engine/orderby/SortKeyEncoder.java` | `encodeFixed8` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| SortKeyEncoder.encodeFixedColumn writer arm | `core/src/main/java/io/questdb/griffin/engine/orderby/SortKeyEncoder.java` | `encodeFixedColumn` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| SortKeyEncoder.encodeFixedWideBatch writer arm | `core/src/main/java/io/questdb/griffin/engine/orderby/SortKeyEncoder.java` | `encodeFixedWideBatch` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| SortKeyMaterializingRecordCursor.materializeOpcode family switch | `core/src/main/java/io/questdb/griffin/engine/orderby/SortKeyMaterializingRecordCursor.java` | `materializeOpcode` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| SortKeyMaterializingRecordCursor.appendValue writer arm | `core/src/main/java/io/questdb/griffin/engine/orderby/SortKeyMaterializingRecordCursor.java` | `appendValue` | keyed on an opcode chosen at setup by a family or copier switch, which is this table's row for the type |
| SortKeyMaterializingRecordCursor.materializeOpcode accessorOf | `core/src/main/java/io/questdb/griffin/engine/orderby/SortKeyMaterializingRecordCursor.java` | `materializeOpcode` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| CopyExportRequestTask.getRequiredAlignmentForSimd family switch | `core/src/main/java/io/questdb/cutlass/parquet/CopyExportRequestTask.java` | `getRequiredAlignmentForSimd` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| CopyExportRequestTask.getRequiredAlignmentForSimd accessorOf | `core/src/main/java/io/questdb/cutlass/parquet/CopyExportRequestTask.java` | `getRequiredAlignmentForSimd` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| HybridColumnMaterializer.exportOpcode family switch | `core/src/main/java/io/questdb/cutlass/parquet/HybridColumnMaterializer.java` | `exportOpcode` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| HybridColumnMaterializer.writeColumnValue writer arm | `core/src/main/java/io/questdb/cutlass/parquet/HybridColumnMaterializer.java` | `writeColumnValue` | the opcode comes from a closed switch at setup (printOpcode, csvOpcode, jsonOpcode, outColumnOpcode, exportOpcode), which refuses an unhandled type; ProtocolOpcodeCoverageTest covers those |
| HybridColumnMaterializer.writeComputedValue writer arm | `core/src/main/java/io/questdb/cutlass/parquet/HybridColumnMaterializer.java` | `writeComputedValue` | the opcode comes from a closed switch at setup (printOpcode, csvOpcode, jsonOpcode, outColumnOpcode, exportOpcode), which refuses an unhandled type; ProtocolOpcodeCoverageTest covers those |
| HybridColumnMaterializer.addColumnData accessorOpcodeOf | `core/src/main/java/io/questdb/cutlass/parquet/HybridColumnMaterializer.java` | `addColumnData` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| HybridColumnMaterializer.exportOpcode accessorOf | `core/src/main/java/io/questdb/cutlass/parquet/HybridColumnMaterializer.java` | `exportOpcode` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| ParquetExportMode.determineExportMode accessorOpcodeOf | `core/src/main/java/io/questdb/cutlass/parquet/ParquetExportMode.java` | `determineExportMode` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| ParquetExportMode.hasComputedBinaryColumn accessorOpcodeOf | `core/src/main/java/io/questdb/cutlass/parquet/ParquetExportMode.java` | `hasComputedBinaryColumn` | moves or sizes bytes by the accessor family's width; no NULL test and no arithmetic |
| PGPipelineEntry.outRecord writer arm | `core/src/main/java/io/questdb/cutlass/pgwire/PGPipelineEntry.java` | `outRecord` | the opcode comes from a closed switch at setup (printOpcode, csvOpcode, jsonOpcode, outColumnOpcode, exportOpcode), which refuses an unhandled type; ProtocolOpcodeCoverageTest covers those |
| ExportQueryProcessor.putValue writer arm | `core/src/main/java/io/questdb/cutlass/http/processors/ExportQueryProcessor.java` | `putValue` | the opcode comes from a closed switch at setup (printOpcode, csvOpcode, jsonOpcode, outColumnOpcode, exportOpcode), which refuses an unhandled type; ProtocolOpcodeCoverageTest covers those |
| JsonQueryProcessorState.doQueryRecord writer arm | `core/src/main/java/io/questdb/cutlass/http/processors/JsonQueryProcessorState.java` | `doQueryRecord` | the opcode comes from a closed switch at setup (printOpcode, csvOpcode, jsonOpcode, outColumnOpcode, exportOpcode), which refuses an unhandled type; ProtocolOpcodeCoverageTest covers those |
| QwpColumnTypeMapper.toWireType tag switch | `core/src/main/java/io/questdb/cutlass/qwp/codec/QwpColumnTypeMapper.java` | `toWireType` | the array element switch of the wire mapping: reached only for an array, tells the element types apart |
| QwpResultBatchBuffer.appendPageFrame tag switch | `core/src/main/java/io/questdb/cutlass/qwp/codec/QwpResultBatchBuffer.java` | `appendPageFrame` | keyed on the opcode appendOpcode chose at setup, a wire-kind switch ProtocolOpcodeCoverageTest covers |
| QwpResultBatchBuffer.appendCell tag switch | `core/src/main/java/io/questdb/cutlass/qwp/codec/QwpResultBatchBuffer.java` | `appendCell` | keyed on the opcode appendOpcode chose at setup, a wire-kind switch ProtocolOpcodeCoverageTest covers |
| LineTcpEventBuffer.addGeoHash tag switch | `core/src/main/java/io/questdb/cutlass/line/tcp/LineTcpEventBuffer.java` | `addGeoHash` | reached only for a GEOHASH type: the switch tells the geohash widths apart |
| col_type::new_decimal match | `core/rust/qdb-core/src/col_type.rs` | `new_decimal` | reached only for a DECIMAL tag: the match tells the decimal widths apart |
| row_groups::time_unit_pow10 match | `core/rust/qdbr/src/parquet_read/row_groups.rs` | `time_unit_pow10` | tells the timestamp units apart; reached only for a timestamp tag |
| row_groups::convert_decimal_in_place match | `core/rust/qdbr/src/parquet_read/row_groups.rs` | `convert_decimal_in_place` | reached only for a DECIMAL tag: the match tells the decimal widths apart |
| row_groups::decimal_null_i128 match | `core/rust/qdbr/src/parquet_read/row_groups.rs` | `decimal_null_i128` | reached only for a DECIMAL tag: the match tells the decimal widths apart |
| row_groups::convert_decimal_narrowing match | `core/rust/qdbr/src/parquet_read/row_groups.rs` | `convert_decimal_narrowing` | reached only for a DECIMAL tag: the match tells the decimal widths apart |
| row_groups::convert_fixed_to_decimal match | `core/rust/qdbr/src/parquet_read/row_groups.rs` | `convert_fixed_to_decimal` | reached only for a DECIMAL tag: the match tells the decimal widths apart |
| row_groups::null_i64_for_decimal match | `core/rust/qdbr/src/parquet_read/row_groups.rs` | `null_i64_for_decimal` | reached only for a DECIMAL tag: the match tells the decimal widths apart |
| row_groups::decimal_tag_size match | `core/rust/qdbr/src/parquet_read/row_groups.rs` | `decimal_tag_size` | reached only for a DECIMAL tag: the match tells the decimal widths apart |
| decimal::decode_fixed_decimal_dict_mode match #1 | `core/rust/qdbr/src/parquet_read/decode/decimal.rs` | `decode_fixed_decimal_dict_mode` | reached only for a DECIMAL tag: the match tells the decimal widths apart |
| decimal::decode_fixed_decimal_dict_mode match #2 | `core/rust/qdbr/src/parquet_read/decode/decimal.rs` | `decode_fixed_decimal_dict_mode` | reached only for a DECIMAL tag: the match tells the decimal widths apart |
| decimal::decode_byte_array_decimal_with_slicer_mode match | `core/rust/qdbr/src/parquet_read/decode/decimal.rs` | `decode_byte_array_decimal_with_slicer_mode` | reached only for a DECIMAL tag: the match tells the decimal widths apart |
| decimal::decode_fixed_decimal_with_slicer_mode match #1 | `core/rust/qdbr/src/parquet_read/decode/decimal.rs` | `decode_fixed_decimal_with_slicer_mode` | reached only for a DECIMAL tag: the match tells the decimal widths apart |
| decimal::decode_fixed_decimal_with_slicer_mode match #2 | `core/rust/qdbr/src/parquet_read/decode/decimal.rs` | `decode_fixed_decimal_with_slicer_mode` | reached only for a DECIMAL tag: the match tells the decimal widths apart |
<!-- sites: end -->

## 6. Manual list

Each entry names a site, what to decide there, and why no build error, test or refusal reaches it.
The list may only shrink: a kit path or a refusal added later moves a site off it. Every run lists
the entries as `manual` items until the author ticks them in a copy of this list (`- [x]`) and
passes the copy with `--manual-done`.

<!-- manual: start -->
1. LoopingRecordSink.copyColumn tag switch (`core/src/main/java/io/questdb/cairo/LoopingRecordSink.java`, `copyColumn`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm refuses a tag it does not list, at run time unless a setup path reaches it; no kit path is mapped to it yet.
2. CoveredColumnDecoder.writeCoveredRow tag switch (`core/src/main/java/io/questdb/cairo/sql/CoveredColumnDecoder.java`, `writeCoveredRow`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm refuses a tag it does not list, at run time unless a setup path reaches it; no kit path is mapped to it yet.
3. CoveredColumnDecoder.writeFixedWidthCovered tag switch (`core/src/main/java/io/questdb/cairo/sql/CoveredColumnDecoder.java`, `writeFixedWidthCovered`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm answers silently for a tag it does not list.
4. PostingIndexWriter.writeVarValue tag switch (`core/src/main/java/io/questdb/cairo/idx/PostingIndexWriter.java`, `writeVarValue`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm answers silently for a tag it does not list.
5. CompiledFilterIRSerializer.bindVariableTypeCode family switch (`core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java`, `bindVariableTypeCode`): decide whether the type behaves as its family's namesake here; if not, give it an arm. Chooses the JIT operand by accessor; signed compares and the sentinel NULL test would be wrong for an unsigned or never-null look-alike; kit path sql.filter_lt runs the parallel JIT mode.
6. CompiledFilterIRSerializer.columnTypeCode family switch (`core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java`, `columnTypeCode`): decide whether the type behaves as its family's namesake here; if not, give it an arm. Chooses the JIT operand by accessor; signed compares and the sentinel NULL test would be wrong for an unsigned or never-null look-alike; kit path sql.filter_lt runs the parallel JIT mode.
7. CompiledFilterIRSerializer.isGenuineIntegerType family switch (`core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java`, `isGenuineIntegerType`): decide whether the type behaves as its family's namesake here; if not, give it an arm. Chooses the JIT operand by accessor; signed compares and the sentinel NULL test would be wrong for an unsigned or never-null look-alike; kit path sql.filter_lt runs the parallel JIT mode.
8. CompiledFilterIRSerializer.i4NullOf family switch (`core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java`, `i4NullOf`): decide whether the type behaves as its family's namesake here; if not, give it an arm. Decides the JIT NULL value or orderability by accessor; kit path sql.filter_lt runs the parallel JIT mode.
9. CompiledFilterIRSerializer.isNumeric family switch (`core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java`, `isNumeric`): decide whether the type behaves as its family's namesake here; if not, give it an arm. Chooses the JIT operand by accessor; signed compares and the sentinel NULL test would be wrong for an unsigned or never-null look-alike; kit path sql.filter_lt runs the parallel JIT mode.
10. CompiledFilterIRSerializer.isWidthSensitiveType family switch (`core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java`, `isWidthSensitiveType`): decide whether the type behaves as its family's namesake here; if not, give it an arm. Chooses the JIT operand by accessor; signed compares and the sentinel NULL test would be wrong for an unsigned or never-null look-alike; kit path sql.filter_lt runs the parallel JIT mode.
11. CompiledFilterIRSerializer.getPredicatePriority0 family switch (`core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java`, `getPredicatePriority0`): decide whether the type behaves as its family's namesake here; if not, give it an arm. Chooses the JIT operand by accessor; signed compares and the sentinel NULL test would be wrong for an unsigned or never-null look-alike; kit path sql.filter_lt runs the parallel JIT mode.
12. CompiledFilterIRSerializer.rejectOrderingComparison family switch (`core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java`, `rejectOrderingComparison`): decide whether the type behaves as its family's namesake here; if not, give it an arm. Decides the JIT NULL value or orderability by accessor; kit path sql.filter_lt runs the parallel JIT mode.
13. CompiledFilterIRSerializer.updateType family switch (`core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java`, `updateType`): decide whether the type behaves as its family's namesake here; if not, give it an arm. Chooses the JIT operand by accessor; signed compares and the sentinel NULL test would be wrong for an unsigned or never-null look-alike; kit path sql.filter_lt runs the parallel JIT mode.
14. CompiledFilterIRSerializer.serializeConstant accessorOf (`core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java`, `serializeConstant`): decide whether the type behaves as its family's namesake here; if not, give it an arm. Decides the JIT NULL value or orderability by accessor; kit path sql.filter_lt runs the parallel JIT mode.
15. FunctionParser.createFunction tag switch (`core/src/main/java/io/questdb/griffin/FunctionParser.java`, `createFunction`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm answers silently for a tag it does not list.
16. FunctionParser.createImplicitCastOrNull tag switch (`core/src/main/java/io/questdb/griffin/FunctionParser.java`, `createImplicitCastOrNull`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm answers silently for a tag it does not list.
17. FunctionParser.functionToConstant0 tag switch (`core/src/main/java/io/questdb/griffin/FunctionParser.java`, `functionToConstant0`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm answers silently for a tag it does not list.
18. SqlCodeGenerator.createSymbolShortCircuit tag switch (`core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`, `createSymbolShortCircuit`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm answers silently for a tag it does not list.
19. SqlCodeGenerator.validateSubQueryColumnAndGetGetter tag switch (`core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`, `validateSubQueryColumnAndGetGetter`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm refuses a tag it does not list, at run time unless a setup path reaches it; no kit path is mapped to it yet.
20. JsonUnnestSource.JsonUnnestSource tag switch (`core/src/main/java/io/questdb/griffin/engine/join/JsonUnnestSource.java`, `JsonUnnestSource`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm answers silently for a tag it does not list.
21. GroupByColumnSink.put tag switch (`core/src/main/java/io/questdb/griffin/engine/groupby/GroupByColumnSink.java`, `put`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm refuses a tag it does not list, at run time unless a setup path reaches it; no kit path is mapped to it yet.
22. GroupByColumnSink.putAt tag switch (`core/src/main/java/io/questdb/griffin/engine/groupby/GroupByColumnSink.java`, `putAt`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm refuses a tag it does not list, at run time unless a setup path reaches it; no kit path is mapped to it yet.
23. ParquetRowGroupFilter.prepareFilterListImpl tag switch #1 (`core/src/main/java/io/questdb/griffin/engine/table/ParquetRowGroupFilter.java`, `prepareFilterListImpl`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm answers silently for a tag it does not list.
24. ParquetRowGroupFilter.prepareFilterListImpl tag switch #2 (`core/src/main/java/io/questdb/griffin/engine/table/ParquetRowGroupFilter.java`, `prepareFilterListImpl`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm answers silently for a tag it does not list.
25. ParquetRowGroupFilter.prepareFilterListImpl tag switch #3 (`core/src/main/java/io/questdb/griffin/engine/table/ParquetRowGroupFilter.java`, `prepareFilterListImpl`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm answers silently for a tag it does not list.
26. WindowAccumulatorDescriptor.contributionKindFor tag switch (`core/src/main/java/io/questdb/griffin/engine/window/WindowAccumulatorDescriptor.java`, `contributionKindFor`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm answers silently for a tag it does not list.
27. WindowAccumulatorDescriptor.resetState tag switch (`core/src/main/java/io/questdb/griffin/engine/window/WindowAccumulatorDescriptor.java`, `resetState`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm answers silently for a tag it does not list.
28. WindowAccumulatorDescriptor.isLongPayload tag switch (`core/src/main/java/io/questdb/griffin/engine/window/WindowAccumulatorDescriptor.java`, `isLongPayload`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm answers silently for a tag it does not list.
29. WindowAccumulatorDescriptor.isWidenedToDouble tag switch (`core/src/main/java/io/questdb/griffin/engine/window/WindowAccumulatorDescriptor.java`, `isWidenedToDouble`): decide whether the type needs an arm of its own; its tag takes the default arm. The default arm answers silently for a tag it does not list.
30. SortKeyEncoder.keyShapeOf family switch (`core/src/main/java/io/questdb/griffin/engine/orderby/SortKeyEncoder.java`, `keyShapeOf`): decide whether the type behaves as its family's namesake here; if not, give it an arm. Encodes a sort key by accessor; a type ordered unlike its family (an unsigned tier) needs its own encoding, which compareOpcode does not gate; kit paths sql.order_asc and sql.order_desc reach it.
31. SortKeyEncoder.SortKeyEncoder accessorOpcodeOf (`core/src/main/java/io/questdb/griffin/engine/orderby/SortKeyEncoder.java`, `SortKeyEncoder`): decide whether the type behaves as its family's namesake here; if not, give it an opcode. Encodes a sort key by accessor; a type ordered unlike its family (an unsigned tier) needs its own encoding, which compareOpcode does not gate; kit paths sql.order_asc and sql.order_desc reach it.
32. SortKeyEncoder.SortKeyEncoder accessorOf (`core/src/main/java/io/questdb/griffin/engine/orderby/SortKeyEncoder.java`, `SortKeyEncoder`): decide whether the type behaves as its family's namesake here; if not, give it an arm. Encodes a sort key by accessor; a type ordered unlike its family (an unsigned tier) needs its own encoding, which compareOpcode does not gate; kit paths sql.order_asc and sql.order_desc reach it.
33. CairoTextWriter.initWriterAndOverrideImportTypes wire-kind switch (`core/src/main/java/io/questdb/cutlass/text/CairoTextWriter.java`, `initWriterAndOverrideImportTypes`): decide which import type the type's wire kind takes; the default arm keeps the detected one. The CSV import's switch over wire kinds has a default arm that keeps the column as detected; kit path ingest.csv reaches it.
34. ParallelCsvFileImporter.initWriterAndOverrideImportMetadata wire-kind switch (`core/src/main/java/io/questdb/cutlass/text/ParallelCsvFileImporter.java`, `initWriterAndOverrideImportMetadata`): decide which import type the type's wire kind takes; the default arm keeps the detected one. The CSV import's switch over wire kinds has a default arm that keeps the column as detected; kit path ingest.csv reaches it.
35. TextMetadataParser.createImportedType wire-kind switch (`core/src/main/java/io/questdb/cutlass/text/TextMetadataParser.java`, `createImportedType`): decide which import type the type's wire kind takes; the default arm keeps the detected one. The CSV import's switch over wire kinds has a default arm that keeps the column as detected; kit path ingest.csv reaches it.
36. decode::decode_int32_dispatch match (`core/rust/qdbr/src/parquet_read/decode.rs`, `decode_int32_dispatch`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A Parquet page decoder by physical type: its wildcard arm answers "not decoded here" for a tag it does not list, so a new tag reaches the next decoder or none without a listing.
37. decode::decode_int64_dispatch match (`core/rust/qdbr/src/parquet_read/decode.rs`, `decode_int64_dispatch`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A Parquet page decoder by physical type: its wildcard arm answers "not decoded here" for a tag it does not list, so a new tag reaches the next decoder or none without a listing.
38. decode::decode_fixed_len_dispatch match #1 (`core/rust/qdbr/src/parquet_read/decode.rs`, `decode_fixed_len_dispatch`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A Parquet page decoder by physical type: its wildcard arm answers "not decoded here" for a tag it does not list, so a new tag reaches the next decoder or none without a listing.
39. decode::decode_fixed_len_dispatch match #2 (`core/rust/qdbr/src/parquet_read/decode.rs`, `decode_fixed_len_dispatch`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A Parquet page decoder by physical type: its wildcard arm answers "not decoded here" for a tag it does not list, so a new tag reaches the next decoder or none without a listing.
40. decode::decode_byte_array_dispatch match #2 (`core/rust/qdbr/src/parquet_read/decode.rs`, `decode_byte_array_dispatch`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A Parquet page decoder by physical type: its wildcard arm answers "not decoded here" for a tag it does not list, so a new tag reaches the next decoder or none without a listing.
41. decode::decode_byte_array_dispatch match #3 (`core/rust/qdbr/src/parquet_read/decode.rs`, `decode_byte_array_dispatch`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A Parquet page decoder by physical type: its wildcard arm answers "not decoded here" for a tag it does not list, so a new tag reaches the next decoder or none without a listing.
42. decode::decode_int96_dispatch match (`core/rust/qdbr/src/parquet_read/decode.rs`, `decode_int96_dispatch`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A Parquet page decoder by physical type: its wildcard arm answers "not decoded here" for a tag it does not list, so a new tag reaches the next decoder or none without a listing.
43. decode::decode_double_dispatch match (`core/rust/qdbr/src/parquet_read/decode.rs`, `decode_double_dispatch`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A Parquet page decoder by physical type: its wildcard arm answers "not decoded here" for a tag it does not list, so a new tag reaches the next decoder or none without a listing.
44. decode::decode_other_fixed_dispatch match (`core/rust/qdbr/src/parquet_read/decode.rs`, `decode_other_fixed_dispatch`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A Parquet page decoder by physical type: its wildcard arm answers "not decoded here" for a tag it does not list, so a new tag reaches the next decoder or none without a listing.
45. row_groups::post_convert match (`core/rust/qdbr/src/parquet_read/row_groups.rs`, `post_convert`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A wildcard arm takes a tag added to the Rust enum without a listing.
46. row_groups::plan_decode_conversion match (`core/rust/qdbr/src/parquet_read/row_groups.rs`, `plan_decode_conversion`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A wildcard arm takes a tag added to the Rust enum without a listing.
47. encode::encode_boolean_dispatch match (`core/rust/qdbr/src/parquet_write/encode.rs`, `encode_boolean_dispatch`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A wildcard arm takes a tag added to the Rust enum without a listing.
48. encode::encode_int32_dispatch match (`core/rust/qdbr/src/parquet_write/encode.rs`, `encode_int32_dispatch`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A wildcard arm takes a tag added to the Rust enum without a listing.
49. encode::encode_int64_dispatch match (`core/rust/qdbr/src/parquet_write/encode.rs`, `encode_int64_dispatch`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A wildcard arm takes a tag added to the Rust enum without a listing.
50. encode::encode_byte_array_dispatch match (`core/rust/qdbr/src/parquet_write/encode.rs`, `encode_byte_array_dispatch`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A wildcard arm takes a tag added to the Rust enum without a listing.
51. encode::encode_fixed_len_dispatch match (`core/rust/qdbr/src/parquet_write/encode.rs`, `encode_fixed_len_dispatch`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A wildcard arm takes a tag added to the Rust enum without a listing.
52. schema::column_type_to_parquet_type match #2 (`core/rust/qdbr/src/parquet_write/schema.rs`, `column_type_to_parquet_type`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A wildcard arm takes a tag added to the Rust enum without a listing.
53. schema::validate_encoding match (`core/rust/qdbr/src/parquet_write/schema.rs`, `validate_encoding`): decide whether the type needs an arm of its own; its tag takes the wildcard arm. A wildcard arm takes a tag added to the Rust enum without a listing.
<!-- manual: end -->

## 7. The worklist format

`worklist.md` starts with a header (the facts file and its sha256, the tree's branch and commit, the
run's time and length, the item counts, a note for every step that did not run) and then one section
per group, in this order: `build-java`, `build-rust`, `build-c`, `refusal`, `kit`, `coverage`,
`manual`. An empty group prints its heading with `(0)`. One line per item:

```text
- [ ] <decision> | <location> | <message>[ | site: <label>]
```

| field | values |
|---|---|
| decision | `name-yourself` (name the type in a switch or a refusal group), `implement-pair` (write the code an admitted pair, opcode or function needs), `add-writer-arm` (an arm keyed by the type's wire kind, NULL policy or order), `fill-driver-answer` (an answer of the type driver), `declare-or-admit` (a guarded site refused the type: declare it in `refused_sites` or add the type's arm), `manual` (an entry of the manual list) |
| location | `` `path:line` `` from the repository root; `` `kit:<path>@<mode>#<value row>` `` for a kit failure; `README "Manual list", item <n>` |
| message | the first line of the compiler's, the test's or the refusal's text, ASCII, at most 200 characters, `\|` written as `/` |
| site | the label of the site's row in `sites.tsv`, or `unmapped` when no row matches, which is a defect of the site map |

A refusal maps to the site it names, and the item sits at the site's method.

A kit failure maps to the row whose kept refusal text it holds (of several rows that keep the same
text, the one whose method the kit's value row names, `setBoolean`:
`BindVariableServiceImpl.setBoolean0`), else to the row whose instrument names its path, else to the
layer the path checks: an HTTP export, PG wire or QWP egress path to its writer's wire-kind switch
(`add-writer-arm`), the CSV import to `TypeManager.getTypeAdapter`, `sql.cast` to the type's
registration in `TypeDrivers.find` (`implement-pair`), and every other path to that registration as
`fill-driver-answer`: the type driver's NULL, column function or relations.

A coverage failure maps to the site its message names (a kept refusal text, or `<method>: <type> is
not handled`) and sits at that site's method; else it maps to the site its test checks and sits at
the test, so each failing test is an item: the copier test of `RelationCoverageTest` to
`RecordToRowCopierUtils.copyOpcode`, its CASE and UNION tests to their pair switches, its cast test,
`FunctionReachTest`'s later-type test, `RelationRulesTest` and `TypeDriverTest` to the type's
registration; else to the first row its instrument names.

There is one item per location and decision; a compiler error repeated by several builds is one
item. The kit and the coverage tests run only when every build group is empty, since a build error
hides the tests behind it.

Exit codes:

| code | meaning |
|---|---|
| 0 | the worklist is empty: every build is clean and the kit and the coverage tests pass with the type declared |
| 1 | the worklist has an item, or a step was skipped |
| 2 | the facts file is invalid, an anchor is missing or appears twice, or the command line is wrong |
| 3 | a step failed in a way the tool cannot parse; the message names the step and its log |

`sites.tsv` is the site map the tool reads: one row per site, with its label, kind, file and
method, the refusal text it raises, the decision an item at it asks for, and its instrument. It is
generated from the instrument table and committed with the tool; the kit reads its guarded sites
from it too.

## 8. Acceptance

A type PR is done when the run exits 0: every build is clean, the kit and the coverage tests pass
with the type in `later-types.txt` (its declared refusals checked as refusals), and the manual list
is worked.

The tool's own acceptance: an author who has not added these types before adds two look-alike
types, a never-null INT and an unsigned INT, from the worklist alone, on a throwaway branch.
Measured: the decisions made outside the worklist, the sites touched against an earlier hand-made
addition of the same types, and the time taken. The numbers of that run: not measured yet.

## 9. Limits

The tool lists where, not what. It does not write how the type prints, parses, compares and widens,
the pairs its relations admit, or its function bodies, and it does not rebuild the committed native
libraries: a CI workflow does, on request. Its native build checks that the code compiles.

Not yet tested, stated plainly: no committed test drives a type through the family-arm guard at the
sites (the guard itself is tested with stub type drivers), and the factories that refuse a type at a
guarded site must release what they already own before they throw, which no committed test covers
either, because no existing type reaches these refusals and no database user can. A type PR is the
first to run them in CI: the kit checks each declared refusal under the memory-leak checker, so its
author should expect, and read, those results.

## 10. The cost of a type

The number of places outside a type's own type driver and registration lines that adding it edits:

- a look-alike type, one that stores, reads and orders like an existing type, names itself in the
  pair switches only (5);
- a new representation adds its wire kind (its switches and writer arms) and, for a new order, its
  compare arm;
- a new NULL policy extends the 18 policy switches once;
- a guarded-site refusal declared in `refused_sites` costs no edit; a site where the type is
  admitted instead, by its own arm, is listed apart from this count.
