# The add-a-type tool

## 1. What this is

Adding a column type to QuestDB means declaring its facts and then making a decision at every
place the engine handles a type in its own way. This tool lists those places for one new type, in
one file, `worklist.md`, from two sources:

- `type_probe.py run` registers the type from a facts file, writes its type driver, builds Java,
  Rust and C, and runs the conformance kit and the coverage tests with the type declared: every
  compiler error and every test failure is an item;
- `audit.py` reads the code on every run and finds every place that decides by a column type, in
  every form (section 5). The places a type like the new one's namesake must decide are items too,
  until the code names the type or `places.tsv` records a decision for them.

It lists where, not what: how the type prints, parses, compares and widens is still the author's
to write, at the places the worklist names.

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

Working an item means one of six decisions (section 6): naming the type in a switch, implementing
a pair the type's relations admit, adding a writer arm, writing a type driver answer, declaring or
admitting a guarded-site refusal, or deciding a place the audit lists and recording the decision in
`places.tsv`. A guarded-site refusal is declared by adding the site's label to `refused_sites` in
the facts file, which costs no edit; it is admitted by adding the type's arm at the site.

The audit runs alone as well, in about ten seconds:

```bash
python3 utils/type-probe/type_probe.py audit summary          # places per form, problems of places.tsv
python3 utils/type-probe/type_probe.py audit like INT         # the places a type like INT must decide
python3 utils/type-probe/type_probe.py audit like INT --type NN_INT --rows   # places.tsv rows to fill in
python3 utils/type-probe/type_probe.py audit check            # places.tsv against the code; exit 1 on a problem
```

Prerequisites: Python 3.11 or newer, a JDK, Maven with the offline cache of the project, cargo,
CMake with a C++ compiler, and the Java client the kit drives, built and installed as the
repository's `CLAUDE.md` describes (the tool runs Maven with `-P local-client`, and the kit with
`build-rust-library` as well, which builds the Rust library with cargo).

Options of `run`: `--out DIR` (default `utils/target/type-probe/<NAME>/`, ignored by git),
`--skip-native` (no cargo, no CMake, and the kit runs on the native libraries an earlier run built,
or on the committed ones; the CMake step builds the bundled zlib in place, which leaves the
`core/src/main/c/share/zlib` submodule dirty), `--skip-kit` (no kit and no coverage tests). A run
with a skipped step never exits 0.

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
refused_sites = []          # guarded sites the type is refused at on purpose (section 4)
```

The accessor family names the type's namesake: the existing type the family is named after (INT
for a type on INT's accessor), whose code the new type is read and written through. The audit lists
the places a type like the namesake must decide (section 5).

INT runs every kit path because the kit holds a recording of each for it. A new type has no
recording, so it runs every path against an invariant instead, and `init` writes the same line.
The invariants take their expectations from the rows as written and the declared NULL policy,
and handle NULL as every existing type does:

- `sql.filter_eq`, `_ne`, `_lt`, `_ge` run once per distinct value (a new type has no literal):
  `=` selects that value's rows, `!=` every other row including the NULL rows, `<` and `>=` split
  the other values by the tier's order and select no NULL row;
- `sql.order_asc`, `_desc` sort NULL lowest (first ascending, last descending), and highest for a
  float tier, as FLOAT and DOUBLE sort NaN;
- `sql.group_by`, `sql.latest_on`: one group or partition per distinct value, the NULL rows in
  one; `sql.join_*`: a NULL key joins a NULL key, an outer join's missing side is the type's NULL;
- `sql.union_null`, `sql.lag`, `sql.sample_by`, `sql.first_last`, `sql.first_not_null`: a NULL
  branch or a missing value is the type's NULL, `first` and `last` keep NULLs, the `_not_null`
  forms skip them;
- `sql.insert_convert`: `INSERT ... SELECT` through every conversion the copiers admit, into and
  out of the type, one row per statement, under each of the three copiers (single-method, chunked,
  looping); they must agree, and give what master's implicit casts give an existing pair (NULL to
  NULL, an integer unchanged within the target's range and refused with "inconvertible value"
  outside it, a float truncated first, text parsed, a CHAR as a digit).

Under NONE the NULL row is the value 0. The kit knows the two NULL policies the existing types
have, SENTINEL and NONE, and refuses a declaration line with any other; a type with a NULL policy
of its own adds its rules to `TypeConformanceInvariants`. A result column of a
wider type, from a function of that type the new type reaches through an implicit cast, holds the
value widened by the tier.

`init` copies every field from an existing type's driver and leaves `wire_kind` and
`signature_char` as `CHANGE-ME`, which the run refuses, so the author decides both. The run checks
the file against the tree before it writes anything and exits 2 with one line per problem,
`facts: <field>: <problem>`: a missing or unknown field, a value that names no constant, a tag that
is taken or is not the next free one, a name or a signature character already taken, a movement that
disagrees with the storage, an implicit cast that names no tag, a refused site that is no guarded
site of `places.tsv`, a `pg_oid` that names no constant, or a new wire kind whose constant name
`WireKind` already has.

`refused_sites` takes the labels of the guarded sites (section 4): `memoized virtual column`,
`SAMPLE BY FILL(PREV)`, `SAMPLE BY FILL(LINEAR)`, `SAMPLE BY FILL(value)`, `COPY bind snapshot`,
`ILP column kind`, `WAL columnar append`, `QWP WAL append`, `Parquet conversion`, `between`,
`= NULL`, `copier conversion`.

## 4. What names a place for a new type

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
  a stub that javac reports for every other answer. A bind variable holds an existing type: a new
  type defines one of a type its values fit, as SYMBOL's holds a STRING, and the kit then checks
  that a refused value names the type the variable holds; or it refuses the definition with an error
  naming the type, which the COPY bind path then reports.
- **Exhaustive switches.** javac lists every switch expression over `ColumnTypeTag` with no default
  arm (the type driver lookup, and the five pair switches: ALTER COLUMN TYPE fixed to fixed, fixed
  to var-size and var-size to fixed, the UNION cast, the CASE cast), and every switch expression
  over a type driver value (`NullPolicy`, `WireKind`, the accessor family, the tier) for a new
  value; rustc every match over the Rust tag enum with no catch-all arm; the C++ compiler the enum
  switches where `-Wswitch` is an error. javac does not check a switch statement, even over an enum:
  the audit tells the two apart. The UNION cast's cell of the type with itself is reached too: once
  one column of a branch needs a cast, `generateCastFunctions` casts every column of that branch.
- **Values the type driver supplies.** A type that shares a value with an existing type (its
  accessor family, wire kind, NULL policy, tier) takes that type's arm at every switch on the value,
  exhaustive or not, with no compile error. The audit lists those switches by namesake (section 5).
  The writer arms behind a wire-kind switch run per row on an opcode the switch chose at setup.
- **The compare arm.** `compareOpcode` refuses a type that does not order as its family
  (`no compare arm for <type> at ORDER BY`): the comparator needs an arm for the type's order.
- **The guarded sites.** The family-arm guard refuses a type unlike its family's namesake where
  the family's code would read the namesake's NULL or order: `no family arm for <type> at <site>:
  add the arm or declare the type like its namesake`, raised at setup, before anything is
  allocated or copied. A type declares the refusal in `refused_sites`, which costs no edit and which
  the kit then checks, or is admitted at the site by its own arm. `places.tsv` decides each guarded
  site `refused`, with its label; `ILP column kind` keeps the cast error ILP already raises
  (`cast error from protocol type`), and `WHERE key column` answers neutrally instead (the column
  stays a filter, which gives a correct result).
- **The kit and the coverage tests.** The type's line of `later-types.txt`, which the tool writes:
  `NAME | DDL | NULL policy | paths | tier | refused sites`. The kit checks a type it has no
  recording for by invariants: values read back as written, NULL behaves as the policy says, rows
  order by the tier, a query compiles or fails naming the type, and a declared site refuses it. The
  coverage tests report an admitted pair, opcode or function without an implementation;
  `FunctionReachTest` lists each function and operator the type reaches through its implicit
  casts, each call the family-arm guard refuses, and each function with a slot it does not try
  (variadic, a DECIMAL or GEOHASH family, a window function). The tables of decisions outside the
  type drivers fail for a new tag until its rows are added: `TypeRelationGoldenTest` (each table
  prints the type as its own `+ <TYPE> row` and `column` lines below it), `ColumnTypeTest`,
  `CastBindVariableTypeTest`, `QueryEngineTypeFactsTest`, `DecimalUtilTest` and the setter pairs of
  `BindVariableServiceImplTest`.
- **Native.** The decode of an unknown tag code refuses the type at the Parquet boundary until the
  tag has its arm in `TryFrom<u8>` for `ColumnTypeTag` (`col_type.rs`), which the kit's Parquet paths
  report. cargo checks qdbr only once qdb-core compiles, so the run after qdb-core's items are worked
  lists qdbr's. The kit runs the tree's native code: the C++ library the CMake step builds and a
  debug Rust library that Maven's `build-rust-library` profile builds, both under
  `core/target/classes/io/questdb` (`bin-local` and `rust`), where they take precedence over the
  committed libraries. The committed native libraries: a CI workflow rebuilds them on request.
- **The audit.** Every other place: section 5.

## 5. The audit

`audit.py` reads `core/src/main/java`, the Rust crates `qdb-core`, `qdbr` and `qdb-parquet-meta`
(test modules left out) and `core/src/main/c` on every run, and finds every place that decides by
a column type. Nothing about a place is stored but a decision and its reason, in `places.tsv`; the
place itself, its line and its form are worked out from the code each time.

**Forms.**

| form | what it is |
|---|---|
| `tag-switch` | a switch over a tag or a whole column type whose arms name tags; javac never checks one |
| `tag-enum-switch` | a switch over `ColumnTypeTag`; checked when it is an expression with no default arm |
| `tag-test` | a statement that compares a tag (`==`, `!=`, `<`, `>=` with a `ColumnType` or `ColumnTypeTag` constant, bare under a static import) or calls a `ColumnType` predicate that tests tags (`isSymbol`, `isTimestamp`, `isDecimal`); one place per statement |
| `tag-table` | a table indexed by a tag, directly or through a variable holding one, and a Rust match from tag codes to tags |
| `value-switch` | a switch over a value a type driver supplies: the accessor family (`accessorOf`, `familyArmOf`, `getAccessor()`, the family opcode), `NullPolicy`, `WireKind`, `RelationKind`, the tier, the movement |
| `value-test` | a statement that tests such a value, calls a family function, or calls a predicate that reads a value (`isIntegral`) |
| `rust-match`, `rust-test` | a Rust match, or a `==`, `!=`, `matches!` or `if let` test, over the tag enum or a value enum |
| `c-switch`, `c-test` | a C or C++ switch or test over the native tag enum, or over tag constants of another name (`json.cpp`'s `qdb_col`) |

Left out on purpose: a switch over an opcode a setup switch chose (a per-row writer arm, reached
only with the opcodes its setup switch returns, which is the place); a call of a relation between
two types (`isConvertibleFrom`, `isSameOrBuiltInWideningCast`), which the relation rules derive from
the type drivers' facts; the switches of the JIT backend, which are over the JIT's operand types
(`data_type_t`); test code.

For each place the scan works out whether a compiler names it for a new tag or value, whether the
family-arm guard covers it (`familyArmOf`, `familyArmOpcodeOf`, `noFamilyArm`, an
`isLikeFamilyNamesake` test before it), which tags or values it names, and the label of its guard.
A place's key is its file, its method, its form and its own code text (the switch selector, the
statement), with whitespace removed, a long text shortened to its head and a hash, and an ordinal
among equal keys in one method. Lines move without changing a key, and a switch added above another
does not move the other's decision. A place whose code text changes is a new place.

**The view by namesake.** `audit like <TYPE>` lists, for a type declared like the existing type
`<TYPE>`:

- the places that name `<TYPE>` and that no compiler checks: `<TYPE>`'s tag takes a path there that
  a new tag does not take (a range test is evaluated for both tags);
- the places that switch on or test a value the type shares with `<TYPE>`: the new type takes
  `<TYPE>`'s arm there, whether a compiler checks the switch or not, since a check names a new
  value, not a shared one; those no guard covers come first;
- the tables indexed by a tag: every new tag needs its entry;
- when the type's NULL policy differs from `<TYPE>`'s (the tool passes the type's own facts), the
  validity batch sites: the comments `// validity batch site:` mark where a column whose NULL lives
  in a validity bitmap would write or move its bits, which no compiler check and no test names.

`<TYPE>`'s own type driver and the definitions of the predicates (whose callers are the places) are
left out. A place that names no namesake treats a new type as it treats every type it does not
name, so it differs for the new type only where it differs for the namesake.

**`places.tsv`.** One row per decision, tab-separated:

```text
file  method  form  anchor  n  decision  reason  type
```

The first five columns are the place's key, as `audit places` prints it and `audit like <TYPE>
--type <NAME> --rows` writes it for every open place. The decisions:

| decision | meaning |
|---|---|
| `not-reached` | no type reaches the place but the ones it names |
| `no-change` | the type needs no change here: its path is right for it |
| `refused` | a guarded site: the reason is its label, or `<label>: <refusal>` for a site that keeps an earlier error |
| `test` | a test names the place: `Class#method`, or `kit:<path>` |

The reason says why, in a sentence. `type` names the type the decision is for; empty, the decision
holds for every type. A place is closed for a type when its code names the type or a row decides it
for that type or for every type; a decision for another type is shown as a precedent. `audit
check` compares the rows with the code and exits 1 on a problem: a decision whose place is gone, a
test that does not exist, a guard label that is not at its place, two decisions for one place and
one type. `run` lists the same problems as items.

## 6. The worklist format

`worklist.md` starts with a header (the facts file and its sha256, the tree's branch and commit, the
run's time and length, the item counts, a note for every step that did not run) and then one section
per group, in this order: `build-java`, `build-rust`, `build-c`, `refusal`, `kit`, `coverage`,
`namesake`. An empty group prints its heading with `(0)`. One line per item:

```text
- [ ] <decision> | <location> | <message>[ | site: <label>]
```

| field | values |
|---|---|
| decision | `name-yourself` (name the type in a switch or a refusal group), `implement-pair` (write the code an admitted pair, opcode or function needs), `add-writer-arm` (an arm keyed by the type's wire kind, NULL policy or order: the item names the switch that chooses an opcode, and the arm goes where that opcode is read, in the per-row writer of the same class), `fill-driver-answer` (an answer of the type driver), `declare-or-admit` (a guarded site refused the type: declare it in `refused_sites` or add the type's arm), `decide-place` (a place of the view by namesake: name the type in its code, or record a decision in `places.tsv`) |
| location | `` `path:line` `` from the repository root; `` `kit:<path>@<mode>#<value row>` `` for a kit failure, `` `kit:<test class>#<test>` `` for one that names no path; `` `<test class>#<test>` `` for a coverage failure that names no place |
| message | the first line of the compiler's, the test's or the refusal's text, at most 200 characters; a coverage failure's every line, joined by ` / `, at most 2,000 characters; for a place, its group, method, form, code and what else covers it; ASCII, `\|` written as `/` |
| site | the place's label (`Class.method form`), the guarded site's label, or `unmapped` when nothing locates the failure |

A compiler error sits in the innermost place around its line and takes the decision that place
asks for; an error in a test's own switch over the tag takes the type's answer there; an error in
the generated type driver is a driver answer. A refusal maps to the guarded site it names, or to
the place its lead belongs to (`no UNION cast`: the UNION cast switch; `no compare arm`: the
comparator). A kit failure maps to the site whose earlier error it holds, else to a place
`places.tsv` decides its path names (of several, the one whose method the value row names), else to
the guarded site its path reaches, else to the code its path checks; a coverage failure maps to
each method its message names, else to the code its test checks. These hints (`LAYERS`,
`KEPT_TEXTS`, `KIT_GUARDS` and `LEADS` in `type_probe.py`) locate a failure for reading; they are
not places.

Some answers go where no single place names them. An explicit cast the type admits (`sql.cast`,
`RelationCoverageTest`'s cast test) is a function factory of its own,
`Cast<From>To<Type>FunctionFactory` under `core/src/main/java/io/questdb/griffin/engine/functions/cast/`,
registered by a line in `core/src/main/resources/function_list.txt`. A CASE escalation
(`RelationCoverageTest`'s CASE test) is an entry of `CaseCommon`'s constructors table with its
function class. A `storage.alter` item names one place of its path, and a conversion the type admits
is written at each: `converters.cpp` `fixedToFixed`, with `converters.h`'s `is_fixed_convertible`
and the `EnumTypeMap` specialisation it asserts, and `DecimalColumnTypeConverter.getLoader`. A type
given a sort-key kind of its own (`SortKeyEncoder.keyKind`) also adds its arms in `encodeFixed8` and
`encodeFixedColumn`. A sort key orders the stored bits, so a type whose NULL word is not the lowest
value of its order (an unsigned INT that keeps INT's NULL) gets no key kind: ORDER BY then takes the
comparator, whose compare arm puts NULL first (`sql.order_*` checks where NULL sorts). The copier
test's item covers every copier conversion the type admits, and each pair needs its arm in all
three copiers: the single-method and the chunked bytecode copiers
(`RecordToRowCopierUtils.generateSingleMethodCopier`, `generateChunkedCopier`) and the looping
copier (`LoopingRecordToRowCopier`: a target arm in the source's `copyFrom<Type>` method; as a
source, an arm in `copyColumn` and a `copyFrom<Type>` of its own). `debug.cairo.copier.type`
(1, 2, 3) forces one of them.

There is one item per location and decision; a compiler error repeated by several builds is one
item. Of several failures at one kit location (two later types failing the same path), the item
keeps the one of the type the run adds, so a rerun lists the same item whatever order the tests ran
in; a test's temporary directory reads as `<tmp>`. The kit and the coverage tests run only when
every build group is empty, since a build error hides the tests behind it. The namesake group is
listed on every run.

The kit runs in two passes. The first runs the kit for the new type alone, with the coverage tests,
in about a minute; while it fails, the worklist lists its items and a note says the whole kit did
not run. Once it passes, the second pass runs the whole kit, every type, about ten minutes, so the
existing types are checked against the type's edits once. The SQL kit's query test
(`TypeConformanceSqlTest.testQueries`) runs every query path of a mode and reports each failing one
as an item of its own; the next mode runs once a mode passes. Every other kit test stops at its
first failing path or mode. To run the kit by hand for some types only, pass
`-Dquestdb.test.kit.types=<label>,...` to Maven.

Exit codes:

| code | meaning |
|---|---|
| 0 | the worklist is empty: every build is clean, the kit and the coverage tests pass with the type declared, and every place of the view by namesake is closed |
| 1 | the worklist has an item, or a step was skipped |
| 2 | the facts file is invalid, an anchor is missing or appears twice, or the command line is wrong |
| 3 | a step failed in a way the tool cannot parse, or `places.tsv` does not read; the message names the step and its log |

## 7. Acceptance

A type PR is done when the run exits 0: every build is clean, the kit and the coverage tests pass
with the type in `later-types.txt` (its declared refusals checked as refusals), and every place of
the view by namesake is closed, by the code or by a row of `places.tsv`. Its rows stay in the
repository as precedents for the next type of the same namesake.

The tool's own acceptance run came before the audit: an author who had not added the type before
added an unsigned INT from the worklist alone, with a 56-entry manual list where the view by
namesake is now. It reached exit 0 in seven runs and 69 minutes; the diff was 57 files, +2,264 -29
lines; 157 edits sat outside the type driver and its registration, in 51 files. The audit's views
hold every entry of that manual list, and the view of each namesake holds 33 of the 35 places a
later review found that the run's instruments did not reach; the other two are reached through the
relation rules, which `RelationCoverageTest` checks.

The Rust unit tests that round-trip every tag (`ColumnTypeTag::VALUES`, `test_lookup_driver`) list
the types by hand, so they skip a new type without failing; add it to both.

## 8. Limits

The tool lists where, not what. It does not write how the type prints, parses, compares and widens,
the pairs its relations admit, or its function bodies, and it does not rebuild the committed native
libraries: a CI workflow does, on request. Its native builds check that the code compiles and give
the kit the tree's native code (section 4). The build lists the tag switches of
`OverloadSoundnessTest` and `ColumnConversionSoundnessTest`; the kit step runs both with the
coverage tests, so an answer there that a later edit makes stale comes back as a coverage item.
`TypeRelationGoldenTest` prints a type registered later as its own row and column lines below each
table; the type PR adds those lines to the expected tables once it has decided its relations.

What the audit cannot tell:

- whether a later type reaches a place at all: a place that names the namesake is listed even where
  no later type gets to it; `not-reached` records that, once;
- a place a type reaches through a relation rather than through its own tag (a UNION with a column
  of another type): the relation rules and `RelationCoverageTest` cover it;
- whether a `test` decision's test reaches its place, or a `no-change` reason is true: the view
  shows each with its reason, for the type's author to read.

A type that stores no NULL (NULL policy NONE) is not supported yet. Its column tops, the rows of a
partition written before the column was added, must read a default value given in SQL when the
column is added, never written into the old rows; that does not exist yet, so those rows read the
family's NULL (INT's -2147483648 for a type on INT's accessor).

Not yet tested, stated plainly: no committed test drives a type through the family-arm guard at the
sites (the guard itself is tested with stub type drivers), and the factories that refuse a type at a
guarded site must release what they already own before they throw, which no committed test covers
either, because no existing type reaches these refusals and no database user can. A type PR is the
first to run them in CI: the kit checks each declared refusal under the memory-leak checker, so its
author should expect, and read, those results.

Places outside the worklist that a type may still meet:

- A type with parameters, as `GEOHASH(n)` and `DECIMAL(p,s)` have, adds its grammar to
  `SqlParser.toColumnType` and `SqlCompilerImpl.addColumnWithType`; a type without parameters parses
  by the names in its facts file.
- The pinned Java client formats a LONG field of `Long.MIN_VALUE` as `nulli` in text ILP (its long
  formatting prints LONG's NULL as `null`), which the server refuses. A type whose values include
  that bit pattern cannot send it through the client's text ILP until the client changes.
- The kit derives a text family's `max` row with a character outside the Basic Multilingual Plane,
  which ILP over HTTP stores as `??` and ILP over UDP as `?` for VARCHAR as well, and CSV import reads
  an empty string back as NULL; a text type leaves those paths out of its kit line or expects those
  rows to fail. `pg.binary` reads a fixed-size value at the width of the type's PostgreSQL type:
  a type sent wider than it stores (an unsigned INT as `int8`) sends its value widened by its tier.
- ILP has no unsigned form: the client sends a value in its family's form, so an unsigned INT's
  largest value goes out as -1. An unsigned type declares `ILP column kind` refused, which
  the kit and the coverage tests then accept, or adds its own ILP parsing.

## 9. Functions over new NULL kinds

The function library tests NULL inline, per row, by comparing a value with its type's reserved
NULL value (`Numbers.LONG_NULL`, NaN). A type that keeps NULL outside its values (a full-range
type, a nullable BYTE or SHORT, with a validity bitmap) or never holds NULL (NOT NULL) cannot reuse
that code as it is: a full-range LONG's smallest value is a real value, and a bitmap type's NULL is
not in the value at all. The functions are unchanged here; the type PR decides how they serve the
new kinds. One way, prototyped on four functions (`=`, `+`, `sum`, a cast to DOUBLE) and measured:

- **NULL kinds.** Every function reports its result's kind at setup: SENTINEL (today's types with
  a reserved NULL value), NONE (today's BOOLEAN, BYTE, SHORT, CHAR), BITMAP, NOT_NULL. A column
  function takes its column's kind (`RecordMetadata.getColumnNullPolicy`, which today follows the
  type driver's `getNullPolicy`).
- **Combining.** A small table gives a call's kind from its arguments' kinds, the same for any
  number of types: no full-range argument, SENTINEL, and today's code runs unchanged; all NOT_NULL,
  NOT_NULL; any other mix, BITMAP.
- **The wrapper, chosen at setup.** Each function's arithmetic is one body over plain values. A
  SENTINEL call runs today's class; NOT_NULL calls the body with no NULL test; BITMAP asks each
  argument `isNull()` and calls the body when none is NULL. A sentinel argument of a BITMAP call is
  lifted first: its reserved value reads as NULL. The wrappers are shared by shape (two LONG
  arguments to a LONG result, one argument to a DOUBLE, an aggregate with a LONG accumulator), not
  written per function; the prototype measured the shared classes as fast as per-function ones.
- **Casts and aggregates.** A cast follows its target: a full-range value cast into a sentinel type
  maps NULL to that type's reserved value. An aggregate skips NULL arguments through one shared
  adapter, and an empty group gives NULL.
- **No silent reach.** The parser admits a full-range argument only into a factory converted for
  it; every other factory refuses with "no matching function", and a test lists the factories in
  scope that are not converted yet.
- **Cost, as the prototype measured it.** Today's types within noise. NOT_NULL no slower than
  SENTINEL, since no NULL test runs. BITMAP 1.2 to 1.5 times the sentinel cost per row in an
  expression, 2 to 3 times with one row in ten NULL, from the per-row `isNull` calls. An aggregate
  over a full-range column has no batch kernel: its `sum` ran 34 to 55 times slower than LONG's
  vectorized `sum`, until frame-level kernels are written for it. Per new type, each function it
  supports takes about 20 lines (the body, the wrapper choice, the marker that admits the type),
  and each shape's shared wrappers are written once.
- **Open.** A NOT NULL column marker reaches a function only through the query's column metadata.
  The operators that add NULL to a column (UNION with a NULL branch, an outer join, `lag`, CASE
  without ELSE) build new metadata and so drop the marker, which is safe. An operator that adds NULL
  and passes the metadata through unchanged would keep NOT_NULL and read a filler as a value: each
  such operator needs checking.

## 10. The tests

`python3 -m unittest discover -s utils/type-probe` runs both suites. `test_audit.py` plants one place
of every form in a small tree and checks the audit finds it, with its fallback and whether a compiler
names it, and checks the decisions against edits of the code (a moved place, a changed one, a switch
added above another, a default arm added). `test_type_probe.py` checks the parsers on outputs cut
from real runs, the facts checks and the registration on the checkout, and the failure mapping.
