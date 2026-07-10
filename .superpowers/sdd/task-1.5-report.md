# Task 1.5 Report: Parse + compile DELETE to a `DeleteOperation` (WAL-only, validated)

## Summary

`DELETE FROM t [[AS] alias] WHERE <pred>` now parses into an `ExecutionModel.DELETE`-typed
`QueryModel` and compiles to `CompiledQuery.DELETE` carrying a `DeleteOperation`. The WHERE
predicate is fully validated at compile time (unknown columns, bad types) by running it through
the same optimiser/codegen pipeline as an ordinary `SELECT`. Non-WAL tables and materialized
views are rejected with dedicated messages. The compile-time factory is discarded immediately
(`survivorFactory = null`); a later task recompiles survivors at WAL-apply time.

## TDD evidence

### RED

Command:
```
mvn -q -pl core test -Dtest=DeleteTest
```

Output (compile failure, as expected — `getDeleteOperation()` didn't exist yet):
```
[ERROR] COMPILATION ERROR :
[ERROR] /home/nick/claude/wt/oss/delete-statement/core/src/test/java/io/questdb/test/griffin/DeleteTest.java:[44,39] error: cannot find symbol
  symbol:   method getDeleteOperation()
  location: variable cc of type CompiledQuery
```

### GREEN

Command:
```
mvn -q -pl core test -Dtest=DeleteTest
```

Surefire summary:
```
Test set: io.questdb.test.griffin.DeleteTest
Tests run: 4, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.799 s
```

Per-test outcomes observed in the log before reverting to the clean 4-test file (see Self-review
below for the extra scratch-test run that produced these):
- `testDeleteCompilesToDeleteType` — compiles, `cc.getType()==CompiledQuery.DELETE`, `getDeleteOperation()` non-null.
- `testDeleteRequiresWhere` — `DELETE FROM t` throws `SqlException` with message `AS, WHERE or table alias expected expected` (contains "WHERE").
- `testDeleteRejectsNonWal` — `DELETE FROM t WHERE x = 1` on a `BYPASS WAL` table throws `DELETE is only supported on WAL tables` (contains "WAL").
- `testDeleteRejectsUnknownColumn` — `DELETE FROM t WHERE nope = 1` throws `Invalid column: nope` (contains "nope").

Regression check — `UpdateTest` (I touched the shared `compileExecutionModel0`/`compileUsingModel`
switches UPDATE also uses):
```
Tests run: 102, Failures: 0, Errors: 0, Skipped: 15, Time elapsed: 3.054 s -- in io.questdb.test.griffin.UpdateTest
```
No regression.

## Files changed

1. **`core/src/test/java/io/questdb/test/griffin/DeleteTest.java`** (new) — the 4 tests exactly as
   given in the brief. License header copied from `UpdateOperation.java`.

2. **`core/src/main/java/io/questdb/griffin/SqlParser.java`**
   - Dispatch: added `if (isDeleteKeyword(tok)) { return parseDelete(lexer, sqlParserCallback); }`
     next to the `isUpdateKeyword` branch in `parse(...)` (~line 5642).
   - New `parseDelete(GenericLexer, SqlParserCallback)`, placed alphabetically between
     `parseDeclare` and `parseDml` (this file's `parse*` helpers are alphabetically ordered).
     Modeled on `parseDmlUpdate`/`parseUpdateClause` with the SET-clause parsing dropped:
     - Expects `FROM`, then a table name (`sansPublicSchema` + `assertNameIsQuotedOrNotAKeyword`,
       mirroring UPDATE's table-name handling exactly).
     - Optional `[AS] alias`, mirroring `parseUpdateClause`'s alias-detection shape (`isAsKeyword`
       branch, then "bare token that isn't AS/WHERE is a table alias" branch).
     - Requires `WHERE`; if the next token isn't `WHERE`, throws
       `SqlException.$(pos, "WHERE clause is required for DELETE; use TRUNCATE TABLE to remove all rows")`.
     - Builds a 3-layer model mirroring UPDATE's `updateQueryModel -> fromModel -> nestedModel`:
       `deleteQueryModel` (`modelType=ExecutionModel.DELETE`, `tableNameExpr`, optional `alias`) ->
       `fromModel` (`SqlUtil.addSelectStar(...)` in place of UPDATE's SET-derived bottom-up
       columns) -> `nestedModel` (`tableNameExpr`, alias copied down, `whereClause`). See "How I
       validated WHERE" below for why the select-star wrapper is load-bearing, not decorative.
     - Trailing-token handling identical to `parseUpdate`: optional semicolon, else
       `errUnexpected`.

3. **`core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java`**
   - Import `io.questdb.griffin.engine.ops.DeleteOperation` (alphabetical).
   - `compileExecutionModel0`: added `case ExecutionModel.DELETE:` (braced, own scope — UPDATE's
     sibling case is unbraced and would collide on variable names otherwise). Resolves the table
     token, opens `getMetadataForWrite` (mirrors UPDATE structurally; not used for anything beyond
     scoping/table-existence in this task since there's no SET-column validation to run against
     it), and runs `optimiser.optimise(deleteQueryModel.getNestedModel(), executionContext, this)`
     — the exact pipeline used for a plain `SELECT` — writing the optimised model back via
     `setNestedModel`. This is the step that actually throws for `nope` in
     `testDeleteRejectsUnknownColumn`.
   - `compileUsingModel`: added `case ExecutionModel.DELETE: compiledQuery.ofDelete(generateDelete((QueryModel) executionModel, executionContext)); break;` next to the UPDATE case, per the brief's exact snippet (note: cast target is the concrete `QueryModel`, and the switch's loop variable is `executionModel`, not `model`).
   - New `generateDelete(QueryModel model, SqlExecutionContext executionContext)`, placed
     alphabetically before `generateExplain`: resolves the table token again (brief's algorithm
     keeps this independent of `compileExecutionModel0`'s resolution, mirroring UPDATE's own
     double-resolution between its `compileExecutionModel0` case and `generateUpdate`), opens
     metadata, rejects non-WAL and mat-view, builds-and-closes a real factory to validate the
     predicate, and returns `new DeleteOperation(tableToken, metadata.getTableId(), metadata.getMetadataVersion(), model.getModelPosition(), null)`.

4. **`core/src/main/java/io/questdb/griffin/CompiledQuery.java`** — import `DeleteOperation`;
   added `DeleteOperation getDeleteOperation();` immediately after `getUpdateOperation()` per the
   brief's explicit placement instruction (this one spot deliberately breaks the file's otherwise
   alphabetical `get*` ordering, on purpose, per the brief).

5. **`core/src/main/java/io/questdb/griffin/CompiledQueryImpl.java`** — import `DeleteOperation`;
   added `deleteOperationDispatcher` field/constructor block (verbatim mirror of
   `updateOperationDispatcher`) and `deleteOp` field, both placed in this file's established
   alphabetical field order (unlike `CompiledQuery.java`, this file has no explicit
   after-UPDATE placement instruction from the brief, and its field/method ordering is strictly
   alphabetical throughout, so I followed that convention instead); `getDeleteOperation()` and
   `ofDelete(...)` likewise placed alphabetically among the other `get*`/`of*` methods; `execute()`
   and `closeAllButSelect()` DELETE cases added next to their UPDATE siblings; `clear()` now also
   resets `deleteOp = null` (not explicitly listed in the brief's wiring checklist, but a direct,
   low-risk mirror of every other operation field there — omitting it would let a stale `deleteOp`
   reference survive into an unrelated subsequent compile).

## How I validated the WHERE clause at compile time

Two things had to both be true, discovered by reading `generateUpdate`/`generateUpdateFactory`/
`optimiser.optimiseUpdate` closely (this was the "intricate" part the brief warned about):

1. **`codeGenerator.generate(...)` (reached via `generateSelectOneShot`) assumes an
   already-optimised model** — it does not call `optimise()` itself. UPDATE gets this for free
   because `compileExecutionModel0`'s `UPDATE` case runs `optimiser.optimiseUpdate(...)` (which
   calls `optimise()` on the nested select model) *before* `compileUsingModel`'s switch ever calls
   `generateUpdate`. So `compileExecutionModel0`'s new `DELETE` case had to do the analogous
   `optimiser.optimise(deleteQueryModel.getNestedModel(), ...)` pass — this alone is what makes
   `nope` fail (`Invalid column: nope`, thrown deep inside `optimise()`, well before `generateDelete`
   is ever reached).

2. **`generateDelete` additionally builds-and-closes a real `RecordCursorFactory`** via
   `generateSelectOneShot(model.getNestedModel(), executionContext, false)` (the same call
   `generateUpdateFactory` makes), then immediately `factory.close()`s it and returns `null` for
   `survivorFactory`. This is belt-and-suspenders beyond step 1: `optimise()` mostly does
   column-resolution/rewriting, while actual `Function` construction/type-checking happens during
   codegen. Building the real factory catches anything `optimise()` alone wouldn't (e.g. WHERE
   expressions that resolve columns fine but fail to type-check or bind to a function).

The `fromModel` layer's `SqlUtil.addSelectStar(fromModel, queryColumnPool, expressionNodePool)`
call is what makes the nested model a legitimate input to both of the above — without it,
`fromModel` would have zero columns, which is not a shape any existing SELECT/UPDATE code path
exercises. With it, `deleteQueryModel.getNestedModel()` is structurally identical to what
`SELECT * FROM t WHERE <pred>` produces, so it rides the exact same battle-tested path a plain
select uses for column validation, rather than a bespoke, less-exercised one.

Verified `checkMatViewModification(TableToken)` reuse (Ambiguity Resolution #2) with a scratch
test (see Self-review): message came back as `cannot modify materialized view [view=mv1]`,
matching the cited idiom exactly.

## Self-review

- **Faithfully mirrors UPDATE?** Yes for the model shape (3 layers, same pool/position wiring,
  same alias-copy-down pattern), the double table-token/metadata resolution between
  `compileExecutionModel0` and the generate-step, and the `checkMatViewModification` reuse. The
  one deliberate improvement over UPDATE: I gave `tableNameExpr` and `deleteQueryModel`'s
  `modelPosition` the *real* table-name-token position (UPDATE hardcodes `0`/uses
  `lexer.getPosition()` at generate-time, which by then just means "end of input" — not
  reused here since it's strictly worse and `DeleteOperation`'s ctor param is literally named
  `tableNamePosition`).
- **Extra verification beyond the brief's 4 tests** (temporarily appended to `DeleteTest.java`,
  run, observed, then reverted so the committed file matches the brief exactly):
  - `DELETE FROM t x WHERE x.x = 1` (bare alias) — compiles.
  - `DELETE FROM t AS x WHERE x.x = 1` (AS alias) — compiles.
  - `DELETE FROM t WHERE x = 1;` (trailing semicolon) — compiles.
  - `DELETE FROM t WHERE x = 1 garbage` (trailing garbage) — throws `unexpected token [garbage]`.
  - Mat-view: `DELETE FROM mv1 WHERE price > 1` against a real `create materialized view` —
    throws `cannot modify materialized view [view=mv1]`.
  All 9 (4 official + 5 scratch) passed before I reverted to the official 4, which I then
  re-ran clean (4/4, output above) as the final state before committing.
- **No other `ExecutionModel.*` switch sites missed** — grepped the whole `core/src/main` tree for
  other files referencing `ExecutionModel.QUERY`/`UPDATE`/`INSERT`; only the 4 files in the
  brief's list (plus the model definitions themselves) do. No hidden `default:`/missing-arm risk
  elsewhere.
- **No `SqlCompilerImpl` subclass needed a matching change** — the only subclass in the OSS tree
  (`SqlCompilerImplTest.SqlCompilerWrapper`) overrides unrelated extension points
  (`parseCreate*Ext`, `parseShowSql`), not `compileExecutionModel0`/`compileUsingModel`.
- **`git diff --check`** — clean, no whitespace issues.

## Concerns

1. **Plain-VIEW rejection not implemented.** UPDATE's `compileUsingModel` case calls both
   `checkViewModification(executionModel)` and `checkMatViewModification(executionModel)` before
   `generateUpdate`. The brief's `generateDelete` algorithm only lists mat-view rejection, and the
   brief's exact `compileUsingModel` DELETE-case snippet has no pre-checks at all, so I implemented
   exactly that (mat-view only, inside `generateDelete`). `DELETE FROM <plain view> WHERE ...`
   is therefore not explicitly guarded — it would likely fail some other way (e.g. via
   `getMetadataForWrite`/`isWalEnabled()` on the view's token) but not with a purpose-built "cannot
   modify view" message. Flagging in case a later task (or reviewer) wants parity with UPDATE here.
2. **The "AS, WHERE or table alias expected expected" double-"expected" wording** on the bare
   `DELETE FROM t` (no WHERE, EOF) path is not a bug I introduced — it's the `tok(lexer, msg)`
   helper appending `" expected"` to a caller string that (by established codebase convention,
   e.g. UPDATE's own `"AS, SET or table alias expected"`) already ends in "expected". Cosmetic,
   pre-existing pattern, only surfaces on this one EOF edge case; the test only asserts the message
   contains "WHERE", which it does.
3. **`generateDelete`'s metadata-open in `compileExecutionModel0` is otherwise-unused** beyond
   scoping — there's no SET-column-style validation to run against it at that stage (unlike
   UPDATE's `validateUpdateColumns`). Kept it because the brief explicitly asked for
   "open getMetadataForWrite" there and it's harmless/cheap, but flagging that it's currently
   just a table-existence/lock guard, not doing double duty.

No blocking issues. All 4 brief tests pass; `UpdateTest` (102 tests) shows no regression.

## Fix: plain-view rejection

Code review flagged Concern #1 above as a HIGH finding: `DELETE FROM <plain_view> WHERE ...`
compiled with no error and produced a `DeleteOperation` targeting the view, because
`generateDelete` called `checkMatViewModification(tableToken)` but not the paired
`checkViewModification(tableToken)` that every other DML site (UPDATE, INSERT, DROP TABLE, etc.)
calls alongside it. `ViewMetadata.isWalEnabled()` returns `true`, so the non-WAL guard earlier in
`generateDelete` doesn't catch a plain view either.

**Fix** (`core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java`, `generateDelete`, ~line
4887): added `checkViewModification(tableToken);` immediately before the existing
`checkMatViewModification(tableToken);` call, matching the ordering used at every other paired
call site in this file (e.g. UPDATE at ~line 3958-3959):

```java
checkViewModification(tableToken);
checkMatViewModification(tableToken);
```

**Test** — new `testDeleteRejectsPlainView` added to `DeleteTest.java`, mirroring the existing
`testDeleteRejects*` style and using the same `create view ... as (select ... sample by 1h)` DDL
`InsertTest.testCannotInsertIntoView` uses for a plain (non-materialized) view over a WAL table:

```java
@Test
public void testDeleteRejectsPlainView() throws Exception {
    assertMemoryLeak(() -> {
        execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
        execute("create view t_view as (select ts, max(x) as x from t sample by 1h)");
        try {
            execute("DELETE FROM t_view WHERE x = 1");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "view");
        }
    });
}
```

Command run:
```
mvn -q -pl core test -Dtest=DeleteTest
```

Surefire summary (pristine, no ERROR/FAIL lines in the full output):
```
-------------------------------------------------------------------------------
Test set: io.questdb.test.griffin.DeleteTest
-------------------------------------------------------------------------------
Tests run: 5, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.832 s -- in io.questdb.test.griffin.DeleteTest
```

All 5 tests pass (4 pre-existing + `testDeleteRejectsPlainView`). Concern #1 is resolved;
`DELETE FROM <plain_view> WHERE ...` now throws `SqlException` containing
`cannot modify view [view=...]`, matching UPDATE/INSERT's behavior exactly.
