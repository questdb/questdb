# Task 1.3 Report: `SecurityContext.authorizeTableDelete` (OSS)

## Status: DONE

## Interface change

Added the new abstract method to `core/src/main/java/io/questdb/cairo/SecurityContext.java`,
placed directly after `authorizeTableTruncate` (per the brief — NOT alphabetically before
`authorizeTableDrop`, which is where strict alphabetical ordering of the rest of the file
would otherwise put it):

```java
    void authorizeTableTruncate(TableToken tableToken);

    void authorizeTableDelete(TableToken tableToken);

    void authorizeTableUpdate(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);
```

No javadoc was added — `authorizeTableTruncate` and its other neighbors (`authorizeTableDrop`,
`authorizeTableRename`, `authorizeTableUpdate`, `authorizeTableVacuum`) carry no javadoc either,
so a bare declaration is consistent with the surrounding style.

## Implementors updated

Discovered by iterating `mvn -q -pl core -am compile` (main sources) then
`mvn -q -pl core -am test-compile` (test sources), plus a repo-wide grep sweep (see
"Verification of completeness" below) to confirm nothing outside `core` implements the
interface.

| Class | File | Variant | Why |
|---|---|---|---|
| `AllowAllSecurityContext` | `core/src/main/java/io/questdb/cairo/security/AllowAllSecurityContext.java` | no-op `{ }` | Mirrors its own `authorizeTableTruncate`, which is a no-op (all `authorize*` methods in this class are no-ops except the read-only-settings special case). |
| `ReadOnlySecurityContext` | `core/src/main/java/io/questdb/cairo/security/ReadOnlySecurityContext.java` | `throw CairoException.authorization().put("Write permission denied").setCacheable(true);` | Mirrors its own `authorizeTableTruncate`, which throws this exact exception (this class throws on every write-type `authorize*` method). |

Both edits were inserted immediately after each class's existing `authorizeTableTruncate`
override, matching the interface ordering and the brief's exact code blocks (Steps 3 and 4).

### `DenyAllSecurityContext` — no change needed

`DenyAllSecurityContext extends ReadOnlySecurityContext` and does **not** override
`authorizeTableTruncate` itself (it only overrides `authorizeHttp`, `authorizeLineTcp`,
`authorizePGWire`, `authorizeSelect(...)`, `authorizeSelectOnAnyColumn`, `authorizeSettings`,
`authorizeSqlEngineAdmin`, `authorizeSystemAdmin`). It inherits `authorizeTableTruncate`'s
throwing behavior straight from `ReadOnlySecurityContext`, and — now that
`ReadOnlySecurityContext.authorizeTableDelete` exists — it inherits that too, with the same
throwing semantics. This is exactly consistent with the "mirror `authorizeTableTruncate`" rule
since `DenyAllSecurityContext` doesn't mirror it via an override either; it inherits it. The
compiler agrees: no error was ever reported for this class.

### Test-source implementors — none required explicit overrides

The task brief flagged several test classes as likely needing an override. All of them turned
out to be subclasses of `AllowAllSecurityContext` / `ReadOnlySecurityContext` /
`DenyAllSecurityContext` that override only narrow, unrelated methods (e.g.
`authorizeSqlEngineAdmin`, `getPrincipal`, `authorizeSelect`) — none override
`authorizeTableTruncate`, so none needed an `authorizeTableDelete` override either; they all
inherit it from their parent concretely, consistently with how they already inherit
`authorizeTableTruncate`. Confirmed by reading each file and by `mvn -q -pl core -am
test-compile` succeeding with zero errors on the very first attempt after the two main-source
fixes:

- `core/src/test/java/io/questdb/test/cutlass/http/line/LineHttpSenderLoggingTest.java` — `TestSecurityContext extends AllowAllSecurityContext`
- `core/src/test/java/io/questdb/test/griffin/QueryRegistryLifecycleTest.java` — `BlockingPrincipalSecurityContext`, `PrincipalSecurityContext` (both `extends AllowAllSecurityContext`), `BlockingSqlEngineAdminSecurityContext`, `DenyingSqlEngineAdminSecurityContext`, `SingleReadPrincipalSecurityContext` (all `extends PrincipalSecurityContext`, transitively `AllowAllSecurityContext`)
- `core/src/test/java/io/questdb/test/griffin/engine/functions/activity/ExportActivityFunctionFactoryTest.java` — `AdminContext extends AllowAllSecurityContext`, `UserContext extends ReadOnlySecurityContext`
- `core/src/test/java/io/questdb/test/griffin/engine/functions/activity/QueryActivityFunctionFactoryTest.java` — `AdminContext`, `CharSequencePrincipalSecurityContext`, `PrincipalSecurityContext` (`extends AllowAllSecurityContext`), `UserContext extends ReadOnlySecurityContext`
- `core/src/test/java/io/questdb/test/griffin/engine/functions/activity/CancelQueryFunctionFactoryTest.java` — `AdminContext`, `RegularUserContext` (`extends AllowAllSecurityContext`), `ReadOnlyUserContext extends ReadOnlySecurityContext`
- `core/src/test/java/io/questdb/test/cairo/view/ViewDependencyGapAuthorizationTest.java` — `DenyBaseTableSelectSecurityContext extends AllowAllSecurityContext`
- `core/src/main/java/io/questdb/cutlass/line/tcp/LineTcpMeasurementEvent.java` — `PrincipalOnlySecurityContext extends DenyAllSecurityContext` (main source, not test, but same situation)

No `*SecurityContextFactory` classes were touched (`AllowAllSecurityContextFactory`,
`ReadOnlySecurityContextFactory`, `ReadOnlyUsersAwareSecurityContextFactory` implement
`SecurityContextFactory`, a different interface — confirmed by grep, left untouched).

## Verification of completeness

Repo-wide grep (not just `core`) for any other implementor, to make sure the `-pl core -am`
build scope wasn't hiding something in `benchmarks`/`utils`/`compat`:

```
grep -rn "implements SecurityContext\b" --include=*.java .
grep -rln "extends AllowAllSecurityContext\|extends ReadOnlySecurityContext\|extends DenyAllSecurityContext" --include=*.java .
```

Both returned matches only under `core/` (main + test) — the 9 classes/files listed above,
all already accounted for. `benchmarks`, `utils`, `compat` have none.

## Compile command + result

1. Added abstract method → `mvn -q -pl core -am compile` → **FAIL**, exactly 2 errors:
   - `AllowAllSecurityContext.java:[35,7] error: ... does not override abstract method authorizeTableDelete(TableToken)`
   - `ReadOnlySecurityContext.java:[35,7] error: ... does not override abstract method authorizeTableDelete(TableToken)`
2. Implemented both → `mvn -q -pl core -am compile` → exit code 0 (clean, only pre-existing `sun.misc.Unsafe` deprecation warnings from Guava, unrelated to this change).
3. `mvn -q -pl core -am test-compile` → exit code 0 on first attempt (no test-source implementor needed an explicit override, per the inheritance analysis above).
4. Re-ran `mvn -q -pl core -am test-compile` once more immediately before committing as a final check → exit code 0.

## Implementors updated: 2 (interface + `AllowAllSecurityContext` + `ReadOnlySecurityContext` = 3 files changed)

## Concerns

- **Interface member placement breaks alphabetical ordering.** The rest of
  `SecurityContext.java`'s `authorizeTable*` group is strictly alphabetical
  (`Create` → `Drop` → `Reindex` → `Rename` → `Truncate` → `Update` → `Vacuum`). Per the brief's
  explicit instruction (and the task description, which repeats it), `authorizeTableDelete` was
  placed right after `authorizeTableTruncate` instead of alphabetically between `Create` and
  `Drop`. This is intentional per the brief (grouping "delete" next to "truncate" as the two
  data-removal operations) but is a deliberate style deviation worth flagging in review in case
  a later pass wants it alphabetized instead. Same placement was mirrored in `AllowAllSecurityContext`
  and `ReadOnlySecurityContext` for consistency with the interface.
- No Enterprise implementors were touched (out of scope — Phase 4 per the task).
- `DeleteOperation.authorize()` (a later task) does not exist yet, so this method is currently
  unused; that's expected for this task's scope.
- Could not run a local Java formatter/spotless check (no such Maven plugin configured in this
  repo) to double-check IntelliJ-format compliance ahead of CI; the inserted blocks were typed
  to exactly match the existing file's indentation/brace/blank-line conventions by eye, mirroring
  the immediately adjacent `authorizeTableTruncate` methods.

## Commit

`7846101b34` — `feat(delete): add SecurityContext.authorizeTableDelete (OSS impls)`
(3 files changed, 11 insertions(+), 0 deletions(-))
