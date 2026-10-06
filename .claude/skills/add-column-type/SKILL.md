---
name: add-column-type
description: Add a column type to QuestDB by working the worklist the add-a-type tool writes (utils/type-probe), until it exits 0
argument-hint: "<NAME> --like <EXISTING> [--facts FILE]"
allowed-tools: Bash, Read, Edit, Write, Grep, Glob
---

# Add a column type

**Usage:** `/add-column-type <NAME> --like <EXISTING> [--facts FILE]`

Adds the column type `<NAME>` to the checkout by following the worklist of
`utils/type-probe/type_probe.py`. The tool lists every place the type needs a decision; this skill
makes those decisions one item at a time and changes nothing the worklist does not name. The
manual is `utils/type-probe/README.md`; read its sections 3 to 7 and 9 before the first edit.

## Rules

- Work on a throwaway branch. The tool edits the tree in place.
- Change only what an item of `worklist.md` names: the file and line of a build item, the site of
  a refusal, kit or coverage item, or the facts file. Write each edit into an edit log, with the
  worklist line it answers. An edit that answers no item is a decision outside the worklist: stop
  and ask.
- An answer may sit away from the item's line, and is still the item's answer: a new class the
  answer uses (the type's function base class, constant, column function, memoizer, import
  adapter), a cast factory with its line in `function_list.txt`, the writer arm in the per-row
  writer of the class the item names, a copier arm in each of the three copiers, the conversion
  rows of a `storage.alter` item. README section 7, "Some answers go where no single row names
  them", says where. Log such an edit against the item it answers.
- Never edit a recording, a golden table or a test's expectation to make an item go away. A kit or
  coverage item is work on the type, at the site it names. The tests that pin an answer for every
  tag check the existing types only, so they list nothing for a new type. The one expected list a
  type writes is `FunctionReachTest`'s, and only for reaches it states as meant.
- Never declare a guarded-site refusal (`refused_sites`) to silence an item without saying so in
  the edit log: a declared refusal is a deliberate answer, "this type is refused here". One label
  can cover several paths: `ILP column kind` covers ILP over TCP, HTTP and UDP.
- Run the tool again after each batch of items, with a new `--out` directory per run, so every
  worklist is kept.

## Procedure

1. Facts. `python3 utils/type-probe/type_probe.py init <NAME> --like <EXISTING> --tag <n> > <NAME>.toml`,
   where `<n>` is NULL's tag (the next free one). Edit every field the type differs in, and decide
   the two `init` leaves as `CHANGE-ME`: `wire_kind` (an existing kind whose bytes and NULL test the
   type shares, or `"new"`) and `signature_char` (a free character, or `"-"`). Skip this step when
   `--facts FILE` is given.
2. Run. `python3 utils/type-probe/type_probe.py run <NAME>.toml --out <dir>/run-<k>`. Exit 2 means
   the facts file or an anchor is wrong: fix what each `facts: <field>: <problem>` line says. Exit 3
   means a step failed in a way the tool cannot parse: read the log it names and report it.
3. Work the items, group by group, in the worklist's order:
   - `fill-driver-answer`: write the type driver's answer the line names (the generated
     `<Name>TypeDriver.java`), or the answer a test reports the driver gets wrong; for a kit path
     the line maps to `TypeDrivers.find`, the type's NULL, column function or relations.
   - `name-yourself`: add the type to the switch or match the line names, in the arm or refusal
     group its facts imply.
   - `add-writer-arm`: give the type's wire kind, NULL policy or order its arm. The item names the
     switch that chooses an opcode; the arm that writes the value goes where that opcode is read,
     in the per-row writer of the same class.
   - `implement-pair`: write the cast, CASE, UNION, copier or function code the pair needs, or
     refuse the pair in the relation it comes from. A copier conversion needs its arm in all three
     copiers: `RecordToRowCopierUtils.generateSingleMethodCopier`, `generateChunkedCopier`, and
     `LoopingRecordToRowCopier` (a target arm in the source's `copyFrom<Type>` method; as a
     source, an arm in `copyColumn` and a `copyFrom<Type>` of its own). The kit's
     `sql.insert_convert` runs each such conversion under all three and lists any difference.
   - `declare-or-admit`: either add the site's label to `refused_sites` (the type is refused there
     on purpose; the kit then checks the refusal) or admit the type with its own arm at the site.
   - `manual`: check the site the entry names; tick it in the manual-done file, as the worklist
     prints it (`- [x] manual | README "Manual list", item N`), and pass the file with
     `--manual-done`. The tool reads a tick by its item number only, so a tick on a wrong number
     silences another item.
   The kit and the coverage tests run only when every build group is empty. The kit step runs the
   new type alone first (about two minutes) and the whole kit (about ten) only once that pass is
   green. To iterate on one kit class, run it for the type alone:
   `mvn -o -B -pl core test -P local-client,build-rust-library -DfailIfNoTests=false
   -Dsurefire.failIfNoSpecifiedTests=false '-Dtest.include=%regex[.*TypeConformanceSqlTest.class]'
   -Dquestdb.test.kit.types=<name>`.
4. Repeat 2 and 3 until the run exits 0. A `site: unmapped` item at a line you wrote is your own
   compile error (it reads `fill-driver-answer`): fix it. A `site: unmapped` item anywhere else is
   a defect of the site map, not of the type: report it. Add one type at a time: the kit runs every
   type of `later-types.txt`, so a type left unfinished puts its failures into the next type's
   worklist (each names its type).

## Answers that go stale

A later decision can make an earlier answer wrong. A manual decision or a `declare-or-admit` that
admits the type somewhere (a cast, a widening, a function such as `between`) changes what the type
reaches, so `FunctionReachTest`'s list and the soundness tests' answers (`OverloadSoundnessTest`,
`ColumnConversionSoundnessTest`) can go stale. The kit step runs all three, so a stale answer comes
back as a coverage item at the test: answer it there, from the type's relations as they now stand.

## Checks the tool does not make

Exit 0 does not cover these; check each by hand before reporting done:

- ORDER BY with LIMIT. The kit checks where NULL sorts in `ORDER BY v` and `ORDER BY v DESC`
  (lowest, as every existing type with a NULL sorts it except FLOAT and DOUBLE, whose NaN sorts
  highest), but runs no LIMIT. Check `ORDER BY v LIMIT 3` both ways on a table holding NULL, the
  smallest and the largest value. A sort key orders the stored bits, so a type whose NULL word is
  not the lowest value of its order (an unsigned INT that keeps INT's NULL) takes no key kind in
  `SortKeyEncoder.keyKind` and puts NULL first in its compare arm.
- Rust unit tests. `ColumnTypeTag::VALUES` (`core/rust/qdb-core/src/col_type.rs`) and
  `test_lookup_driver` (`col_driver/mod.rs`) list the tags by hand, so their tests skip a new type
  without failing. Add it to both and run `cargo test --lib` in `core/rust/qdb-core`.

## The worklist format

```text
- [ ] <decision> | <location> | <message>[ | site: <label>]
```

Groups in order: `build-java`, `build-rust`, `build-c`, `refusal`, `kit`, `coverage`, `manual`.
Exit codes: 0 empty, 1 items or a skipped step, 2 invalid facts, anchor or command line, 3 a step
the tool cannot parse.

## Done

The run exits 0 with no skipped step: every build is clean, the kit and the coverage tests pass with
the type in `later-types.txt`, and the manual list is worked; and the checks above are made. Report
the runs, the items worked, the edit log and what the checks found.
