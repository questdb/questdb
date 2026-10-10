"""Tests of the add-a-type tool. Run from the repository root:

    python3 -m unittest discover -s utils/type-probe

The parsers run on outputs cut from real runs (testdata/); the facts checks and the registration
run on the checkout the tests sit in, the registration on a copy of its anchored files; the
failure mapping on a few places made here, and on one scan of the checkout (audit.py).
"""

import contextlib
import io
import re
import shutil
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest import mock

import audit
import type_probe as tp

DATA = Path(__file__).resolve().parent / 'testdata'
REPO = Path(__file__).resolve().parents[2]
JAVA = 'core/src/main/java/io/questdb/'
_SCAN = []


def data(name):
    return (DATA / name).read_text(encoding='utf-8')


def fixture_sites():
    """A few places, with the refused decisions of the guards among them."""
    def place(file, method, form, anchor, line=10, last=20, values=()):
        return audit.Place(file=JAVA + file, line=line, method=method, form=form, anchor=anchor, values=frozenset(values), last_line=last)

    places = [
        place('cutlass/line/tcp/QwpWalAppender.java', 'appendToWalColumnar', 'value-test', 'qwp'),
        place('cutlass/line/LineUtils.java', 'columnKind', 'value-switch', 'ilp'),
        place('griffin/SqlCodeGenerator.java', 'isFixedSizePrevSlotEligible', 'value-switch', 'prev'),
        place('griffin/engine/groupby/SampleByFillValueRecordCursorFactory.java', 'createPlaceHolderFunction', 'value-test', 'value'),
        place('cairo/wal/WalColumnarRowAppender.java', 'unsupportedColumnType', 'value-test', 'wal'),
        place('griffin/SqlCodeGenerator.java', 'generateCastFunction', 'tag-enum-switch', 'union', 3000, 3200),
        place('griffin/SqlCodeGenerator.java', 'generateCastFunction', 'tag-switch', 'inner', 3100, 3110),
        place('griffin/engine/orderby/SortKeyEncoder.java', 'keyKind', 'value-switch', 'key'),
        place('cairo/wal/WalColumnarRowAppender.java', 'putFixedColumn', 'value-switch', 'policy', 500, 600, ['NullPolicy.SENTINEL']),
    ]
    refused = {'qwp': 'QWP WAL append', 'ilp': 'ILP column kind: cast error from protocol type', 'prev': 'SAMPLE BY FILL(PREV)',
               'value': 'SAMPLE BY FILL(value)', 'wal': 'WAL columnar append'}
    decisions = [audit.Decision(*p.key(), 'refused', refused[p.anchor]) for p in places if p.anchor in refused]
    return tp.Sites(audit.Vocabulary(), places, decisions, REPO)


def real_sites():
    """The places of the checkout and its places.tsv, scanned once for the whole run."""
    if not _SCAN:
        _SCAN.append(tp.Sites.load(tp.Tree(REPO)))
    return _SCAN[0]


def valid_facts(tree, name='PROBE_INT', like='INT'):
    """A valid facts file for a new type registered next, as init writes it with its choices made."""
    text = tp.init_facts(tree, name, like, tree.null_code())
    free = next(c for c in 'pqrvwxyzgcmoy' if c not in {s.lower() for s in tree.signature_chars()})
    text = text.replace('wire_kind = "CHANGE-ME"', 'wire_kind = "new"').replace('signature_char = "CHANGE-ME"', f'signature_char = "{free}"')
    return tomllib.loads(text)


class ParserTest(unittest.TestCase):
    def test_javac_errors(self):
        diags = tp.parse_javac(data('javac-main.txt'), REPO)
        self.assertEqual(7, len(diags))
        first = diags[0]
        self.assertEqual('core/src/main/java/io/questdb/cutlass/text/types/TypeManager.java', first.file)
        self.assertEqual(149, first.line)
        self.assertEqual('the switch expression does not cover all possible input values', first.message)
        self.assertIn('NnIntTypeDriver is not abstract and does not override abstract method setNull(long,long) in TypeDriver',
                      [d.message for d in diags])
        # warnings and notes are no errors
        self.assertFalse(any('warning' in d.message or 'Unsafe' in d.message for d in diags))

    def test_javac_missing_symbol_names_the_symbol(self):
        diags = tp.parse_javac(data('javac-symbol.txt'), REPO)
        self.assertEqual(['cannot find symbol: variable defineBindVariable', 'cannot find symbol: variable nullConstant'],
                         [d.message for d in diags])
        self.assertEqual([67, 69], [d.line for d in diags])

    def test_cargo_json(self):
        diags = tp.parse_cargo(data('cargo-check.json'))
        # the library and its test build report each match once each; one diagnostic per place
        self.assertEqual(5, len(diags))
        places = sorted((Path(d.file).name, d.line) for d in diags)
        self.assertEqual([('col_type.rs', 194), ('col_type.rs', 236), ('col_type.rs', 275), ('col_type.rs', 316), ('mod.rs', 61)], places)
        self.assertTrue(all(d.file.startswith('/repo/core/rust/qdb-core/src/') for d in diags))
        self.assertTrue(all(d.message.startswith('non-exhaustive patterns') for d in diags))

    def test_cmake_log(self):
        diags = tp.parse_cmake(data('cmake.txt'), '/repo')
        self.assertEqual([('core/src/main/c/share/column_type.h', 110, 13), ('core/src/main/c/share/converters.h', 63, 13)],
                         [(d.file, d.line, d.col) for d in diags])
        self.assertEqual("enumeration value 'NN_INT' not handled in switch [-Werror,-Wswitch]", diags[0].message)

    def test_surefire_report(self):
        failures = tp.parse_surefire(data('TEST-ingest.xml'))
        self.assertEqual(['testQwp[UINT32]', 'testIlpTcp[UINT32]', 'testIlpHttp[UINT32]'], [f.name for f in failures])
        self.assertEqual('io.questdb.test.cairo.types.TypeConformanceIngestTest', failures[0].classname)
        self.assertIn('no family arm for UINT32 at QWP WAL append', failures[0].text)
        # an error element counts as a failure too
        coverage = tp.parse_surefire(data('TEST-coverage.xml'))
        self.assertEqual(['testUnionHasACastForEveryAdmittedPair', 'testOpcodeFunctionsHandleEveryType'], [f.name for f in coverage])

    def test_refusal_regex(self):
        text = '\n'.join([
            'cairo error: no family arm for UINT32 at SAMPLE BY FILL(value): add the arm or declare the type like its namesake',
            'BINARY with NN_INT: CairoException [0] no UNION cast for NN_INT to STRING at UNION: implement the cast or refuse the pair in the UNION matrix',
            'rejected [status=INTERNAL_ERROR, error=no compare arm for UINT32 at ORDER BY: add a compare arm or declare the type ordered like its family]',
            'write failed (no family arm for UINT32 at QWP WAL append: add the arm or declare the type like its namesake)',
            'no family arm here',
        ])
        # a closing bracket the server's wrapper adds after the text is not part of it
        self.assertEqual([
            ('no family arm', 'UINT32', 'SAMPLE BY FILL(value)', 'add the arm or declare the type like its namesake'),
            ('no UNION cast', 'NN_INT to STRING', 'UNION', 'implement the cast or refuse the pair in the UNION matrix'),
            ('no compare arm', 'UINT32', 'ORDER BY', 'add a compare arm or declare the type ordered like its family'),
            ('no family arm', 'UINT32', 'QWP WAL append', 'add the arm or declare the type like its namesake'),
        ], tp.find_refusals(text))
        # a site label never holds ": ", so the first ": " after "at" ends it
        self.assertEqual('= NULL', tp.find_refusals('no family arm for X at = NULL: add the arm')[0][2])


class SitesTest(unittest.TestCase):
    def test_refusal_drops_a_stack_frame_on_its_line(self):
        text = ('expected:<[]> but was:<[comparator: uint32 is not handled: CairoException: [0] no compare arm for '
                'UINT32 at ORDER BY: add a compare arm or declare the type ordered like its family at '
                'io.questdb.cairo.CairoException.instance(CairoException.java:622)')
        self.assertEqual([('no compare arm', 'UINT32', 'ORDER BY',
                           'add a compare arm or declare the type ordered like its family')], tp.find_refusals(text))

    def test_declarable_sites(self):
        sites = fixture_sites()
        self.assertEqual({'ILP column kind', 'QWP WAL append', 'SAMPLE BY FILL(PREV)', 'SAMPLE BY FILL(value)', 'WAL columnar append'},
                         set(sites.declarable()))
        self.assertEqual('no family arm for <type> at QWP WAL append', sites.declarable()['QWP WAL append'].message)
        # a guard that keeps the error it raised before raises that
        self.assertEqual('cast error from protocol type', sites.declarable()['ILP column kind'].message)

    def test_refusal_maps_to_the_guard_or_the_place_of_its_lead(self):
        sites = fixture_sites()
        self.assertEqual('SAMPLE BY FILL(value)', sites.by_refusal('no family arm', 'SAMPLE BY FILL(value)').site)
        self.assertEqual(('RecordComparatorCompiler.comparatorOpcode', 'add-writer-arm'),
                         (sites.by_refusal('no compare arm', 'ORDER BY').site, sites.by_refusal('no compare arm', 'ORDER BY').decision))
        self.assertEqual('SqlCodeGenerator.generateCastFunction', sites.by_refusal('no UNION cast', 'UNION').site)
        self.assertIsNone(sites.by_refusal('no family arm', 'nowhere'))

    def test_kept_text_maps_to_its_site(self):
        sites = fixture_sites()
        site = sites.by_kept_text('table: dst_w1, column: v; cast error from protocol type: LONG to column type: UINT32')
        self.assertEqual(('ILP column kind', 'declare-or-admit'), (site.site, site.decision))
        self.assertEqual('ColumnTypeConverter.convertFromFixedSize', sites.by_kept_text('error: unsupported conversion').site)

    def test_kit_path_names_its_places_by_value_row(self):
        sites = real_sites()
        self.assertEqual('setBoolean0', sites.by_instrument('sql.bind_value', 'setBoolean')[0].method)
        self.assertEqual('setVarchar0', sites.by_instrument('sql.bind_value', 'setVarchar')[0].method)
        self.assertEqual('ILP column kind', sites.by_kit_guard('ingest.ilp-tcp').site)

    def test_the_innermost_place_at_a_line(self):
        sites = fixture_sites()
        file = JAVA + 'griffin/SqlCodeGenerator.java'
        self.assertEqual('SqlCodeGenerator.generateCastFunction tag-switch', sites.at(file, 3105).site)
        self.assertEqual(('SqlCodeGenerator.generateCastFunction tag-enum-switch', 'name-yourself'),
                         (sites.at(file, 3150).site, sites.at(file, 3150).decision))
        # a switch over the NULL policy asks for the arm the per-row writer reads
        self.assertEqual('add-writer-arm', sites.at(JAVA + 'cairo/wal/WalColumnarRowAppender.java', 550).decision)
        self.assertIsNone(sites.at(file, 1))

    def test_committed_places_read(self):
        sites = real_sites()
        self.assertEqual({'memoized virtual column', 'SAMPLE BY FILL(PREV)', 'SAMPLE BY FILL(LINEAR)', 'SAMPLE BY FILL(value)',
                          'COPY bind snapshot', 'ILP column kind', 'WAL columnar append', 'QWP WAL append', 'Parquet conversion',
                          'between', '= NULL', 'copier conversion'}, set(sites.declarable()))
        self.assertEqual([], audit.check(REPO, sites.places, sites.decisions))

    def test_every_hint_names_existing_code(self):
        guards = tp.guard_labels(real_sites().decisions)
        for prefix, file, method, decision in tp.LAYERS + tuple((t, f, m, d) for t, f, m, d in tp.KEPT_TEXTS) + tp.LEADS:
            self.assertIn(method, [name for name, _first, _last in tp.method_spans(REPO / file)], f'{prefix}: {file} {method}')
            self.assertIn(decision, tp.DECISIONS)
        for prefix, label in tp.KIT_GUARDS:
            self.assertIn(label, guards, prefix)


class FactsTest(unittest.TestCase):
    tree = tp.Tree(REPO)
    guards = tp.guard_labels(audit.load_decisions(REPO / audit.PLACES_FILE))

    def problems(self, facts):
        return tp.validate_facts(facts, self.tree, self.guards)

    def assertProblem(self, facts, field, fragment):
        problems = self.problems(facts)
        hits = [p for p in problems if p.startswith(f'facts: {field}:') and fragment in p]
        self.assertTrue(hits, f'no problem of {field} with "{fragment}" in {problems}')

    def test_init_writes_a_complete_file(self):
        text = tp.init_facts(self.tree, 'PROBE_INT', 'INT', self.tree.null_code())
        facts = tomllib.loads(text)
        self.assertEqual('W4', facts['physical']['movement'])
        self.assertEqual('INT', facts['physical']['accessor'])
        self.assertEqual(['PROBE_INT', 'INT', 'LONG', 'FLOAT', 'DOUBLE', 'TIMESTAMP', 'DATE', 'DECIMAL'], facts['relations']['implicit_casts'])
        self.assertEqual([], facts['kit']['refused_sites'])
        # the two choices init leaves to the author fail validation until made
        problems = self.problems(facts)
        self.assertTrue(any('wire_kind: still CHANGE-ME' in p for p in problems), problems)
        self.assertTrue(any('signature_char' in p for p in problems), problems)

    def test_taken_signature_chars(self):
        taken = self.tree.signature_chars()
        # DATE's argument list holds a comment with a comma; the pseudo types' come from the descriptor
        self.assertIn('m', taken)
        self.assertIn('i', taken)
        self.assertIn('c', taken)
        date = self.tree.driver_facts('DATE')
        self.assertEqual("'m'", date[10])
        self.assertEqual('PgTypeOids.PG_TIMESTAMP', date[9])

    def test_valid_facts(self):
        self.assertEqual([], self.problems(valid_facts(self.tree)))

    def test_every_error(self):
        f = valid_facts(self.tree)
        del f['physical']['accessor']
        self.assertProblem(f, 'physical.accessor', 'missing')
        f = valid_facts(self.tree)
        f['physical']['colour'] = 'blue'
        self.assertProblem(f, 'physical.colour', 'unknown field')
        f = valid_facts(self.tree)
        f['physical']['movement'] = 'W3'
        self.assertProblem(f, 'physical.movement', 'is no Movement')
        f = valid_facts(self.tree)
        f['physical']['null_policy'] = 'MAYBE'
        self.assertProblem(f, 'physical.null_policy', 'is no NullPolicy')
        f = valid_facts(self.tree)
        f['type']['tag'] = self.tree.tags()['INT']
        self.assertProblem(f, 'type.tag', 'is taken by INT')
        f = valid_facts(self.tree)
        f['type']['tag'] = self.tree.null_code() + 3
        self.assertProblem(f, 'type.tag', 'is not the next free tag')
        f = valid_facts(self.tree, name='PROBE_INT')
        f['type']['name'] = 'DOUBLE'
        self.assertProblem(f, 'type.name', 'is taken by a tag')
        f = valid_facts(self.tree)
        f['functions']['signature_char'] = 'i'
        self.assertProblem(f, 'functions.signature_char', 'is taken')
        # a pseudo type's signature character is taken too
        f['functions']['signature_char'] = 'c'
        self.assertProblem(f, 'functions.signature_char', 'is taken')
        f = valid_facts(self.tree)
        f['type']['storage'] = 'var'
        self.assertProblem(f, 'physical.movement', 'disagrees with type.storage')
        f = valid_facts(self.tree)
        f['relations']['implicit_casts'].append('FLOAT128')
        self.assertProblem(f, 'relations.implicit_casts', 'names no tag')
        f = valid_facts(self.tree)
        f['kit']['refused_sites'] = ['SAMPLE BY FILL(value)', 'SAMPLE BY FILL(VALUE)']
        problems = self.problems(f)
        self.assertEqual(1, len([p for p in problems if p.startswith('facts: kit.refused_sites:')]), problems)
        self.assertProblem(f, 'kit.refused_sites', "'SAMPLE BY FILL(VALUE)'")
        f = valid_facts(self.tree)
        f['protocols']['pg_oid'] = 'PG_INT3'
        self.assertProblem(f, 'protocols.pg_oid', 'PgTypeOids')
        f = valid_facts(self.tree)
        f['physical']['wire_kind'] = 'NO_SUCH_KIND'
        self.assertProblem(f, 'physical.wire_kind', 'is no WireKind')

    def test_new_wire_kind_needs_a_free_name(self):
        f = valid_facts(self.tree, name='PROBE_INT')
        f['type']['name'] = 'IPV4'
        self.assertProblem(f, 'physical.wire_kind', 'WireKind already has')

    def test_declared_refusals(self):
        f = valid_facts(self.tree)
        f['kit']['refused_sites'] = ['SAMPLE BY FILL(value)', 'ILP column kind', 'WAL columnar append']
        self.assertEqual([], self.problems(f))
        self.assertIn('| SAMPLE BY FILL(value), ILP column kind, WAL columnar append', tp.later_types_line(f))


ANCHORED = [rel for _, rel in tp.ANCHORS]


class RegistrationTest(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        for rel in set(ANCHORED) | {f'{tp.CAIRO_DIR}/{n}.java' for n in ('PhysicalDescriptor', 'NullPolicy', 'RelationKind', 'CastTarget', 'PgTypeOids', 'IntTypeDriver', 'TypeDriver', 'ColumnTypeDriver')}:
            target = self.dir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(REPO / rel, target)
        self.tree = tp.Tree(self.dir)
        self.facts = valid_facts(self.tree)

    def tearDown(self):
        shutil.rmtree(self.dir)

    def snapshot(self):
        return {p: p.read_bytes() for p in sorted(self.dir.rglob('*')) if p.is_file()}

    def test_register_inserts_above_the_anchors_and_moves_null(self):
        null = self.tree.null_code()
        tp.register(self.tree, self.facts, [])
        tags = self.tree.tags()
        self.assertEqual(null, tags['PROBE_INT'])
        self.assertEqual(null + 1, tags['NULL'])
        constants = self.tree.read(f'{tp.CAIRO_DIR}/ColumnType.java')
        self.assertRegex(constants, rf'public static final short PROBE_INT = \w+ \+ 1; // = {null};\n\s*// type-registration: column type constant')
        self.assertRegex(constants, rf'short NULL = PROBE_INT \+ 1;\s*// = {null + 1}; ALWAYS the last')
        self.assertIn('nameTypeMap.put("probe_int", PROBE_INT);', constants)
        self.assertIn('case PROBE_INT -> ProbeIntTypeDriver.INSTANCE;', self.tree.read(f'{tp.CAIRO_DIR}/TypeDrivers.java'))
        self.assertIn(f'ProbeInt = {null},', self.tree.read('core/rust/qdb-core/src/col_type.rs'))
        native = self.tree.read('core/src/main/c/share/column_type.h')
        self.assertIn(f'PROBE_INT = {null},', native)
        self.assertIn(f'NULL_ = {null + 1},', native)
        self.assertIn('PROBE_INT', self.tree.enum('WireKind'))

    def test_second_run_changes_nothing(self):
        tp.register(self.tree, self.facts, [])
        tp.write_generated(self.tree, self.facts, [])
        before = self.snapshot()
        # the second run validates against the registered tree, then writes the same
        self.assertEqual([], tp.validate_facts(self.facts, self.tree, tp.guard_labels(audit.load_decisions(REPO / audit.PLACES_FILE))))
        self.assertEqual([], tp.register(self.tree, self.facts, []))
        tp.write_generated(self.tree, self.facts, [])
        self.assertEqual(before, self.snapshot())

    def test_facts_follow_the_facts_file_and_answers_stay(self):
        tp.write_generated(self.tree, self.facts, [])
        driver = self.dir / tp.CAIRO_DIR / 'ProbeIntTypeDriver.java'
        # the author writes an answer
        driver.write_text(driver.read_text(encoding='utf-8').replace('                setNull\n', '                (addr, count) -> Vect.setMemoryInt(addr, 0, count)\n'), encoding='utf-8')
        self.facts['physical']['null_word'] = '0'
        self.facts['relations']['cast_target'] = 'NEVER'
        tp.write_generated(self.tree, self.facts, [])
        text = driver.read_text(encoding='utf-8')
        self.assertIn('CastTarget.NEVER', text)
        self.assertNotIn('Numbers.encodeLowHighInts', text)
        self.assertIn('(addr, count) -> Vect.setMemoryInt(addr, 0, count)', text)

    def test_another_later_types_driver_answer_is_a_driver_item(self):
        # two types added together: the second type's run builds the first type's driver too, whose
        # answers its author has not written yet
        tp.write_generated(self.tree, self.facts, [])
        first = f'{tp.CAIRO_DIR}/ProbeIntTypeDriver.java'
        line = next(i for i, l in enumerate(self.tree.read(first).split('\n'), 1) if l.strip() == 'nullConstant,')
        diag = tp.Diag(first, line, 0, 'cannot find symbol: variable nullConstant')
        item = tp.build_item('build-java', diag, fixture_sites(), self.tree, f'{tp.CAIRO_DIR}/ProbeUintTypeDriver.java')
        self.assertEqual(('fill-driver-answer', ''), (item.decision, item.site))

    def test_existing_wire_kind_adds_none(self):
        self.facts['physical']['wire_kind'] = 'INT'
        tp.register(self.tree, self.facts, [])
        self.assertNotIn('PROBE_INT', self.tree.enum('WireKind'))

    def test_missing_or_doubled_anchor(self):
        rel = f'{tp.CAIRO_DIR}/TypeDrivers.java'
        path = self.dir / rel
        text = path.read_text(encoding='utf-8')
        path.write_text(text.replace(tp.anchor_comment('type driver lookup'), 'nothing here'), encoding='utf-8')
        with self.assertRaises(tp.UsageError) as e:
            tp.register(self.tree, self.facts, [])
        self.assertIn('"type driver lookup" is missing', e.exception.problems[0])
        line = next(l for l in text.split('\n') if tp.anchor_comment('type driver lookup') in l)
        path.write_text(text.replace(line, line + '\n' + line), encoding='utf-8')
        with self.assertRaises(tp.UsageError) as e:
            tp.register(self.tree, self.facts, [])
        self.assertIn('appears 2 times', e.exception.problems[0])

    def test_fixed_size_driver_is_a_facts_instance(self):
        text = tp.driver_source(self.facts, tree=self.tree)
        self.assertIn('public final class ProbeIntTypeDriver extends FixedSizeTypeDriver', text)
        self.assertIn('WireKind.PROBE_INT', text)
        self.assertIn('Numbers.encodeLowHighInts(Numbers.INT_NULL, Numbers.INT_NULL)', text)
        self.assertIn('import io.questdb.std.Numbers;', text)
        self.assertNotRegex(text, r'\$\{')
        # the six code answers are names javac reports, one per line
        for answer in ('defineBindVariable', 'nullConstant', 'typeConstant', 'columnFunction', 'nullAppender', 'setNull'):
            self.assertRegex(text, rf'\n\s+{answer},?\n')

    def test_var_size_driver_stubs_every_other_answer(self):
        self.facts['type']['storage'] = 'var'
        self.facts['physical']['movement'] = 'VAR'
        text = tp.driver_source(self.facts, tree=self.tree)
        self.assertIn('implements ColumnTypeDriver', text)
        self.assertIn('setNullAnswer();', text)
        self.assertIn('defineBindVariableAnswer;', text)
        self.assertEqual(2, text.count('configureAuxMemMA('))
        self.assertNotRegex(text, r'\$\{')

    def test_later_types_line(self):
        tp.write_generated(self.tree, self.facts, [])
        line = (self.dir / tp.LATER_TYPES_FILE).read_text(encoding='utf-8').split('\n')[0]
        paths = ' '.join(tp.KIT_PATHS)
        self.assertEqual(f'PROBE_INT | probe_int | SENTINEL | {paths} | I32 |', line)


class WorklistTest(unittest.TestCase):
    facts = {'kit': {'refused_sites': ['SAMPLE BY FILL(PREV)']}}

    def test_one_item_per_location_whatever_the_test_order(self):
        def at(type_label, message):
            return tp.Item('kit', 'implement-pair', '`kit:sql.cast@single-nojit#-`',
                           f'type={type_label} row=- path=sql.cast mode=single-nojit: {message}', 'TypeDrivers.find')
        other, own = at('nn_int', '5 casts break an invariant'), at('uint32', '9 casts break an invariant')
        for order in ([other, own], [own, other]):
            self.assertEqual([own], tp.sort_items(order, 'uint32'))
            self.assertEqual([other], tp.sort_items(order))

    def test_temporary_directory_reads_the_same_on_every_run(self):
        self.assertEqual('error: write : [-1] cannot insert rows out of order. Table=<tmp>/dbRoot/ins_n0o~',
                         tp.ascii_message('error: write : [-1] cannot insert rows out of order. '
                                          'Table=/tmp/junit8367132894678967833/dbRoot/ins_n0o~'))

    def test_message_rule(self):
        self.assertEqual('a / b', tp.ascii_message('a | b\nsecond line'))
        self.assertEqual('caf? ok', tp.ascii_message('café ok'))
        long = tp.ascii_message('x' * 300)
        self.assertEqual(200, len(long))
        self.assertTrue(long.endswith('...'))
        # a place keeps its whole description; a coverage failure keeps every line
        self.assertEqual(300, len(tp.Item('namesake', 'decide-place', 'here', 'x' * 300).line().split(' | ')[2]))
        self.assertEqual('expected:<BYTE -> CHAR / SHORT -> CHAR>',
                         tp.Item('coverage', 'implement-pair', 'here', 'expected:<BYTE -> CHAR\nSHORT -> CHAR>').line().split(' | ')[2])

    def test_render(self):
        items = tp.sort_items([
            tp.Item('namesake', 'decide-place', '`core/c.java:12`', 'second'),
            tp.Item('build-java', 'name-yourself', '`core/b.java:20`', 'switch | not covered', 'SqlCodeGenerator.generateCastFunction tag-enum-switch'),
            tp.Item('build-java', 'name-yourself', '`core/b.java:3`', 'the same switch'),
            tp.Item('build-java', 'name-yourself', '`core/b.java:20`', 'repeat of the first'),
            tp.Item('namesake', 'decide-place', '`core/c.java:9`', 'first'),
        ])
        text = tp.render_worklist('NN_INT', '/x/NN_INT.toml', 'ab12', '`b` at `c`', __import__('datetime').datetime(2026, 10, 4, 10, 2), 400, items, ['the kit did not run'])
        lines = text.split('\n')
        self.assertEqual('# Worklist: NN_INT', lines[0])
        self.assertIn('- Facts: `/x/NN_INT.toml` (sha256 ab12)', lines)
        self.assertIn('- Run: 2026-10-04 10:02, 6 min 40 s', lines)
        self.assertIn('- Items: 4 (build-java 2, build-rust 0, build-c 0, refusal 0, kit 0, coverage 0, namesake 2)', lines)
        self.assertIn('- Note: the kit did not run', lines)
        headings = [l for l in lines if l.startswith('## ')]
        self.assertEqual(['## build-java (2)', '## build-rust (0)', '## build-c (0)', '## refusal (0)', '## kit (0)', '## coverage (0)', '## namesake (2)'], headings)
        body = [l for l in lines if l.startswith('- [ ]')]
        self.assertEqual([
            '- [ ] name-yourself | `core/b.java:3` | the same switch',
            '- [ ] name-yourself | `core/b.java:20` | switch / not covered | site: SqlCodeGenerator.generateCastFunction tag-enum-switch',
            '- [ ] decide-place | `core/c.java:9` | first',
            '- [ ] decide-place | `core/c.java:12` | second',
        ], body)
        for line in body:
            self.assertRegex(line, r'^- \[ \] (' + '|'.join(tp.DECISIONS) + r') \| [^|]+ \| [^|]+( \| site: [^|]+)?$')

    def test_failures_map_to_sites(self):
        sites = fixture_sites()
        items = [i for f in tp.parse_surefire(data('TEST-ingest.xml')) for i in tp.failure_items(f, sites, {'kit': {'refused_sites': []}})]
        self.assertEqual([
            ('refusal', 'declare-or-admit', 'QWP WAL append'),
            ('kit', 'declare-or-admit', 'ILP column kind'),
            ('kit', 'declare-or-admit', 'ILP column kind'),
        ], [(i.group, i.decision, i.site) for i in items])
        self.assertEqual('`kit:ingest.ilp-tcp@nonwal-day#min`', items[1].location)
        tree = tp.Tree(REPO)
        coverage = tp.sort_items([i for f in tp.parse_surefire(data('TEST-coverage.xml')) for i in tp.failure_items(f, sites, self.facts, tree)])
        # the two pairs refused at UNION are one place to decide, located at the UNION cast switch
        self.assertEqual([
            ('refusal', 'implement-pair', 'SqlCodeGenerator.generateCastFunction'),
            ('coverage', 'implement-pair', 'SortKeyEncoder.keyKind value-switch'),
        ], [(i.group, i.decision, i.site) for i in coverage])
        self.assertRegex(coverage[0].location, r'^`core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:\d+`$')
        self.assertIn('no UNION cast for NN_INT to STRING at UNION', coverage[0].message)

    def test_declared_refusal_is_no_item(self):
        failure = tp.Failure('io.questdb.test.cairo.types.TypeConformanceSqlTest', 'testQueries[UINT32]',
                             'type=UINT32 row=- path=sql.fill_prev mode=single-nojit: x',
                             'no family arm for UINT32 at SAMPLE BY FILL(PREV): add the arm or declare the type like its namesake\n'
                             'no family arm for UINT32 at SAMPLE BY FILL(value): add the arm or declare the type like its namesake')
        items = tp.failure_items(failure, fixture_sites(), self.facts)
        self.assertEqual([('declare-or-admit', 'SAMPLE BY FILL(value)')], [(i.decision, i.site) for i in items])

    def test_unmapped_failure_is_flagged(self):
        failure = tp.Failure('io.questdb.test.cairo.types.TypeConformanceSqlTest', 'testQueries[UINT32]',
                             'type=UINT32 row=- path=other.nowhere mode=single-nojit: odd', '')
        self.assertEqual('unmapped', tp.failure_items(failure, fixture_sites(), self.facts)[0].site)

    def test_kit_failure_listing_two_paths_gives_an_item_per_path(self):
        # the SQL kit runs every path of a mode and reports the failing ones together
        message = ('type=UINT32 row=- path=sql.cast mode=single-nojit: first\n'
                   'type=UINT32 row=r2 path=sql.copy_bind mode=single-nojit: second\nmore of the second')
        failure = tp.Failure('io.questdb.test.cairo.types.TypeConformanceSqlTest', 'testQueries[uint32]', message,
                             'java.lang.AssertionError: ' + message + '\n\tat io.questdb.test.X.y(X.java:1)')
        items = tp.failure_items(failure, fixture_sites(), self.facts)
        self.assertEqual(['`kit:sql.cast@single-nojit#-`', '`kit:sql.copy_bind@single-nojit#r2`'], [i.location for i in items])
        self.assertEqual('type=UINT32 row=r2 path=sql.copy_bind mode=single-nojit: second\nmore of the second', items[1].message)

    def test_failure_on_a_path_no_place_names_maps_to_its_layer(self):
        def item(path, sites=None):
            failure = tp.Failure('io.questdb.test.cairo.types.TypeConformanceSqlTest', 'testQueries[UINT32]',
                                 f'type=UINT32 row=null path={path} mode=nonwal-day: the value arrived as NULL', '')
            found = tp.failure_items(failure, sites or fixture_sites(), self.facts)[0]
            return found.decision, found.site
        self.assertEqual(('add-writer-arm', 'PGPipelineEntry.outColumnOpcode'), item('pg.binary'))
        self.assertEqual(('add-writer-arm', 'ExportQueryProcessor.csvOpcode'), item('http.csv'))
        self.assertEqual(('fill-driver-answer', 'TypeDrivers.find'), item('storage.insert'))
        self.assertEqual(('implement-pair', 'TypeDrivers.find'), item('sql.cast'))
        # a path that reaches a guard maps to the guard; one places.tsv names maps to that place
        self.assertEqual(('declare-or-admit', 'ILP column kind'), item('ingest.ilp-tcp'))
        self.assertIn(item('storage.alter', real_sites()), {('implement-pair', 'DecimalColumnTypeConverter.getLoader tag-switch'),
                                                            ('implement-pair', 'converters.Java_io_questdb_griffin_ConvertersNative_fixedToFixed c-switch')})

    def test_order_and_conversion_failures_map_to_their_arms(self):
        def item(path):
            failure = tp.Failure('io.questdb.test.cairo.types.TypeConformanceSqlTest', 'testQueries[UINT32]',
                                 f'type=UINT32 row=null path={path} mode=single-nojit: NULL sorts mid-range', '')
            found = tp.failure_items(failure, fixture_sites(), self.facts)[0]
            return found.decision, found.site
        # both orders go to the comparator, whose compare arm decides where NULL sorts
        self.assertEqual(('add-writer-arm', 'RecordComparatorCompiler.comparatorOpcode'), item('sql.order_asc'))
        self.assertEqual(item('sql.order_asc'), item('sql.order_desc'))
        self.assertEqual(('implement-pair', 'RecordToRowCopierUtils.copyOpcode'), item('sql.insert_convert'))

    def test_coverage_failure_naming_two_methods_gives_two_items(self):
        # a coverage test that lists every function that does not handle the type
        message = 'expected:<[]> but was:<[fixedTargetOpcode: nn_int is not handled, columnKind: nn_int is not handled]>'
        failure = tp.Failure('io.questdb.test.cairo.types.GeneratedAccessorCoverageTest', 'testCopierRunsForEveryType', message, message)
        items = tp.failure_items(failure, real_sites(), self.facts)
        self.assertEqual(['ILP column kind', 'ParquetColumnTypeConverter.fixedTargetOpcode value-test'], sorted(i.site for i in items))

    def test_coverage_failure_maps_to_the_relation_its_test_checks(self):
        sites = fixture_sites()

        def item(cls, test, message):
            failure = tp.Failure(f'io.questdb.test.griffin.{cls}', test, message, message)
            found = tp.failure_items(failure, sites, self.facts)[0]
            return found.decision, found.location, found.site
        self.assertEqual(('implement-pair', '`RelationCoverageTest#testCopierHasAnArmForEveryAdmittedPair`', 'RecordToRowCopierUtils.copyOpcode'),
                         item('RelationCoverageTest', 'testCopierHasAnArmForEveryAdmittedPair', 'expected:<BYTE -> CHAR'))
        self.assertEqual(('implement-pair', '`RelationCoverageTest#testCaseEscalationHasAnImplementation`', 'CaseCommon.castRow'),
                         item('RelationCoverageTest', 'testCaseEscalationHasAnImplementation', 'expected:<> but was:<NN_INT -> LONG: no cast'))
        # the soundness tests the kit step runs with the coverage tests: an answer of their switches
        self.assertEqual(('name-yourself', '`OverloadSoundnessTest#testDeclaredWideningRowsAreImplemented`', 'OverloadSoundnessTest.isExplicitCast'),
                         item('OverloadSoundnessTest', 'testDeclaredWideningRowsAreImplemented', 'no widening row leads to DECIMAL'))
        self.assertEqual(('implement-pair', '`ColumnConversionSoundnessTest#testEveryAdmittedConversionHasAConverter`', 'ColumnTypeConverter.convertFromFixedSize'),
                         item('ColumnConversionSoundnessTest', 'testEveryAdmittedConversionHasAConverter', 'UINT32 -> BYTE: x'))
        self.assertEqual(('fill-driver-answer', '`FunctionReachTest#testLaterTypesReachNoOtherTypesFunction`', 'TypeDrivers.find'),
                         item('FunctionReachTest', 'testLaterTypesReachNoOtherTypesFunction', 'expected:<> but was:<nn_int -> !=(BYTE, nn_int)'))
        self.assertEqual(('fill-driver-answer', '`TypeDriverTest#testSizesMatchReferenceTables`', 'TypeDrivers.find'),
                         item('TypeDriverTest', 'testSizesMatchReferenceTables', 'isFixedSize 41 expected:<false> but was:<true>'))
        self.assertEqual('SortKeyEncoder.keyKind', item('GeneratedAccessorCoverageTest', 'testEveryAccessorHasAKey', 'NN_INT has no key kind')[2])

    def test_build_items(self):
        sites = real_sites()
        tree = tp.Tree(REPO)
        driver = f'{tp.CAIRO_DIR}/NnIntTypeDriver.java'
        diag = tp.Diag(driver, 41, 0, 'cannot find symbol: variable setNull')
        self.assertEqual('fill-driver-answer', tp.build_item('build-java', diag, sites, tree, driver).decision)
        union = JAVA + 'griffin/SqlCodeGenerator.java'
        pair = next(p for p in sites.places if p.file == union and p.method == 'generateCastFunction' and p.form == 'tag-enum-switch')
        item = tp.build_item('build-java', tp.Diag(union, pair.line, 0, 'the switch expression does not cover all possible input values'), sites, tree, driver)
        self.assertEqual(('name-yourself', 'SqlCodeGenerator.generateCastFunction tag-enum-switch'), (item.decision, item.site))
        test_switch = 'core/src/test/java/io/questdb/test/griffin/OverloadSoundnessTest.java'
        line = next(first for name, first, _last in tp.method_spans(REPO / test_switch) if name == 'isExplicitCast')
        item = tp.build_item('build-java', tp.Diag(test_switch, line + 1, 0, 'the switch expression does not cover all possible input values'), sites, tree, driver)
        self.assertEqual(('name-yourself', 'OverloadSoundnessTest.isExplicitCast test switch'), (item.decision, item.site))
        item = tp.build_item('build-java', tp.Diag('core/src/main/java/io/questdb/std/Os.java', 1, 0, 'odd'), sites, tree, driver)
        self.assertEqual(('fill-driver-answer', 'unmapped'), (item.decision, item.site))

    def test_namesake_items(self):
        sites = real_sites()
        facts = valid_facts(tp.Tree(REPO))
        items = tp.namesake_items(sites, facts)
        view = audit.like(sites.vocabulary, sites.places, sites.decisions, 'INT', set(), REPO, 'PROBE_INT', 'PROBE_INT')
        # the facts share INT's values but its wire kind, which is the type's own
        self.assertGreater(len(items), len(view.open('names')))
        self.assertTrue(all(i.group == 'namesake' and i.decision == 'decide-place' for i in items))
        self.assertTrue(all(re.match(r'^`core/[^`]+:\d+`$', i.location) for i in items))
        self.assertFalse(any('WireKind.INT' in i.message for i in items))
        # a decision for every type closes its place
        first = view.open('table')[0].place
        closed = tp.Sites(sites.vocabulary, sites.places, sites.decisions + [audit.Decision(*first.key(), 'not-reached', 'r')], REPO)
        self.assertEqual(len(items) - 1, len(tp.namesake_items(closed, facts)))


class KitStepTest(unittest.TestCase):
    def kit_profiles(self, **kwargs):
        """The Maven profiles the kit step runs with, its Maven call captured instead of run."""
        calls = []
        with tempfile.TemporaryDirectory() as d:
            tree = tp.Tree(d)

            def run(cmd, log_path, cwd, env=None, timeout=None):
                calls.append([str(c) for c in cmd])
                reports = tree.path(tp.SUREFIRE_DIR)
                reports.mkdir(parents=True, exist_ok=True)
                (reports / 'TEST-Probe.xml').write_text('<testsuite/>', encoding='utf-8')
                return 0, ''

            with mock.patch.object(tp, 'run_logged', run):
                tp.kit(tree, Path(d) / 'out', **kwargs)
        cmd = calls[0]
        return cmd[cmd.index('-P') + 1].split(',')

    def kit_calls(self, is_type_red):
        """The Maven calls of a kit step for UINT32 whose first pass, the type alone, fails or not."""
        calls = []
        with tempfile.TemporaryDirectory() as d:
            tree = tp.Tree(d)

            def run(cmd, log_path, cwd, env=None, timeout=None):
                calls.append([str(c) for c in cmd])
                reports = tree.path(tp.SUREFIRE_DIR)
                reports.mkdir(parents=True, exist_ok=True)
                case = '<failure message="m">m</failure>' if is_type_red and len(calls) == 1 else ''
                (reports / 'TEST-Probe.xml').write_text(f'<testsuite><testcase classname="X" name="t">{case}</testcase></testsuite>', encoding='utf-8')
                return 0, ''

            with mock.patch.object(tp, 'run_logged', run):
                _, is_whole_kit = tp.kit(tree, Path(d) / 'out', only='UINT32')
        return calls, is_whole_kit

    def test_kit_runs_the_type_alone_before_the_whole_kit(self):
        only = '-Dquestdb.test.kit.types=UINT32'
        calls, is_whole_kit = self.kit_calls(is_type_red=True)
        self.assertEqual((1, False), (len(calls), is_whole_kit))
        self.assertIn(only, calls[0])
        calls, is_whole_kit = self.kit_calls(is_type_red=False)
        self.assertEqual((2, True), (len(calls), is_whole_kit))
        self.assertIn(only, calls[0])
        self.assertNotIn(only, calls[1])

    def test_kit_loads_the_trees_rust_library(self):
        # the kit runs the type's Rust answers, as it runs the C++ library the CMake step builds
        self.assertEqual(['local-client', 'build-rust-library'], self.kit_profiles())
        self.assertEqual(['local-client'], self.kit_profiles(is_rust_from_tree=False))


class ExitCodeTest(unittest.TestCase):
    def run_main(self, *argv):
        err = io.StringIO()
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(io.StringIO()):
            code = tp.main(list(argv))
        return code, err.getvalue()

    def test_codes(self):
        self.assertEqual(0, tp.exit_code([], []))
        self.assertEqual(1, tp.exit_code([tp.Item('namesake', 'decide-place', 'x', 'y')], []))
        self.assertEqual(1, tp.exit_code([], ['build-c']))

    def test_bad_command_line(self):
        self.assertEqual(2, self.run_main('fly')[0])
        self.assertEqual(2, self.run_main('init', 'X')[0])
        code, err = self.run_main('init', 'PROBE_INT', '--like', 'NO_SUCH_TYPE', '--tag', '41')
        self.assertEqual(2, code)
        self.assertIn('--like NO_SUCH_TYPE', err)

    def test_invalid_facts_exit_2_and_write_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            facts = Path(d) / 'BAD.toml'
            facts.write_text('[type]\nname = "BAD"\n', encoding='utf-8')
            before = (REPO / f'{tp.CAIRO_DIR}/ColumnTypeTag.java').read_bytes()
            code, err = self.run_main('run', str(facts), '--out', d)
            self.assertEqual(2, code)
            self.assertTrue(all(re.match(r'^facts: [\w.]+: ', line) for line in err.strip().split('\n')), err)
            self.assertIn('facts: physical: missing', err)
            self.assertEqual(before, (REPO / f'{tp.CAIRO_DIR}/ColumnTypeTag.java').read_bytes())
            self.assertFalse((Path(d) / 'worklist.md').exists())


if __name__ == '__main__':
    unittest.main()
