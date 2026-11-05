"""Tests of the audit. Run from the repository root:

    python3 -m unittest discover -s utils/type-probe

Each test plants places in a small tree written to a temporary directory: one place of every form
the audit reads, and the edits that must change or keep what it reports. The sources are kept here
as strings, so the repository's Java formatter never sees them.
"""

import shutil
import tempfile
import textwrap
import unittest
from pathlib import Path

import audit

CAIRO = 'core/src/main/java/io/questdb/cairo'

VOCABULARY = {
    f'{CAIRO}/ColumnTypeTag.java': """
        package io.questdb.cairo;

        public enum ColumnTypeTag {
            UNDEFINED(0),
            BOOLEAN(1),
            INT(5),
            LONG(6),
            TIMESTAMP(8),
            SYMBOL(12),
            DECIMAL8(28),
            DECIMAL64(31),
            NULL(41),
            UNKNOWN(-1);

            ColumnTypeTag(int code) {
            }
        }
        """,
    f'{CAIRO}/ColumnType.java': """
        package io.questdb.cairo;

        public final class ColumnType {
            public static final short UNDEFINED = 0;
            public static final short BOOLEAN = 1;
            public static final short INT = 5;
            public static final short LONG = 6;
            public static final short TIMESTAMP = 8;
            public static final short SYMBOL = 12;
            public static final short DECIMAL8 = 28;
            public static final short DECIMAL64 = 31;
            public static final short NULL = 41;
            public static final short MAX_TAG = NULL;
            public static final int TIMESTAMP_NANO = 1 << 18 | TIMESTAMP;

            public static boolean isConvertibleFrom(int fromType, int toType) {
                return fromType == toType;
            }

            public static boolean isIntegral(int columnType) {
                return RelationRules.kind(tagOf(columnType)) == RelationKind.INT;
            }

            public static boolean isSymbol(int columnType) {
                return columnType == SYMBOL;
            }

            public static short tagOf(int type) {
                return (short) (type & 0xFF);
            }
        }
        """,
    f'{CAIRO}/PhysicalDescriptor.java': """
        package io.questdb.cairo;

        public final class PhysicalDescriptor {
            public enum Accessor {
                INT, LONG, SYMBOL
            }

            public enum Arithmetic {
                I32, I64, NONE
            }

            public enum Movement {
                W4, W8, VAR
            }
        }
        """,
    f'{CAIRO}/NullPolicy.java': 'package io.questdb.cairo;\npublic enum NullPolicy {\n    NONE, SENTINEL\n}\n',
    f'{CAIRO}/WireKind.java': 'package io.questdb.cairo;\npublic enum WireKind {\n    INT, LONG\n}\n',
    f'{CAIRO}/RelationKind.java': 'package io.questdb.cairo;\npublic enum RelationKind {\n    INT, TEXT\n}\n',
    f'{CAIRO}/CastTarget.java': 'package io.questdb.cairo;\npublic enum CastTarget {\n    ALWAYS, NEVER\n}\n',
    f'{CAIRO}/IntTypeDriver.java': """
        package io.questdb.cairo;

        public final class IntTypeDriver {
            public static final TypeFacts FACTS = new TypeFacts(
                    ColumnTypeTag.INT,
                    PhysicalDescriptor.Movement.W4,
                    PhysicalDescriptor.Arithmetic.I32,
                    PhysicalDescriptor.Accessor.INT,
                    NullPolicy.SENTINEL,
                    WireKind.INT,
                    RelationKind.INT,
                    32,
                    new short[]{ColumnType.INT},
                    23,
                    'i',
                    0,
                    0L,
                    CastTarget.ALWAYS,
                    "INT"
            );

            boolean isOwnType(int columnType) {
                return columnType == ColumnType.INT;
            }
        }
        """,
    'core/rust/qdb-core/src/col_type.rs': """
        pub enum ColumnArithmetic {
            I32,
            I64,
        }

        pub enum ColumnTypeTag {
            Boolean = 1,
            Int = 5,
            Long = 6,
            Symbol = 12,
        }
        """,
    'core/src/main/c/share/column_type.h': """
        enum class ColumnType : int {
          UNDEFINED = 0,
          BOOLEAN = 1,
          INT = 5,
          LONG = 6,
          TIMESTAMP_MICRO = 8,
          SYMBOL = 12,
          NULL_ = 41,
          TIMESTAMP_NANO = 1 << 18 | TIMESTAMP_MICRO,
        };
        """,
}

PLANTED_JAVA = """
    package io.questdb.griffin;

    import io.questdb.cairo.ColumnType;
    import io.questdb.cairo.ColumnTypeTag;
    import io.questdb.cairo.NullPolicy;
    import io.questdb.cairo.PhysicalDescriptor;
    import io.questdb.cairo.TypeDriver;

    public class Planted {
        private static final int[] SIZES = new int[64];

        boolean agreeingRange(int tag) {
            return tag > ColumnType.UNDEFINED;
        }

        boolean comment(int type) {
            // type == ColumnType.LONG is a comment
            String s = "type == ColumnType.LONG";
            return s.isEmpty();
        }

        boolean comparison(int type) {
            return ColumnType.tagOf(type) == ColumnType.INT || type == ColumnType.TIMESTAMP_NANO;
        }

        int enumSwitchExpression(ColumnTypeTag tag) {
            return switch (tag) {
                case BOOLEAN, INT, LONG, TIMESTAMP, SYMBOL, DECIMAL8, DECIMAL64, UNDEFINED, NULL, UNKNOWN -> 1;
            };
        }

        int enumSwitchStatement(ColumnTypeTag tag) {
            switch (tag) {
                case INT -> {
                    return 1;
                }
                case LONG -> {
                    return 2;
                }
            }
            return 0;
        }

        int familyRouting(int type) {
            int opcode = PhysicalDescriptor.accessorOpcodeOf(type);
            return opcode;
        }

        void fillNulls(long address, long count) {
            java.util.Arrays.fill(new long[(int) count], address);
            // validity batch site: a column with a validity bitmap would mark these rows NULL here
        }

        int guardedSwitch(TypeDriver driver) {
            return switch (PhysicalDescriptor.familyArmOf(driver, "planted site")) {
                case INT -> 4;
                default -> 8;
            };
        }

        boolean predicate(int type) {
            if (ColumnType.isSymbol(type)) {
                return true;
            }
            return ColumnType.isConvertibleFrom(type, ColumnType.LONG);
        }

        boolean range(int type) {
            return ColumnType.tagOf(type) < ColumnType.DECIMAL8;
        }

        int table(int type) {
            return SIZES[ColumnType.tagOf(type)];
        }

        int tableByVariable(int type) {
            final short tag = ColumnType.tagOf(type);
            return SIZES[tag];
        }

        int tagSwitchWithDefault(int type) {
            switch (ColumnType.tagOf(type)) {
                case ColumnType.INT:
                    return 1;
                default:
                    return 0;
            }
        }

        int tagSwitchWithoutDefault(int type) {
            int r = 0;
            switch (ColumnType.tagOf(type)) {
                case ColumnType.LONG -> r = 2;
                case ColumnType.SYMBOL -> r = 3;
            }
            return r;
        }

        int twinSwitches(int type) {
            switch (ColumnType.tagOf(type)) {
                case ColumnType.INT:
                    return 1;
                default:
                    break;
            }
            switch (ColumnType.tagOf(type)) {
                case ColumnType.INT:
                    return 2;
                default:
                    return 0;
            }
        }

        int valueSwitch(PhysicalDescriptor.Accessor accessor) {
            return switch (accessor) {
                case INT -> 4;
                case LONG -> 8;
                case SYMBOL -> 4;
            };
        }

        boolean valueTest(TypeDriver driver) {
            return driver.getNullPolicy() == NullPolicy.SENTINEL;
        }

        int writerArm(int opcode) {
            switch (opcode) {
                case ColumnType.INT:
                    return 4;
                default:
                    return 0;
            }
        }
    }
    """

PLANTED_BARE = """
    package io.questdb.griffin;

    import static io.questdb.cairo.ColumnType.*;

    public class Bare {
        boolean bare(int tag) {
            return tag == LONG;
        }
    }
    """

PLANTED_RUST = """
    use qdb_core::col_type::{ColumnArithmetic, ColumnTypeTag};

    fn arithmetic(a: ColumnArithmetic) -> bool {
        match a {
            ColumnArithmetic::I32 => true,
            ColumnArithmetic::I64 => false,
        }
    }

    fn decode(code: u8) -> Option<ColumnTypeTag> {
        match code {
            1 => Some(ColumnTypeTag::Boolean),
            5 => Some(ColumnTypeTag::Int),
            6 => Some(ColumnTypeTag::Long),
            _ => None,
        }
    }

    fn exhaustive(tag: ColumnTypeTag) -> u8 {
        match tag {
            ColumnTypeTag::Boolean | ColumnTypeTag::Int => 1,
            ColumnTypeTag::Long | ColumnTypeTag::Symbol => {
                let s = "ColumnTypeTag::Int";
                s.len() as u8
            }
        }
    }

    fn optional(tag: Option<ColumnTypeTag>) -> bool {
        tag == Some(ColumnTypeTag::Long)
    }

    fn test(tag: ColumnTypeTag) -> bool {
        tag == ColumnTypeTag::Symbol || matches!(tag, ColumnTypeTag::Int)
    }

    fn tuple(enc: u8, tag: ColumnTypeTag) -> u8 {
        match (enc, tag) {
            (0, ColumnTypeTag::Long) => 1,
            (_, _) => 0,
        }
    }

    fn wildcard<'a>(tag: ColumnTypeTag, name: &'a str) -> u8 {
        match tag {
            ColumnTypeTag::Int => 1,
            _ => name.len() as u8,
        }
    }

    #[cfg(test)]
    mod tests {
        fn in_test(tag: ColumnTypeTag) -> bool {
            tag == ColumnTypeTag::Int
        }
    }
    """

PLANTED_NATIVE = """
    #include "column_type.h"

    namespace qdb_col {
        constexpr int32_t INT = 5;
        constexpr int32_t LONG = 6;
    }

    int unchecked(int t) {
        switch (t) {
            case qdb_col::INT: return 1;
            case qdb_col::LONG: return 2;
        }
        return 0;
    }

    #pragma GCC diagnostic push
    #pragma GCC diagnostic error "-Wswitch"
    int checked(ColumnType t) {
        switch (t) {
            case ColumnType::INT: return 1;
            case ColumnType::LONG: return 2;
        }
        return 0;
    }
    #pragma GCC diagnostic pop

    int with_default(ColumnType t) {
        switch (t) {
            case ColumnType::SYMBOL: return 1;
            default: return 0;
        }
    }

    bool test(int t) {
        return t == qdb_col::INT;
    }
    """

PLANTED = {
    'core/src/main/java/io/questdb/griffin/Planted.java': PLANTED_JAVA,
    'core/src/main/java/io/questdb/griffin/Bare.java': PLANTED_BARE,
    'core/rust/qdbr/src/planted.rs': PLANTED_RUST,
    'core/src/main/c/share/planted.cpp': PLANTED_NATIVE,
    'core/src/test/java/io/questdb/test/PlantedTest.java': 'class PlantedTest {\n    public void testIt() {\n    }\n}\n',
}


class TreeTest(unittest.TestCase):
    """A tree of the vocabulary and the planted sources, rewritten by each test as it needs."""

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix='audit-'))
        for rel, text in {**VOCABULARY, **PLANTED}.items():
            self.write(rel, text)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def write(self, rel, text):
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(text).lstrip('\n'), encoding='utf-8')

    def edit(self, rel, old, new):
        path = self.root / rel
        text = path.read_text(encoding='utf-8')
        self.assertIn(old, text)
        path.write_text(text.replace(old, new, 1), encoding='utf-8')

    def scan(self):
        return audit.scan(self.root)

    def places(self, method, form=None):
        _v, places = self.scan()
        return [p for p in places if p.method == method and (form is None or p.form == form)]

    def one(self, method, form=None):
        found = self.places(method, form)
        self.assertEqual(1, len(found), f'{method} {form}: {found}')
        return found[0]


class FormTest(TreeTest):
    """Every form of place is found, with its names, its fallback and whether a compiler names it."""

    def test_tag_switch_with_a_default_arm(self):
        p = self.one('tagSwitchWithDefault')
        self.assertEqual(('tag-switch', {'INT'}, 'default', False), (p.form, set(p.tags), p.fallback, p.is_checked))

    def test_tag_switch_without_a_default_arm(self):
        p = self.one('tagSwitchWithoutDefault')
        self.assertEqual(('tag-switch', {'LONG', 'SYMBOL'}, 'none', False), (p.form, set(p.tags), p.fallback, p.is_checked))

    def test_enum_switch_expression_is_checked_and_statement_is_not(self):
        self.assertTrue(self.one('enumSwitchExpression', 'tag-enum-switch').is_checked)
        p = self.one('enumSwitchStatement', 'tag-enum-switch')
        self.assertEqual(({'INT', 'LONG'}, False), (set(p.tags), p.is_checked))

    def test_comparison_names_a_tag_and_an_encoded_constant(self):
        p = self.one('comparison')
        self.assertEqual(('tag-test', {'INT', 'TIMESTAMP'}), (p.form, set(p.tags)))

    def test_bare_comparison_under_a_static_import(self):
        self.assertEqual({'LONG'}, set(self.one('bare').tags))

    def test_predicate_call_names_its_tags_and_a_relation_is_no_place(self):
        p = self.one('predicate')
        self.assertEqual(('tag-test', {'SYMBOL'}), (p.form, set(p.tags)))

    def test_range_comparison(self):
        p = self.one('range')
        self.assertEqual(('tag-test', (('<', 28),)), (p.form, p.ranges))

    def test_tables_by_tag(self):
        self.assertEqual('tag-table', self.one('table').form)
        self.assertEqual('tag-table', self.one('tableByVariable', 'tag-table').form)

    def test_value_switch_and_its_guard(self):
        p = self.one('valueSwitch')
        self.assertEqual(('value-switch', {'Accessor.INT', 'Accessor.LONG', 'Accessor.SYMBOL'}, True, False),
                         (p.form, set(p.values), p.is_checked, p.is_guarded))
        g = self.one('guardedSwitch')
        self.assertEqual((True, 'planted site', False), (g.is_guarded, g.label, g.is_checked))

    def test_value_tests(self):
        self.assertEqual({'NullPolicy.SENTINEL'}, set(self.one('valueTest').values))
        p = self.one('familyRouting')
        self.assertEqual(('value-test', set(), False), (p.form, set(p.values), p.is_guarded))

    def test_no_place_in_comments_strings_or_writer_arms(self):
        self.assertEqual([], self.places('comment'))
        self.assertEqual([], self.places('writerArm'))

    def test_rust_matches(self):
        self.assertEqual((True, {'BOOLEAN', 'INT', 'LONG', 'SYMBOL'}), (self.one('exhaustive').is_checked, set(self.one('exhaustive').tags)))
        self.assertFalse(self.one('wildcard').is_checked)
        tuple_match = self.one('tuple')
        self.assertEqual(({'LONG'}, False), (set(tuple_match.tags), tuple_match.is_checked))
        self.assertEqual(('rust-match', {'Arithmetic.I32', 'Arithmetic.I64'}, True),
                         (self.one('arithmetic').form, set(self.one('arithmetic').values), self.one('arithmetic').is_checked))

    def test_rust_decode_table(self):
        self.assertEqual('tag-table', self.one('decode').form)

    def test_rust_tests_outside_test_modules(self):
        self.assertEqual({'SYMBOL', 'INT'}, set(self.one('test', 'rust-test').tags))
        self.assertEqual({'LONG'}, set(self.one('optional').tags))
        self.assertEqual([], self.places('in_test'))

    def test_validity_marker(self):
        p = self.one('fillNulls')
        self.assertEqual(('validity-marker', 'acolumnwithavaliditybitmapwouldmarktheserowsNULLhere', set(), False),
                         (p.form, p.anchor, set(p.tags), p.is_checked))

    def test_native_switches_and_tests(self):
        self.assertEqual(({'INT', 'LONG'}, False), (set(self.one('unchecked').tags), self.one('unchecked').is_checked))
        self.assertTrue(self.one('checked').is_checked)
        self.assertFalse(self.one('with_default').is_checked)
        self.assertEqual(('c-test', {'INT'}), (self.one('test', 'c-test').form, set(self.one('test', 'c-test').tags)))


class ViewTest(TreeTest):
    """The view by namesake lists what a type declared like INT must look at."""

    def view(self, decisions=(), own='', type_name=''):
        v, places = self.scan()
        return audit.like(v, places, list(decisions), 'INT', audit.type_values(self.root, 'INT'), self.root, own, type_name)

    def like_int(self):
        view = self.view()
        return (view.values, {i.place.method for i in view.open('names')}, {i.place.method for i in view.open('shares')},
                {i.place.method for i in view.open('table')}, {p.method for p in view.checked})

    def test_values_come_from_the_type_driver(self):
        values, *_ = self.like_int()
        self.assertIn('Accessor.INT', values)
        self.assertIn('NullPolicy.SENTINEL', values)

    def test_groups(self):
        _values, named, borrowed, tables, checked = self.like_int()
        self.assertEqual({'tagSwitchWithDefault', 'enumSwitchStatement', 'comparison', 'range', 'twinSwitches', 'wildcard', 'test',
                          'unchecked'}, named)
        self.assertEqual({'valueSwitch', 'guardedSwitch', 'valueTest', 'familyRouting', 'arithmetic'}, borrowed)
        self.assertEqual({'table', 'tableByVariable', 'decode'}, tables)
        self.assertEqual({'enumSwitchExpression', 'exhaustive', 'checked'}, checked)

    def test_namesake_driver_and_agreeing_range_are_not_listed(self):
        _values, named, *_ = self.like_int()
        self.assertNotIn('isOwnType', named)
        self.assertNotIn('agreeingRange', named)

    def test_decisions_close_places_for_every_type_or_one(self):
        _v, places = self.scan()
        table = next(p for p in places if p.method == 'table')
        test = next(p for p in places if p.method == 'valueTest')
        decisions = [audit.Decision(*table.key(), 'not-reached', 'no later type reaches it'),
                     audit.Decision(*test.key(), 'no-change', 'reads the policy, which the type answers', 'UINT32')]
        for_uint32 = self.view(decisions, type_name='UINT32')
        self.assertEqual({'table', 'valueTest'}, {i.place.method for i in for_uint32.closed()})
        for_another = self.view(decisions, type_name='NN_INT')
        self.assertEqual({'table'}, {i.place.method for i in for_another.closed()})
        precedent = next(i for i in for_another.items if i.place.method == 'valueTest')
        self.assertEqual(['UINT32'], [d.type for d in precedent.precedents])

    def test_a_place_that_names_the_new_type_is_closed(self):
        self.edit('core/src/main/java/io/questdb/griffin/Planted.java', 'case ColumnType.INT:\n                return 1;\n            default:\n                return 0;',
                  'case ColumnType.INT:\n                return 1;\n            case ColumnType.BOOLEAN:\n                return 2;\n            default:\n                return 0;')
        view = self.view(own='BOOLEAN')
        self.assertIn('tagSwitchWithDefault', {i.place.method for i in view.closed('names')})

    def test_rows_for_the_author(self):
        rows = audit.render_rows(self.view(), 'UINT32').splitlines()
        self.assertEqual('\t'.join(audit.COLUMNS), rows[0])
        self.assertTrue(all(r.endswith('\t\t\tUINT32') for r in rows[1:]))
        self.assertEqual(len(self.view().open()), len(rows) - 1)

    def test_validity_sites_only_for_a_null_policy_of_its_own(self):
        v, places = self.scan()
        values = audit.type_values(self.root, 'INT')
        self.assertEqual([], self.view().open('validity'))
        own = audit.like(v, places, [], 'INT', (values - {'NullPolicy.SENTINEL'}) | {'NullPolicy.NONE'}, self.root)
        self.assertEqual({'fillNulls'}, {i.place.method for i in own.open('validity')})

    def test_a_default_arm_takes_away_the_compiler_check(self):
        self.edit('core/src/main/java/io/questdb/griffin/Planted.java', 'UNKNOWN -> 1;', 'UNKNOWN -> 1;\n            default -> 0;')
        _values, named, _b, _t, checked = self.like_int()
        self.assertIn('enumSwitchExpression', named)
        self.assertNotIn('enumSwitchExpression', checked)


class DecisionTest(TreeTest):
    """Stored decisions are kept by their place's key, and checked against the code."""

    def store(self, *rows):
        lines = ['\t'.join(audit.COLUMNS)] + ['\t'.join(str(c) for c in r) for r in rows]
        self.write(audit.PLACES_FILE, '\n'.join(lines) + '\n')

    def key(self, method, n=1):
        p = [p for p in self.places(method) if p.n == n][0]
        return [p.file, p.method, p.form, p.anchor, p.n]

    def problems(self):
        _v, places = self.scan()
        return audit.check(self.root, places, audit.load_decisions(self.root / audit.PLACES_FILE))

    def test_valid_decisions(self):
        self.store(self.key('tagSwitchWithDefault') + ['not-reached', 'a reason', ''],
                   self.key('guardedSwitch') + ['refused', 'planted site', ''],
                   self.key('valueTest') + ['test', 'PlantedTest#testIt', ''],
                   self.key('tagSwitchWithoutDefault') + ['no-change', 'one type', 'UINT32'],
                   self.key('tagSwitchWithoutDefault') + ['no-change', 'another type', 'NN_INT'])
        self.assertEqual([], self.problems())

    def test_a_moved_place_keeps_its_decision(self):
        self.store(self.key('tagSwitchWithDefault') + ['not-reached', 'a reason', ''])
        self.edit('core/src/main/java/io/questdb/griffin/Planted.java', 'public class Planted {',
                  'public class Planted {\n\n    int added(int type) {\n        return 0;\n    }\n')
        self.assertEqual([], self.problems())

    def test_a_switch_added_above_does_not_move_another_decision(self):
        second = self.key('twinSwitches', 2)
        self.store(second + ['not-reached', 'the second', ''])
        self.edit('core/src/main/java/io/questdb/griffin/Planted.java', '    int twinSwitches(int type) {\n',
                  '    int twinSwitches(int type) {\n        switch (ColumnType.tagOf(type + 1)) {\n'
                  '            case ColumnType.LONG:\n                return 9;\n            default:\n                break;\n        }\n')
        self.assertEqual([], self.problems())
        self.assertEqual(second, self.key('twinSwitches', 2))

    def test_a_changed_place_leaves_its_decision_gone(self):
        self.store(self.key('tagSwitchWithDefault') + ['not-reached', 'a reason', ''])
        self.edit('core/src/main/java/io/questdb/griffin/Planted.java', 'switch (ColumnType.tagOf(type)) {\n            case ColumnType.INT:\n                return 1;\n            default:\n                return 0;',
                  'switch (ColumnType.tagOf(type + 0)) {\n            case ColumnType.INT:\n                return 1;\n            default:\n                return 0;')
        self.assertEqual(['gone'], [p.split(':')[0] for p in self.problems()])

    def test_a_missing_test_and_a_wrong_label(self):
        self.store(self.key('valueTest') + ['test', 'PlantedTest#testRenamed', ''],
                   self.key('guardedSwitch') + ['refused', 'another site', ''])
        self.assertEqual(['lost its test', 'wrong label'], sorted(p.split(':')[0] for p in self.problems()))

    def test_the_same_place_decided_twice_for_one_type(self):
        self.store(self.key('valueTest') + ['no-change', 'one', 'UINT32'], self.key('valueTest') + ['not-reached', 'two', 'UINT32'])
        self.assertEqual(['twice'], [p.split(':')[0] for p in self.problems()])

    def test_a_guard_without_a_label_in_the_code_may_be_refused(self):
        self.edit('core/src/main/java/io/questdb/griffin/Planted.java', '    int valueSwitch(PhysicalDescriptor.Accessor accessor) {\n',
                  '    int valueSwitch(PhysicalDescriptor.Accessor accessor) {\n        if (!PhysicalDescriptor.isLikeFamilyNamesake(null)) {\n'
                  '            return 0;\n        }\n')
        self.store(self.key('valueSwitch') + ['refused', 'planted kind: an earlier error', ''])
        self.assertEqual([], self.problems())

    def test_a_bad_store_is_a_usage_error(self):
        for text in ('file\tmethod\n', '\t'.join(audit.COLUMNS) + '\nf\tm\ttag-test\ta\t1\tmaybe\tr\t\n',
                     '\t'.join(audit.COLUMNS) + '\nf\tm\ttag-test\ta\t1\tno-change\t\t\n'):
            self.write(audit.PLACES_FILE, text)
            with self.assertRaises(audit.UsageError):
                audit.load_decisions(self.root / audit.PLACES_FILE)


class TextTest(unittest.TestCase):
    """The text helpers keep offsets and read conditions as the scanners need."""

    def test_stripping_keeps_offsets_and_blanks_literals(self):
        text = 'a = "x == ColumnType.INT"; // b == ColumnType.INT\nc = \'"\';'
        stripped = audit.strip_java(text)
        self.assertEqual(len(text), len(stripped))
        self.assertNotIn('ColumnType', stripped)
        self.assertEqual(text.index('c ='), stripped.index('c ='))

    def test_rust_raw_strings_and_lifetimes(self):
        text = 'fn f<\'a>(x: &\'a str) { let s = r#"ColumnTypeTag::Int"#; }'
        stripped = audit.strip_rust(text)
        self.assertNotIn('ColumnTypeTag', stripped)
        self.assertIn("<'a>", stripped)

    def test_statement_span_leaves_out_else_and_arrow_labels(self):
        s = 'if (a) { x(); } else if (t == ColumnType.INT) { y(); }'
        a, b = audit.statement_span(s, s.index('=='))
        self.assertEqual('if (t == ColumnType.INT) ', s[a:b])
        s = 'switch (k) { case A -> t == ColumnType.INT; }'
        a, b = audit.statement_span(s, s.index('=='))
        self.assertEqual('t == ColumnType.INT', s[a:b])

    def test_evaluate(self):
        v = audit.Vocabulary(tags={'UNDEFINED': 0, 'GEOBYTE': 14, 'GEOLONG': 17}, aliases={'UNDEFINED': 'UNDEFINED', 'GEOBYTE': 'GEOBYTE', 'GEOLONG': 'GEOLONG'})
        cond = 'if (!(t >= ColumnType.GEOBYTE && t <= ColumnType.GEOLONG)) '
        self.assertTrue(audit.evaluate(cond, v, 5))
        self.assertFalse(audit.evaluate(cond, v, 15))
        self.assertIsNone(audit.evaluate('if (t > ColumnType.UNDEFINED && isOk()) ', v, 5))


if __name__ == '__main__':
    unittest.main()
