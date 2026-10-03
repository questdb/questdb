/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/


package io.questdb.test.cairo.types;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RelationRules;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.conditional.CaseCommon;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.TreeSet;

/**
 * The coverage check of F34 (FR-016): a relation the rules derive must never admit a pair of
 * types that nothing implements, because such a pair passes the compiler and then writes nothing
 * or fails at run time. For every registered type (every kit type, a type registered later
 * included), each test lists the pairs a relation admits without an implementation and compares
 * the list with today's known gaps. A new gap fails, and so does a closed one: the list may only
 * shrink, by editing it here. ALTER COLUMN TYPE has its own check, which converts every admitted
 * pair ({@code ColumnConversionSoundnessTest}).
 */
public class RelationCoverageTest extends AbstractCairoTest {

    @Test
    public void testCaseEscalationHasAnImplementation() throws Exception {
        // rule E admits a pair of branch types with a common type; each branch must read as the
        // common type, through a cast factory or through its own getter, which its overload row
        // or its built-in widening declares (OverloadSoundnessTest checks every row against the
        // getters), and the common type must have a CASE function
        final Field constructors = CaseCommon.class.getDeclaredField("constructors");
        constructors.setAccessible(true);
        final ObjList<?> caseConstructors = (ObjList<?>) constructors.get(null);
        final TreeSet<String> gaps = new TreeSet<>();
        for (int i = 0, n = TypeConformanceTypes.ALL.size(); i < n; i++) {
            final TypeConformanceTypes.Entry from = TypeConformanceTypes.ALL.getQuick(i);
            final int[] row = RelationRules.caseEscalation(from.columnType);
            for (int k = 0; k < row.length; k += 2) {
                final int other = row[k];
                final int common = row[k + 1];
                if (caseConstructors.getQuiet(ColumnType.tagOf(common)) == null) {
                    gaps.add(from.label + " with " + ColumnType.nameOf(other) + " -> " + ColumnType.nameOf(common) + ": no CASE function");
                }
                for (int branch : new int[]{from.columnType, other}) {
                    if (ColumnType.tagOf(branch) != ColumnType.tagOf(common)
                            && CaseCommon.getCastFactory(branch, common) == null
                            && !isDeclaredOverload(ColumnType.tagOf(branch), ColumnType.tagOf(common))
                            && !ColumnType.isBuiltInWideningCast(branch, common)) {
                        gaps.add(ColumnType.nameOf(branch) + " -> " + ColumnType.nameOf(common) + ": no cast");
                    }
                }
            }
        }
        assertGaps("""
                """, gaps);
    }

    @Test
    public void testCopierHasAnArmForEveryAdmittedPair() throws Exception {
        // INSERT admits a pair by isConvertibleFrom; the copiers must have an arm for it (rule K)
        final Method copyOpcode = RecordToRowCopierUtils.class.getDeclaredMethod("copyOpcode", int.class, int.class);
        copyOpcode.setAccessible(true);
        final Field none = RecordToRowCopierUtils.class.getDeclaredField("COPY_NONE");
        none.setAccessible(true);
        final int copyNone = none.getInt(null);
        final TreeSet<String> gaps = new TreeSet<>();
        for (int i = 0, n = TypeConformanceTypes.ALL.size(); i < n; i++) {
            final TypeConformanceTypes.Entry from = TypeConformanceTypes.ALL.getQuick(i);
            for (int j = 0; j < n; j++) {
                final TypeConformanceTypes.Entry to = TypeConformanceTypes.ALL.getQuick(j);
                if (ColumnType.isPersisted(ColumnType.tagOf(to.columnType))
                        && ColumnType.isConvertibleFrom(from.columnType, to.columnType)
                        && (int) copyOpcode.invoke(null, from.columnType, to.columnType) == copyNone) {
                    gaps.add(from.label + " -> " + to.label);
                }
            }
        }
        // the copiers write nothing for these and the column stays NULL; see
        // issues/copier-admitted-pairs-without-arm and TypeRelationGoldenTest.testCopierGaps
        assertGaps("""
                BYTE -> CHAR
                DATE -> CHAR
                DOUBLE -> CHAR
                FLOAT -> CHAR
                LONG -> CHAR
                SHORT -> CHAR
                SYMBOL -> TIMESTAMP
                SYMBOL -> TIMESTAMP_NS
                TIMESTAMP -> CHAR
                TIMESTAMP_NS -> CHAR
                """, gaps);
    }

    @Test
    public void testExplicitCastHasAFunctionForEveryAdmittedPair() throws Exception {
        // W, C and N admit a cast between two types; cast(x AS T) must resolve to a function
        final Method isNarrowingCast = ColumnType.class.getDeclaredMethod("isNarrowingCast", int.class, int.class);
        isNarrowingCast.setAccessible(true);
        final Method isWideningCast = ColumnType.class.getDeclaredMethod("isWideningCast0", short.class, short.class);
        isWideningCast.setAccessible(true);
        assertMemoryLeak(() -> {
            // a source is a table column, so a type a table cannot store (INTERVAL) is no source here
            final StringBuilder ddl = new StringBuilder("CREATE TABLE t (");
            for (int i = 0, n = TypeConformanceTypes.ALL.size(), m = 0; i < n; i++) {
                final TypeConformanceTypes.Entry entry = TypeConformanceTypes.ALL.getQuick(i);
                if (ColumnType.isPersisted(ColumnType.tagOf(entry.columnType))) {
                    ddl.append(m++ > 0 ? ", " : "").append("c").append(i).append(' ').append(entry.ddl);
                }
            }
            execute(ddl.append(')').toString());
            final TreeSet<String> gaps = new TreeSet<>();
            for (int i = 0, n = TypeConformanceTypes.ALL.size(); i < n; i++) {
                final TypeConformanceTypes.Entry from = TypeConformanceTypes.ALL.getQuick(i);
                if (!ColumnType.isPersisted(ColumnType.tagOf(from.columnType))) {
                    continue;
                }
                for (int j = 0; j < n; j++) {
                    final TypeConformanceTypes.Entry to = TypeConformanceTypes.ALL.getQuick(j);
                    if (ColumnType.tagOf(from.columnType) == ColumnType.tagOf(to.columnType)) {
                        continue;
                    }
                    if (ColumnType.isBuiltInWideningCast(from.columnType, to.columnType)
                            || (boolean) isWideningCast.invoke(null, ColumnType.tagOf(from.columnType), ColumnType.tagOf(to.columnType))
                            || (boolean) isNarrowingCast.invoke(null, from.columnType, to.columnType)) {
                        try (RecordCursorFactory ignore = select("SELECT cast(c" + i + " AS " + to.ddl + ") FROM t")) {
                            // resolves
                        } catch (SqlException e) {
                            gaps.add(from.label + " -> " + to.label + ": " + e.getFlyweightMessage());
                        }
                    }
                }
            }
            assertGaps("""
                    """, gaps);
        });
    }

    @Test
    public void testUnionHasACastForEveryAdmittedPair() throws Exception {
        // the union relation (commonWideningType, built on W and C) gives every pair of column types
        // a common type, with STRING as the fallback, and SqlCodeGenerator.generateCastFunction then
        // converts each branch or refuses it ("unsupported cast", a loud refusal and no gap). A pair
        // whose branch reaches no arm at all is a silent gap: the branch's cast function is dropped
        // and its columns shift (an assertion under -ea); see issues/sql-union-cast-dropped-branch
        assertMemoryLeak(() -> {
            final StringBuilder ddl = new StringBuilder("CREATE TABLE t (");
            for (int i = 0, n = TypeConformanceTypes.ALL.size(), m = 0; i < n; i++) {
                final TypeConformanceTypes.Entry entry = TypeConformanceTypes.ALL.getQuick(i);
                if (ColumnType.isPersisted(ColumnType.tagOf(entry.columnType))) {
                    ddl.append(m++ > 0 ? ", " : "").append("c").append(i).append(' ').append(entry.ddl);
                }
            }
            execute(ddl.append(')').toString());
            final TreeSet<String> gaps = new TreeSet<>();
            for (int i = 0, n = TypeConformanceTypes.ALL.size(); i < n; i++) {
                final TypeConformanceTypes.Entry a = TypeConformanceTypes.ALL.getQuick(i);
                for (int j = 0; j < n; j++) {
                    final TypeConformanceTypes.Entry b = TypeConformanceTypes.ALL.getQuick(j);
                    if (!ColumnType.isPersisted(ColumnType.tagOf(a.columnType)) || !ColumnType.isPersisted(ColumnType.tagOf(b.columnType))) {
                        continue;
                    }
                    try (RecordCursorFactory ignore = select("SELECT c" + i + " x FROM t UNION ALL SELECT c" + j + " FROM t")) {
                        // converts
                    } catch (SqlException ignore) {
                        // a loud refusal
                    } catch (AssertionError e) {
                        gaps.add(a.label + " with " + b.label);
                    }
                }
            }
            assertGaps("""
                    DECIMAL(18,3) with DOUBLE[]
                    DECIMAL(18,3) with DOUBLE[][]
                    DECIMAL(5,2) with DOUBLE[]
                    DECIMAL(5,2) with DOUBLE[][]
                    DECIMAL128 with DOUBLE[]
                    DECIMAL128 with DOUBLE[][]
                    DECIMAL16 with DOUBLE[]
                    DECIMAL16 with DOUBLE[][]
                    DECIMAL256 with DOUBLE[]
                    DECIMAL256 with DOUBLE[][]
                    DECIMAL32 with DOUBLE[]
                    DECIMAL32 with DOUBLE[][]
                    DECIMAL64 with DOUBLE[]
                    DECIMAL64 with DOUBLE[][]
                    DECIMAL8 with DOUBLE[]
                    DECIMAL8 with DOUBLE[][]
                    DOUBLE[] with DECIMAL(18,3)
                    DOUBLE[] with DECIMAL(5,2)
                    DOUBLE[] with DECIMAL128
                    DOUBLE[] with DECIMAL16
                    DOUBLE[] with DECIMAL256
                    DOUBLE[] with DECIMAL32
                    DOUBLE[] with DECIMAL64
                    DOUBLE[] with DECIMAL8
                    DOUBLE[] with LONG128
                    DOUBLE[][] with DECIMAL(18,3)
                    DOUBLE[][] with DECIMAL(5,2)
                    DOUBLE[][] with DECIMAL128
                    DOUBLE[][] with DECIMAL16
                    DOUBLE[][] with DECIMAL256
                    DOUBLE[][] with DECIMAL32
                    DOUBLE[][] with DECIMAL64
                    DOUBLE[][] with DECIMAL8
                    DOUBLE[][] with LONG128
                    LONG128 with DOUBLE[]
                    LONG128 with DOUBLE[][]
                    LONG128 with VARCHAR
                    VARCHAR with LONG128
                    """, gaps);
        });
    }

    private static boolean isDeclaredOverload(short fromTag, short toTag) {
        for (short t : RelationRules.implicitCasts(fromTag)) {
            if (t == toTag) {
                return true;
            }
        }
        return false;
    }

    private static void assertGaps(String expected, TreeSet<String> gaps) {
        final StringBuilder actual = new StringBuilder();
        for (String gap : gaps) {
            actual.append(gap).append('\n');
        }
        TestUtils.assertEquals(expected, actual);
    }
}
