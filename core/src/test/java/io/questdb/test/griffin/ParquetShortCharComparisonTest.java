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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class ParquetShortCharComparisonTest extends AbstractCairoTest {
    @Test
    public void testCharBindVariableRangesMatchNative() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 10);
        assertMemoryLeak(() -> {
            createTables();
            final int savedJitMode = sqlExecutionContext.getJitMode();
            try {
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_ENABLED}) {
                    sqlExecutionContext.setJitMode(jitMode);
                    for (String variable : new String[]{":bound", "$1"}) {
                        for (String op : new String[]{"<", "<=", ">", ">="}) {
                            for (boolean reversed : new boolean[]{false, true}) {
                                bindVariableService.clear();
                                setCharBound(variable, '5');
                                final String predicate = reversed ? variable + " " + op + " s" : "s " + op + " " + variable;
                                try (RecordCursorFactory factory = select("SELECT s FROM p WHERE " + predicate)) {
                                    for (char bound : new char[]{'0', '5', 'A', '\u8000', '\uffff', 0}) {
                                        setCharBound(variable, bound);
                                        printSql("SELECT s FROM n WHERE " + predicate);
                                        final String expected = sink.toString();
                                        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().sizeMayVary().returns(expected);
                                    }
                                }
                            }
                        }
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(savedJitMode);
            }
        });
    }

    @Test
    public void testCharLiteralRangesMatchNative() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 10);
        assertMemoryLeak(() -> {
            createTables();
            final int savedJitMode = sqlExecutionContext.getJitMode();
            try {
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_ENABLED}) {
                    sqlExecutionContext.setJitMode(jitMode);
                    for (String bound : new String[]{"'0'", "'5'", "'A'", "'\u8000'", "'\uffff'", "null::char"}) {
                        for (String op : new String[]{"<", "<=", ">", ">="}) {
                            assertMatchesNative("s " + op + " " + bound);
                            assertMatchesNative(bound + " " + op + " s");
                        }
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(savedJitMode);
            }
        });
    }

    @Test
    public void testEqualityAndOtherConditionsStillPrune() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 10);
        assertMemoryLeak(() -> {
            createTables();
            for (String predicate : new String[]{"s = '5'", "s = '1' OR s = '5'", "s < '5' AND s > 40", "s > 40 AND s < '5'"}) {
                ParquetRowGroupFilter.resetRowGroupsSkipped();
                assertMatchesNative(predicate);
                Assert.assertTrue(predicate, ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            }
        });
    }

    @Test
    public void testReportedShortCharComparison() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 10);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (s SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO n SELECT (x / 10)::short, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(100)");
            execute("INSERT INTO n VALUES (99, '2024-01-02T00:00:00.000000Z')");
            execute("CREATE TABLE p AS (SELECT * FROM n) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            for (String op : new String[]{"<", "<="}) {
                assertQuery("SELECT count() AS cnt, min(s) AS lo, max(s) AS hi FROM n WHERE s " + op + " '5'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("cnt\tlo\thi\n91\t1\t10\n");
                assertQuery("SELECT count() AS cnt, min(s) AS lo, max(s) AS hi FROM p WHERE s " + op + " '5'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("cnt\tlo\thi\n91\t1\t10\n");
            }
        });
    }

    private void assertMatchesNative(String predicate) throws Exception {
        assertSqlCursors("SELECT s FROM n WHERE " + predicate, "SELECT s FROM p WHERE " + predicate);
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE n (s SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO n SELECT (x - 41)::short, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(140)");
        // Negative SHORTs become unsigned CHARs in range comparisons. Include both SHORT limits,
        // values around the digit 5 and its code point 53, and a native active partition.
        execute("INSERT INTO n VALUES (-32768, '2024-01-01T00:03:00Z'), (32767, '2024-01-01T00:04:00Z'), (99, '2024-01-02T00:00:00Z')");
        execute("CREATE TABLE p AS (SELECT * FROM n) TIMESTAMP(ts) PARTITION BY DAY");
        execute("ALTER TABLE p CONVERT PARTITION TO PARQUET WHERE ts >= 0");
    }

    private void setCharBound(String variable, char bound) throws Exception {
        if (variable.charAt(0) == ':') {
            bindVariableService.setChar("bound", bound);
        } else {
            bindVariableService.setChar(0, bound);
        }
    }
}
