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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TemporalSharedKeyTest extends AbstractCairoTest {
    @Test
    public void testFullFatSymbolKeyMetadataFollowsMasterDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createMixedSymbolRows();
            for (int kind = 0; kind < 2; kind++) {
                for (int master = 0; master < 2; master++) {
                    for (int slave = 0; slave < 2; slave++) {
                        final String sql = "SELECT m.id,s.k,s.payload,s.k='A' eq,length(s.k) len FROM "
                                + mixedSymbolJoin(kind, master == 1, slave == 1);
                        try (RecordCursorFactory factory = compileFullFat(sql)) {
                            Assert.assertEquals(master == 0, factory.getMetadata().isSymbolTableStatic(1));
                            Assert.assertEquals(slave == 0, factory.getMetadata().isSymbolTableStatic(2));
                            for (int cursor = 0; cursor < 2; cursor++) {
                                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                        .sizeMayVary().returns("""
                                                id\tk\tpayload\teq\tlen
                                                1\t\t\tfalse\t-1
                                                2\tB\tb\tfalse\t1
                                                3\tC\tc\tfalse\t1
                                                4\tA\ta\ttrue\t1
                                                5\t\tn\tfalse\t-1
                                                """);
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testTemporalSymbolConsumersUseFinalInputMetadata() throws Exception {
        assertMemoryLeak(() -> {
            createMixedSymbolRows();
            for (int kind = 0; kind < 2; kind++) {
                final String input = "(SELECT m.id mid,s.k sk FROM " + mixedSymbolJoin(kind, true, false) + ")";
                final ObjList<String> queries = new ObjList<>();
                queries.add("SELECT mid FROM " + input + " WHERE sk='A' ORDER BY mid");
                queries.add("SELECT mid FROM " + input + " WHERE sk LIKE 'A%' ORDER BY mid");
                queries.add("SELECT sk='A' eq,count() n FROM " + input + " GROUP BY eq ORDER BY eq");
                final ObjList<String> expected = new ObjList<>();
                expected.add("mid\n4\n");
                expected.add("mid\n4\n");
                expected.add("eq\tn\nfalse\t4\ntrue\t1\n");
                for (int i = 0, n = queries.size(); i < n; i++) {
                    try (RecordCursorFactory factory = compileFullFat(queries.getQuick(i))) {
                        for (int cursor = 0; cursor < 2; cursor++) {
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(expected.getQuick(i));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testIntKeysKeepMatchesAndUnmatchedMasterRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_ts_shared_m(id INT,a INT,b INT,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_ts_shared_s(id INT,k INT,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lp_ts_shared_m VALUES(11,1,2,2),(22,1,1,4),(33,2,2,6)");
            execute("INSERT INTO lp_ts_shared_s VALUES(10,1,1),(20,2,3),(30,2,5)");
            for (int kind = 0; kind < 2; kind++) {
                final String join = kind == 0 ? "ASOF" : "LT";
                assertRowsAfterCompilerClose("SELECT m.id mid,s.id sid,m.a,m.b,s.k FROM lp_ts_shared_m m "
                        + join + " JOIN lp_ts_shared_s s ON m.a=s.k AND m.b=s.k", """
                        mid\tsid\ta\tb\tk
                        11\tnull\t1\t2\tnull
                        22\t10\t1\t1\t1
                        33\t30\t2\t2\t2
                        """);
            }
        });
    }

    @Test
    public void testInternalNamesReserveLaterKeysAndKeepWildcardSchema() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_ts_shared_m(id INT,a INT,b INT,c INT,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_ts_shared_s(id INT,k INT,__questdb_temporal_key_0 INT,"
                    + "__QUESTDB_TEMPORAL_KEY_1 INT,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lp_ts_shared_m VALUES(11,1,2,7,2),(22,1,1,7,4),(33,2,2,8,6)");
            execute("INSERT INTO lp_ts_shared_s VALUES(10,1,7,100,1),(20,2,8,200,5)");
            for (int kind = 0; kind < 2; kind++) {
                final String join = kind == 0 ? "ASOF" : "LT";
                // The repeated k slot precedes the real key named __questdb_temporal_key_0.
                assertRowsAfterCompilerClose("SELECT m.id mid,s.* FROM lp_ts_shared_m m " + join
                        + " JOIN lp_ts_shared_s s ON m.c=s.__questdb_temporal_key_0 AND m.a=s.k AND m.b=s.k", """
                        mid\tid\tk\t__questdb_temporal_key_0\t__QUESTDB_TEMPORAL_KEY_1\tts
                        11\tnull\tnull\tnull\tnull\t
                        22\t10\t1\t7\t100\t1970-01-01T00:00:00.000001Z
                        33\t20\t2\t8\t200\t1970-01-01T00:00:00.000005Z
                        """);
            }
        });
    }

    @Test
    public void testSymbolKeysUseMatchingMasterDictionaryAfterRelocation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_ts_shared_m(id INT,a SYMBOL,b SYMBOL,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_ts_shared_s(id INT,k SYMBOL,payload SYMBOL,ts TIMESTAMP) TIMESTAMP(ts)");
            // a assigns A=0/B=1, while b and k assign B=0/A=1.
            execute("INSERT INTO lp_ts_shared_m VALUES(1,'A','B',2),(2,'B','B',4),(3,'A','A',6),(4,null,null,8)");
            execute("INSERT INTO lp_ts_shared_s VALUES(10,'B','first',1),(20,'A','second',3),(30,null,'null-key',7)");
            for (int kind = 0; kind < 2; kind++) {
                final String join = kind == 0 ? "ASOF" : "LT";
                for (int order = 0; order < 2; order++) {
                    final String condition = order == 0 ? "m.a=s.k AND m.b=s.k" : "m.b=s.k AND m.a=s.k";
                    assertRowsAfterCompilerClose("SELECT m.id mid,m.a,m.b,s.id sid,s.k,s.payload FROM lp_ts_shared_m m "
                            + join + " JOIN lp_ts_shared_s s ON " + condition, """
                            mid\ta\tb\tsid\tk\tpayload
                            1\tA\tB\tnull\t\t
                            2\tB\tB\t10\tB\tfirst
                            3\tA\tA\t20\tA\tsecond
                            4\t\t\t30\t\tnull-key
                            """);
                }
            }
        });
    }

    private static String mixedSymbolJoin(int kind, boolean isMasterDynamic, boolean isSlaveDynamic) {
        final String master = isMasterDynamic
                ? "(SELECT * FROM lp_ts_mixed_m UNION ALL SELECT * FROM lp_ts_mixed_m WHERE false ORDER BY ts)"
                : "lp_ts_mixed_m";
        final String slave = isSlaveDynamic
                ? "(SELECT * FROM (SELECT * FROM lp_ts_mixed_s UNION ALL SELECT * FROM lp_ts_mixed_s WHERE false ORDER BY ts) TIMESTAMP(ts))"
                : "lp_ts_mixed_s";
        return master + " m " + (isMasterDynamic ? "TIMESTAMP(ts) " : "")
                + (kind == 0 ? "ASOF" : "LT") + " JOIN " + slave + " s ON m.k=s.k";
    }

    private void assertRowsAfterCompilerClose(String sql, String expected) throws Exception {
        RecordCursorFactory retained = null;
        try {
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compiler.setFullFatJoins(true);
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                try (RecordCursorFactory ignored = compiler.compile("SELECT missing FROM lp_ts_shared_m", sqlExecutionContext)
                        .getRecordCursorFactory()) {
                    Assert.fail("invalid column must fail after retaining the join factory");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column: missing");
                }
                try (RecordCursorFactory ignored = compiler.compile("SELECT id FROM lp_ts_shared_m LIMIT 1", sqlExecutionContext)
                        .getRecordCursorFactory()) {
                    compiler.clear();
                }
            }
            assertFactory(retained).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                    .sizeMayVary().returns(expected);
        } finally {
            Misc.free(retained);
        }
    }

    private RecordCursorFactory compileFullFat(String sql) throws SqlException {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            compiler.setFullFatJoins(true);
            return compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
        }
    }

    private void createMixedSymbolRows() throws SqlException {
        execute("CREATE TABLE lp_ts_mixed_m(id INT,k SYMBOL,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE lp_ts_mixed_s(id INT,k SYMBOL,payload SYMBOL,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_ts_mixed_m VALUES(1,'A',2),(2,'B',4),(3,'C',6),(4,'A',8),(5,null,10)");
        execute("INSERT INTO lp_ts_mixed_s VALUES(10,'B','b',1),(20,'A','a',3),(30,'C','c',5),(40,null,'n',9)");
    }
}
