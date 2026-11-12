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

import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class InsertNullTest extends AbstractCairoTest {
    private static final int NULL_INSERTS = 3;
    private static final String[][] TYPES = {
            // type name, null value
            {"boolean", "false"},
            {"byte", "0"},
            {"short", "0"},
            {"char", ""},
            {"int", "null"},
            {"long", "null"},
            {"date", ""},
            {"timestamp", ""},
            {"timestamp_ns", ""},
            {"float", "null"},
            {"double", "null"},
            {"string", ""},
            {"symbol", ""},
            {"long256", ""},
            {"binary", ""},
            {"geohash(5b)", ""},
            {"geohash(15b)", ""},
            {"geohash(31b)", ""},
            {"geohash(60b)", ""},
            {"decimal(2,0)", ""},
            {"decimal(4,0)", ""},
            {"decimal(8,0)", ""},
            {"decimal(16,0)", ""},
            {"decimal(32,0)", ""},
            {"decimal(64,0)", ""}
    };

    @Test
    public void testInsertNull() throws Exception {
        for (int i = 0; i < TYPES.length; i++) {
            if (i > 0) {
                setUp();
            }
            try {
                final String[] type = TYPES[i];
                assertQuery("x")
                        .ddl(String.format("create table x (value %s)", type[0]))
                        .mutateWith(String.format("insert into x select null from long_sequence(%d)", NULL_INSERTS))
                        .expectSize()
                        .returns("value\n", expectedNullInserts("value\n", type[1], NULL_INSERTS, true));
            } finally {
                tearDown();
            }
        }
    }

    @Test
    public void testInsertNullFromDistinctIntoColumnList() throws Exception {
        // the column list puts the NULL column between columns of other types in the table, and the
        // DISTINCT map record keeps no bytes for it
        assertMemoryLeak(() -> {
            execute("CREATE TABLE xx (c LONG, k INT, v INT, d DOUBLE)");
            execute("INSERT INTO xx (v, k, c, d) SELECT DISTINCT x::INT, null, count(), null FROM long_sequence(3)");
            assertQuery("xx ORDER BY v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            c\tk\tv\td
                            1\tnull\t1\tnull
                            1\tnull\t2\tnull
                            1\tnull\t3\tnull
                            """);
        });
    }

    @Test
    public void testInsertNullFromDistinctOnDesignatedColumnMustFail() throws Exception {
        _testInsertNullFromDistinctOnDesignatedColumnMustFail("TIMESTAMP", "", "2024-01-01T00:00:00.000000Z");
    }

    @Test
    public void testInsertNullFromDistinctOnDesignatedNSColumnMustFail() throws Exception {
        _testInsertNullFromDistinctOnDesignatedColumnMustFail("TIMESTAMP_NS", "", "2024-01-01T00:00:00.000000000Z");
    }

    @Test
    public void testInsertNullFromDistinctOnDesignatedWalColumnMustFail() throws Exception {
        _testInsertNullFromDistinctOnDesignatedColumnMustFail("TIMESTAMP", " WAL", "2024-01-01T00:00:00.000000Z");
    }

    @Test
    public void testInsertNullFromEmptyDistinctOnDesignatedColumn() throws Exception {
        // no row reaches the writer, so the NULL designated timestamp fails nothing
        assertMemoryLeak(() -> {
            execute("CREATE TABLE xx (sym SYMBOL, y TIMESTAMP, c LONG) TIMESTAMP(y) PARTITION BY DAY");
            execute("INSERT INTO xx SELECT DISTINCT x::SYMBOL, null, count() FROM long_sequence(3) WHERE x < 0");
            assertQuery("xx")
                    .noLeakCheck()
                    .timestamp("y")
                    .expectSize()
                    .returns("sym\ty\tc\n");
        });
    }

    @Test
    public void testInsertNullFromSelectOnDesignatedColumnMustFail() throws Exception {
        _testInsertNullFromSelectOnDesignatedColumnMustFail("timestamp");
    }

    @Test
    public void testInsertNullFromSelectOnDesignatedNSColumnMustFail() throws Exception {
        _testInsertNullFromSelectOnDesignatedColumnMustFail("timestamp_ns");
    }

    @Test
    public void testInsertNullFromValuesOnDesignatedColumnMustFail() throws Exception {
        _testInsertNullFromValuesOnDesignatedColumnMustFail("timestamp");
    }

    @Test
    public void testInsertNullFromValuesOnDesignatedNSColumnMustFail() throws Exception {
        _testInsertNullFromValuesOnDesignatedColumnMustFail("timestamp_ns");
    }

    @Test
    public void testInsertNullThenFilterEq() throws Exception {
        for (int i = 0; i < TYPES.length; i++) {
            if (i > 0) {
                setUp();
            }
            try {
                final String[] type = TYPES[i];
                assertQuery("x where value = null")
                        .ddl(String.format("create table x (value %s)", type[0]))
                        .mutateWith(String.format("insert into x select null from long_sequence(%d)", NULL_INSERTS))
                        .returns("value\n", expectedNullInserts("value\n", type[1], NULL_INSERTS, !isNotNullable(type[0])));
            } finally {
                tearDown();
            }
        }
    }

    @Test
    public void testInsertNullThenFilterIsNotNull() throws Exception {
        for (int i = 0; i < TYPES.length; i++) {
            if (i > 0) {
                setUp();
            }
            try {
                final String[] type = TYPES[i];
                assertQuery("x where value is not null")
                        .ddl(String.format("create table x (value %s)", type[0]))
                        .mutateWith(String.format("insert into x select null from long_sequence(%d)", NULL_INSERTS))
                        .expectSize(isNotNullable(type[0]))
                        .returns("value\n", expectedNullInserts("value\n", type[1], NULL_INSERTS, isNotNullable(type[0])));
            } finally {
                tearDown();
            }
        }
    }

    @Test
    public void testInsertNullThenFilterIsNull() throws Exception {
        for (int i = 0; i < TYPES.length; i++) {
            if (i > 0) {
                setUp();
            }
            try {
                final String[] type = TYPES[i];
                assertQuery("x where value is null")
                        .ddl(String.format("create table x (value %s)", type[0]))
                        .mutateWith(String.format("insert into x select null from long_sequence(%d)", NULL_INSERTS))
                        .returns("value\n", expectedNullInserts("value\n", type[1], NULL_INSERTS, !isNotNullable(type[0])));
            } finally {
                tearDown();
            }
        }
    }

    @Test
    public void testInsertNullThenFilterNotEq() throws Exception {
        for (int i = 0; i < TYPES.length; i++) {
            if (i > 0) {
                setUp();
            }
            try {
                final String[] type = TYPES[i];
                assertQuery("x where value != null")
                        .ddl(String.format("create table x (value %s)", type[0]))
                        .mutateWith(String.format("insert into x select null from long_sequence(%d)", NULL_INSERTS))
                        .expectSize(isNotNullable(type[0]))
                        .returns("value\n", expectedNullInserts("value\n", type[1], NULL_INSERTS, isNotNullable(type[0])));
            } finally {
                tearDown();
            }
        }
    }

    private static boolean isNotNullable(String type) {
        return Chars.equalsLowerCaseAscii(type, "short") ||
                Chars.equalsLowerCaseAscii(type, "byte") ||
                Chars.equalsLowerCaseAscii(type, "boolean");
    }

    private void _testInsertNullFromDistinctOnDesignatedColumnMustFail(
            String timestampType,
            String walClause,
            String expectedTimestamp
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE xx (sym SYMBOL, y " + timestampType + ", c LONG) TIMESTAMP(y) PARTITION BY DAY" + walClause);
            // an aggregate keeps DISTINCT from turning into GROUP BY, so the INSERT reads the DISTINCT
            // map record, which keeps no bytes for the NULL column
            try {
                execute("INSERT INTO xx SELECT DISTINCT 'AA', null, count() FROM long_sequence(3)");
                Assert.fail();
            } catch (CairoException expected) {
                TestUtils.assertContains(expected.getFlyweightMessage(), "designated timestamp column cannot be NULL");
            }
            // the failed INSERT left the table empty and writable
            execute("INSERT INTO xx VALUES ('BB', '2024-01-01', 1)");
            drainWalQueue();
            assertQuery("xx")
                    .noLeakCheck()
                    .timestamp("y")
                    .expectSize()
                    .returns("sym\ty\tc\nBB\t" + expectedTimestamp + "\t1\n");
        });
    }

    private void _testInsertNullFromSelectOnDesignatedColumnMustFail(String timestampType) throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertQuery("xx")
                        .ddl("create table xx (sym symbol, y #TIMESTAMP_TYPE) timestamp(y)".replace("#TIMESTAMP_TYPE", timestampType))
                        .mutateWith("insert into xx select 'AA', null from long_sequence(1)")
                        .timestamp("y")
                        .returns("sym\ty\n", "y\n");
                Assert.fail();
            } catch (CairoException expected) {
                Assert.assertTrue(expected.getMessage().contains("designated timestamp column cannot be NULL"));
            }
        });
    }

    private void _testInsertNullFromValuesOnDesignatedColumnMustFail(String timestamp) throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertQuery("xx")
                        .ddl("create table xx (sym symbol, y #TIMESTAMP_TYPE) timestamp(y)".replace("#TIMESTAMP_TYPE", timestamp))
                        .mutateWith("insert into xx values('AA', null)")
                        .timestamp("y")
                        .returns("sym\ty\n", "y\n");
                Assert.fail();
            } catch (SqlException expected) {
                Assert.assertEquals("[28] designated timestamp column cannot be NULL", expected.getMessage());
            }
        });
    }

    static String expectedNullInserts(String header, String nullValue, int count, boolean expectsOutput) {
        StringSink sb = Misc.getThreadLocalSink();
        sb.put(header);
        if (expectsOutput) {
            for (int i = 0; i < count; i++) {
                sb.put(nullValue).put("\n");
            }
        }
        return sb.toString();
    }
}
