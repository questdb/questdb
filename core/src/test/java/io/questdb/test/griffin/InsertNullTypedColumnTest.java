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
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.std.BytecodeAssembler;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.RecordToRowCopierTestUtils.CopierMode;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Inserts NULL-typed source columns, the type of an untyped NULL in the select list, into every column
 * type. Some sources keep no storage for such a column: the map records of DISTINCT, in the fixed-size
 * and the var-size key layout, and the rows a sort materializes. A typed getter returns the bytes of a
 * neighbouring column there, so the copier has to store NULL without reading the record. The NULL
 * columns come first, in the middle and last. {@code debug.cairo.copier.type} forces each copier
 * implementation, and {@link #assertForcedCopier(String)} checks that the compiler builds the forced
 * one. Plain tables drive {@code TableWriter.RowImpl}, WAL tables {@code WalWriter.RowImpl}.
 */
@RunWith(Parameterized.class)
public class InsertNullTypedColumnTest extends AbstractCairoTest {
    // a copy of a fixed-size column is estimated at 18 bytes of bytecode and a chunk at 6000 bytes, so
    // 350 more columns split the chunked copier into two methods, with NULL columns in both
    private static final int CHUNKED_FILLER_COUNT = 350;
    // every column type, with the value it shows for NULL
    private static final String[][] TYPES = {
            {"BOOLEAN", "false"},
            {"BYTE", "0"},
            {"SHORT", "0"},
            {"CHAR", ""},
            {"INT", "null"},
            {"LONG", "null"},
            {"FLOAT", "null"},
            {"DOUBLE", "null"},
            {"DATE", ""},
            {"TIMESTAMP", ""},
            {"TIMESTAMP_NS", ""},
            {"SYMBOL", ""},
            {"STRING", ""},
            {"VARCHAR", ""},
            {"BINARY", ""},
            {"UUID", ""},
            {"IPv4", ""},
            {"LONG256", ""},
            {"GEOHASH(5b)", ""},
            {"GEOHASH(15b)", ""},
            {"GEOHASH(31b)", ""},
            {"GEOHASH(60b)", ""},
            {"DECIMAL(2,0)", ""},
            {"DECIMAL(4,0)", ""},
            {"DECIMAL(9,0)", ""},
            {"DECIMAL(18,0)", ""},
            {"DECIMAL(38,0)", ""},
            {"DECIMAL(76,0)", ""},
            {"DOUBLE[]", "null"},
            {"DOUBLE[][]", "null"}
    };
    private final CopierMode copierMode;
    private final boolean isWal;

    public InsertNullTypedColumnTest(CopierMode copierMode, boolean isWal) {
        this.copierMode = copierMode;
        this.isWal = isWal;
    }

    @Parameterized.Parameters(name = "{0}-wal={1}")
    public static Collection<Object[]> data() {
        final List<Object[]> data = new ArrayList<>();
        for (CopierMode copierMode : CopierMode.values()) {
            data.add(new Object[]{copierMode, false});
            data.add(new Object[]{copierMode, true});
        }
        return data;
    }

    @Override
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierMode.getCopierType());
    }

    @Test
    public void testDistinctWithAggregate() throws Exception {
        // an aggregate keeps DISTINCT from turning into GROUP BY, and the DISTINCT map holds the NULL
        // columns in a fixed-size key, at no width
        assertNullColumns(
                "SELECT DISTINCT",
                "INT", "v",
                "LONG", "count()",
                "FROM src",
                """
                        1\t2
                        2\t1
                        3\t1
                        """
        );
    }

    @Test
    public void testDistinctWithVarcharKey() throws Exception {
        // the VARCHAR key puts the DISTINCT map in the var-size key layout
        assertNullColumns(
                "SELECT DISTINCT",
                "VARCHAR", "s",
                "LONG", "count()",
                "FROM src",
                """
                        \t1
                        a\t2
                        bb\t1
                        """
        );
    }

    @Test
    public void testDistinctWithWindowFunction() throws Exception {
        assertNullColumns(
                "SELECT DISTINCT",
                "INT", "v",
                "LONG", "lag(v) OVER ()",
                "FROM src",
                """
                        1\tnull
                        1\t1
                        2\t1
                        3\t2
                        """
        );
    }

    @Test
    public void testProjection() throws Exception {
        // the NULL columns are constant functions here, the common way an INSERT selects NULL
        assertNullColumns(
                "SELECT",
                "INT", "v",
                "LONG", "v::LONG",
                "FROM src",
                """
                        1\t1
                        1\t1
                        2\t2
                        3\t3
                        """
        );
    }

    @Test
    public void testSortOverUnionAll() throws Exception {
        // UNION ALL has no random access, so the sort copies the rows, without the NULL columns
        assertNullColumns(
                "SELECT",
                "INT", "v",
                "LONG", "v::LONG",
                "FROM (SELECT v FROM src UNION ALL SELECT v FROM src) ORDER BY v",
                """
                        1\t1
                        1\t1
                        1\t1
                        1\t1
                        2\t2
                        2\t2
                        3\t3
                        3\t3
                        """
        );
    }

    private static void appendNullColumns(StringBuilder ddl, StringBuilder select, int lo, int hi) {
        for (int i = lo; i < hi; i++) {
            if (i > 0) {
                ddl.append(", ");
                select.append(", ");
            }
            ddl.append('n').append(i).append(' ').append(TYPES[i][0]);
            select.append("null n").append(i);
        }
    }

    /**
     * Asserts that the copier the compiler builds for {@code INSERT INTO dst <selectSql>} is the one this
     * parameter forces, using the same metadata, column filter and configuration as the INSERT.
     */
    private void assertForcedCopier(String selectSql) throws Exception {
        try (
                RecordCursorFactory factory = select(selectSql);
                TableReader dst = getReader("dst")
        ) {
            final EntityColumnFilter columnFilter = new EntityColumnFilter();
            columnFilter.of(dst.getMetadata().getColumnCount());
            Assert.assertEquals(
                    copierMode,
                    RecordToRowCopierTestUtils.copierModeOf(RecordToRowCopierUtils.generateCopier(
                            new BytecodeAssembler(),
                            factory.getMetadata(),
                            dst.getMetadata(),
                            columnFilter,
                            configuration
                    ))
            );
        }
    }

    /**
     * Inserts a row of NULL columns, one of each type, around a key column {@code k} and a value column
     * {@code w}, and asserts that the NULL columns read back as NULL and k and w as {@code expectedKw}.
     */
    private void assertNullColumns(
            String selectKeyword,
            String keyType,
            String key,
            String valueType,
            String value,
            String from,
            String expectedKw
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (ts TIMESTAMP, v INT, s VARCHAR) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO src VALUES
                    ('2024-01-01T00:00:00.000000Z', 1, 'a'),
                    ('2024-01-01T12:00:00.000000Z', 1, 'bb'),
                    ('2024-01-02T00:00:00.000000Z', 2, 'a'),
                    ('2024-01-03T00:00:00.000000Z', 3, null)
                    """);

            final int fillerCount = copierMode == CopierMode.CHUNKED ? CHUNKED_FILLER_COUNT : 0;
            final StringBuilder ddl = new StringBuilder("CREATE TABLE dst (");
            final StringBuilder select = new StringBuilder(selectKeyword).append(' ');
            // NULL columns first, between k and w, and last
            final int third = TYPES.length / 3;
            appendNullColumns(ddl, select, 0, third);
            ddl.append(", k ").append(keyType);
            select.append(", ").append(key).append(" k");
            appendNullColumns(ddl, select, third, 2 * third);
            ddl.append(", w ").append(valueType);
            select.append(", ").append(value).append(" w");
            for (int i = 0; i < fillerCount; i++) {
                ddl.append(", f").append(i).append(' ').append(keyType);
                select.append(", ").append(key).append(" f").append(i);
            }
            appendNullColumns(ddl, select, 2 * third, TYPES.length);
            if (isWal) {
                ddl.append(", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                select.append(", '2024-01-01'::TIMESTAMP ts");
            } else {
                ddl.append(')');
            }
            select.append(' ').append(from);
            execute(ddl);
            assertForcedCopier(select.toString());

            execute("INSERT INTO dst " + select);
            if (isWal) {
                drainWalQueue();
            }

            final StringBuilder query = new StringBuilder("SELECT ");
            final StringBuilder nulls = new StringBuilder();
            for (int i = 0; i < TYPES.length; i++) {
                query.append('n').append(i).append(", ");
                nulls.append(TYPES[i][1]).append('\t');
            }
            query.append("k, w FROM dst ORDER BY k, w");
            final StringBuilder expected = new StringBuilder();
            for (int i = 0; i < TYPES.length; i++) {
                expected.append('n').append(i).append('\t');
            }
            expected.append("k\tw\n");
            for (String kw : expectedKw.split("\n")) {
                expected.append(nulls).append(kw).append('\n');
            }
            assertQuery(query.toString())
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected.toString());
        });
    }
}
