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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A NULL CHAR is char 0. Inserting one into a SYMBOL or VARCHAR column has to store NULL, as it does
 * for CHAR and STRING columns, not a one-character U+0000 value.
 */
public class InsertNullCharTest extends AbstractCairoTest {

    @Test
    public void testInsertAsSelectNonWal() throws Exception {
        assertInsertAsSelect("BYPASS WAL");
    }

    @Test
    public void testInsertAsSelectWal() throws Exception {
        assertInsertAsSelect("WAL");
    }

    @Test
    public void testInsertValuesNonWal() throws Exception {
        assertInsertValues("BYPASS WAL");
    }

    @Test
    public void testInsertValuesWal() throws Exception {
        assertInsertValues("WAL");
    }

    private void assertInsertAsSelect(String walClause) throws Exception {
        assertMemoryLeak(() -> {
            createTable(walClause);
            // the CASE has no ELSE branch value, so its type is CHAR and the second row is a NULL CHAR
            execute("""
                    INSERT INTO t
                    SELECT CASE WHEN x = 1 THEN 'a' ELSE NULL END,
                           CASE WHEN x = 1 THEN 'a' ELSE NULL END,
                           CASE WHEN x = 1 THEN 'a' ELSE NULL END,
                           CASE WHEN x = 1 THEN 'a' ELSE NULL END,
                           CASE WHEN x = 1 THEN 'a' ELSE NULL END,
                           x::TIMESTAMP
                    FROM long_sequence(2)""");
            drainWalQueue();
            assertRows();
        });
    }

    private void assertInsertValues(String walClause) throws Exception {
        assertMemoryLeak(() -> {
            createTable(walClause);
            execute("INSERT INTO t VALUES ('a', 'a', 'a', 'a', 'a', 1)");
            execute("INSERT INTO t VALUES (NULL::CHAR, NULL::CHAR, NULL::CHAR, NULL::CHAR, NULL::CHAR, 2)");
            drainWalQueue();
            assertRows();
        });
    }

    private void assertRows() throws Exception {
        assertQuery("SELECT s, si, c, str, v FROM t ORDER BY ts")
                .expectSize()
                .returns("""
                        s\tsi\tc\tstr\tv
                        a\ta\ta\ta\ta
                        \t\t\t\t
                        """);
        assertQuery("""
                SELECT s IS NULL s_null, si IS NULL si_null, c IS NULL c_null, str IS NULL str_null, v IS NULL v_null,
                  length(s) s_len, length(v) v_len
                FROM t ORDER BY ts""")
                .expectSize()
                .returns("""
                        s_null\tsi_null\tc_null\tstr_null\tv_null\ts_len\tv_len
                        false\tfalse\tfalse\tfalse\tfalse\t1\t1
                        true\ttrue\ttrue\ttrue\ttrue\t-1\t-1
                        """);
        for (String column : new String[]{"s", "si", "c", "str", "v"}) {
            assertQuery("SELECT count() FROM t WHERE " + column + " IS NULL")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n1\n");
        }
    }

    private void createTable(String walClause) throws Exception {
        execute("CREATE TABLE t (s SYMBOL, si SYMBOL INDEX, c CHAR, str STRING, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walClause);
    }
}
