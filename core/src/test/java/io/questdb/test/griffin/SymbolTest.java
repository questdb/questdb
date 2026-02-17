/*******************************************************************************
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

public class SymbolTest extends AbstractCairoTest {

    @Test
    public void testIndexedScanOverColTopColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create atomic table x as ( " +
                            "  select rnd_symbol('AB', 'BC', 'CD') sym, timestamp_sequence('2022-02-24', 1000000L) ts " +
                            "  from long_sequence(10000)" +
                            "), index(sym) timestamp(ts) partition by DAY BYPASS WAL;"
            );
            execute("alter table x add column sym_top symbol index;");
            execute("insert into x values('EF', '2022-02-01T00:00','STWEFZMJCWHFQEDUY');");

            assertQueryNoLeakCheck(
                    "sym\tts\tsym_top\n" +
                            "EF\t2022-02-01T00:00:00.000000Z\tSTWEFZMJCWHFQEDUY\n",
                    "x where \"sym_top\" = 'STWEFZMJCWHFQEDUY' order by ts asc",
                    "ts",
                    true
            );
            assertQueryNoLeakCheck(
                    "sym\tts\tsym_top\n" +
                            "EF\t2022-02-01T00:00:00.000000Z\tSTWEFZMJCWHFQEDUY\n",
                    "x where \"sym_top\" = 'STWEFZMJCWHFQEDUY' order by ts desc",
                    "ts###desc",
                    true
            );
        });
    }

    @Test
    public void testNullIsReturnedAfterReaderReload() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY;");
            final String query = "select * from x where sym not in ('foo', 'bar')";
            // Load table reader on empty table.
            assertQueryNoLeakCheck(
                    "sym\tts\n",
                    query,
                    "ts",
                    true,
                    false,
                    false
            );
            execute(
                    "insert into x values ('foo', '2024-05-14T16:00:00.000000Z')," +
                            "('bar', '2024-05-14T16:00:01.000000Z')," +
                            "('baz', '2024-05-14T16:00:02.000000Z')," +
                            "(null, '2024-05-14T16:00:02.000000Z');"
            );
            // The reader should refresh contain null flag in symbol readers,
            // so NULL row should be included in the result set.
            assertQueryNoLeakCheck(
                    "sym\tts\n" +
                            "baz\t2024-05-14T16:00:02.000000Z\n" +
                            "\t2024-05-14T16:00:02.000000Z\n",
                    query,
                    "ts",
                    true,
                    false,
                    false
            );
        });
    }

    @Test
    public void testNullIsReturnedAfterReaderReload2() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY;");
            final String query = "select * from x where sym not in ('foo', 'bar')";
            // Load table reader on empty table.
            assertQueryNoLeakCheck(
                    "sym\tts\n",
                    query,
                    "ts",
                    true,
                    false,
                    false
            );
            // Insert only NULL rows.
            execute(
                    "insert into x values (null, '2024-05-14T16:00:01.000000Z')," +
                            "(null, '2024-05-14T16:00:02.000000Z')," +
                            "(null, '2024-05-14T16:00:03.000000Z');"
            );
            // The reader should refresh contain null flag in symbol readers,
            // so NULL rows should be included in the result set.
            assertQueryNoLeakCheck(
                    "sym\tts\n" +
                            "\t2024-05-14T16:00:01.000000Z\n" +
                            "\t2024-05-14T16:00:02.000000Z\n" +
                            "\t2024-05-14T16:00:03.000000Z\n",
                    query,
                    "ts",
                    true,
                    false,
                    false
            );
        });
    }

    @Test
    public void testNullSymbolOrderByRegression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (" +
                    " sym SYMBOL capacity 256 CACHE index capacity 256," +
                    " timestamp TIMESTAMP" +
                    ") timestamp (timestamp)");
            execute("insert into x select" +
                    " rnd_symbol(100, 2, 4, 2) sym," +
                    " '2024-03-05T12:13'::timestamp timestamp" +
                    " from long_sequence(51)");
            engine.releaseAllWriters();
            assertQueryNoLeakCheck(
                    "sym\nZVDZ\nYR\nYPH\nXWC\nXG\nVLTO\nUWD\nUQSR\nULOF\nTMH\nSXU\nSXU\nSXU\nSDOT\nRFB\nPH\n" +
                            "PH\nOWLP\nOWLP\nLU\nKWZ\nJSHR\nIBBT\nHYHB\nHWVD\nGLHM\nGLHM\nGLHM\nFZ\nFZ\nFMQN\nFLOP\n" +
                            "FF\nFDT\nEHBH\nEDYY\nEDYY\nEDRQ\nCPSW\n\n\n\n\n\n\n\n\n\n\n\n\n",
                    "select sym from x" +
                            " where timestamp in '2024-03-05T12:13'" +
                            " order by sym desc",
                    null,
                    true,
                    false,
                    false
            );
        });
    }

    @Test
    public void testSelectSymbolUsingBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table logs ( id symbol capacity 2)");
            execute("insert into logs select x::string from long_sequence(10)");

            for (int i = 1; i < 11; i++) {
                assertQueryNoLeakCheck("id\n" + i + "\n", "select * from logs where id = '" + i + "'", null, true);
            }
        });
    }

    @Test
    public void testSelectSymbolUsingLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table logs ( id symbol capacity 2)");
            execute("insert into logs select x::string from long_sequence(10)");

            for (int i = 1; i < 11; i++) {
                bindVariableService.clear();
                bindVariableService.setStr("id", String.valueOf(i));

                assertQueryNoLeakCheck("id\n" + i + "\n", "select * from logs where id = :id", null, true);
            }
        });
    }
}
