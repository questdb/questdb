/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import org.junit.Test;

public class LatestByTest extends AbstractGriffinTest {

    @Test
    public void testSymbolInPredicate_singleElement() throws Exception {
        assertMemoryLeak(() -> {
            String createStmt = "CREATE table trades(symbol symbol, side symbol, timestamp timestamp) timestamp(timestamp);";
            compiler.compile(createStmt, sqlExecutionContext);
            executeInsert("insert into trades VALUES ('BTC', 'buy', 1609459199000000);");
            String expected = "symbol\tside\ttimestamp\n" +
                    "BTC\tbuy\t2020-12-31T23:59:59.000000Z\n";
            String query = "SELECT * FROM trades\n" +
                    "WHERE symbol in ('BTC') and side in 'buy'\n" +
                    "LATEST ON timestamp PARTITION BY symbol;";
            assertSql(query, expected);
        });
    }

    @Test
    public void testLatestByDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new FilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return Files.openRO(name);
                }
            };
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b') s, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    ") timestamp(ts) Partition by DAY");

            assertQuery("s\tts\n" +
                            "a\t1970-01-02T23:00:00.000000Z\n" +
                            "b\t1970-01-03T00:00:00.000000Z\n",
                    "t " +
                            "where s in ('a', 'b') " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestBySymbolEmpty() throws Exception {
        assertMemoryLeak(() -> {
            ff = new FilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan any partition, searched symbol values don't exist in symbol table
                    if (Chars.contains(name, "1970-01-01") || Chars.contains(name, "1970-01-02")) {
                        return -1;
                    }
                    return Files.openRO(name);
                }
            };

            compile("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('g', 'd', 'f') s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                    "from long_sequence(40)" +
                    ") timestamp(ts) Partition by DAY");

            assertQuery("x\ts\tts\n",
                    "t where s in ('a', 'b') latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestBySymbolManyDistinctValues() throws Exception {
        assertMemoryLeak(() -> {

            compile("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol(10000, 1, 15, 1000) s, " +
                    "timestamp_sequence(0, 1000*1000L) ts " +
                    "from long_sequence(1000000)" +
                    ") timestamp(ts) Partition by DAY");

            String distinctSymbols = selectDistinctSym("t", 500, "s");

            engine.releaseInactive();

            ff = new FilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in other partitions
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return Files.openRO(name);
                }
            };

            assertQuery("min\tmax\n" +
                            "1970-01-11T15:33:16.000000Z\t1970-01-12T13:46:39.000000Z\n",
                    "select min(ts), max(ts) from (select ts, x, s from t latest on ts partition by s)",
                    null,
                    false,
                    true);

            assertQuery("min\tmax\n" +
                            "1970-01-11T16:57:53.000000Z\t1970-01-12T13:46:05.000000Z\n",
                    "select min(ts), max(ts) from (" +
                            "select ts, x, s " +
                            "from t " +
                            "where s in (" + distinctSymbols + ") " +
                            "latest on ts partition by s" +
                            ")",
                    null,
                    false,
                    true);
        });
    }

    @Test
    public void testLatestBySymbolUnfilteredDoesNotDoFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new FilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return Files.openRO(name);
                }
            };

            compile("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) Partition by DAY");

            assertQuery("ts\tx\ts\n" +
                            "1970-01-02T22:00:00.000000Z\t47\tb\n" +
                            "1970-01-02T23:00:00.000000Z\t48\ta\n" +
                            "1970-01-03T00:00:00.000000Z\t49\t\n",
                    "select ts, x, s from t latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestBySymbolWithNoNulls() throws Exception {
        assertMemoryLeak(() -> {
            ff = new FilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return Files.openRO(name);
                }
            };

            compile("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', 'c', 'd', 'e', 'f') s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) Partition by DAY");

            assertQuery("ts\tx\ts\n" +
                            "1970-01-02T17:00:00.000000Z\t42\td\n" +
                            "1970-01-02T19:00:00.000000Z\t44\te\n" +
                            "1970-01-02T21:00:00.000000Z\t46\tc\n" +
                            "1970-01-02T22:00:00.000000Z\t47\tb\n" +
                            "1970-01-02T23:00:00.000000Z\t48\ta\n" +
                            "1970-01-03T00:00:00.000000Z\t49\tf\n",
                    "select ts, x, s from t latest on ts partition by s",
                    "ts",
                    true,
                    true);

            assertQuery("ts\tx\ts\n" +
                            "1970-01-03T00:00:00.000000Z\t49\tf\n" +
                            "1970-01-02T19:00:00.000000Z\t44\te\n" +
                            "1970-01-02T17:00:00.000000Z\t42\td\n" +
                            "1970-01-02T21:00:00.000000Z\t46\tc\n" +
                            "1970-01-02T22:00:00.000000Z\t47\tb\n" +
                            "1970-01-02T23:00:00.000000Z\t48\ta\n",
                    "select ts, x, s from t latest on ts partition by s order by s desc",
                    null,
                    true,
                    true);
        });
    }

    @Test
    public void testLatestWithFilterByDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new FilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return Files.openRO(name);
                }
            };

            compile("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) Partition by DAY");

            assertQuery("x\ts\tts\n" +
                            "44\tb\t1970-01-02T19:00:00.000000Z\n" +
                            "48\ta\t1970-01-02T23:00:00.000000Z\n",
                    "t " +
                            "where s in ('a', 'b') and x%2 = 0 " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestWithFilterByDoesNotNeedFullScanValueNotInSymbolTable() throws Exception {
        ff = new FilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // Query should not scan the first partition
                // all the latest values are in the second, third partition
                if (Chars.contains(name, "1970-01-01")) {
                    return -1;
                }
                return Files.openRO(name);
            }
        };

        assertQuery("x\ts\tts\n" +
                        "44\tb\t1970-01-02T19:00:00.000000Z\n" +
                        "48\ta\t1970-01-02T23:00:00.000000Z\n",
                "t " +
                        "where s in ('a', 'b', 'c') and x%2 = 0 " +
                        "latest on ts partition by s",
                "create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', null) s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "ts",
                "insert into t values (1000, 'c', '1970-01-02T20:00')",
                "x\ts\tts\n" +
                        "44\tb\t1970-01-02T19:00:00.000000Z\n" +
                        "1000\tc\t1970-01-02T20:00:00.000000Z\n" +
                        "48\ta\t1970-01-02T23:00:00.000000Z\n",
                true,
                true,
                true);
    }

    @Test
    public void testLatestWithNullInSymbolFilterDoesNotDoFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new FilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return Files.openRO(name);
                }
            };

            compile("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) Partition by DAY");

            assertQuery("x\ts\tts\n" +
                            "48\ta\t1970-01-02T23:00:00.000000Z\n" +
                            "49\t\t1970-01-03T00:00:00.000000Z\n",
                    "t where s in ('a', null) latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestWithoutSymbolFilterDoesNotDoFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new FilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return Files.openRO(name);
                }
            };

            compile("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) Partition by DAY");

            assertQuery("x\ts\tts\n" +
                            "35\ta\t1970-01-02T10:00:00.000000Z\n" +
                            "47\tb\t1970-01-02T22:00:00.000000Z\n" +
                            "49\t\t1970-01-03T00:00:00.000000Z\n",
                    "t where x%2 = 1 latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestWithJoinNonIndexed() throws Exception {
        testLatestByWithJoin(false);
    }

    @Test
    public void testLatestWithJoinIndexed() throws Exception {
        testLatestByWithJoin(true);
    }

    private void testLatestByWithJoin(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table r (symbol symbol, value long, ts timestamp)" +
                    (indexed ? ", index(symbol) " : " ") + "timestamp(ts) partition by day", sqlExecutionContext);
            executeInsert("insert into r values ('xyz', 1, '2022-11-02T01:01:01')");
            compiler.compile("create table t (symbol symbol, value long, ts timestamp)" +
                    (indexed ? ", index(symbol) " : " ") + "timestamp(ts) partition by day", sqlExecutionContext);
            executeInsert("insert into t values ('xyz', 42, '2022-11-02T01:01:01')");

            String query = "with r as (select symbol, value v from r where symbol = 'xyz' latest on ts partition by symbol),\n" +
                    " t as (select symbol, value v from t where symbol = 'xyz' latest on ts partition by symbol)\n" +
                    "select r.symbol, r.v subscribers, t.v followers\n" +
                    "from r\n" +
                    "join t on symbol";
            try (
                    RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()
            ) {
                assertCursor(
                        "symbol\tsubscribers\tfollowers\n" +
                                "xyz\t1\t42\n",
                        factory,
                        false,
                        true,
                        false
                );
            }
        });
    }

    private String selectDistinctSym(String table, int count, String columnName) throws SqlException {
        StringSink sink = new StringSink();
        try (RecordCursorFactory factory = compiler.compile("select distinct " + columnName + " from " + table + " order by " + columnName + " limit " + count, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    if (i++ > 0) {
                        sink.put(',');
                    }
                    sink.put('\'').put(record.getSym(0)).put('\'');
                }
            }
        }
        return sink.toString();
    }
}
