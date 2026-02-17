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

import io.questdb.cairo.CairoException;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;

public class ShowCreateTableTest extends AbstractCairoTest {

    @Test
    public void testBypassWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE 'network_nodes' (\s
                    \ttimestamp TIMESTAMP,
                    \tnode_name SYMBOL CAPACITY 65536 CACHE INDEX CAPACITY 65536,
                    \thost_ip IPv4,
                    \tiprange_start IPv4,
                    \tiprange_end IPv4,
                    \tnode_type SYMBOL CAPACITY 1024 CACHE,
                    \tlocation SYMBOL CAPACITY 8 CACHE,
                    \tinterface SYMBOL CAPACITY 64 CACHE,
                    \tprotocol SYMBOL CAPACITY 64 CACHE,
                    \tstatus SYMBOL CAPACITY 8 CACHE,
                    \tip_subnet STRING,
                    \tvlan INT,
                    \tcomment STRING
                    ) timestamp(timestamp) PARTITION BY NONE BYPASS WAL
                    WITH maxUncommittedRows=500000, o3MaxLag=600000000us;""");

            printSql("SHOW CREATE TABLE network_nodes;");
            String printedSql = sink.toString().replace("ddl\n", "");

            execute("drop table network_nodes;");

            execute(printedSql);

            printSql("SHOW CREATE TABLE network_nodes;");

            TestUtils.assertEquals(sink.toString().replace("ddl\n", ""), printedSql);
        });
    }

    @Test
    public void testDedup() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, s symbol, i int ) timestamp(ts) partition by day wal dedup upsert keys(ts, s, i)");
            assertSql("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL,
                            \ti INT
                            ) timestamp(ts) PARTITION BY DAY
                            DEDUP UPSERT KEYS(ts,s,i);
                            """,
                    "show create table foo");
        });
    }

    @Test
    public void testDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, s symbol ) timestamp(ts)");
            assertSql("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            ) timestamp(ts) PARTITION BY NONE BYPASS WAL;
                            """,
                    "show create table foo");
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, s symbol ) timestamp(ts) partition by year bypass wal;");
            assertPlanNoLeakCheck("show create table foo", "show_create_table of: foo\n");
        });
    }

    @Test
    public void testInVolumeNotFound() throws Exception {

        Assume.assumeFalse(Os.isWindows());

        assertMemoryLeak(() -> {
            final File volume = temp.newFolder("other_path");
            final String volumeAlias = "foobar";
            final String volumePath = volume.getAbsolutePath();
            try (Path path = new Path()) {
                configuration.getVolumeDefinitions().of(volumeAlias + "->" + volumePath, path, root);
            }
            execute("create table foo (ts timestamp) timestamp(ts) partition by day wal in volume foobar");

            configuration.getVolumeDefinitions().clear();
            try {
                assertExceptionNoLeakCheck("show create table foo");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not find volume alias for table");
            }
        });
    }

    @Test
    public void testManyOtherColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(0, 1000000000) k," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n" +
                    " from long_sequence(10)" +
                    ") timestamp (timestamp);");
            assertSql("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \ti INT,
                            \tsym SYMBOL,
                            \tamt DOUBLE,
                            \ttimestamp TIMESTAMP,
                            \tb BOOLEAN,
                            \tc STRING,
                            \td DOUBLE,
                            \te FLOAT,
                            \tf SHORT,
                            \tg DATE,
                            \tik SYMBOL,
                            \tj LONG,
                            \tk TIMESTAMP,
                            \tl BYTE,
                            \tm BINARY,
                            \tn STRING
                            ) timestamp(timestamp) PARTITION BY NONE BYPASS WAL;
                            """,
                    "show create table foo");
        });
    }

    @Test
    public void testMinimalDdl() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp)");
            assertSql("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP
                            );
                            """,
                    "show create table foo");
        });
    }

    @Test
    public void testOtherColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp, s symbol capacity 256)");
            assertSql("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            );
                            """,
                    "show create table foo");
        });
    }

    @Test
    public void testPartitioning() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, s symbol ) timestamp(ts) partition by year wal;");
            assertSql("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            ) timestamp(ts) PARTITION BY YEAR;
                            """,
                    "show create table foo");
        });
    }

    @Test
    public void testPartitioningButBypassingWAL() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, s symbol ) timestamp(ts) partition by year bypass wal;");
            assertSql("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            ) timestamp(ts) PARTITION BY YEAR BYPASS WAL;
                            """,
                    "show create table foo");
        });
    }

    @Test
    public void testShowCreateTableUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 ( ts timestamp, s symbol ) timestamp(ts)");
            execute("create table t2 ( ts timestamp, s symbol ) timestamp(ts)");
            execute("create table t3 ( ts timestamp, s symbol ) timestamp(ts)");
            assertSql("""
                            ddl
                            CREATE TABLE 't1' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            ) timestamp(ts) PARTITION BY NONE BYPASS WAL;
                            CREATE TABLE 't2' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            ) timestamp(ts) PARTITION BY NONE BYPASS WAL;
                            CREATE TABLE 't3' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            ) timestamp(ts) PARTITION BY NONE BYPASS WAL;
                            """,
                    "show create table t1 union show create table t2 union show create table t3");
        });
    }

    @Test
    public void testSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp, s symbol capacity 512 nocache)");
            assertSql("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL NOCACHE
                            );
                            """,
                    "show create table foo");
        });
    }

    @Test
    public void testSymbolAndIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp, s symbol capacity 512 nocache index capacity 1024)");
            assertSql("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL NOCACHE INDEX CAPACITY 1024
                            );
                            """,
                    "show create table foo");
        });
    }

    @Test
    public void testTableDoesNotExist() throws Exception {
        assertMemoryLeak(() -> assertException("show create table foo;", 18, "table does not exist"));
    }

    @Test
    public void testTtlOneDay() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1D");
            assertSql("""
                    ddl
                    CREATE TABLE 'tango' (\s
                    \tts TIMESTAMP
                    ) timestamp(ts) PARTITION BY HOUR TTL 1 DAY BYPASS WAL;
                    """, "SHOW CREATE TABLE tango");
        });
    }

    @Test
    public void testTtlOneHour() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 HOUR");
            assertSql("""
                    ddl
                    CREATE TABLE 'tango' (\s
                    \tts TIMESTAMP
                    ) timestamp(ts) PARTITION BY HOUR TTL 1 HOUR BYPASS WAL;
                    """, "SHOW CREATE TABLE tango");
        });
    }

    @Test
    public void testTtlOneMonth() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1M");
            assertSql("""
                    ddl
                    CREATE TABLE 'tango' (\s
                    \tts TIMESTAMP
                    ) timestamp(ts) PARTITION BY HOUR TTL 1 MONTH BYPASS WAL;
                    """, "SHOW CREATE TABLE tango");
        });
    }

    @Test
    public void testTtlOneWeek() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1W");
            assertSql("""
                    ddl
                    CREATE TABLE 'tango' (\s
                    \tts TIMESTAMP
                    ) timestamp(ts) PARTITION BY HOUR TTL 1 WEEK BYPASS WAL;
                    """, "SHOW CREATE TABLE tango");
        });
    }

    @Test
    public void testTtlOneYear() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1Y");
            assertSql("""
                    ddl
                    CREATE TABLE 'tango' (\s
                    \tts TIMESTAMP
                    ) timestamp(ts) PARTITION BY HOUR TTL 1 YEAR BYPASS WAL;
                    """, "SHOW CREATE TABLE tango");
        });
    }

    @Test
    public void testTtlTwoHours() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 2H");
            assertSql("""
                    ddl
                    CREATE TABLE 'tango' (\s
                    \tts TIMESTAMP
                    ) timestamp(ts) PARTITION BY HOUR TTL 2 HOURS BYPASS WAL;
                    """, "SHOW CREATE TABLE tango");
        });
    }

    @Test
    public void testTtlTwoWeeks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 2W");
            assertSql("""
                    ddl
                    CREATE TABLE 'tango' (\s
                    \tts TIMESTAMP
                    ) timestamp(ts) PARTITION BY HOUR TTL 2 WEEKS BYPASS WAL;
                    """, "SHOW CREATE TABLE tango");
        });
    }

    @Test
    public void testTtlTwoYears() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 2Y");
            assertSql("""
                    ddl
                    CREATE TABLE 'tango' (\s
                    \tts TIMESTAMP
                    ) timestamp(ts) PARTITION BY HOUR TTL 2 YEARS BYPASS WAL;
                    """, "SHOW CREATE TABLE tango");
        });
    }

    @Test
    public void testWithMaxUncommittedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, s symbol ) " +
                    "with maxUncommittedRows=1234");
            assertSql("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            );
                            """,
                    "show create table foo");
        });
    }

    // o3MaxLag does not allow plain numbers in `CREATE TABLE`
    // You must provide a unit. This differs from server.conf which
    // allows you to give a plain value.
    // The divergence exists between `Numbers.parseMicros` and `SqlUtil.expectMicros`
    @Test
    public void testWithMaxUncommittedRowsAndO3MaxLag() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, s symbol ) " +
                    "with maxUncommittedRows=1234, o3MaxLag=1s");
            assertSql("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            );
                            """,
                    "show create table foo");
        });
    }

    @Test
    public void testWithO3MaxLag() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, s symbol ) " +
                    "with o3MaxLag=1s");
            assertSql("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            );
                            """,
                    "show create table foo");
        });
    }
}
