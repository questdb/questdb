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
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.MemoryTag;
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
            assertQuery("show create table foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL,
                            \ti INT
                            ) timestamp(ts) PARTITION BY DAY
                            DEDUP UPSERT KEYS(ts,s,i);
                            """);
        });
    }

    @Test
    public void testDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, s symbol ) timestamp(ts)");
            assertQuery("show create table foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            ) timestamp(ts) PARTITION BY NONE BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, s symbol ) timestamp(ts) partition by year bypass wal;");
            assertQuery("show create table foo")
                    .noLeakCheck()
                    .assertsPlan("show_create_table of: foo\n");
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
            assertQuery("show create table foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testMatViewRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, v double) timestamp(ts) partition by day wal");
            execute("create materialized view base_1h as (select ts, max(v) from base sample by 1h) partition by week");
            drainWalAndViewQueues();
            assertExceptionNoLeakCheck(
                    "show create table base_1h",
                    18,
                    "table name expected, got view or materialized view name"
            );
        });
    }

    @Test
    public void testMatViewRejectedCaseInsensitive() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, v double) timestamp(ts) partition by day wal");
            execute("create materialized view base_1h as (select ts, max(v) from base sample by 1h) partition by week");
            drainWalAndViewQueues();
            // the keyword casing must not change the outcome - the position still points at the name
            assertExceptionNoLeakCheck(
                    "ShOw CrEaTe TaBlE base_1h",
                    18,
                    "table name expected, got view or materialized view name"
            );
        });
    }

    @Test
    public void testMatViewRejectedQuotedName() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, v double) timestamp(ts) partition by day wal");
            execute("create materialized view 'base 1h' as (select ts, max(v) from base sample by 1h) partition by week");
            drainWalAndViewQueues();
            // the position points at the opening quote of the quoted identifier
            assertExceptionNoLeakCheck(
                    "show create table 'base 1h'",
                    18,
                    "table name expected, got view or materialized view name"
            );
        });
    }

    @Test
    public void testMatViewStillAccessibleViaShowCreateMatView() throws Exception {
        // a materialized view rejected by SHOW CREATE TABLE must remain reachable via the dedicated statement
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, v double) timestamp(ts) partition by day wal");
            execute("create materialized view base_1h as (select ts, max(v) from base sample by 1h) partition by week");
            drainWalAndViewQueues();
            printSql("show create materialized view base_1h");
            TestUtils.assertContains(sink.toString(), "CREATE MATERIALIZED VIEW 'base_1h'");
        });
    }

    @Test
    public void testMissingNameReportsDoesNotExist() throws Exception {
        // the existence check must run before the view/matview check, so an unknown
        // name reports "does not exist" rather than the "got view" message
        assertMemoryLeak(() -> assertExceptionNoLeakCheck(
                "show create table nope",
                18,
                "table does not exist"
        ));
    }

    @Test
    public void testMinimalDdl() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp)");
            assertQuery("show create table foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP
                            );
                            """);
        });
    }

    @Test
    public void testOtherColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp, s symbol capacity 256)");
            assertQuery("show create table foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            );
                            """);
        });
    }

    @Test
    public void testParquetCompression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE foo (ts TIMESTAMP, d DOUBLE PARQUET(default, ZSTD(3))) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SHOW CREATE TABLE foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \td DOUBLE PARQUET(default, zstd(3))
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testParquetCompressionLevelZero() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE foo (ts TIMESTAMP, a INT PARQUET(PLAIN, GZIP(0))) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SHOW CREATE TABLE foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ta INT PARQUET(plain, gzip(0))
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testParquetCompressionUncompressed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE foo (ts TIMESTAMP, d DOUBLE PARQUET(default, UNCOMPRESSED)) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SHOW CREATE TABLE foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \td DOUBLE PARQUET(default, uncompressed)
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testParquetCompressionWithSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE foo (ts TIMESTAMP, s SYMBOL PARQUET(default, ZSTD)) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SHOW CREATE TABLE foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL PARQUET(default, zstd)
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testParquetEncoding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE foo (ts TIMESTAMP, a INT PARQUET(DELTA_BINARY_PACKED)) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SHOW CREATE TABLE foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ta INT PARQUET(delta_binary_packed)
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testParquetEncodingAndCompression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE foo (ts TIMESTAMP, a INT PARQUET(DELTA_BINARY_PACKED, ZSTD(3))) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SHOW CREATE TABLE foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ta INT PARQUET(delta_binary_packed, zstd(3))
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testParquetBloomFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE foo (ts TIMESTAMP, a VARCHAR PARQUET(BLOOM_FILTER)) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SHOW CREATE TABLE foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ta VARCHAR PARQUET(bloom_filter)
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testParquetBloomFilterWithEncoding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE foo (ts TIMESTAMP, a INT PARQUET(PLAIN, BLOOM_FILTER)) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SHOW CREATE TABLE foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ta INT PARQUET(plain, bloom_filter)
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testParquetBloomFilterWithEncodingAndCompression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE foo (ts TIMESTAMP, a INT PARQUET(DELTA_BINARY_PACKED, ZSTD(3), BLOOM_FILTER)) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SHOW CREATE TABLE foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ta INT PARQUET(delta_binary_packed, zstd(3), bloom_filter)
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testParquetBloomFilterRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE foo (ts TIMESTAMP, a INT PARQUET(DELTA_BINARY_PACKED, ZSTD(3), BLOOM_FILTER), b VARCHAR PARQUET(BLOOM_FILTER)) TIMESTAMP(ts) PARTITION BY DAY");

            printSql("SHOW CREATE TABLE foo;");
            String printedSql = sink.toString().replace("ddl\n", "");

            execute("DROP TABLE foo;");
            execute(printedSql);

            printSql("SHOW CREATE TABLE foo;");
            TestUtils.assertEquals(sink.toString().replace("ddl\n", ""), printedSql);
        });
    }

    @Test
    public void testParquetBloomFilterMixedColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE foo (ts TIMESTAMP, a INT PARQUET(BLOOM_FILTER), b VARCHAR PARQUET(DELTA_LENGTH_BYTE_ARRAY, BLOOM_FILTER), c DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SHOW CREATE TABLE foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ta INT PARQUET(bloom_filter),
                            \tb VARCHAR PARQUET(delta_length_byte_array, bloom_filter),
                            \tc DOUBLE
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testParquetIgnoredOnLegacyMetaFormat() throws Exception {
        // Per-column parquet config lives at offset 20 of each column entry, which earlier
        // meta layouts used for unrelated data (e.g., the upper half of the removed columnHash).
        // SHOW CREATE TABLE must not emit PARQUET(...) for columns whose meta format predates
        // the field, otherwise the output is invalid DDL like PARQUET(unknown(133), ...).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE foo (ts TIMESTAMP, a INT, b LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            TableToken token = engine.verifyTableName("foo");
            try (
                    MemoryMARW mem = Vm.getCMARWInstance();
                    Path path = new Path()
            ) {
                path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
                mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
                int columnCount = mem.getInt(TableUtils.META_OFFSET_COUNT);
                for (int i = 0; i < columnCount; i++) {
                    long off = TableUtils.META_OFFSET_COLUMN_TYPES + i * TableUtils.META_COLUMN_DATA_SIZE + 20;
                    // bit 24 set (explicit flag), arbitrary bytes for encoding/compression/level
                    mem.putInt(off, 0x035ACA85);
                }
                // Force isMetaFormatUpToDate() to return false so the legacy-format guard kicks in.
                mem.putInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION, 0);
            }

            engine.releaseAllReaders();
            try (MetadataCacheWriter w = engine.getMetadataCache().writeLock()) {
                w.clearCache();
            }
            engine.getMetadataCache().onStartupAsyncHydrator();

            assertQuery("SHOW CREATE TABLE foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ta INT,
                            \tb LONG
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testPartitioning() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, s symbol ) timestamp(ts) partition by year wal;");
            assertQuery("show create table foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            ) timestamp(ts) PARTITION BY YEAR;
                            """);
        });
    }

    @Test
    public void testPartitioningButBypassingWAL() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, s symbol ) timestamp(ts) partition by year bypass wal;");
            assertQuery("show create table foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            ) timestamp(ts) PARTITION BY YEAR BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testShowCreateTableUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 ( ts timestamp, s symbol ) timestamp(ts)");
            execute("create table t2 ( ts timestamp, s symbol ) timestamp(ts)");
            execute("create table t3 ( ts timestamp, s symbol ) timestamp(ts)");
            assertQuery("show create table t1 union show create table t2 union show create table t3")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp, s symbol capacity 512 nocache)");
            assertQuery("show create table foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL NOCACHE
                            );
                            """);
        });
    }

    @Test
    public void testSymbolAndIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp, s symbol capacity 512 nocache index capacity 1024)");
            assertQuery("show create table foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL NOCACHE INDEX CAPACITY 1024
                            );
                            """);
        });
    }

    @Test
    public void testTableDoesNotExist() throws Exception {
        assertMemoryLeak(() -> assertQuery("show create table foo;")
                .fails(18, "table does not exist"));
    }

    @Test
    public void testTtlOneDay() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1D");
            assertQuery("SHOW CREATE TABLE tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'tango' (\s
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY HOUR TTL 1 DAY BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testTtlOneHour() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 HOUR");
            assertQuery("SHOW CREATE TABLE tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'tango' (\s
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY HOUR TTL 1 HOUR BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testTtlOneMonth() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1M");
            assertQuery("SHOW CREATE TABLE tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'tango' (\s
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY HOUR TTL 1 MONTH BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testTtlOneWeek() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1W");
            assertQuery("SHOW CREATE TABLE tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'tango' (\s
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY HOUR TTL 1 WEEK BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testTtlOneYear() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1Y");
            assertQuery("SHOW CREATE TABLE tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'tango' (\s
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY HOUR TTL 1 YEAR BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testTtlTwoHours() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 2H");
            assertQuery("SHOW CREATE TABLE tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'tango' (\s
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY HOUR TTL 2 HOURS BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testTtlTwoWeeks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 2W");
            assertQuery("SHOW CREATE TABLE tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'tango' (\s
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY HOUR TTL 2 WEEKS BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testTtlTwoYears() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 2Y");
            assertQuery("SHOW CREATE TABLE tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'tango' (\s
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY HOUR TTL 2 YEARS BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testViewRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, v double) timestamp(ts) partition by day wal");
            execute("create view base_view as (select ts, max(v) from base sample by 1h)");
            drainWalAndViewQueues();
            assertExceptionNoLeakCheck(
                    "show create table base_view",
                    18,
                    "table name expected, got view or materialized view name"
            );
        });
    }

    @Test
    public void testViewRejectedQuotedName() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, v double) timestamp(ts) partition by day wal");
            execute("create view 'base view' as (select ts, max(v) from base sample by 1h)");
            drainWalAndViewQueues();
            assertExceptionNoLeakCheck(
                    "show create table 'base view'",
                    18,
                    "table name expected, got view or materialized view name"
            );
        });
    }

    @Test
    public void testViewStillAccessibleViaShowCreateView() throws Exception {
        // a view rejected by SHOW CREATE TABLE must remain reachable via the dedicated statement
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, v double) timestamp(ts) partition by day wal");
            execute("create view base_view as (select ts, max(v) from base sample by 1h)");
            drainWalAndViewQueues();
            printSql("show create view base_view");
            TestUtils.assertContains(sink.toString(), "CREATE VIEW 'base_view'");
        });
    }

    @Test
    public void testWithMaxUncommittedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, s symbol ) " +
                    "with maxUncommittedRows=1234");
            assertQuery("show create table foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            );
                            """);
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
            assertQuery("show create table foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            );
                            """);
        });
    }

    @Test
    public void testWithO3MaxLag() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, s symbol ) " +
                    "with o3MaxLag=1s");
            assertQuery("show create table foo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'foo' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            );
                            """);
        });
    }
}
