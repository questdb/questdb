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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

public class AddIndexTest extends AbstractCairoTest {

    @Test
    public void testAddBitmapIndexOverParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO x VALUES
                    ('a', 1, '2024-01-01'), ('b', 2, '2024-01-01T12:00'),
                    ('a', 3, '2024-01-02'), ('b', 4, '2024-01-02T12:00')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE BITMAP");
            assertQuery("SELECT v, ts FROM x WHERE s = 'a'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            v\tts
                            1\t2024-01-01T00:00:00.000000Z
                            3\t2024-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAddIndexToColumnWithTop() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table trades as (
                                select\s
                                    rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                    rnd_double() price,\s
                                    timestamp_sequence(172800000000, 1) ts\s
                                from long_sequence(1000)
                            ) timestamp(ts) partition by DAY"""
            );
            execute("alter table trades add column sym2 symbol");
            execute("alter table trades alter column sym2 add index");

            assertQuery("trades where sym2 = 'ABB'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("sym\tprice\tts\tsym2\n");
        });
    }

    @Test
    public void testAddIndexToColumnWithTop2() throws Exception {
        assertMemoryLeak(() -> {
            int rowCount = (int) configuration.getDataAppendPageSize() / Integer.BYTES + 1;
            execute(
                    "create table trades as (\n" +
                            "    select \n" +
                            "        rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                            "        rnd_double() price, \n" +
                            "        timestamp_sequence(172800000000, 1) ts \n" +
                            "    from long_sequence(" + rowCount + ")\n" +
                            ") timestamp(ts) partition by DAY"
            );

            execute("alter table trades add column sym2 symbol");
            execute(
                    "insert into trades \n" +
                            "    select \n" +
                            "        rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                            "        rnd_double() price, \n" +
                            "        timestamp_sequence(172800000000 + " + rowCount + ", 1) ts, \n" +
                            "        rnd_symbol('ABB', 'HBC', 'DXR') sym2 \n" +
                            "    from long_sequence(" + rowCount + ")\n"
            );

            execute("alter table trades alter column sym2 add index");
            // While row count is derived from append page size, the expected row count value is hardcoded
            // as a string. Test will fail should append page size change.
            assertQuery("select count(*) from trades where sym2 = 'ABB'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            175654
                            """);
        });
    }

    @Test
    public void testAddIndexToIndexedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table trades as (
                                select\s
                                    rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                    rnd_double() price,\s
                                    timestamp_sequence(172800000000, 36000000) ts\s
                                from long_sequence(10000)
                            ) timestamp(ts) partition by DAY"""
            );
            execute("alter table trades alter column sym add index");

            try {
                execute("alter table trades alter column sym add index");
                Assert.fail();
            } catch (SqlException | CairoException e) {
                Assert.assertEquals(12, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "already indexed");
            }
        });
    }

    @Test
    public void testAlterTableAlterColumnSyntaxError1() throws Exception {
        assertQuery("alter table trades alter columnz")
                .ddl("""
                        create table trades as (
                            select\s
                                rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360) ts\s
                            from long_sequence(30)
                        ) timestamp(ts) partition by DAY""")
                .fails(25, "'column' or 'partition' expected");
    }

    @Test
    public void testAlterTableAlterColumnSyntaxError2() throws Exception {
        assertQuery("alter table trades alter column price add index")
                .ddl("""
                        create table trades as (
                            select\s
                                rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360) ts\s
                            from long_sequence(30)
                        ) timestamp(ts) partition by DAY""")
                .fails(32, "indexes are only supported for symbol type [column=price, type=DOUBLE]");
    }

    @Test
    public void testAlterTableAlterColumnSyntaxError3() throws Exception {
        assertQuery("alter table trades alter column sym add index")
                .ddl("""
                        create table trades as (
                            select\s
                                rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360) ts\s
                            from long_sequence(30)
                        ), index(sym) timestamp(ts) partition by DAY""")
                .fails(12, "column is already indexed [column=sym]");
    }

    @Test
    public void testAlterTableAttachPartitionSyntaxError1() throws Exception {
        assertQuery("alter table trades attach bucket")
                .ddl("""
                        create table trades as (
                            select\s
                                rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360) ts\s
                            from long_sequence(30)
                        ) timestamp(ts) partition by DAY""")
                .fails(26, "'partition' expected");
    }

    @Test
    public void testAlterTableDropColumnSyntaxError1() throws Exception {
        assertQuery("alter table trades drop bucket")
                .ddl("""
                        create table trades as (
                            select\s
                                rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360) ts\s
                            from long_sequence(30)
                        ) timestamp(ts) partition by DAY""")
                .fails(24, "'column' or 'partition' expected");
    }

    @Test
    public void testAlterTableRenameColumnSyntaxError1() throws Exception {
        assertQuery("alter table trades rename bucket")
                .ddl("""
                        create table trades as (
                            select\s
                                rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360) ts\s
                            from long_sequence(30)
                        ) timestamp(ts) partition by DAY""")
                .fails(26, "'column' expected");
    }

    @Test
    public void testBeforeAndAfterIndex() throws Exception {
        final String expected = """
                sym\tprice\tts
                ABB\t0.8043224099968393\t1970-01-03T00:00:00.000000Z
                HBC\t0.6508594025855301\t1970-01-03T00:00:00.001080Z
                HBC\t0.7905675319675964\t1970-01-03T00:00:00.001440Z
                ABB\t0.22452340856088226\t1970-01-03T00:00:00.001800Z
                ABB\t0.3491070363730514\t1970-01-03T00:00:00.002160Z
                ABB\t0.7611029514995744\t1970-01-03T00:00:00.002520Z
                ABB\t0.4217768841969397\t1970-01-03T00:00:00.002880Z
                HBC\t0.0367581207471136\t1970-01-03T00:00:00.003240Z
                HBC\t0.8799634725391621\t1970-01-03T00:00:00.004680Z
                HBC\t0.5249321062686694\t1970-01-03T00:00:00.005040Z
                HBC\t0.1911234617573182\t1970-01-03T00:00:00.006480Z
                ABB\t0.5793466326862211\t1970-01-03T00:00:00.006840Z
                ABB\t0.42281342727402726\t1970-01-03T00:00:00.008280Z
                HBC\t0.810161274171258\t1970-01-03T00:00:00.008640Z
                HBC\t0.022965637512889825\t1970-01-03T00:00:00.009360Z
                ABB\t0.7763904674818695\t1970-01-03T00:00:00.009720Z
                HBC\t0.0011075361080621349\t1970-01-03T00:00:00.010440Z
                """;

        assertQuery("select * from trades where (sym = 'ABB' or sym = 'HBC')")
                .ddl("""
                        create table trades as (
                            select\s
                                rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360) ts\s
                            from long_sequence(30)
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("alter table trades alter column sym add index")
                .timestamp("ts")
                .returns(expected, expected);
    }

    @Test
    public void testFailedAddIndexKeepsColumnOfNonPartitionedTable() throws Exception {
        assertFailedAddIndexKeepsColumn("BITMAP", "Index forward scan on: s", false);
    }

    @Test
    public void testFailedCoveringAddIndexKeepsColumnOfNonPartitionedTable() throws Exception {
        assertFailedAddIndexKeepsColumn("POSTING INCLUDE (v)", "CoveringIndex on: s", true);
    }

    @Test
    public void testFailedPostingAddIndexKeepsColumnOfNonPartitionedTable() throws Exception {
        assertFailedAddIndexKeepsColumn("POSTING", "Index forward scan on: s", false);
    }

    @Test
    public void testFailedPostingAddIndexRemovesItsFilesFromEveryPartition() throws Exception {
        final SymbolReadFailingFacade ff = new SymbolReadFailingFacade("2024-01-03", "s.d");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO x VALUES
                    ('a', 1, '2024-01-01'), ('b', 2, '2024-01-01T12:00'),
                    ('a', 3, '2024-01-02'), ('b', 4, '2024-01-02T12:00'),
                    ('a', 5, '2024-01-03'), ('b', 6, '2024-01-03T12:00')
                    """);
            // The build indexes 2024-01-01 and 2024-01-02, then fails reading the active partition's column.
            assertAddIndexFails(ff, "POSTING INCLUDE (v)");
            for (int day = 1; day <= 3; day++) {
                assertColumnFiles("2024-01-0" + day, "[s.d]");
            }
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            assertQuery("SELECT v, ts FROM x WHERE s = 'a'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex on: s")
                    .returns("""
                            v\tts
                            1\t2024-01-01T00:00:00.000000Z
                            3\t2024-01-02T00:00:00.000000Z
                            5\t2024-01-03T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFailedTypeChangeIndexBuildRemovesConvertedColumnOfNonPartitionedTable() throws Exception {
        assertFailedTypeChangeRemovesConvertedColumn("", "default", "default");
    }

    @Test
    public void testFailedTypeChangeIndexBuildRemovesConvertedColumnOfPartitionedTable() throws Exception {
        // The build indexes 2024-01-01 and 2024-01-02, then fails reading the active partition's converted column.
        assertFailedTypeChangeRemovesConvertedColumn(
                "PARTITION BY DAY BYPASS WAL",
                "2024-01-03",
                "2024-01-01", "2024-01-02", "2024-01-03"
        );
    }

    private static void assertAddIndexFails(SymbolReadFailingFacade ff, String indexClause) throws Exception {
        ff.isFailing = true;
        try {
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE " + indexClause);
            Assert.fail("the index build must fail");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "could not read symbol column during indexing");
        } finally {
            ff.isFailing = false;
        }
    }

    // Lists the files of column s in a partition directory of table x, or in its table directory for an empty name.
    private static void assertColumnFiles(String partitionName, String expected) {
        File dir = new File(configuration.getDbRoot(), engine.verifyTableName("x").getDirName());
        if (!partitionName.isEmpty()) {
            dir = new File(dir, partitionName);
        }
        final String[] files = dir.list((_, name) -> name.startsWith("s."));
        Assert.assertNotNull(files);
        Arrays.sort(files);
        Assert.assertEquals(partitionName, expected, Arrays.toString(files));
    }

    private void assertFailedAddIndexKeepsColumn(String indexClause, String plan, boolean isCovering) throws Exception {
        final SymbolReadFailingFacade ff = new SymbolReadFailingFacade("default", "s.d");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG)");
            execute("INSERT INTO x VALUES ('a', 1), ('b', 2), ('a', 3)");
            assertAddIndexFails(ff, indexClause);
            assertColumnFiles("default", "[s.d]");
            assertQuery("SELECT s, v FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tv
                            a\t1
                            b\t2
                            a\t3
                            """);
            execute("INSERT INTO x VALUES ('c', 4)");
            assertQuery("SELECT s, v FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tv
                            a\t1
                            b\t2
                            a\t3
                            c\t4
                            """);
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE " + indexClause);
            assertQuery("SELECT v FROM x WHERE s = 'a'")
                    .noLeakCheck()
                    .supportsRandomAccess(!isCovering)
                    .expectSize(isCovering)
                    .withPlanContaining(plan)
                    .returns("""
                            v
                            1
                            3
                            """);
            assertQuery("SELECT v FROM x WHERE s = 'b'")
                    .noLeakCheck()
                    .supportsRandomAccess(!isCovering)
                    .expectSize(isCovering)
                    .withPlanContaining(plan)
                    .returns("""
                            v
                            2
                            """);
        });
    }

    // ALTER COLUMN TYPE builds the index of the column it converts s to. A failed build commits nothing, and the next
    // writer open restores the _meta the change swapped: no version of the table names the converted column, so its
    // files go with the index's, and s keeps its type and rows.
    private void assertFailedTypeChangeRemovesConvertedColumn(String partitionClause, String failingPartition, String... partitions) throws Exception {
        final SymbolReadFailingFacade ff = new SymbolReadFailingFacade(failingPartition, "s.d.");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (s VARCHAR, v LONG, ts TIMESTAMP) TIMESTAMP(ts) " + partitionClause);
            execute("INSERT INTO x VALUES ('a', 1, '2024-01-01'), ('b', 2, '2024-01-02'), ('a', 3, '2024-01-03')");
            ff.isFailing = true;
            try {
                execute("ALTER TABLE x ALTER COLUMN s TYPE SYMBOL INDEX TYPE POSTING");
                Assert.fail("the index build must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not read symbol column during indexing");
            } finally {
                ff.isFailing = false;
            }
            // The partitions hold the files of the VARCHAR column only, and the table directory no symbol map of s.
            for (String partition : partitions) {
                assertColumnFiles(partition, "[s.d, s.i]");
            }
            assertColumnFiles("", "[]");

            engine.releaseAllWriters();
            execute("INSERT INTO x VALUES ('c', 4, '2024-01-03T12:00')");
            try (TableReader reader = getReader("x")) {
                Assert.assertEquals(ColumnType.VARCHAR, reader.getMetadata().getColumnType(reader.getMetadata().getColumnIndex("s")));
            }
            assertQuery("SELECT s, v FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tv
                            a\t1
                            b\t2
                            a\t3
                            c\t4
                            """);
            execute("ALTER TABLE x ALTER COLUMN s TYPE SYMBOL INDEX TYPE POSTING");
            assertQuery("SELECT v FROM x WHERE s = 'a'")
                    .noLeakCheck()
                    .withPlanContaining("Index forward scan on: s")
                    .returns("""
                            v
                            1
                            3
                            """);
        });
    }

    // Fails every read of one partition's symbol column file while armed: an I/O error in the middle of an index
    // build.
    private static class SymbolReadFailingFacade extends TestFilesFacadeImpl {
        private final String fileNamePrefix;
        private final String partitionName;
        private volatile boolean isFailing;
        private volatile long symbolFd = -1;

        private SymbolReadFailingFacade(String partitionName, String fileNamePrefix) {
            this.partitionName = partitionName;
            this.fileNamePrefix = fileNamePrefix;
        }

        @Override
        public boolean close(long fd) {
            if (fd == symbolFd) {
                symbolFd = -1;
            }
            return super.close(fd);
        }

        @Override
        public long openRO(LPSZ name) {
            return trackSymbolFd(name, super.openRO(name));
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            return trackSymbolFd(name, super.openRW(name, opts));
        }

        @Override
        public long read(long fd, long buf, long len, long offset) {
            if (isFailing && fd == symbolFd) {
                return -1;
            }
            return super.read(fd, buf, len, offset);
        }

        private long trackSymbolFd(LPSZ name, long fd) {
            if (fd > -1 && Utf8s.containsAscii(name, partitionName + Files.SEPARATOR + fileNamePrefix)) {
                symbolFd = fd;
            }
            return fd;
        }
    }
}
