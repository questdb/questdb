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

package io.questdb.test.cairo.idx;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The one-shot POSTING index builds, ALTER TABLE ADD INDEX, a column type change to an indexed SYMBOL and REINDEX,
 * index a partition into a first value file, then seal it into a new one. Nothing reads the first generation, so the
 * build leaves only the sealed generation in each partition directory: one value file, and only the cover files of
 * that generation.
 */
@RunWith(Parameterized.class)
public class PostingIndexBuildFilesTest extends AbstractCairoTest {
    private static final Pattern COVER_FILE = Pattern.compile("k\\.pc[0-9]+\\..*\\.([0-9]+)");
    private static final long DAY = 86_400_000_000L;
    private static final int DAYS = 3;
    // k.pv.<sealTxn>, or k.pv.<columnNameTxn>.<sealTxn> for a column that has a name txn
    private static final Pattern VALUE_FILE = Pattern.compile("k\\.pv\\.(?:[0-9]+\\.)?([0-9]+)");
    private final boolean isCovering;

    public PostingIndexBuildFilesTest(boolean isCovering) {
        this.isCovering = isCovering;
    }

    @Parameterized.Parameters(name = "INCLUDE={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    @Test
    public void testAddIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTable(false, "BYPASS WAL");
            execute("ALTER TABLE t ALTER COLUMN k ADD INDEX TYPE POSTING" + includeClause());
            assertOneGenerationPerPartition(DAYS);
            assertIndexMatchesScan();
        });
    }

    @Test
    public void testAddIndexOverParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTable(false, "BYPASS WAL");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '1970-01-01', '1970-01-02'");
            execute("ALTER TABLE t ALTER COLUMN k ADD INDEX TYPE POSTING" + includeClause());
            assertOneGenerationPerPartition(DAYS);
            assertIndexMatchesScan();
        });
    }

    @Test
    public void testAddIndexWal() throws Exception {
        assertMemoryLeak(() -> {
            createTable(false, "WAL");
            execute("ALTER TABLE t ALTER COLUMN k ADD INDEX TYPE POSTING" + includeClause());
            drainWalQueue();
            assertOneGenerationPerPartition(DAYS);
            assertIndexMatchesScan();
        });
    }

    @Test
    public void testAlterColumnTypeWal() throws Exception {
        Assume.assumeFalse("a column type change takes no INCLUDE columns", isCovering);
        assertMemoryLeak(() -> {
            createTable("VARCHAR", "WAL");
            execute("ALTER TABLE t ALTER COLUMN k TYPE SYMBOL INDEX TYPE POSTING");
            drainWalQueue();
            assertOneGenerationPerPartition(DAYS);
            assertIndexMatchesScan();
        });
    }

    // The parquet rewrite reindexes every indexed column of a parquet partition into a new directory version.
    @Test
    public void testForceReindexParquet() throws Exception {
        // Under ADAPTIVE the rewrite's seal waits for the save point, and the purge deletes the superseded generation.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            createTable(true, "BYPASS WAL");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '1970-01-01', '1970-01-02'");
            final List<Path> parquetPartitions = partitionDirs();
            execute("ALTER TABLE t FORCE REINDEX PARQUET");
            final List<Path> partitions = partitionDirs();
            for (int i = 0; i < 2; i++) {
                Assert.assertNotEquals("the reindex wrote a new directory version", parquetPartitions.get(i), partitions.get(i));
            }
            assertOneGenerationPerPartition(DAYS);
            assertIndexMatchesScan();
        });
    }

    @Test
    public void testReindex() throws Exception {
        assertMemoryLeak(() -> {
            createTable(true, "BYPASS WAL");
            engine.releaseAllWriters();
            execute("REINDEX TABLE t COLUMN k LOCK EXCLUSIVE");
            assertOneGenerationPerPartition(DAYS);
            assertIndexMatchesScan();
        });
    }

    private void assertIndexMatchesScan() throws Exception {
        for (int k = 0; k < 4; k++) {
            assertSqlCursors(
                    "SELECT ts, k, c0, c1, c2 FROM t WHERE k::STRING = 'k" + k + "'",
                    "SELECT ts, k, c0, c1, c2 FROM t WHERE k = 'k" + k + "'"
            );
        }
    }

    private void assertOneGenerationPerPartition(int partitionCount) throws IOException {
        final List<Path> partitions = partitionDirs();
        Assert.assertEquals(partitions.toString(), partitionCount, partitions.size());
        for (Path partition : partitions) {
            final List<String> valueFiles = new ArrayList<>();
            final List<String> coverFiles = new ArrayList<>();
            try (Stream<Path> files = Files.list(partition)) {
                files.map(p -> p.getFileName().toString()).sorted().forEach(name -> {
                    if (VALUE_FILE.matcher(name).matches()) {
                        valueFiles.add(name);
                    } else if (COVER_FILE.matcher(name).matches()) {
                        coverFiles.add(name);
                    }
                });
            }
            final String where = partition.getFileName() + ": " + valueFiles + " " + coverFiles;
            Assert.assertEquals("value files in " + where, 1, valueFiles.size());
            final Matcher valueFile = VALUE_FILE.matcher(valueFiles.get(0));
            Assert.assertTrue(valueFile.matches());
            final String sealTxn = valueFile.group(1);
            // the three INCLUDE columns and the designated timestamp (cairo.posting.index.auto.include.timestamp)
            Assert.assertEquals("cover files in " + where, isCovering ? 4 : 0, coverFiles.size());
            for (String coverFile : coverFiles) {
                final Matcher matcher = COVER_FILE.matcher(coverFile);
                Assert.assertTrue(matcher.matches());
                Assert.assertEquals("generation of a cover file in " + where, sealTxn, matcher.group(1));
            }
        }
    }

    private void createTable(boolean isIndexed, String walClause) throws Exception {
        createTable(isIndexed ? "SYMBOL INDEX TYPE POSTING" + includeClause() : "SYMBOL", walClause);
    }

    // 4 symbols over DAYS day partitions, in a column k of the given type
    private void createTable(String kType, String walClause) throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, k " + kType + ", c0 LONG, c1 LONG, c2 VARCHAR) TIMESTAMP(ts) PARTITION BY DAY "
                + walClause);
        final StringBuilder sql = new StringBuilder("INSERT INTO t VALUES ");
        for (int v = 0; v < 40; v++) {
            final long ts = (v % DAYS) * DAY + (v + 1) * 1_000_000L;
            sql.append(v == 0 ? "" : ", ").append('(').append(ts).append("::TIMESTAMP, 'k").append(v % 4).append("', ")
                    .append(v).append(", ").append(-v).append(", 'v").append(v).append("')");
        }
        execute(sql);
        drainWalQueue();
    }

    private String includeClause() {
        return isCovering ? " INCLUDE (c0, c1, c2)" : "";
    }

    // The partition directories of t, in partition order.
    private List<Path> partitionDirs() throws IOException {
        final TableToken token = engine.verifyTableName("t");
        final List<Path> partitions = new ArrayList<>();
        try (Stream<Path> paths = Files.list(Paths.get(root, token.getDirName()))) {
            paths.filter(p -> Files.isDirectory(p) && p.getFileName().toString().startsWith("1970-")).sorted().forEach(partitions::add);
        }
        return partitions;
    }
}
