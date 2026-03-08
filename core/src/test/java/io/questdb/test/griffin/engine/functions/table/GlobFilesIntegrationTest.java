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

package io.questdb.test.griffin.engine.functions.table;

import io.questdb.PropertyKey;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

/**
 * Integration tests for the glob() pseudo-table function.
 * Tests both the glob() function in pseudo-table position and glob() as a filter.
 */
public class GlobFilesIntegrationTest extends AbstractCairoTest {
    @BeforeClass
    public static void setUpStatic() throws Exception {
        inputRoot = TestUtils.unchecked(() -> temp.newFolder("glob_test").getAbsolutePath());
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_ROOT, inputRoot);
        AbstractCairoTest.setUpStatic();
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        setupTestFiles();
    }

    /**
     * Test files() with ORDER BY diskSize DESC and LIMIT - gets largest files.
     * SELECT * FROM files('/root') ORDER BY diskSize DESC, path LIMIT 3
     */
    @Test
    public void testFilesOrderByDiskSizeDescLimit() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\tdiskSize\tdiskSizeHuman\tmodifiedTime\n" +
                        "reports" + File.separator + "2023_report.csv\t15076\t14.7 KiB\t1970-04-26T17:46:55.076Z\n" +
                        "reports" + File.separator + "monthly_report.csv\t15076\t14.7 KiB\t1970-04-26T17:46:55.076Z\n" +
                        "test.csv\t15076\t14.7 KiB\t1970-04-26T17:46:55.076Z\n",
                "select path, diskSize, diskSizeHuman, modifiedTime from files('" + inputRoot + "') order by diskSize desc, path limit 3"
        ));
    }

    /**
     * Test files() with ORDER BY modifiedTime and LIMIT.
     * SELECT * FROM files('/root') ORDER BY modifiedTime LIMIT 2
     */
    @Test
    public void testFilesOrderByModifiedTimeLimit() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\tdiskSize\tdiskSizeHuman\tmodifiedTime\n" +
                        "temp" + File.separator + "backup.sql\t45\t45.0 B\t1970-04-26T17:46:40.045Z\n" +
                        "data" + File.separator + "file1.parquet\t256\t256.0 B\t1970-04-26T17:46:40.256Z\n",
                "select path, diskSize, diskSizeHuman, modifiedTime from files('" + inputRoot + "') order by modifiedTime, path limit 2"
        ));
    }

    /**
     * Test files() with multiple columns in ORDER BY and LIMIT.
     * SELECT * FROM files('/root') ORDER BY diskSize, path LIMIT 4
     */
    @Test
    public void testFilesOrderByMultipleColumnsLimit() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\tdiskSize\tdiskSizeHuman\tmodifiedTime\n" +
                        "temp" + File.separator + "backup.sql\t45\t45.0 B\t1970-04-26T17:46:40.045Z\n" +
                        "data" + File.separator + "file1.parquet\t256\t256.0 B\t1970-04-26T17:46:40.256Z\n" +
                        "data" + File.separator + "file2.parquet\t256\t256.0 B\t1970-04-26T17:46:40.256Z\n" +
                        "data" + File.separator + "nested" + File.separator + "deep_file.parquet\t256\t256.0 B\t1970-04-26T17:46:40.256Z\n",
                "select path, diskSize, diskSizeHuman, modifiedTime from files('" + inputRoot + "') order by diskSize, path limit 4"
        ));
    }

    /**
     * Test files() with LIMIT clause - should return only the first N results.
     * SELECT * FROM files('/root') LIMIT 2
     */
    @Test
    public void testFilesWithLimit() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\tdiskSize\tdiskSizeHuman\tmodifiedTime\n" +
                        "data" + File.separator + "file1.parquet\t256\t256.0 B\t1970-04-26T17:46:40.256Z\n" +
                        "data" + File.separator + "file2.parquet\t256\t256.0 B\t1970-04-26T17:46:40.256Z\n",
                "select path, diskSize, diskSizeHuman, modifiedTime from files('" + inputRoot + "') order by path limit 2"
        ));
    }

    /**
     * Test glob() filter with case-sensitive extension matching.
     * SELECT * FROM files('/root') WHERE glob(path, '*.PARQUET')
     */
    @Test
    public void testGlobFilterCaseSensitive() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\tdiskSize\tdiskSizeHuman\tmodifiedTime\n",
                "select path, diskSize, diskSizeHuman, modifiedTime from files('" + inputRoot + "') where glob(path, '*.PARQUET') order by path"
        ));
    }

    /**
     * Test glob() with ORDER BY path DESC and LIMIT.
     * Demonstrates ordering by path in reverse and limiting results.
     * SELECT * FROM files('/root') WHERE glob(path, '*.csv') ORDER BY path DESC LIMIT 2
     */
    @Test
    public void testGlobFilterOrderByPathDescLimit() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\tdiskSize\tdiskSizeHuman\tmodifiedTime\n" +
                        "test.csv\t15076\t14.7 KiB\t1970-04-26T17:46:55.076Z\n" +
                        "reports" + File.separator + "monthly_report.csv\t15076\t14.7 KiB\t1970-04-26T17:46:55.076Z\n",
                "select path, diskSize, diskSizeHuman, modifiedTime from files('" + inputRoot + "') where glob(path, '*.csv') order by path desc limit 2"
        ));
    }

    /**
     * Test glob() filter with parquet extension.
     * SELECT * FROM files('/root') WHERE glob(path, '*.parquet')
     */
    @Test
    public void testGlobFilterParquetOnly() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\tdiskSize\tdiskSizeHuman\tmodifiedTime\n" +
                        "data" + File.separator + "file1.parquet\t256\t256.0 B\t1970-04-26T17:46:40.256Z\n" +
                        "data" + File.separator + "file2.parquet\t256\t256.0 B\t1970-04-26T17:46:40.256Z\n" +
                        "data" + File.separator + "nested" + File.separator + "deep_file.parquet\t256\t256.0 B\t1970-04-26T17:46:40.256Z\n" +
                        "reports" + File.separator + "2024_report.parquet\t256\t256.0 B\t1970-04-26T17:46:40.256Z\n",
                "select path, diskSize, diskSizeHuman, modifiedTime from files('" + inputRoot + "') where glob(path, '*.parquet') order by path"
        ));
    }

    /**
     * Test glob() filter with sql extension.
     * SELECT * FROM files('/root') WHERE glob(path, '*.sql')
     */
    @Test
    public void testGlobFilterSqlOnly() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\tdiskSize\tdiskSizeHuman\tmodifiedTime\n" +
                        "temp" + File.separator + "backup.sql\t45\t45.0 B\t1970-04-26T17:46:40.045Z\n",
                "select path, diskSize, diskSizeHuman, modifiedTime from files('" + inputRoot + "') where glob(path, '*.sql') order by path"
        ));
    }

    /**
     * Test glob() with LIMIT clause combined with WHERE filter.
     * SELECT * FROM files('/root') WHERE glob(path, '*.parquet') LIMIT 2
     */
    @Test
    public void testGlobFilterWithLimit() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\tdiskSize\tdiskSizeHuman\tmodifiedTime\n" +
                        "data" + File.separator + "file1.parquet\t256\t256.0 B\t1970-04-26T17:46:40.256Z\n" +
                        "data" + File.separator + "file2.parquet\t256\t256.0 B\t1970-04-26T17:46:40.256Z\n",
                "select path, diskSize, diskSizeHuman, modifiedTime from files('" + inputRoot + "') where glob(path, '*.parquet') order by path limit 2"
        ));
    }

    /**
     * Test glob() with files() function in WHERE clause - matching by extension.
     * SELECT * FROM files('/root') WHERE glob(path, '*.csv')
     */
    @Test
    public void testGlobInFilterClause() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\tdiskSize\tdiskSizeHuman\tmodifiedTime\n" +
                        "reports" + File.separator + "2023_report.csv\t15076\t14.7 KiB\t1970-04-26T17:46:55.076Z\n" +
                        "reports" + File.separator + "monthly_report.csv\t15076\t14.7 KiB\t1970-04-26T17:46:55.076Z\n" +
                        "test.csv\t15076\t14.7 KiB\t1970-04-26T17:46:55.076Z\n",
                "select path, diskSize, diskSizeHuman, modifiedTime from files('" + inputRoot + "') where glob(path, '*.csv') order by path"
        ));
    }

    /**
     * Test glob() matching nested paths with LIMIT.
     * SELECT * FROM files('/root') WHERE glob(path, 'data/*') LIMIT 3
     */
    @Test
    public void testGlobNestedPathWithLimit() throws Exception {
        assertMemoryLeak(() ->
                {
                    String expected = """
                            path\tdiskSize\tdiskSizeHuman
                            data/file1.parquet\t256\t256.0 B
                            data/file2.parquet\t256\t256.0 B
                            data/nested/deep_file.parquet\t256\t256.0 B
                            """.replace("/", File.separator);

                    String query = "select path, diskSize, diskSizeHuman from files('" + inputRoot + "') where glob(path, 'data/*') order by path limit 3"
                            .replace("/", File.separator);

                    assertSql(expected, query);
                }
        );
    }

    private void createTestFile(String relativePath) {
        createTestFile(relativePath, getFileSize(relativePath));
    }

    private void createTestFile(String relativePath, int size) {
        try (Path path = new Path()) {
            path.of(inputRoot).concat(relativePath);
            long fd = Files.openRW(path.$());
            if (fd > -1) {
                try {
                    long mem = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
                    try {
                        for (int i = 0; i < size; i++) {
                            Unsafe.getUnsafe().putByte(mem + i, (byte) ('A' + (i % 26)));
                        }
                        Files.write(fd, mem, size, 0);
                    } finally {
                        Unsafe.free(mem, size, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Files.close(fd);
                }
            }
            Files.setLastModified(path.$(), 10000000000L + size);
        }
    }

    private int getFileSize(String relativePath) {
        if (relativePath.endsWith(".csv")) return 15076;
        if (relativePath.endsWith(".parquet")) return 256;
        if (relativePath.endsWith(".json")) return 1234890;
        if (relativePath.endsWith(".sql")) return 45;
        if (relativePath.endsWith(".txt")) return 1289;
        return 100;
    }

    private void setupTestFiles() {
        FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(inputRoot).$();
            if (ff.exists(path.$())) {
                ff.rmdir(path);
            }
            ff.mkdir(path.$(), 493);

            createTestFile("test.csv");

            // data directory
            ff.mkdir(path.of(inputRoot).concat("data").$(), 493);
            ff.mkdir(path.of(inputRoot).concat("data").concat("nested").$(), 493);
            createTestFile("data" + File.separator + "file1.parquet");
            createTestFile("data" + File.separator + "file2.parquet");
            createTestFile("data" + File.separator + "nested" + File.separator + "deep_file.parquet");

            // reports directory
            ff.mkdir(path.of(inputRoot).concat("reports").$(), 493);
            createTestFile("reports" + File.separator + "monthly_report.csv");
            createTestFile("reports" + File.separator + "2023_report.csv");
            createTestFile("reports" + File.separator + "2024_report.parquet");

            // temp directory
            ff.mkdir(path.of(inputRoot).concat("temp").$(), 493);
            createTestFile("temp" + File.separator + "backup.sql");
        }
    }
}