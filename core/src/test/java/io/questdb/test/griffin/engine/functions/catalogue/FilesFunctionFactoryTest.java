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

package io.questdb.test.griffin.engine.functions.catalogue;

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
 * Unit tests for the files() function and FilesFunctionFactory.
 * Tests various aspects of file listing and directory traversal functionality.
 */
public class FilesFunctionFactoryTest extends AbstractCairoTest {
    private static String testRoot;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        testRoot = TestUtils.unchecked(() -> temp.newFolder("files_test").getAbsolutePath());
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_ROOT, testRoot);
        AbstractCairoTest.setUpStatic();
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        setupTestFiles();
    }

    /**
     * Test that files() function returns all required columns.
     * Verifies the column structure: path, diskSize, diskSizeHuman, modifiedTime
     */
    @Test
    public void testFilesColumnsExist() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        path\tdiskSize\tdiskSizeHuman
                        test.txt\t100\t100.0 B
                        """,
                "select path, diskSize, diskSizeHuman from files('" + testRoot + "') where path = 'test.txt'"
        ));
    }

    /**
     * Test files() with empty directory result.
     * When glob pattern matches no files, returns empty result set.
     */
    @Test
    public void testFilesEmptyResult() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\tdiskSize\tdiskSizeHuman\tmodifiedTime\n",
                "select path, diskSize, diskSizeHuman, modifiedTime from files('" + testRoot + "') where path like '%.nonexistent'"
        ));
    }

    /**
     * Test that files() correctly reports file sizes.
     * Verifies both the numeric diskSize and the human-readable diskSizeHuman columns.
     */
    @Test
    public void testFilesSizesAccurate() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        path\tdiskSize\tdiskSizeHuman
                        large.bin\t1048576\t1.0 MiB
                        test.txt\t100\t100.0 B
                        """,
                "select path, diskSize, diskSizeHuman from files('" + testRoot + "') where path in ('test.txt', 'large.bin') order by path"
        ));
    }

    /**
     * Test files() with subdirectory traversal.
     * Verifies that files in nested directories are correctly discovered.
     */
    @Test
    public void testFilesNestedDirectories() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\tdiskSize\n" +
                        "subdir" + File.separator + "nested.txt\t50\n",
                "select path, diskSize from files('" + testRoot + "') where path like 'subdir%' order by path"
        ));
    }

    /**
     * Test files() with COUNT aggregation.
     * Verifies that files() works correctly with SQL aggregation functions.
     */
    @Test
    public void testFilesCount() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        count
                        3
                        """,
                "select count(*) from files('" + testRoot + "')"
        ));
    }

    /**
     * Test files() with SUM aggregation on diskSize.
     * Verifies that numeric columns can be aggregated.
     */
    @Test
    public void testFilesSumDiskSize() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        sum
                        1048726
                        """,
                "select sum(diskSize) from files('" + testRoot + "')"
        ));
    }

    /**
     * Test files() with GROUP BY operation.
     * Tests grouping by diskSize values.
     */
    @Test
    public void testFilesGroupBy() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        diskSize\tcount
                        50\t1
                        100\t1
                        1048576\t1
                        """,
                "select diskSize, count(*) as count from files('" + testRoot + "') group by diskSize order by diskSize"
        ));
    }

    /**
     * Test files() with DISTINCT operation.
     * Verifies that DISTINCT works on file paths.
     */
    @Test
    public void testFilesDistinct() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\n" +
                        "large.bin\n" +
                        "subdir" + File.separator + "nested.txt\n" +
                        "test.txt\n",
                "select distinct path from files('" + testRoot + "') order by path"
        ));
    }

    /**
     * Test files() result ordering consistency.
     * Verifies that results can be sorted by different columns.
     */
    @Test
    public void testFilesOrderByPath() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\n" +
                        "large.bin\n" +
                        "subdir" + File.separator + "nested.txt\n" +
                        "test.txt\n",
                "select path from files('" + testRoot + "') order by path"
        ));
    }

    /**
     * Test files() with WHERE clause on diskSize.
     * Filters files by size.
     */
    @Test
    public void testFilesWhereSize() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        path\tdiskSize
                        test.txt\t100
                        """,
                "select path, diskSize from files('" + testRoot + "') where diskSize > 50 and diskSize < 1000 order by path"
        ));
    }

    /**
     * Test files() function with non-existent path should return empty.
     * Verifies graceful handling of missing directories.
     */
    @Test
    public void testFilesNonexistentPath() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\tdiskSize\tdiskSizeHuman\tmodifiedTime\n",
                "select path, diskSize, diskSizeHuman, modifiedTime from files('" + testRoot + "/nonexistent')"
        ));
    }

    /**
     * Test files() with LIMIT and ORDER BY combined.
     * Verifies pagination support.
     */
    @Test
    public void testFilesLimitOrdering() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        path\tdiskSize
                        large.bin\t1048576
                        test.txt\t100
                        """,
                "select path, diskSize from files('" + testRoot + "') order by diskSize desc limit 2"
        ));
    }

    /**
     * Test that modifiedTime column contains valid timestamp values.
     * Verifies that dates can be selected and are non-null.
     */
    @Test
    public void testFilesLastModifiedColumn() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "path\n" +
                        "large.bin\n" +
                        "subdir" + File.separator + "nested.txt\n" +
                        "test.txt\n",
                "select path from files('" + testRoot + "') where modifiedTime is not null order by path"
        ));
    }

    /**
     * Test files() with WHERE clause filtering on path pattern.
     * Verifies pattern matching functionality.
     */
    @Test
    public void testFilesWherePathPattern() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        path
                        test.txt
                        """,
                "select path from files('" + testRoot + "') where path like 'test%' order by path"
        ));
    }

    private void createTestFile(String relativePath, int size) {
        try (Path path = new Path()) {
            path.of(testRoot).concat(relativePath);
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

    private void setupTestFiles() {
        FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(testRoot).$();
            if (ff.exists(path.$())) {
                ff.rmdir(path);
            }
            ff.mkdir(path.$(), 493);

            // Create test files with different sizes
            createTestFile("test.txt", 100);
            createTestFile("large.bin", 1048576); // 1 MB

            // Create nested directory with file
            ff.mkdir(path.of(testRoot).concat("subdir").$(), 493);
            createTestFile("subdir" + File.separator + "nested.txt", 50);
        }
    }
}
