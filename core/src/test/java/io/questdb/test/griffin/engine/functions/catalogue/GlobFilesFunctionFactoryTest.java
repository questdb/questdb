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
import io.questdb.griffin.engine.functions.catalogue.GlobFilesFunctionFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8StringList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;

public class GlobFilesFunctionFactoryTest extends AbstractCairoTest {

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

    @Test
    public void testFuzzGlobMatch() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        String chars = "abcdefghijklmnopqrstuvwxyz0123456789_.";
        StringSink nameSink = new StringSink();
        StringSink patternSink = new StringSink();

        for (int iter = 0; iter < 1000; iter++) {
            // Generate random filename (3-15 chars)
            int nameLen = 3 + rnd.nextInt(13);
            nameSink.clear();
            for (int i = 0; i < nameLen; i++) {
                nameSink.put(chars.charAt(rnd.nextInt(chars.length())));
            }
            String name = nameSink.toString();

            // Test 1: exact pattern should always match
            assertGlobMatch(name, name, true);

            // Test 2: * should match everything
            assertGlobMatch(name, "*", true);

            // Test 3: pattern with single ? per char should match
            patternSink.clear();
            for (int i = 0; i < nameLen; i++) {
                patternSink.put('?');
            }
            assertGlobMatch(name, patternSink.toString(), true);

            // Test 4: wrong length ? pattern should not match
            if (nameLen > 1) {
                assertGlobMatch(name, patternSink.subSequence(1, patternSink.length()).toString(), false);
            }

            // Test 5: prefix* should match
            if (nameLen > 2) {
                int prefixLen = 1 + rnd.nextInt(nameLen - 1);
                patternSink.clear();
                patternSink.put(name, 0, prefixLen).put('*');
                assertGlobMatch(name, patternSink.toString(), true);
            }

            // Test 6: *suffix should match
            if (nameLen > 2) {
                int suffixStart = 1 + rnd.nextInt(nameLen - 1);
                patternSink.clear();
                patternSink.put('*').put(name, suffixStart, nameLen);
                assertGlobMatch(name, patternSink.toString(), true);
            }

            // Test 7: [abc] bracket matching
            char firstChar = name.charAt(0);
            patternSink.clear();
            patternSink.put('[').put(firstChar).put(']');
            if (nameLen > 1) {
                patternSink.put('*');
            }
            assertGlobMatch(name, patternSink.toString(), true);

            // Negated bracket should not match
            patternSink.clear();
            patternSink.put("[!").put(firstChar).put(']');
            if (nameLen > 1) {
                patternSink.put('*');
            }
            assertGlobMatch(name, patternSink.toString(), false);
        }
    }

    @Test
    public void testFuzzRandomDirectoryStructure() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        FilesFacade ff = configuration.getFilesFacade();
        HashSet<String> createdFiles = new HashSet<>();

        try (Path path = new Path()) {
            String fuzzRoot = inputRoot + File.separator + "fuzz";
            ff.mkdir(path.of(fuzzRoot).$(), 493);
            String[] extensions = {".parquet", ".csv", ".txt", ".log"};
            String[] prefixes = {"data", "file", "test", "report"};

            for (int i = 0; i < 50; i++) {
                String prefix = prefixes[rnd.nextInt(prefixes.length)];
                String ext = extensions[rnd.nextInt(extensions.length)];
                int num = rnd.nextInt(100);
                String fileName = prefix + "_" + num + ext;
                String relativePath = "fuzz" + File.separator + fileName;

                if (!createdFiles.contains(relativePath)) {
                    createTestFile(relativePath, 100 + rnd.nextInt(900));
                    createdFiles.add(relativePath);
                }
            }

            StringSink dirPath = new StringSink();
            StringSink relativePath = new StringSink();
            for (int depth = 1; depth <= 3; depth++) {
                dirPath.clear();
                dirPath.put("fuzz");
                for (int d = 0; d < depth; d++) {
                    dirPath.put(File.separator).put("level").put(d);
                }
                ff.mkdir(path.of(inputRoot).concat(dirPath).$(), 493);

                for (int i = 0; i < 5; i++) {
                    String ext = extensions[rnd.nextInt(extensions.length)];
                    relativePath.clear();
                    relativePath.put(dirPath).put(File.separator).put("nested_").put(depth).put('_').put(i).put(ext);
                    createTestFile(relativePath.toString(), 50 + rnd.nextInt(200));
                    createdFiles.add(relativePath.toString());
                }
            }

            try (
                    DirectUtf8StringList resultFiles = new DirectUtf8StringList(256, 16);
                    DirectUtf8StringList tempPaths = new DirectUtf8StringList(256, 16);
                    Path workingPath = new Path(MemoryTag.NATIVE_PATH)
            ) {
                IntList offsets = new IntList();
                Utf8StringSink fileNameSink = new Utf8StringSink();

                // Test 1: *.parquet should find all parquet files in fuzz/
                offsets.clear();
                Utf8String pattern1 = new Utf8String("fuzz/*.parquet");
                GlobFilesFunctionFactory.parseGlobPattern(pattern1, offsets);
                GlobFilesFunctionFactory.globFiles(ff, pattern1, workingPath, inputRoot, fileNameSink, resultFiles, tempPaths, offsets);
                int parquetCount = 0;
                for (String f : createdFiles) {
                    if (f.startsWith("fuzz" + File.separator) && f.endsWith(".parquet") && !f.contains("level")) {
                        parquetCount++;
                    }
                }
                Assert.assertEquals("*.parquet count mismatch", parquetCount, resultFiles.size());

                // Test 2: ** should find all files recursively
                offsets.clear();
                Utf8String pattern2 = new Utf8String("fuzz/**");
                GlobFilesFunctionFactory.parseGlobPattern(pattern2, offsets);
                GlobFilesFunctionFactory.globFiles(ff, pattern2, workingPath, inputRoot, fileNameSink, resultFiles, tempPaths, offsets);
                Assert.assertEquals("** count mismatch", createdFiles.size(), resultFiles.size());

                // Test 3: **/*.csv should find all csv files at any depth
                offsets.clear();
                Utf8String pattern3 = new Utf8String("fuzz/**/*.csv");
                GlobFilesFunctionFactory.parseGlobPattern(pattern3, offsets);
                GlobFilesFunctionFactory.globFiles(ff, pattern3, workingPath, inputRoot, fileNameSink, resultFiles, tempPaths, offsets);
                int csvCount = 0;
                for (String f : createdFiles) {
                    if (f.endsWith(".csv")) {
                        csvCount++;
                    }
                }
                Assert.assertEquals("**/*.csv count mismatch", csvCount, resultFiles.size());

                // Test 4: data_??.* should match data_XX.* (two digit)
                offsets.clear();
                Utf8String pattern4 = new Utf8String("fuzz/data_??.*");
                GlobFilesFunctionFactory.parseGlobPattern(pattern4, offsets);
                GlobFilesFunctionFactory.globFiles(ff, pattern4, workingPath, inputRoot, fileNameSink, resultFiles, tempPaths, offsets);
                for (int i = 0; i < resultFiles.size(); i++) {
                    String resultPath = resultFiles.getQuick(i).toString();
                    // Extract filename and verify it matches data_XX pattern (2 digits)
                    String fileName = resultPath.substring(resultPath.lastIndexOf(File.separator) + 1);
                    Assert.assertTrue("Should match data_??.* pattern: " + fileName,
                            fileName.matches("data_\\d\\d\\..*"));
                }
            }
        }
    }

    @Test
    public void testGlobAbsolutePath() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n7\n", "select count(*) cnt from glob('" + inputRoot + "/data/*.parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('" + inputRoot + "/data/*.parquet') where path like '%file1.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('" + inputRoot + "/data/*.parquet') where path like '%file2.parquet'", null, false, true);
        });
    }

    @Test
    public void testGlobAllColumns() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file1.parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file1.parquet') where diskSize > 0", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file1.parquet') where modifiedTime > 0", null, false, true);
        });
    }

    @Test
    public void testGlobAllParquetFiles() throws Exception {
        assertMemoryLeak(() -> {
            // Count all parquet files in the entire test directory
            // data: 7 + nested: 2 + level3: 2 + level4: 1 = 12
            // reports: 2 + 2022: 4 + 2023: 4 + 2024: 2 = 12
            // archive: 2022: 2 + 2023/01: 2 + 2023/06: 2 = 6
            // logs: 1
            // temp: 3 + staging: 2 = 5
            // partitioned: 2 + 3 + 1 = 6
            // mixed: 10
            // Total: 12 + 12 + 6 + 1 + 5 + 6 + 10 = 52
            assertQuery("cnt\n52\n", "select count(*) cnt from glob('**/*.parquet')", null, false, true);
        });
    }

    @Test
    public void testGlobArchiveMultipleYears() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n6\n", "select count(*) cnt from glob('archive/**/*.parquet')", null, false, true);
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('archive/2022/**/*.parquet')", null, false, true);
            assertQuery("cnt\n4\n", "select count(*) cnt from glob('archive/2023/**/*.parquet')", null, false, true);
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('archive/2023/01/**/*.parquet')", null, false, true);
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('archive/2023/06/**/*.parquet')", null, false, true);
            assertQuery("cnt\n0\n", "select count(*) cnt from glob('archive/2023/07/**/*.parquet')", null, false, true);
        });
    }

    @Test
    public void testGlobAsteriskInMiddle() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/f*1.parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/f*1.parquet') where path like '%file1.parquet'", null, false, true);
            assertQuery("cnt\n0\n", "select count(*) cnt from glob('data/f*1.parquet') where path like '%file2.parquet'", null, false, true);
        });
    }

    @Test
    public void testGlobAsteriskPattern() throws Exception {
        assertMemoryLeak(() -> {
            // Should match 7 parquet files: file1, file2, file3, fileA, fileB, test_2023, test_2024
            assertQuery("cnt\n7\n", "select count(*) cnt from glob('data/*.parquet')", null, false, true);
            // Verify each file is present
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/*.parquet') where path like '%file1.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/*.parquet') where path like '%file2.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/*.parquet') where path like '%file3.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/*.parquet') where path like '%fileA.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/*.parquet') where path like '%fileB.parquet'", null, false, true);
            assertQuery("cnt\n0\n", "select count(*) cnt from glob('data/*.parquet') where path like '%readme.txt'", null, false, true);
        });
    }

    @Test
    public void testGlobBracketNegation() throws Exception {
        assertMemoryLeak(() -> {
            // [!1] should match file2, file3, fileA, fileB (any single char except '1')
            assertQuery("cnt\n4\n", "select count(*) cnt from glob('data/file[!1].parquet')", null, false, true);
            assertQuery("cnt\n0\n", "select count(*) cnt from glob('data/file[!1].parquet') where path like '%file1.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file[!1].parquet') where path like '%file2.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file[!1].parquet') where path like '%file3.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file[!1].parquet') where path like '%fileA.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file[!1].parquet') where path like '%fileB.parquet'", null, false, true);
        });
    }

    @Test
    public void testGlobBracketPattern() throws Exception {
        assertMemoryLeak(() -> {
            // [12] should match file1 and file2, but not file3
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('data/file[12].parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file[12].parquet') where path like '%file1.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file[12].parquet') where path like '%file2.parquet'", null, false, true);
            assertQuery("cnt\n0\n", "select count(*) cnt from glob('data/file[12].parquet') where path like '%file3.parquet'", null, false, true);
        });
    }

    @Test
    public void testGlobBracketRange() throws Exception {
        assertMemoryLeak(() -> {
            // [1-3] should match file1, file2, and file3
            assertQuery("cnt\n3\n", "select count(*) cnt from glob('data/file[1-3].parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file[1-3].parquet') where path like '%file1.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file[1-3].parquet') where path like '%file2.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file[1-3].parquet') where path like '%file3.parquet'", null, false, true);
        });
    }

    @Test
    public void testGlobCombinedPatterns() throws Exception {
        assertMemoryLeak(() -> {
            // Combine * and ?: file?.p* should match file1.parquet, file2.parquet, file3.parquet, fileA.parquet, fileB.parquet
            assertQuery("cnt\n5\n", "select count(*) cnt from glob('data/file?.p*')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file?.p*') where path like '%file1.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file?.p*') where path like '%file2.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file?.p*') where path like '%fileA.parquet'", null, false, true);
        });
    }

    @Test
    public void testGlobDeepLevel4() throws Exception {
        assertMemoryLeak(() -> {
            // Test 4 levels deep: data/nested/level3/level4/deepest.parquet
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/nested/level3/level4/*.parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/**/level4/*.parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/nested/**/deepest.parquet')", null, false, true);
        });
    }

    @Test
    public void testGlobDeepNesting() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('archive/2023/01/backup/*.parquet')", null, false, true);
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('archive/2023/01/**/*.parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('archive/2023/01/**/*.parquet') where path like '%data1.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('archive/2023/01/**/*.parquet') where path like '%data2.parquet'", null, false, true);
        });
    }

    @Test
    public void testGlobDisabledNoCopyRoot() throws Exception {
        assertMemoryLeak(() -> {
            String oldInputRoot = inputRoot;
            try {
                inputRoot = null;
                assertException(
                        "select * from glob('*.parquet')",
                        14,
                        "'cairo.sql.copy.root' is not set"
                );
            } finally {
                inputRoot = oldInputRoot;
            }
        });
    }

    @Test
    public void testGlobDoubleStarAtEnd() throws Exception {
        assertMemoryLeak(() -> {
            // ** at end should find all files recursively under data/
            // data/ has 9 files, nested/ has 3 files, level3/ has 2 files, level4/ has 1 file = 15 total
            assertQuery("cnt\n15\n", "select count(*) cnt from glob('data/**')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/**') where path like '%file1.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/**') where path like '%deep.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/**') where path like '%level3.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/**') where path like '%deepest.parquet'", null, false, true);
        });
    }

    @Test
    public void testGlobDoubleStarMiddle() throws Exception {
        assertMemoryLeak(() -> {
            // ** in middle finds parquet files recursively under reports/
            // reports/ has 2 parquet, 2022/ has 4, 2023/ has 4, 2024/ has 2 = 12 total parquet
            assertQuery("cnt\n12\n", "select count(*) cnt from glob('reports/**/*.parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('reports/**/*.parquet') where path like '%summary.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('reports/**/*.parquet') where path like '%overview.parquet'", null, false, true);
            // q1.parquet appears in 2022, 2023, and 2024
            assertQuery("cnt\n3\n", "select count(*) cnt from glob('reports/**/*.parquet') where path like '%q1.parquet'", null, false, true);
            assertQuery("cnt\n3\n", "select count(*) cnt from glob('reports/**/*.parquet') where path like '%q2.parquet'", null, false, true);
        });
    }

    @Test
    public void testGlobDoubleStarRecursive() throws Exception {
        assertMemoryLeak(() -> {
            // **/*.csv should find all csv files recursively from root
            // data/data.csv, data/nested/nested.csv, reports/metrics.csv, reports/2023/annual.csv, logs/app.csv = 5 total
            assertQuery("cnt\n5\n", "select count(*) cnt from glob('**/*.csv')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('**/*.csv') where path like '%data.csv'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('**/*.csv') where path like '%metrics.csv'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('**/*.csv') where path like '%annual.csv'", null, false, true);
        });
    }

    @Test
    public void testGlobEmptyDirectory() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "path\tdiskSize\tdiskSizeHuman\tmodifiedTime\n",
                "select * from glob('empty/*.parquet')",
                null, false, true
        ));
    }

    @Test
    public void testGlobEmptyPattern() throws Exception {
        assertMemoryLeak(() -> assertException(
                "select * from glob('')",
                19,
                "glob pattern cannot be null or empty"
        ));
    }

    @Test
    public void testGlobEmptyResult() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "path\tdiskSize\tdiskSizeHuman\tmodifiedTime\n",
                "select * from glob('nonexistent/*.xyz')",
                null, false, true
        ));
    }

    @Test
    public void testGlobExactMatch() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file1.parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file1.parquet') where path like '%file1.parquet'", null, false, true);
            assertQuery("cnt\n0\n", "select count(*) cnt from glob('data/file1.parquet') where path like '%file2.parquet'", null, false, true);
        });
    }

    @Test
    public void testGlobFilesDoubleStarAtEnd() {
        try (
                DirectUtf8StringList resultFiles = new DirectUtf8StringList(256, 16);
                DirectUtf8StringList tempPaths = new DirectUtf8StringList(256, 16);
                Path workingPath = new Path(MemoryTag.NATIVE_PATH)
        ) {
            IntList offsets = new IntList();
            Utf8String pattern = new Utf8String("data/**");
            GlobFilesFunctionFactory.parseGlobPattern(pattern, offsets);

            Utf8StringSink fileNameSink = new Utf8StringSink();
            GlobFilesFunctionFactory.globFiles(
                    configuration.getFilesFacade(),
                    pattern,
                    workingPath,
                    inputRoot,
                    fileNameSink,
                    resultFiles,
                    tempPaths,
                    offsets
            );

            // data/ has 15 files total (9 in data/, 3 in nested/, 2 in level3/, 1 in level4/)
            Assert.assertEquals("Should find all files recursively under data/", 15, resultFiles.size());
        }
    }

    @Test
    public void testGlobFilesExactPath() {
        try (
                DirectUtf8StringList resultFiles = new DirectUtf8StringList(256, 16);
                DirectUtf8StringList tempPaths = new DirectUtf8StringList(256, 16);
                Path workingPath = new Path(MemoryTag.NATIVE_PATH)
        ) {
            IntList offsets = new IntList();
            Utf8String pattern = new Utf8String("data/file1.parquet");
            GlobFilesFunctionFactory.parseGlobPattern(pattern, offsets);

            Utf8StringSink fileNameSink = new Utf8StringSink();
            GlobFilesFunctionFactory.globFiles(
                    configuration.getFilesFacade(),
                    pattern,
                    workingPath,
                    inputRoot,
                    fileNameSink,
                    resultFiles,
                    tempPaths,
                    offsets
            );

            Assert.assertEquals(1, resultFiles.size());
            Assert.assertTrue(resultFiles.getQuick(0).toString().endsWith("file1.parquet"));
        }
    }

    @Test
    public void testGlobFilesNoMatch() {
        try (
                DirectUtf8StringList resultFiles = new DirectUtf8StringList(256, 16);
                DirectUtf8StringList tempPaths = new DirectUtf8StringList(256, 16);
                Path workingPath = new Path(MemoryTag.NATIVE_PATH)
        ) {
            IntList offsets = new IntList();
            Utf8String pattern = new Utf8String("nonexistent/*.xyz");
            GlobFilesFunctionFactory.parseGlobPattern(pattern, offsets);

            Utf8StringSink fileNameSink = new Utf8StringSink();
            GlobFilesFunctionFactory.globFiles(
                    configuration.getFilesFacade(),
                    pattern,
                    workingPath,
                    inputRoot,
                    fileNameSink,
                    resultFiles,
                    tempPaths,
                    offsets
            );

            Assert.assertEquals(0, resultFiles.size());
        }
    }

    @Test
    public void testGlobFilesSimplePattern() {
        try (
                DirectUtf8StringList resultFiles = new DirectUtf8StringList(256, 16);
                DirectUtf8StringList tempPaths = new DirectUtf8StringList(256, 16);
                Path workingPath = new Path(MemoryTag.NATIVE_PATH)
        ) {
            IntList offsets = new IntList();
            Utf8String pattern = new Utf8String("data/*.parquet");
            GlobFilesFunctionFactory.parseGlobPattern(pattern, offsets);

            Utf8StringSink fileNameSink = new Utf8StringSink();
            GlobFilesFunctionFactory.globFiles(
                    configuration.getFilesFacade(),
                    pattern,
                    workingPath,
                    inputRoot,
                    fileNameSink,
                    resultFiles,
                    tempPaths,
                    offsets
            );
            Assert.assertEquals(7, resultFiles.size());
        }
    }

    @Test
    public void testGlobLogsDaily() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n3\n", "select count(*) cnt from glob('logs/daily/*.log')", null, false, true);
            assertQuery("cnt\n3\n", "select count(*) cnt from glob('logs/daily/2023-01-0?.log')", null, false, true);
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('logs/daily/2023-01-0[12].log')", null, false, true);
        });
    }

    @Test
    public void testGlobMatchAsterisk() {
        assertGlobMatch("hello.txt", "*.txt", true);
        assertGlobMatch("hello.txt", "hello.*", true);
        assertGlobMatch("hello.txt", "*", true);
        assertGlobMatch("hello.txt", "h*o.txt", true);
        assertGlobMatch("hello.txt", "*.csv", false);
        assertGlobMatch("hello.txt", "world.*", false);
        assertGlobMatch("hello.txt", "h*", true);
        assertGlobMatch("hello.txt", "*o.txt", true);
        assertGlobMatch("hello.txt", "*ell*", true);
        assertGlobMatch("hello.txt", "**", true);
    }

    @Test
    public void testGlobMatchBracketEdgeCases() {
        // [a-] should treat - as literal
        assertGlobMatch("-", "[a-]", true);
        assertGlobMatch("a", "[a-]", true);
        assertGlobMatch("b", "[a-]", false);

        // [-a] should treat - as literal
        assertGlobMatch("-", "[-a]", true);
        assertGlobMatch("a", "[-a]", true);
        assertGlobMatch("b", "[-a]", false);

        // [] first ] is literal
        assertGlobMatch("]", "[]a]", true);
        assertGlobMatch("a", "[]a]", true);
        assertGlobMatch("b", "[]a]", false);
    }

    @Test
    public void testGlobMatchBracketNegation() {
        assertGlobMatch("a", "[!bc]", true);
        assertGlobMatch("b", "[!bc]", false);
        assertGlobMatch("c", "[!bc]", false);
        assertGlobMatch("d", "[!bc]", true);
        assertGlobMatch("a", "[!a-c]", false);
        assertGlobMatch("d", "[!a-c]", true);
        assertGlobMatch("z", "[!a-m]", true);
    }

    @Test
    public void testGlobMatchBracketRange() {
        assertGlobMatch("a", "[a-z]", true);
        assertGlobMatch("m", "[a-z]", true);
        assertGlobMatch("z", "[a-z]", true);
        assertGlobMatch("A", "[a-z]", false);
        assertGlobMatch("0", "[0-9]", true);
        assertGlobMatch("5", "[0-9]", true);
        assertGlobMatch("a", "[0-9]", false);
        assertGlobMatch("A", "[A-Z]", true);
        assertGlobMatch("M", "[A-Z]", true);
    }

    @Test
    public void testGlobMatchBracketSet() {
        assertGlobMatch("a", "[abc]", true);
        assertGlobMatch("b", "[abc]", true);
        assertGlobMatch("c", "[abc]", true);
        assertGlobMatch("d", "[abc]", false);
        assertGlobMatch("file1.txt", "file[123].txt", true);
        assertGlobMatch("file4.txt", "file[123].txt", false);
    }

    @Test
    public void testGlobMatchComplexPatterns() {
        assertGlobMatch("test_file_2023.parquet", "test_*_????.parquet", true);
        assertGlobMatch("test_file_23.parquet", "test_*_????.parquet", false);
        assertGlobMatch("data_2023_01_15.csv", "data_????_??_??.csv", true);
        assertGlobMatch("file_abc.txt", "file_[a-z][a-z][a-z].txt", true);
        assertGlobMatch("file_ab1.txt", "file_[a-z][a-z][a-z].txt", false);
    }

    @Test
    public void testGlobMatchEdgeCases() {
        // empty strings
        assertGlobMatch("", "", true);
        assertGlobMatch("", "*", true);
        assertGlobMatch("a", "", false);
        assertGlobMatch("", "a", false);

        // only wildcards
        assertGlobMatch("anything", "*", true);
        assertGlobMatch("a", "?", true);
        assertGlobMatch("ab", "??", true);
        assertGlobMatch("abc", "???", true);
        assertGlobMatch("abc", "****", true);

        // consecutive wildcards
        assertGlobMatch("abc", "a**c", true);
        assertGlobMatch("abc", "*a*b*c*", true);
        assertGlobMatch("abc", "**a**b**c**", true);

        // special characters in filename
        assertGlobMatch("file.txt", "*.txt", true);
        assertGlobMatch("file_name.txt", "*_*.txt", true);
        assertGlobMatch("file-name.txt", "*-*.txt", true);

        // bracket at boundaries
        assertGlobMatch("a", "[a-z]", true);
        assertGlobMatch("z", "[a-z]", true);
        assertGlobMatch("0", "[0-9]", true);
        assertGlobMatch("9", "[0-9]", true);
    }

    @Test
    public void testGlobMatchEscape() {
        // Skip on Windows,  backslash is the path separator, not an escape character
        Assume.assumeTrue(!Os.isWindows());
        assertGlobMatch("*.txt", "\\*.txt", true);
        assertGlobMatch("hello.txt", "\\*.txt", false);
        assertGlobMatch("?.txt", "\\?.txt", true);
        assertGlobMatch("a.txt", "\\?.txt", false);
        assertGlobMatch("[abc].txt", "\\[abc\\].txt", true);
        assertGlobMatch("a.txt", "\\[abc\\].txt", false);
        assertGlobMatch("\\test.txt", "\\\\test.txt", true);
    }

    @Test
    public void testGlobMatchExact() {
        assertGlobMatch("hello", "hello", true);
        assertGlobMatch("hello", "world", false);
        assertGlobMatch("", "", true);
        assertGlobMatch("hello", "", false);
        assertGlobMatch("", "hello", false);
        assertGlobMatch("hello.txt", "hello.txt", true);
        assertGlobMatch("HELLO.txt", "hello.txt", false);
    }

    @Test
    public void testGlobMatchQuestionMark() {
        assertGlobMatch("a", "?", true);
        assertGlobMatch("abc", "???", true);
        assertGlobMatch("ab", "???", false);
        assertGlobMatch("abcd", "???", false);
        assertGlobMatch("file1.txt", "file?.txt", true);
        assertGlobMatch("file12.txt", "file?.txt", false);
        assertGlobMatch("a1b2c3", "??????", true);
    }

    @Test
    public void testGlobMixedBracketPatterns() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n3\n", "select count(*) cnt from glob('mixed/[abc].parquet')", null, false, true);
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('mixed/a[ab].parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('mixed/[x-z][x-z][x-z].parquet')", null, false, true);
        });
    }

    @Test
    public void testGlobMixedDirectory() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n10\n", "select count(*) cnt from glob('mixed/*.parquet')", null, false, true);
            // Single character names
            assertQuery("cnt\n3\n", "select count(*) cnt from glob('mixed/?.parquet')", null, false, true);
            // Two character names
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('mixed/??.parquet')", null, false, true);
            // Three character names
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('mixed/???.parquet')", null, false, true);
        });
    }

    @Test
    public void testGlobMultipleDoubleStarError() throws Exception {
        assertMemoryLeak(() -> assertException(
                "select * from glob('data/**/*.parquet/**/*.csv')",
                19,
                "cannot use multiple '**' in one path"
        ));
    }

    @Test
    public void testGlobMultipleExtensions() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('logs/*.parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('logs/*.parquet') where path like '%app.parquet'", null, false, true);
            assertQuery("cnt\n0\n", "select count(*) cnt from glob('logs/*.parquet') where path like '%app.csv'", null, false, true);

            assertQuery("cnt\n1\n", "select count(*) cnt from glob('logs/*.csv')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('logs/*.csv') where path like '%app.csv'", null, false, true);
            assertQuery("cnt\n0\n", "select count(*) cnt from glob('logs/*.csv') where path like '%app.parquet'", null, false, true);
        });
    }

    @Test
    public void testGlobNestedDirectories() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('data/nested/*.parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/nested/*.parquet') where path like '%deep.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/nested/*.parquet') where path like '%extra.parquet'", null, false, true);
        });
    }

    @Test
    public void testGlobPartitionedDirectory() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n6\n", "select count(*) cnt from glob('partitioned/**/*.parquet')", null, false, true);
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('partitioned/year=2022/*.parquet')", null, false, true);
            assertQuery("cnt\n3\n", "select count(*) cnt from glob('partitioned/year=2023/*.parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('partitioned/year=2024/*.parquet')", null, false, true);
        });
    }

    @Test
    public void testGlobPartitionedWithWildcard() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n6\n", "select count(*) cnt from glob('partitioned/year=*/*.parquet')", null, false, true);
            assertQuery("cnt\n6\n", "select count(*) cnt from glob('partitioned/year=202?/*.parquet')", null, false, true);
            assertQuery("cnt\n3\n", "select count(*) cnt from glob('partitioned/year=202[3]/*.parquet')", null, false, true);
        });
    }

    @Test
    public void testGlobPathTraversalNotAllowed() throws Exception {
        assertMemoryLeak(() -> {
            assertException(
                    "select * from glob('../etc/passwd')",
                    19,
                    "path traversal '..' is not allowed in glob pattern"
            );
            assertException(
                    "select * from glob('data/../secret.txt')",
                    19,
                    "path traversal '..' is not allowed in glob pattern"
            );
            assertException(
                    "select * from glob('../**/*.parquet')",
                    19,
                    "path traversal '..' is not allowed in glob pattern"
            );
            assertException(
                    "select * from glob('" + inputRoot + "/../secret.txt')",
                    19,
                    "path traversal '..' is not allowed in glob pattern"
            );
        });
    }

    @Test
    public void testGlobQuestionMark() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n5\n", "select count(*) cnt from glob('data/file?.parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file?.parquet') where path like '%file1.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file?.parquet') where path like '%file2.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file?.parquet') where path like '%file3.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file?.parquet') where path like '%fileA.parquet'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('data/file?.parquet') where path like '%fileB.parquet'", null, false, true);
        });
    }

    @Test
    public void testGlobQuestionMarkMultiple() throws Exception {
        assertMemoryLeak(() -> {
            // app_??.log should match app_01.log and app_02.log, but not app_1.log (single digit)
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('logs/app_??.log')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('logs/app_??.log') where path like '%app_01.log'", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('logs/app_??.log') where path like '%app_02.log'", null, false, true);
            assertQuery("cnt\n0\n", "select count(*) cnt from glob('logs/app_??.log') where path like '%app_1.log'", null, false, true);
        });
    }

    @Test
    public void testGlobReportsYearPattern() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n4\n", "select count(*) cnt from glob('reports/2022/*.parquet')", null, false, true);
            assertQuery("cnt\n4\n", "select count(*) cnt from glob('reports/2023/*.parquet')", null, false, true);
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('reports/2024/*.parquet')", null, false, true);
            assertQuery("cnt\n3\n", "select count(*) cnt from glob('reports/*/q1.parquet')", null, false, true);
        });
    }

    @Test
    public void testGlobTempDirectory() throws Exception {
        assertMemoryLeak(() -> {
            // temp/ has 5 files, staging/ has 2 files
            assertQuery("cnt\n7\n", "select count(*) cnt from glob('temp/**')", null, false, true);
            assertQuery("cnt\n3\n", "select count(*) cnt from glob('temp/data_*.parquet')", null, false, true);
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('temp/staging/*.parquet')", null, false, true);
        });
    }

    @Test
    public void testGlobTempNumberedFiles() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cnt\n3\n", "select count(*) cnt from glob('temp/data_???.parquet')", null, false, true);
            assertQuery("cnt\n1\n", "select count(*) cnt from glob('temp/data_001.parquet')", null, false, true);
            assertQuery("cnt\n2\n", "select count(*) cnt from glob('temp/data_00[12].parquet')", null, false, true);
        });
    }

    @Test
    public void testHasGlob() {
        // Skip on Windows, backslash is the path separator, not an escape character
        Assume.assumeTrue(!Os.isWindows());
        assertHasGlob("*.txt", true);
        assertHasGlob("file?.txt", true);
        assertHasGlob("[abc].txt", true);
        assertHasGlob("hello.txt", false);
        assertHasGlob("\\*.txt", false);
        assertHasGlob("\\?.txt", false);
        assertHasGlob("\\[abc].txt", false);
        assertHasGlob("file\\*name.txt", false);
        assertHasGlob("file*\\?name.txt", true);
    }

    @Test
    public void testParseGlobPatternAbsolute() {
        IntList offsets = new IntList();
        GlobFilesFunctionFactory.parseGlobPattern(new Utf8String("/data/files/*.parquet"), offsets);
        Assert.assertEquals(6, offsets.size());
        Assert.assertEquals(1, offsets.getQuick(0));
        Assert.assertEquals(5, offsets.getQuick(1));
        Assert.assertEquals(6, offsets.getQuick(2));
        Assert.assertEquals(11, offsets.getQuick(3));
        Assert.assertEquals(12, offsets.getQuick(4));
        Assert.assertEquals(21, offsets.getQuick(5));
    }

    @Test
    public void testParseGlobPatternConsecutiveSeparators() {
        IntList offsets = new IntList();
        GlobFilesFunctionFactory.parseGlobPattern(new Utf8String("a//b///c"), offsets);
        Assert.assertEquals(6, offsets.size());
    }

    @Test
    public void testParseGlobPatternEmpty() {
        IntList offsets = new IntList();
        GlobFilesFunctionFactory.parseGlobPattern(new Utf8String(""), offsets);
        Assert.assertEquals(0, offsets.size());
    }

    @Test
    public void testParseGlobPatternEscapedGlobChars() {
        // Skip on Windows, backslash is the path separator, not an escape character
        Assume.assumeTrue(!Os.isWindows());
        IntList offsets = new IntList();
        offsets.clear();
        GlobFilesFunctionFactory.parseGlobPattern(new Utf8String("a/\\*/b"), offsets);
        Assert.assertEquals(6, offsets.size());
        Assert.assertEquals(0, offsets.getQuick(0));
        Assert.assertEquals(1, offsets.getQuick(1));
        Assert.assertEquals(2, offsets.getQuick(2));
        Assert.assertEquals(4, offsets.getQuick(3));
        Assert.assertEquals(5, offsets.getQuick(4));
        Assert.assertEquals(6, offsets.getQuick(5));
        offsets.clear();
        GlobFilesFunctionFactory.parseGlobPattern(new Utf8String("a/\\\\/b"), offsets);
        Assert.assertEquals(6, offsets.size());
    }

    @Test
    public void testParseGlobPatternSimple() {
        IntList offsets = new IntList();
        GlobFilesFunctionFactory.parseGlobPattern(new Utf8String("data/files/*.parquet"), offsets);
        Assert.assertEquals(6, offsets.size());
        Assert.assertEquals(0, offsets.getQuick(0));
        Assert.assertEquals(4, offsets.getQuick(1));
        Assert.assertEquals(5, offsets.getQuick(2));
        Assert.assertEquals(10, offsets.getQuick(3));
        Assert.assertEquals(11, offsets.getQuick(4));
        Assert.assertEquals(20, offsets.getQuick(5));
    }

    @Test
    public void testParseGlobPatternSingleSegment() {
        IntList offsets = new IntList();
        GlobFilesFunctionFactory.parseGlobPattern(new Utf8String("*.parquet"), offsets);
        Assert.assertEquals(2, offsets.size());
        Assert.assertEquals(0, offsets.getQuick(0));
        Assert.assertEquals(9, offsets.getQuick(1));
    }

    @Test
    public void testParseGlobPatternTrailingSeparator() {
        IntList offsets = new IntList();
        GlobFilesFunctionFactory.parseGlobPattern(new Utf8String("data/files/"), offsets);
        Assert.assertEquals(4, offsets.size());
    }

    private void assertGlobMatch(String name, String pattern, boolean expected) {
        Utf8String nameUtf8 = new Utf8String(name);
        Utf8String patternUtf8 = new Utf8String(pattern);
        boolean result = GlobFilesFunctionFactory.globMatch(nameUtf8, patternUtf8, 0, patternUtf8.size());
        Assert.assertEquals("Pattern '" + pattern + "' matching '" + name + "'", expected, result);
    }

    private void assertHasGlob(String pattern, boolean expected) {
        Utf8String patternUtf8 = new Utf8String(pattern);
        boolean result = GlobFilesFunctionFactory.hasGlob(patternUtf8, 0, patternUtf8.size());
        Assert.assertEquals("hasGlob('" + pattern + "')", expected, result);
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

    /**
     * Creates test directory structure:
     * <pre>
     * inputRoot/
     * ├── data/                           (9 files)
     * │   ├── file1.parquet
     * │   ├── file2.parquet
     * │   ├── file3.parquet
     * │   ├── fileA.parquet
     * │   ├── fileB.parquet
     * │   ├── test_2023.parquet
     * │   ├── test_2024.parquet
     * │   ├── readme.txt
     * │   ├── data.csv
     * │   └── nested/                     (3 files)
     * │       ├── deep.parquet
     * │       ├── extra.parquet
     * │       ├── nested.csv
     * │       └── level3/                 (2 files)
     * │           ├── level3.parquet
     * │           ├── level3_backup.parquet
     * │           └── level4/             (1 file)
     * │               └── deepest.parquet
     * ├── reports/                        (3 files)
     * │   ├── summary.parquet
     * │   ├── overview.parquet
     * │   ├── metrics.csv
     * │   ├── 2022/                       (4 files)
     * │   │   ├── q1.parquet
     * │   │   ├── q2.parquet
     * │   │   ├── q3.parquet
     * │   │   └── q4.parquet
     * │   ├── 2023/                       (5 files)
     * │   │   ├── q1.parquet
     * │   │   ├── q2.parquet
     * │   │   ├── q3.parquet
     * │   │   ├── q4.parquet
     * │   │   └── annual.csv
     * │   └── 2024/                       (2 files)
     * │       ├── q1.parquet
     * │       └── q2.parquet
     * ├── archive/
     * │   ├── 2022/
     * │   │   └── 12/
     * │   │       └── backup/             (2 files)
     * │   │           ├── old1.parquet
     * │   │           └── old2.parquet
     * │   └── 2023/
     * │       ├── 01/
     * │       │   └── backup/             (2 files)
     * │       │       ├── data1.parquet
     * │       │       └── data2.parquet
     * │       └── 06/
     * │           └── backup/             (2 files)
     * │               ├── mid1.parquet
     * │               └── mid2.parquet
     * ├── logs/                           (9 files)
     * │   ├── app.parquet
     * │   ├── app.csv
     * │   ├── app.log
     * │   ├── app_01.log
     * │   ├── app_02.log
     * │   ├── app_1.log
     * │   ├── system.log
     * │   ├── debug.log
     * │   ├── error.log
     * │   └── daily/                      (3 files)
     * │       ├── 2023-01-01.log
     * │       ├── 2023-01-02.log
     * │       └── 2023-01-03.log
     * ├── empty/                          (0 files)
     * ├── temp/                           (5 files)
     * │   ├── cache.tmp
     * │   ├── session.dat
     * │   ├── data_001.parquet
     * │   ├── data_002.parquet
     * │   ├── data_003.parquet
     * │   └── staging/                    (2 files)
     * │       ├── upload.parquet
     * │       └── pending.parquet
     * ├── partitioned/
     * │   ├── year=2022/                  (2 files)
     * │   │   ├── part-0001.parquet
     * │   │   └── part-0002.parquet
     * │   ├── year=2023/                  (3 files)
     * │   │   ├── part-0001.parquet
     * │   │   ├── part-0002.parquet
     * │   │   └── part-0003.parquet
     * │   └── year=2024/                  (1 file)
     * │       └── part-0001.parquet
     * └── mixed/                          (10 files)
     *     ├── a.parquet
     *     ├── b.parquet
     *     ├── c.parquet
     *     ├── aa.parquet
     *     ├── ab.parquet
     *     ├── abc.parquet
     *     ├── xyz.parquet
     *     ├── file_v1.parquet
     *     ├── file_v2.parquet
     *     └── file_v10.parquet
     * </pre>
     */
    private void setupTestFiles() {
        FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(inputRoot).$();
            if (ff.exists(path.$())) {
                ff.rmdir(path);
            }
            ff.mkdir(path.$(), 493);

            // === data/ directory (9 files + nested subdirectories) ===
            ff.mkdir(path.of(inputRoot).concat("data").$(), 493);
            createTestFile("data" + File.separator + "file1.parquet", 1024);
            createTestFile("data" + File.separator + "file2.parquet", 2048);
            createTestFile("data" + File.separator + "file3.parquet", 512);
            createTestFile("data" + File.separator + "fileA.parquet", 600);
            createTestFile("data" + File.separator + "fileB.parquet", 700);
            createTestFile("data" + File.separator + "readme.txt", 100);
            createTestFile("data" + File.separator + "data.csv", 500);
            createTestFile("data" + File.separator + "test_2023.parquet", 800);
            createTestFile("data" + File.separator + "test_2024.parquet", 900);

            // data/nested/ - one level deep
            ff.mkdir(path.of(inputRoot).concat("data").concat("nested").$(), 493);
            createTestFile("data" + File.separator + "nested" + File.separator + "deep.parquet", 256);
            createTestFile("data" + File.separator + "nested" + File.separator + "nested.csv", 300);
            createTestFile("data" + File.separator + "nested" + File.separator + "extra.parquet", 310);

            // data/nested/level3/ - two levels deep
            ff.mkdir(path.of(inputRoot).concat("data").concat("nested").concat("level3").$(), 493);
            createTestFile("data" + File.separator + "nested" + File.separator + "level3" + File.separator + "level3.parquet", 128);
            createTestFile("data" + File.separator + "nested" + File.separator + "level3" + File.separator + "level3_backup.parquet", 130);

            // data/nested/level3/level4 - three levels deep
            ff.mkdir(path.of(inputRoot).concat("data").concat("nested").concat("level3").concat("level4").$(), 493);
            createTestFile("data" + File.separator + "nested" + File.separator + "level3" + File.separator + "level4" + File.separator + "deepest.parquet", 64);

            // === reports/ directory with year subdirectories ===
            ff.mkdir(path.of(inputRoot).concat("reports").$(), 493);
            createTestFile("reports" + File.separator + "summary.parquet", 1500);
            createTestFile("reports" + File.separator + "metrics.csv", 800);
            createTestFile("reports" + File.separator + "overview.parquet", 1200);

            ff.mkdir(path.of(inputRoot).concat("reports").concat("2022").$(), 493);
            createTestFile("reports" + File.separator + "2022" + File.separator + "q1.parquet", 300);
            createTestFile("reports" + File.separator + "2022" + File.separator + "q2.parquet", 310);
            createTestFile("reports" + File.separator + "2022" + File.separator + "q3.parquet", 320);
            createTestFile("reports" + File.separator + "2022" + File.separator + "q4.parquet", 330);

            ff.mkdir(path.of(inputRoot).concat("reports").concat("2023").$(), 493);
            createTestFile("reports" + File.separator + "2023" + File.separator + "q1.parquet", 400);
            createTestFile("reports" + File.separator + "2023" + File.separator + "q2.parquet", 450);
            createTestFile("reports" + File.separator + "2023" + File.separator + "q3.parquet", 480);
            createTestFile("reports" + File.separator + "2023" + File.separator + "q4.parquet", 500);
            createTestFile("reports" + File.separator + "2023" + File.separator + "annual.csv", 600);

            ff.mkdir(path.of(inputRoot).concat("reports").concat("2024").$(), 493);
            createTestFile("reports" + File.separator + "2024" + File.separator + "q1.parquet", 350);
            createTestFile("reports" + File.separator + "2024" + File.separator + "q2.parquet", 380);

            // === archive/ with deep nesting ===
            ff.mkdir(path.of(inputRoot).concat("archive").$(), 493);
            ff.mkdir(path.of(inputRoot).concat("archive").concat("2022").$(), 493);
            ff.mkdir(path.of(inputRoot).concat("archive").concat("2022").concat("12").$(), 493);
            ff.mkdir(path.of(inputRoot).concat("archive").concat("2022").concat("12").concat("backup").$(), 493);
            createTestFile("archive" + File.separator + "2022" + File.separator + "12" + File.separator + "backup" + File.separator + "old1.parquet", 150);
            createTestFile("archive" + File.separator + "2022" + File.separator + "12" + File.separator + "backup" + File.separator + "old2.parquet", 160);

            ff.mkdir(path.of(inputRoot).concat("archive").concat("2023").$(), 493);
            ff.mkdir(path.of(inputRoot).concat("archive").concat("2023").concat("01").$(), 493);
            ff.mkdir(path.of(inputRoot).concat("archive").concat("2023").concat("01").concat("backup").$(), 493);
            createTestFile("archive" + File.separator + "2023" + File.separator + "01" + File.separator + "backup" + File.separator + "data1.parquet", 200);
            createTestFile("archive" + File.separator + "2023" + File.separator + "01" + File.separator + "backup" + File.separator + "data2.parquet", 220);

            ff.mkdir(path.of(inputRoot).concat("archive").concat("2023").concat("06").$(), 493);
            ff.mkdir(path.of(inputRoot).concat("archive").concat("2023").concat("06").concat("backup").$(), 493);
            createTestFile("archive" + File.separator + "2023" + File.separator + "06" + File.separator + "backup" + File.separator + "mid1.parquet", 180);
            createTestFile("archive" + File.separator + "2023" + File.separator + "06" + File.separator + "backup" + File.separator + "mid2.parquet", 185);

            // === logs/ with multiple file types ===
            ff.mkdir(path.of(inputRoot).concat("logs").$(), 493);
            createTestFile("logs" + File.separator + "app.parquet", 150);
            createTestFile("logs" + File.separator + "app.csv", 160);
            createTestFile("logs" + File.separator + "app.log", 170);
            createTestFile("logs" + File.separator + "app_01.log", 180);
            createTestFile("logs" + File.separator + "app_02.log", 190);
            createTestFile("logs" + File.separator + "app_1.log", 175);
            createTestFile("logs" + File.separator + "system.log", 200);
            createTestFile("logs" + File.separator + "debug.log", 210);
            createTestFile("logs" + File.separator + "error.log", 220);

            // logs/daily/ with dated files
            ff.mkdir(path.of(inputRoot).concat("logs").concat("daily").$(), 493);
            createTestFile("logs" + File.separator + "daily" + File.separator + "2023-01-01.log", 100);
            createTestFile("logs" + File.separator + "daily" + File.separator + "2023-01-02.log", 110);
            createTestFile("logs" + File.separator + "daily" + File.separator + "2023-01-03.log", 120);

            // === empty/ directory (empty) ===
            ff.mkdir(path.of(inputRoot).concat("empty").$(), 493);

            // === temp/ with mixed content ===
            ff.mkdir(path.of(inputRoot).concat("temp").$(), 493);
            createTestFile("temp" + File.separator + "cache.tmp", 50);
            createTestFile("temp" + File.separator + "session.dat", 60);
            createTestFile("temp" + File.separator + "data_001.parquet", 70);
            createTestFile("temp" + File.separator + "data_002.parquet", 80);
            createTestFile("temp" + File.separator + "data_003.parquet", 90);

            ff.mkdir(path.of(inputRoot).concat("temp").concat("staging").$(), 493);
            createTestFile("temp" + File.separator + "staging" + File.separator + "upload.parquet", 700);
            createTestFile("temp" + File.separator + "staging" + File.separator + "pending.parquet", 710);

            // === partitioned/ directory with partition-like structure ===
            ff.mkdir(path.of(inputRoot).concat("partitioned").$(), 493);
            ff.mkdir(path.of(inputRoot).concat("partitioned").concat("year=2022").$(), 493);
            createTestFile("partitioned" + File.separator + "year=2022" + File.separator + "part-0001.parquet", 500);
            createTestFile("partitioned" + File.separator + "year=2022" + File.separator + "part-0002.parquet", 510);

            ff.mkdir(path.of(inputRoot).concat("partitioned").concat("year=2023").$(), 493);
            createTestFile("partitioned" + File.separator + "year=2023" + File.separator + "part-0001.parquet", 520);
            createTestFile("partitioned" + File.separator + "year=2023" + File.separator + "part-0002.parquet", 530);
            createTestFile("partitioned" + File.separator + "year=2023" + File.separator + "part-0003.parquet", 540);

            ff.mkdir(path.of(inputRoot).concat("partitioned").concat("year=2024").$(), 493);
            createTestFile("partitioned" + File.separator + "year=2024" + File.separator + "part-0001.parquet", 550);

            // === mixed/ directory with special naming patterns ===
            ff.mkdir(path.of(inputRoot).concat("mixed").$(), 493);
            createTestFile("mixed" + File.separator + "a.parquet", 30);
            createTestFile("mixed" + File.separator + "b.parquet", 31);
            createTestFile("mixed" + File.separator + "c.parquet", 32);
            createTestFile("mixed" + File.separator + "aa.parquet", 33);
            createTestFile("mixed" + File.separator + "ab.parquet", 34);
            createTestFile("mixed" + File.separator + "abc.parquet", 35);
            createTestFile("mixed" + File.separator + "xyz.parquet", 36);
            createTestFile("mixed" + File.separator + "file_v1.parquet", 40);
            createTestFile("mixed" + File.separator + "file_v2.parquet", 41);
            createTestFile("mixed" + File.separator + "file_v10.parquet", 42);
        }
    }
}
