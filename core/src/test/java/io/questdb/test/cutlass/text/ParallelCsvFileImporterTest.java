/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.text;

import io.questdb.PropertyKey;
import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cutlass.text.Atomicity;
import io.questdb.cutlass.text.CopyJob;
import io.questdb.cutlass.text.CopyRequestJob;
import io.questdb.cutlass.text.ParallelCsvFileImporter;
import io.questdb.cutlass.text.ParallelCsvFileImporter.PartitionInfo;
import io.questdb.cutlass.text.TextImportException;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.IOURing;
import io.questdb.std.IOURingFacade;
import io.questdb.std.IOURingFacadeImpl;
import io.questdb.std.IOURingImpl;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelCsvFileImporterTest extends AbstractCairoTest {
    private static final Rnd rnd = new Rnd();
    private static final String stringTypeName = ColumnType.nameOf(ColumnType.VARCHAR);

    @Override
    @Before
    public void setUp() {
        super.setUp();
        rnd.reset();
        inputRoot = TestUtils.getCsvRoot();
        inputWorkRoot = TestUtils.unchecked(() -> temp.newFolder("imports" + System.nanoTime()).getAbsolutePath());
    }

    @Test
    public void testAssignPartitionsToWorkers() {
        ObjList<PartitionInfo> partitions = new ObjList<>();

        partitions.add(new PartitionInfo(1, "A", 10));
        partitions.add(new PartitionInfo(2, "B", 70));
        partitions.add(new PartitionInfo(3, "C", 50));
        partitions.add(new PartitionInfo(4, "D", 100));
        partitions.add(new PartitionInfo(5, "E", 5));

        int tasks = ParallelCsvFileImporter.assignPartitions(partitions, 2);

        TestUtils.assertEquals(
                partitions, new ObjList<>(
                        new PartitionInfo(1, "A", 10, 0),
                        new PartitionInfo(4, "D", 100, 0),
                        new PartitionInfo(5, "E", 5, 0),
                        new PartitionInfo(2, "B", 70, 1),
                        new PartitionInfo(3, "C", 50, 1)
                )
        );
        Assert.assertEquals(2, tasks);
    }

    @Test
    public void testBacklogTableCleanup() throws Exception {
        for (int i = 0; i < 6; i++) {
            testStatusLogCleanup(i);
        }
    }

    @Test
    public void testFindChunkBoundariesForFileWithLongLines() throws Exception {
        executeWithPool(
                3, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) ->
                        assertChunkBoundariesFor("test-quotes-small.csv", list(0, 0, 90, 2, 185, 3, 256, 5), 3)
        );
    }

    @Test
    public void testFindChunkBoundariesForFileWithNoQuotes() throws Exception {
        executeWithPool(
                3, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) ->
                        assertChunkBoundariesFor("test-import.csv", list(0, 0, 4565, 44, 9087, 87, 13612, 130), 3)
        );
    }

    @Test
    public void testFindChunkBoundariesInFileWithOneLongLine() throws Exception {
        executeWithPool(
                2, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) ->
                        assertChunkBoundariesFor("test-quotes-oneline.csv", list(0, 0, 252, 2), 2)
        );
    }

    @Test
    public void testFindChunkBoundariesInFileWithOneLongLineWithManyWorkers() throws Exception {
        executeWithPool(
                7, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) ->
                        assertChunkBoundariesFor("test-quotes-oneline.csv", list(0, 0, 252, 2), 7)
        );
    }

    @Test
    public void testFindChunkBoundariesInLargerCsv() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) ->
                        assertChunkBoundariesFor("test-quotes-big.csv", list(0, 0, 16797, 254, 33514, 503, 50216, 752, 66923, 1002), 4)
        );
    }

    @Test
    public void testFindChunkBoundariesWith1WorkerForFileWithLongLines() throws Exception {
        executeWithPool(
                1, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) ->
                        assertChunkBoundariesFor("test-quotes-small.csv", list(0, 0, 256, 0), 1)
        );
    }

    @Test
    public void testImportAllTypesIntoExistingTable() throws Exception {
        executeWithPool(4, 8, this::importAllIntoExisting);
    }

    @Test
    public void testImportAllTypesIntoExistingTableBrokenRename() throws Exception {
        AtomicInteger counter = new AtomicInteger(0);
        FilesFacade brokenRename = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (counter.incrementAndGet() < 11) {
                    return Files.FILES_RENAME_ERR_EXDEV;
                }
                return super.rename(from, to);
            }
        };
        executeWithPool(4, 8, brokenRename, this::importAllIntoExisting);
    }

    @Test
    public void testImportAllTypesIntoNewTable() throws Exception {
        executeWithPool(4, 8, this::importAllIntoNew);
    }

    @Test
    public void testImportAllTypesWithGaps() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(
                            compiler,
                            "create table alltypes (\n" +
                                    "  bo boolean,\n" +
                                    "  by byte,\n" +
                                    "  sh short,\n" +
                                    "  ch char,\n" +
                                    "  in_ int,\n" +
                                    "  lo long,\n" +
                                    "  dat date, \n" +
                                    "  tstmp timestamp, \n" +
                                    "  ft float,\n" +
                                    "  db double,\n" +
                                    "  str string,\n" +
                                    "  sym symbol,\n" +
                                    "  l256 long256," +
                                    "  ge geohash(20b)" +
                                    ") timestamp(tstmp) partition by DAY;", sqlExecutionContext
                    );
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("alltypes", "test-alltypes-with-gaps.csv", 1, PartitionBy.DAY, (byte) ',', "tstmp", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", true, null);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }

                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "bo\tby\tsh\tch\tin_\tlo\tdat\ttstmp\tft\tdb\tstr\tsym\tl256\tge\n" +
                                    "false\t106\t22716\tG\t1\t1\t1970-01-02T00:00:00.000Z\t1970-01-02T00:00:00.000000Z\t1.1\t1.2\ts1\tsy1\t0x0adaa43b7700522b82f4e8d8d7b8c41a985127d17ca3926940533c477c927a33\tu33d\n" +
                                    "false\t0\t8654\tS\t2\t2\t1970-01-03T00:00:00.000Z\t1970-01-03T00:00:00.000000Z\t2.1\t2.2\ts2\tsy2\t0x593c9b7507c60ec943cd1e308a29ac9e645f3f4104fa76983c50b65784d51e37\tu33d\n" +
                                    "false\t104\t0\tT\t3\t3\t1970-01-04T00:00:00.000Z\t1970-01-04T00:00:00.000000Z\t3.1\t3.2\ts3\tsy3\t0x30cb58d11566e857a87063d9dba8961195ddd1458f633b7f285307c11a7072d1\tu33d\n" +
                                    "false\t105\t31772\t\t4\t4\t1970-01-05T00:00:00.000Z\t1970-01-05T00:00:00.000000Z\t4.1\t4.2\ts4\tsy4\t0x64ad74a1e1e5e5897c61daeff695e8be6ab8ea52090049faa3306e2d2440176e\tu33d\n" +
                                    "false\t123\t8110\tE\tnull\t5\t1970-01-06T00:00:00.000Z\t1970-01-06T00:00:00.000000Z\t5.1\t5.2\ts5\tsy5\t0x5a86aaa24c707fff785191c8901fd7a16ffa1093e392dc537967b0fb8165c161\tu33d\n" +
                                    "false\t98\t25729\tM\t6\tnull\t1970-01-07T00:00:00.000Z\t1970-01-07T00:00:00.000000Z\t6.1\t6.2\ts6\tsy6\t0x8fbdd90a38ecfaa89b71e0b7a1d088ada82ff4bad36b72c47056f3fabd4cfeed\tu33d\n" +
                                    "false\t44\t-19823\tU\t7\tnull\t1970-01-08T00:00:00.000Z\t1970-01-08T00:00:00.000000Z\t7.1\t7.2\ts7\tsy7\t0xfb87e052526d72b5faf2f76f0f4bd855bc983a6991a2e7c78c671857b35a8755\tu33d\n" +
                                    "true\t102\t5672\tS\t8\t8\t\t1970-01-09T00:00:00.000000Z\t8.1\t8.2\ts8\tsy8\t0x6df9f4797b131d69aa4f08d320dde2dc72cb5a65911401598a73264e80123440\tu33d\n" +
                                    "false\t73\t-5962\tE\t9\t9\t1970-01-10T00:00:00.000Z\t1970-01-10T00:00:00.000000Z\t9.1\t9.2\t\tsy9\t0xdc33dd2e6ea8cc86a6ef5e562486cceb67886eea99b9dd07ba84e3fba7f66cd6\tu33d\n" +
                                    "true\t61\t-17553\tD\t10\t10\t1970-01-11T00:00:00.000Z\t1970-01-11T00:00:00.000000Z\t10.1\t10.2\ts10\t\t0x83e9d33db60120e69ba3fb676e3280ed6a6e16373be3139063343d28d3738449\tu33d\n",
                            "select * from alltypes",
                            "tstmp",
                            true,
                            false,
                            true
                    );
                }
        );
    }

    @Test
    public void testImportAllTypesWithGapsIndexed() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(
                            compiler,
                            "create table alltypes (\n" +
                                    "  bo boolean,\n" +
                                    "  by byte,\n" +
                                    "  sh short,\n" +
                                    "  ch char,\n" +
                                    "  in_ int,\n" +
                                    "  lo long,\n" +
                                    "  dat date, \n" +
                                    "  tstmp timestamp, \n" +
                                    "  ft float,\n" +
                                    "  db double,\n" +
                                    "  str string,\n" +
                                    "  sym symbol index capacity 64,\n" +
                                    "  l256 long256," +
                                    "  ge geohash(20b)" +
                                    ") timestamp(tstmp) partition by DAY;", sqlExecutionContext
                    );
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("alltypes", "test-alltypes-with-gaps.csv", 1, PartitionBy.DAY, (byte) ',', "tstmp", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }

                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "bo\tby\tsh\tch\tin_\tlo\tdat\ttstmp\tft\tdb\tstr\tsym\tl256\tge\n" +
                                    "false\t106\t22716\tG\t1\t1\t1970-01-02T00:00:00.000Z\t1970-01-02T00:00:00.000000Z\t1.1\t1.2\ts1\tsy1\t0x0adaa43b7700522b82f4e8d8d7b8c41a985127d17ca3926940533c477c927a33\tu33d\n" +
                                    "false\t0\t8654\tS\t2\t2\t1970-01-03T00:00:00.000Z\t1970-01-03T00:00:00.000000Z\t2.1\t2.2\ts2\tsy2\t0x593c9b7507c60ec943cd1e308a29ac9e645f3f4104fa76983c50b65784d51e37\tu33d\n" +
                                    "false\t104\t0\tT\t3\t3\t1970-01-04T00:00:00.000Z\t1970-01-04T00:00:00.000000Z\t3.1\t3.2\ts3\tsy3\t0x30cb58d11566e857a87063d9dba8961195ddd1458f633b7f285307c11a7072d1\tu33d\n" +
                                    "false\t105\t31772\t\t4\t4\t1970-01-05T00:00:00.000Z\t1970-01-05T00:00:00.000000Z\t4.1\t4.2\ts4\tsy4\t0x64ad74a1e1e5e5897c61daeff695e8be6ab8ea52090049faa3306e2d2440176e\tu33d\n" +
                                    "false\t123\t8110\tE\tnull\t5\t1970-01-06T00:00:00.000Z\t1970-01-06T00:00:00.000000Z\t5.1\t5.2\ts5\tsy5\t0x5a86aaa24c707fff785191c8901fd7a16ffa1093e392dc537967b0fb8165c161\tu33d\n" +
                                    "false\t98\t25729\tM\t6\tnull\t1970-01-07T00:00:00.000Z\t1970-01-07T00:00:00.000000Z\t6.1\t6.2\ts6\tsy6\t0x8fbdd90a38ecfaa89b71e0b7a1d088ada82ff4bad36b72c47056f3fabd4cfeed\tu33d\n" +
                                    "false\t44\t-19823\tU\t7\tnull\t1970-01-08T00:00:00.000Z\t1970-01-08T00:00:00.000000Z\t7.1\t7.2\ts7\tsy7\t0xfb87e052526d72b5faf2f76f0f4bd855bc983a6991a2e7c78c671857b35a8755\tu33d\n" +
                                    "true\t102\t5672\tS\t8\t8\t\t1970-01-09T00:00:00.000000Z\t8.1\t8.2\ts8\tsy8\t0x6df9f4797b131d69aa4f08d320dde2dc72cb5a65911401598a73264e80123440\tu33d\n" +
                                    "false\t73\t-5962\tE\t9\t9\t1970-01-10T00:00:00.000Z\t1970-01-10T00:00:00.000000Z\t9.1\t9.2\t\tsy9\t0xdc33dd2e6ea8cc86a6ef5e562486cceb67886eea99b9dd07ba84e3fba7f66cd6\tu33d\n" +
                                    "true\t61\t-17553\tD\t10\t10\t1970-01-11T00:00:00.000Z\t1970-01-11T00:00:00.000000Z\t10.1\t10.2\ts10\t\t0x83e9d33db60120e69ba3fb676e3280ed6a6e16373be3139063343d28d3738449\tu33d\n",
                            "select * from alltypes", "tstmp", true, false, true
                    );
                }
        );
    }

    @Test
    public void testImportCleansUpAllTemporaryFiles() throws Exception {
        executeWithPool(
                4, 16, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    compiler.compile("create table t ( ts timestamp, line string, description string, d double ) timestamp(ts) partition by MONTH;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.setMinChunkSize(10);
                        importer.of("t", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);

                        refreshTablesInBaseEngine();
                        assertQueryNoLeakCheck(
                                "count\n1000\n", "select count(*) from t",
                                null, false, false, true
                        );

                        String[] foundFiles = new File(inputWorkRoot).list();
                        Assert.assertTrue(foundFiles == null || foundFiles.length == 0);
                    }
                }
        );
    }

    @Test
    public void testImportCsvFailsOnStructureParsingIO() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long read(long fd, long buf, long len, long offset) {
                return -1L;
            }
        };

        executeWithPool(
                4, 8, ff, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.setMinChunkSize(1);
                        importer.of("tab4", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not read from file");
                    }
                }
        );
    }

    @Test
    public void testImportCsvFromFileWithBadColumnNamesInHeaderIntoNewTableFiltersOutBadCharacters() throws Exception {
        executeWithPool(
                4, 16, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.setMinChunkSize(10);
                        importer.of("tab24", "test-badheadernames.csv", 1, PartitionBy.MONTH, (byte) ',', "Ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }
                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "Line\tTs\tD\tDescRipTION\n" +
                                    "line1\t1970-01-02T00:00:00.000000Z\t0.490933692472\tdesc 1\n" +
                                    "line2\t1970-01-03T00:00:00.000000Z\t0.105484410855\tdesc 2\n",
                            "select * from tab24", "ts", true, false, true
                    );
                }
        );
    }

    @Test
    public void testImportCsvIntoExistingTableWithColumnReorder() throws Exception {
        executeWithPool(
                16, 16, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    compiler.compile("create table t ( ts timestamp, line string, description string, d double ) timestamp(ts) partition by MONTH;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 16)) {
                        importer.setMinChunkSize(10);
                        importer.of("t", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true, null);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }
                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "line\tts\td\tdescription\n" +
                                    "line991\t1972-09-18T00:00:00.000000Z\t0.744582123075\tdesc 991\n" +
                                    "line992\t1972-09-19T00:00:00.000000Z\t0.107142280151\tdesc 992\n" +
                                    "line993\t1972-09-20T00:00:00.000000Z\t0.0974353165713\tdesc 993\n" +
                                    "line994\t1972-09-21T00:00:00.000000Z\t0.81272025622\tdesc 994\n" +
                                    "line995\t1972-09-22T00:00:00.000000Z\t0.566736320714\tdesc 995\n" +
                                    "line996\t1972-09-23T00:00:00.000000Z\t0.415739766699\tdesc 996\n" +
                                    "line997\t1972-09-24T00:00:00.000000Z\t0.378956184893\tdesc 997\n" +
                                    "line998\t1972-09-25T00:00:00.000000Z\t0.736755687844\tdesc 998\n" +
                                    "line999\t1972-09-26T00:00:00.000000Z\t0.910141500002\tdesc 999\n" +
                                    "line1000\t1972-09-27T00:00:00.000000Z\t0.918270255022\tdesc 1000\n",
                            "select line, ts, d, description from t limit -10",
                            "ts", true, false, true
                    );
                }
        );
    }

    @Test
    public void testImportCsvIntoExistingTableWithSymbol() throws Exception {
        executeWithPool(
                8, 4, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    compiler.compile("create table tab1 ( line symbol, ts timestamp, d double, description string) timestamp(ts) partition by MONTH;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 8)) {
                        importer.setMinChunkSize(10);
                        importer.of("tab1", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }
                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "line\tts\td\tdescription\n" +
                                    "line991\t1972-09-18T00:00:00.000000Z\t0.744582123075\tdesc 991\n" +
                                    "line992\t1972-09-19T00:00:00.000000Z\t0.107142280151\tdesc 992\n" +
                                    "line993\t1972-09-20T00:00:00.000000Z\t0.0974353165713\tdesc 993\n" +
                                    "line994\t1972-09-21T00:00:00.000000Z\t0.81272025622\tdesc 994\n" +
                                    "line995\t1972-09-22T00:00:00.000000Z\t0.566736320714\tdesc 995\n" +
                                    "line996\t1972-09-23T00:00:00.000000Z\t0.415739766699\tdesc 996\n" +
                                    "line997\t1972-09-24T00:00:00.000000Z\t0.378956184893\tdesc 997\n" +
                                    "line998\t1972-09-25T00:00:00.000000Z\t0.736755687844\tdesc 998\n" +
                                    "line999\t1972-09-26T00:00:00.000000Z\t0.910141500002\tdesc 999\n" +
                                    "line1000\t1972-09-27T00:00:00.000000Z\t0.918270255022\tdesc 1000\n",
                            "select line, ts, d, description from tab1 limit -10",
                            "ts", true, false, true
                    );
                }
        );
    }

    @Test
    public void testImportCsvIntoExistingTableWithSymbolsReordered() throws Exception {
        executeWithPool(
                8, 4, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {

                    final String tableName = "tableName";
                    compiler.compile("create table " + tableName + " ( ts timestamp, line symbol, d double, description symbol) timestamp(ts) partition by MONTH;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 8)) {
                        importer.setMinChunkSize(10);
                        importer.of(tableName, "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }
                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "line\tts\td\tdescription\n" +
                                    "line991\t1972-09-18T00:00:00.000000Z\t0.744582123075\tdesc 991\n" +
                                    "line992\t1972-09-19T00:00:00.000000Z\t0.107142280151\tdesc 992\n" +
                                    "line993\t1972-09-20T00:00:00.000000Z\t0.0974353165713\tdesc 993\n" +
                                    "line994\t1972-09-21T00:00:00.000000Z\t0.81272025622\tdesc 994\n" +
                                    "line995\t1972-09-22T00:00:00.000000Z\t0.566736320714\tdesc 995\n" +
                                    "line996\t1972-09-23T00:00:00.000000Z\t0.415739766699\tdesc 996\n" +
                                    "line997\t1972-09-24T00:00:00.000000Z\t0.378956184893\tdesc 997\n" +
                                    "line998\t1972-09-25T00:00:00.000000Z\t0.736755687844\tdesc 998\n" +
                                    "line999\t1972-09-26T00:00:00.000000Z\t0.910141500002\tdesc 999\n" +
                                    "line1000\t1972-09-27T00:00:00.000000Z\t0.918270255022\tdesc 1000\n",
                            "select line, ts, d, description from " + tableName + " limit -10",
                            "ts", true, false, true
                    );
                }
        );
    }

    @Test
    public void testImportCsvIntoNewTable() throws Exception {
        testImportCsvIntoNewTable0("tab25");
    }

    @Test
    public void testImportCsvIntoNewTableVanilla() throws Exception {
        // this does not use io_uring even on Linux
        ioURingFacade = new IOURingFacadeImpl() {
            @Override
            public boolean isAvailable() {
                return false;
            }
        };

        executeWithPool(
                4, 16, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    final String tableName = "tab27";
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.setMinChunkSize(10);
                        importer.of(tableName, "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }
                    refreshTablesInBaseEngine();
                    assertQuery(
                            "cnt\n" +
                                    "1000\n",
                            "select count(*) cnt from " + tableName,
                            null, false, true
                    );
                    assertQueryNoLeakCheck(
                            "line\tts\td\tdescription\n" +
                                    "line991\t1972-09-18T00:00:00.000000Z\t0.744582123075\tdesc 991\n" +
                                    "line992\t1972-09-19T00:00:00.000000Z\t0.107142280151\tdesc 992\n" +
                                    "line993\t1972-09-20T00:00:00.000000Z\t0.0974353165713\tdesc 993\n" +
                                    "line994\t1972-09-21T00:00:00.000000Z\t0.81272025622\tdesc 994\n" +
                                    "line995\t1972-09-22T00:00:00.000000Z\t0.566736320714\tdesc 995\n" +
                                    "line996\t1972-09-23T00:00:00.000000Z\t0.415739766699\tdesc 996\n" +
                                    "line997\t1972-09-24T00:00:00.000000Z\t0.378956184893\tdesc 997\n" +
                                    "line998\t1972-09-25T00:00:00.000000Z\t0.736755687844\tdesc 998\n" +
                                    "line999\t1972-09-26T00:00:00.000000Z\t0.910141500002\tdesc 999\n" +
                                    "line1000\t1972-09-27T00:00:00.000000Z\t0.918270255022\tdesc 1000\n",
                            "select * from " + tableName + " limit -10",
                            "ts", true, false, true
                    );
                }
        );
    }

    @Test
    public void testImportCsvSmallerFileBuffer() throws Exception {
        // the buffer is enough to fit only a few lines
        setProperty(PropertyKey.CAIRO_SQL_COPY_BUFFER_SIZE, 256);
        testImportCsvIntoNewTable0("tab26");
    }

    @Test
    public void testImportCsvWithLongTsIntoExistingTable() throws Exception {
        executeWithPool(
                3, 16, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(
                            compiler,
                            "CREATE TABLE reading (\n" +
                                    "  readingTypeId SYMBOL,\n" +
                                    "  value FLOAT,\n" +
                                    "  readingDate TIMESTAMP\n" +
                                    ") timestamp (readingDate) PARTITION BY DAY;", sqlExecutionContext
                    );

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 3)) {
                        importer.of("reading", "test-quotes-rawts.csv", 1, -1, (byte) ',', null, null, true, null);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }
                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "readingTypeId\tvalue\treadingDate\n" +
                                    "electricity.gbp.saving\t3600.0\t2020-01-01T00:00:00.000001Z\n" +
                                    "electricity.gbp.saving\t3600.0\t2020-01-01T00:00:00.000002Z\n" +
                                    "electricity.power.hour\t0.101\t2020-01-01T00:00:00.000003Z\n",
                            "select * from reading",
                            "readingDate", true, false, true
                    );
                }
        );
    }

    // missing symbols column is filled with nulls
    @Test
    public void testImportCsvWithMissingAndReorderedSymbolColumns() throws Exception {
        executeWithPool(
                8, 4, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab2 (other symbol, txt symbol, line symbol, ts timestamp, d symbol) timestamp(ts) partition by MONTH;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 8)) {
                        importer.setMinChunkSize(1);
                        importer.of("tab2", "test-quotes-small.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }
                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "other\ttxt\tline\tts\td\n" +
                                    "\tsome text\r\nspanning two lines\tline1\t2022-05-10T11:52:00.000000Z\t111.11\n" +
                                    "\tsome text\r\nspanning \r\nmany \r\nmany \r\nmany \r\nlines\tline2\t2022-05-11T11:52:00.000000Z\t222.22\n" +
                                    "\tsingle line text without quotes\tline3\t2022-05-11T11:52:00.001000Z\t333.33\n",
                            "select * from tab2 limit -10",
                            "ts", true, false, true
                    );
                }
        );
    }

    // all rows in the file fail on timestamp parsing so indexing phase will return empty result
    @Test
    public void testImportCsvWithTimestampNotMatchingInputFormatFails() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab3", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        Assert.assertEquals("All rows were skipped. Possible reasons: timestamp format mismatch or rows exceed maximum line length (65k).", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testImportEmptyCsv() throws Exception {
        executeWithPool(
                4, 16, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.setMinChunkSize(10);
                        importer.of(
                                "t",
                                "test-quotes-empty.csv",
                                1,
                                PartitionBy.MONTH,
                                (byte) ',',
                                "ts",
                                "yyyy-MM-ddTHH:mm:ss.SSSSSSZ",
                                true,
                                null
                        );
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "ignored empty input file [file='");
                    }
                }
        );
    }

    @Test
    public void testImportFailsOnBoundaryScanningIO() throws Exception {
        FilesFacade brokenFf = new TestFilesFacadeImpl() {
            @Override
            public long read(long fd, long buf, long len, long offset) {
                if (offset > 30000) {
                    return -1L;
                } else {
                    return super.read(fd, buf, len, offset);
                }
            }
        };

        executeWithPool(
                4, 8, brokenFf, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.setMinChunkSize(1);
                        importer.of("tab5", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), "import failed [phase=boundary_check, msg=`could not read import file");
                    }
                }
        );
    }

    @Test
    public void testImportFailsOnDataImportIO() throws Exception {

        // this is testing vanilla data copy method, ensure that io uring is disabled
        ioURingFacade = new IOURingFacadeImpl() {
            @Override
            public boolean isAvailable() {
                return false;
            }
        };

        FilesFacade brokenFf = new TestFilesFacadeImpl() {
            @Override
            public long read(long fd, long buf, long len, long offset) {
                if (offset == 31 && len == 1940) {
                    return -1;
                }
                return super.read(fd, buf, len, offset);
            }
        };

        executeWithPool(
                4, 8, brokenFf, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.setMinChunkSize(1);
                        importer.of("tab7", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), "import failed [phase=partition_import, msg=`could not read from file");
                    }
                }
        );
    }

    @Test
    public void testImportFailsOnFileOpenInBoundaryCheckPhase() throws Exception {
        FilesFacade brokenFf = new TestFilesFacadeImpl() {
            int count = 0;

            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, "test-quotes-big.csv")) {
                    if (count++ > 1) {
                        return -1;
                    }
                }
                return super.openRO(name);
            }
        };

        assertImportFailsInPhase("tab8", brokenFf, "boundary_check");
    }

    @Test
    public void testImportFailsOnFileOpenInBuildSymbolIndexPhase() throws Exception {
        FilesFacade brokenFf = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, "line.v") && stackContains("PhaseBuildSymbolIndex")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertImportFailsInPhase("tab14", brokenFf, "build_symbol_index");
    }

    @Test
    //"[5] Can't remove import directory path='C:\Users\bolo\AppData\Local\Temp\junit2458364502615821703\imports1181738016629600\tab\1972\2_1' errno=5"
    public void testImportFailsOnFileOpenInDataImportPhase() throws Exception {
        FilesFacade brokenFf = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, "3_1")) {
                    return -1;
                }
                return super.openRO(name);
            }
        };

        assertImportFailsInPhase("tab11", brokenFf, "partition_import");
    }

    @Test
    public void testImportFailsOnFileOpenInIndexingPhase() throws Exception {
        FilesFacade brokenFf = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, "test-quotes-big.csv") && stackContains("CsvFileIndexer")) {
                    return -1;
                }
                return super.openRO(name);
            }
        };

        assertImportFailsInPhase("tab9", brokenFf, "indexing");
    }

    @Test
    public void testImportFailsOnFileOpenInSymbolKeysUpdatePhase() throws Exception {
        FilesFacade brokenFf = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, "line.r") && stackContains("PhaseUpdateSymbolKeys")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertImportFailsInPhase("tab13", brokenFf, "update_symbol_keys");
    }

    @Test
    public void testImportFailsOnFileOpenInSymbolMergePhase() throws Exception {
        FilesFacade brokenFf = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, "line.c")) {
                    return -1;
                }
                return super.openRO(name);
            }
        };

        assertImportFailsInPhase("tab12", brokenFf, "symbol_table_merge");
    }

    @Test
    public void testImportFailsOnFileSortingInIndexingPhase() throws Exception {
        FilesFacade brokenFf = new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (Arrays.stream(new Exception().getStackTrace())
                        .anyMatch(ste -> ste.getClassName().endsWith("CsvFileIndexer") && ste.getMethodName().equals("sort"))) {
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }
        };

        assertImportFailsInPhase("tab10", brokenFf, "indexing");
    }

    @Test
    public void testImportFailsOnSourceFileIndexingIO() throws Exception {
        FilesFacade brokenFf = new TestFilesFacadeImpl() {
            @Override
            public long read(long fd, long buf, long len, long offset) {
                if (offset == 0 && len == 16797) {
                    return -1;
                }
                return super.read(fd, buf, len, offset);
            }
        };

        executeWithPool(
                4, 8, brokenFf, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.setMinChunkSize(1);
                        importer.of("tab6", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), "import failed [phase=indexing, msg=`could not read file");
                    }
                }
        );
    }

    @Test
    public void testImportFileFailsWhenImportingTextIntoBinaryColumn() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab36 ( ts timestamp, line string, d double, description binary ) timestamp(ts) partition by day;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab36", "test-quotes-big.csv", 1, PartitionBy.DAY, (byte) ',', "ts", null, true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), "cannot import text into BINARY column [index=3]");
                    }
                }
        );
    }

    @Test
    public void testImportFileFailsWhenIntermediateFilesCantBeMovedAndTargetDirCantBeCreated() throws Exception {
        String tab41 = "tab41";
        String dirName = tab41 + TableUtils.SYSTEM_TABLE_NAME_SUFFIX;

        FilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (Utf8s.containsAscii(path, File.separator + dirName + File.separator + "1970-06" + configuration.getAttachPartitionSuffix())) {
                    return -1;
                }
                return super.mkdirs(path, mode);
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                return Files.FILES_RENAME_ERR_EXDEV;
            }
        };

        testImportThrowsException(ff, tab41, "test-quotes-big.csv", PartitionBy.MONTH, "ts", null, "could not create partition directory");
    }

    @Test
    public void testImportFileFailsWhenIntermediateFilesCantBeMovedOrCopied() throws Exception {
        FilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int copy(LPSZ from, LPSZ to) {
                if (Utf8s.containsAscii(from, "tab42")) {
                    return -1;
                }
                return super.copy(from, to);
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                return Files.FILES_RENAME_ERR_EXDEV;
            }
        };

        testImportThrowsException(ff, "tab42", "test-quotes-big.csv", PartitionBy.MONTH, "ts", null, "could not copy partition file");
    }

    @Test
    public void testImportFileFailsWhenIntermediateFilesCantBeMovedToTargetDirForUnexpectedReason() throws Exception {
        FilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                return Files.FILES_RENAME_ERR_OTHER;
            }
        };

        testImportThrowsException(ff, "tab40", "test-quotes-big.csv", PartitionBy.MONTH, "ts", null, "could not copy partition file");
    }

    @Test
    public void testImportFileFailsWhenIntermediateTableDirectoryExistAndCantBeDeleted() throws Exception {
        String tab34 = "tab34";
        String tab34_0 = tab34 + "_0";
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                if (Utf8s.endsWithAscii(path, tab34_0)) {
                    return true;
                } else if (Utf8s.endsWithAscii(path, tab34_0 + File.separator + "_txn")) {
                    return true;
                }
                return super.exists(path);
            }

            @Override
            public boolean isDirOrSoftLinkDir(LPSZ path) {
                return exists(path);
            }

            @Override
            public boolean rmdir(Path name, boolean lazy) {
                if (Utf8s.endsWithAscii(name, tab34_0)) {
                    return false;
                }
                return super.rmdir(name, lazy);
            }
        };

        testImportThrowsException(
                ff,
                "tab34",
                "test-quotes-big.csv",
                PartitionBy.MONTH,
                "ts",
                null,
                "could not overwrite [tableName=" + tab34_0 + "]`]"
        );
    }

    @Test
    public void testImportFileFailsWhenIntermediateTableDirectoryIsMangled() throws Exception {
        String tab33 = "tab33";
        CharSequence fakeExists = tab33 + "_0";
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                if (Utf8s.endsWithAscii(path, fakeExists)) {
                    return true;
                }
                return super.exists(path);
            }

            @Override
            public boolean isDirOrSoftLinkDir(LPSZ path) {
                return exists(path);
            }
        };

        testImportThrowsException(ff, tab33, "test-quotes-big.csv", PartitionBy.MONTH, "ts", null, "import failed [phase=partition_import, msg=`name is reserved [tableName=" + tab33 + "_0]`]");
    }

    @Test
    public void testImportFileFailsWhenTargetTableDirectoryIsMangled() throws Exception {
        String tabex3 = "tabex3";
        CharSequence dirName = tabex3 + TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
        try (Path p = Path.getThreadLocal(root).concat(dirName).slash()) {
            TestFilesFacadeImpl.INSTANCE.mkdir(p.$(), configuration.getMkDirMode());
        }

        refreshTablesInBaseEngine();
        testImportThrowsException(tabex3, "test-quotes-big.csv", PartitionBy.MONTH, "ts", null, "name is reserved [tableName=" + tabex3 + "]");
    }

    @Test
    public void testImportFileFailsWhenTargetTableNameIsInvalid() throws Exception {
        testImportThrowsException(TestFilesFacadeImpl.INSTANCE, "../t", "test-quotes-big.csv", PartitionBy.MONTH, "ts", null, "invalid table name [table=../t]");
    }

    @Test
    public void testImportFileFailsWhenTimestampColumnIsMissingInInputFile() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab37 ( tstmp timestamp, line string, d double, description string ) timestamp(tstmp) partition by day;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab37", "test-quotes-big.csv", 1, PartitionBy.DAY, (byte) ',', "ts", null, true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), "invalid timestamp column [name='ts']");
                    }
                }
        );
    }

    @Test
    public void testImportFileFailsWhenWorkDirCantBeCreated() throws Exception {
        FilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdir(LPSZ path, int mode) {
                if (Utf8s.containsAscii(path, "tab39")) {
                    return -1;
                }
                return super.mkdir(path, mode);
            }
        };

        testImportThrowsException(ff, "tab39", "test-quotes-big.csv", PartitionBy.MONTH, "ts", null, "could not create temporary import work directory");
    }

    @Test
    public void testImportFileFailsWhenWorkDirectoryDoesNotExistAndCantBeCreated() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            final String tempDir = inputWorkRoot + File.separator;

            @Override
            public boolean exists(LPSZ path) {
                if (Utf8s.equalsAscii(tempDir, path)) {
                    return false;
                }
                return super.exists(path);
            }

            @Override
            public boolean rmdir(Path name, boolean lazy) {
                if (Utf8s.equalsAscii(tempDir, name)) {
                    return false;
                }
                return super.rmdir(name, lazy);
            }
        };

        testImportThrowsException(ff, "tab35", "test-quotes-big.csv", PartitionBy.MONTH, "ts", null, "could not create import work root directory");
    }

    @Test
    public void testImportFileFailsWhenWorkDirectoryExistAndCantBeDeleted() throws Exception {
        CharSequence dirName = "tab34";
        String mangledPartDir = dirName + "_0";
        FilesFacade ff = new TestFilesFacadeImpl() {

            @Override
            public boolean exists(LPSZ path) {
                if (Utf8s.endsWithAscii(path, mangledPartDir)) {
                    return true;
                } else if (Utf8s.endsWithAscii(path, mangledPartDir + File.separator + "_txn")) {
                    return true;
                }
                return super.exists(path);
            }

            @Override
            public boolean isDirOrSoftLinkDir(LPSZ path) {
                return exists(path);
            }

            @Override
            public boolean rmdir(Path name, boolean lazy) {
                if (Utf8s.endsWithAscii(name, mangledPartDir)) {
                    return false;
                }
                return super.rmdir(name, lazy);
            }
        };

        refreshTablesInBaseEngine();
        testImportThrowsException(
                ff,
                "tab34",
                "test-quotes-big.csv",
                PartitionBy.MONTH,
                "ts",
                null,
                "could not overwrite [tableName=" + mangledPartDir + "]`]"
        );
    }

    @Test
    public void testImportFileSetsDateColumnToNullIfCsvStructureCheckCantDetectACommonFormat() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab38 ( line string, ts timestamp, d date, txt string ) timestamp(ts) partition by day;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab38", "test-quotes-small.csv", 1, PartitionBy.DAY, (byte) ',', "ts", null, true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }

                    refreshTablesInBaseEngine();
                    assertQuery(
                            "line\tts\td\ttxt\n" +
                                    "line1\t2022-05-10T11:52:00.000000Z\t\tsome text\r\nspanning two lines\n" +
                                    "line2\t2022-05-11T11:52:00.000000Z\t\tsome text\r\nspanning \r\nmany \r\nmany \r\nmany \r\nlines\n" +
                                    "line3\t2022-05-11T11:52:00.001000Z\t\tsingle line text without quotes\n",
                            "select * from tab38", "ts", true, true
                    );
                }
        );
    }

    @Test
    public void testImportFileSkipsLinesLongerThan65kChars() throws Exception {
        executeWithPool(
                8, 4, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab ( ts timestamp, description string) timestamp(ts) partition by MONTH;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 8)) {
                        importer.setMinChunkSize(10);
                        importer.of("tab", "test-row-over-65k.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }
                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "ts\tdescription\n" +
                                    "2022-05-11T11:52:00.000000Z\tb\n",
                            "select * from tab",
                            "ts", true, false, true
                    );
                }
        );
    }

    @Test
    public void testImportFileWhenImportingAfterColumnWasRecreated() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab62 ( line string, ts timestamp, d double, txt string ) timestamp(ts) partition by day;", sqlExecutionContext);
                    execute(compiler, "alter table tab62 drop column line;", sqlExecutionContext);
                    execute(compiler, "alter table tab62 add column line symbol;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab62", "test-quotes-small.csv", 1, PartitionBy.DAY, (byte) ',', "ts", null, true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }
                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "count\n3\n",
                            "select count() from tab62", null, false, false, true
                    );
                }
        );
    }

    @Test
    public void testImportFileWhenImportingAfterColumnWasRecreatedNoHeader() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab44 ( line string, ts timestamp, d double, txt string ) timestamp(ts) partition by day;", sqlExecutionContext);
                    execute(compiler, "alter table tab44 drop column txt;", sqlExecutionContext);
                    execute(compiler, "alter table tab44 add column txt symbol;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab44", "test-noheader.csv", 1, PartitionBy.DAY, (byte) ',', "ts", null, false);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }
                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "count\n3\n",
                            "select count() from tab44", null, false, false, true
                    );
                }
        );
    }

    @Test
    public void testImportFileWithHeaderButDifferentColumnOrderWhenTargetTableDoesExistSuccess() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    compiler.compile("create table tab51 ( ts timestamp, line string, d double, description string ) timestamp(ts) partition by month;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab51", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", null, true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }

                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "count\n1000\n",
                            "select count(*) from tab51", null, false, false, true
                    );
                }
        );
    }

    @Test
    public void testImportFileWithHeaderButInputPartitionByNotMatchingTargetTables() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab45 ( ts timestamp, s string, d double, i int ) timestamp(ts) partition by DAY;", sqlExecutionContext);
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab45", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) -1, "ts", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (Exception e) {
                        Assert.assertEquals("declared partition by unit doesn't match table's", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testImportFileWithHeaderButMissingTimestampColumn() throws Exception {
        testImportThrowsException("tabex2", "test-quotes-big.csv", PartitionBy.DAY, "ts1", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", "timestamp column 'ts1' not found in file header");
    }

    @Test
    public void testImportFileWithHeaderButMissingTimestampColumnName() throws Exception {
        testImportThrowsException("test44", "test-quotes-big.csv", PartitionBy.MONTH, null, "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", "timestamp column must be set when importing to new table");
    }

    @Test
    public void testImportFileWithHeaderButPartitionByNotSpecifiedAndTargetTableDoesntExist() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab49", "test-quotes-big.csv", 1, -1, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (Exception e) {
                        Assert.assertEquals("partition by unit must be set when importing to new table", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testImportFileWithHeaderButPartitionByNotSpecifiedAndTargetTableIsNotPartitioned() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab46 ( ts timestamp, s string, d double, i int ) timestamp(ts);", sqlExecutionContext);
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab46", "test-quotes-big.csv", 1, -1, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (Exception e) {
                        Assert.assertEquals("target table is not partitioned", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testImportFileWithHeaderButPartitionBySetToNone() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab48", "test-quotes-big.csv", 1, PartitionBy.NONE, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), "partition strategy for parallel import cannot be NONE");
                    }
                }
        );
    }

    @Test
    public void testImportFileWithHeaderButTargetTableIsNotPartitioned2() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab47 ( ts timestamp, s string, d double, i int ) timestamp(ts);", sqlExecutionContext);
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab47", "test-quotes-big.csv", 1, -1, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (Exception e) {
                        Assert.assertEquals("target table is not partitioned", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testImportFileWithHeaderButWrongTypeOfTimestampColumn() throws Exception {
        testImportThrowsException("test44", "test-quotes-big.csv", PartitionBy.MONTH, "d", null, "column is not a timestamp [no=2, name='d']");
    }

    @Test
    public void testImportFileWithHeaderIntoExistingTableFailsBecauseInputColumnCountIsLargerThanTables() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab59 ( line string, ts timestamp, d double ) timestamp(ts) partition by MONTH;", sqlExecutionContext);
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab59", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail("exception expected");
                    } catch (Exception e) {
                        Assert.assertEquals("column count mismatch [textColumnCount=4, tableColumnCount=3, table=tab59]", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testImportFileWithHeaderIntoExistingTableWhenInputColumnCountIsSmallerThanTablesSucceedsAndInsertsNullIntoMissingColumns() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab58 ( line string, ts timestamp, d double, description string, i int, l long ) timestamp(ts) partition by MONTH;", sqlExecutionContext);
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab58", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }

                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "count\ticount\tlcount\n1000\t1000\t1000\n",
                            "select count(*), sum( case when i is null then 1 else 0 end) icount, sum( case when l is null then 1 else 0 end) lcount from tab58", null, false, false, true
                    );
                }
        );
    }

    @Test//it fails even though ts column name and format are specified
    public void testImportFileWithHeaderIntoNewTableFailsBecauseTsColCantBeFoundInFileHeader() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab56", "test-quotes-oneline.csv", 1, PartitionBy.DAY, (byte) ',', "ts2", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", false);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        Assert.assertEquals("timestamp column 'ts2' not found in file header", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testImportFileWithHeaderWhenTargetTableDoesntExistSuccess() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab50", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);

                        refreshTablesInBaseEngine();
                        assertQueryNoLeakCheck(
                                "count\n1000\n",
                                "select count(*) from tab50", null, false, false, true
                        );
                    }
                }
        );
    }

    @Test
    public void testImportFileWithHeaderWithForceHeaderIntoNewTableFailsBecauseColumnNamesRepeat() throws Exception {
        assertColumnNameException("test-header-dupvalues.csv", true, "duplicate column name found [no=4,name=e]");
    }

    @Test
    public void testImportFileWithHeaderWithForceHeaderIntoNewTableFailsBecauseColumnNamesRepeatWithDifferentCase() throws Exception {
        assertColumnNameException("test-header-dupvalues-differentcase.csv", true, "duplicate column name found [no=4,name=e]");
    }

    @Test
    public void testImportFileWithHeaderWithoutForceHeaderIntoNewTableFailsBecauseColumnNamesRepeat() throws Exception {
        assertColumnNameException("test-header-dupvalues.csv", false, "duplicate column name found [no=4,name=e]");
    }

    @Test
    public void testImportFileWithHeaderWithoutForceHeaderIntoNewTableFailsBecauseColumnNamesRepeatWithDifferentCase() throws Exception {
        assertColumnNameException("test-header-dupvalues-differentcase.csv", false, "duplicate column name found [no=4,name=e]");
    }

    @Test
    public void testImportFileWithIncompleteHeaderWithForceHeaderIntoNewTable() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab61", "test-header-missing.csv", 1, PartitionBy.DAY, (byte) ',', "ts", null, true);
                        importer.process(AllowAllSecurityContext.INSTANCE);

                        refreshTablesInBaseEngine();
                        assertQueryNoLeakCheck(
                                "ts\tf3\tf3_\tf3__\tf4\n" +
                                        "1972-09-28T00:00:00.000000Z\ta1\tb1\ta1\te1\n" +
                                        "1972-09-28T00:00:00.000000Z\ta2\tb2\ta2\te2\n", "select * from tab61", "ts", true, false, true
                        );
                    }
                }
        );
    }

    @Test
    public void testImportFileWithIncompleteHeaderWithForceHeaderIntoNewTableFailsOnUniqueColumnNameGeneration() throws Exception {
        assertColumnNameException("test-header-missing-long.csv", true, "Failed to generate unique name for column [no=22]");
    }

    @Test
    public void testImportFileWithNoHeaderIntoExistingTableFailsBecauseTsPositionInTableIsDifferentFromFile() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab53 ( ts timestamp, s string, d double, i int ) timestamp(ts) partition by day;", sqlExecutionContext);
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab53", "test-noheader.csv", 1, PartitionBy.DAY, (byte) ',', null, null, false);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        Assert.assertEquals("column is not a timestamp [no=0, name='']", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testImportFileWithNoHeaderIntoExistingTableSucceedsBecauseTsPositionInTableIsSameAsInFile() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab57 ( s string, ts timestamp, d double, s2 string ) timestamp(ts) partition by day;", sqlExecutionContext);
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab57", "test-noheader.csv", 1, PartitionBy.DAY, (byte) ',', null, null, false);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }
                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck("count\n3\n", "select count(*) from tab57", null, false, false, true);
                }
        );
    }

    @Test
    public void testImportFileWithNoHeaderIntoNewTableFailsBecauseTsColCantBeFoundInFileHeader() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab54", "test-noheader.csv", 1, PartitionBy.DAY, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", false);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        Assert.assertEquals("timestamp column 'ts' not found in file header", e.getMessage());
                    }
                }
        );
    }

    @Test
    //when there is no header and header is not forced then target tabel columns get following names : f0, f1, ..., fN
    public void testImportFileWithNoHeaderIntoNewTableSucceedsBecauseSyntheticColumnNameIsUsed() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab55", "test-noheader.csv", 1, PartitionBy.DAY, (byte) ',', "f1", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", false);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }
                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck("count\n3\n", "select count(*) from tab55", null, false, false, true);
                }
        );
    }

    @Test
    public void testImportFileWithoutHeader() throws Exception {
        testImportThrowsException("tabex1", "test-quotes-oneline.csv", PartitionBy.MONTH, "ts", null, "column is not a timestamp [no=1, name='ts']");
    }

    @Test
    public void testImportFileWithoutHeaderWithForceHeaderIntoNewTableFailsBecauseColumnNamesRepeat() throws Exception {
        assertColumnNameException("test-noheader-dupvalues.csv", true, "duplicate column name found [no=3,name=_100i]");
    }

    @Test
    public void testImportFileWithoutHeaderWithForceHeaderIntoNewTableFailsBecauseColumnNamesRepeatWithDifferentCase() throws Exception {
        assertColumnNameException("test-noheader-dupvalues-differentcase.csv", true, "duplicate column name found [no=3,name=_100i]");
    }

    @Test
    public void testImportFileWithoutHeaderWithoutForceHeaderIntoNewTableFailsBecauseColumnNamesRepeat() throws Exception {
        assertColumnNameException("test-noheader-dupvalues.csv", false, "duplicate column name found [no=3,name=_100i]");
    }

    @Test
    public void testImportFileWithoutHeaderWithoutForceHeaderIntoNewTableFailsBecauseColumnNamesRepeatWithDifferentCase() throws Exception {
        assertColumnNameException("test-noheader-dupvalues-differentcase.csv", false, "duplicate column name found [no=3,name=_100i]");
    }

    @Test
    @Ignore("the cursor returns more rows than expected")
    public void testImportIntoExistingTableWithIndex() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    compiler.compile(
                            "create table alltypes (\n" +
                                    "  bo boolean,\n" +
                                    "  by byte,\n" +
                                    "  sh short,\n" +
                                    "  ch char,\n" +
                                    "  in_ int,\n" +
                                    "  lo long,\n" +
                                    "  dat date, \n" +
                                    "  tstmp timestamp, \n" +
                                    "  ft float,\n" +
                                    "  db double,\n" +
                                    "  str string,\n" +
                                    "  sym symbol index,\n" +
                                    "  l256 long256," +
                                    "  ge geohash(20b)" +
                                    ") timestamp(tstmp) partition by DAY;", sqlExecutionContext
                    );
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("alltypes", "test-alltypes.csv", 1, PartitionBy.DAY, (byte) ',', "tstmp", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }

                    // verify that the index is present
                    try (TableReader reader = getReader("alltypes")) {
                        TableReaderMetadata metadata = reader.getMetadata();
                        int columnIndex = metadata.getColumnIndex("sym");
                        Assert.assertTrue("Column sym must exist", columnIndex >= 0);

                        BitmapIndexReader indexReader = reader.getBitmapIndexReader(0, columnIndex, BitmapIndexReader.DIR_FORWARD);
                        Assert.assertNotNull(indexReader);
                        Assert.assertTrue(indexReader.getKeyCount() > 0);
                        Assert.assertTrue(indexReader.getValueMemorySize() > 0);

                        // expect only the very first row in zero partition to have 'sy1' symbol value
                        StaticSymbolTable symbolTable = reader.getSymbolTable(columnIndex);
                        RowCursor ic = indexReader.getCursor(true, TableUtils.toIndexKey(symbolTable.keyOf("sy1")), 0, 1);
                        Assert.assertTrue(ic.hasNext());
                        Assert.assertEquals(0, ic.next());
                        Assert.assertFalse(ic.hasNext());
                    }

                    // run a query that uses the index
                    assertQueryNoLeakCheck(
                            "bo\tby\tsh\tch\tin_\tlo\tdat\ttstmp\tft\tdb\tstr\tsym\tl256\tge\n" +
                                    "false\t106\t22716\tG\t1\t1\t1970-01-02T00:00:00.000Z\t1970-01-02T00:00:00.000000Z\t1.1000\t1.2\ts1\tsy1\t0x0adaa43b7700522b82f4e8d8d7b8c41a985127d17ca3926940533c477c927a33\tu33d\n" +
                                    "true\t61\t-17553\tD\t10\t10\t1970-01-11T00:00:00.000Z\t1970-01-11T00:00:00.000000Z\t10.1000\t10.2\ts10\tsy10\t0x83e9d33db60120e69ba3fb676e3280ed6a6e16373be3139063343d28d3738449\tu33d\n",
                            "select * from alltypes where sym in ('sy1','sy10')", "tstmp", true, false, true
                    );
                }
        );
    }

    @Test
    public void testImportIntoNonEmptyTableReturnsError() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab52 ( ts timestamp, s string, d double, i int ) timestamp(ts) partition by day;", sqlExecutionContext);
                    execute(compiler, "insert into tab52 select cast(x as timestamp), 'a', x, x from long_sequence(10);", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab52", "test-quotes-big.csv", 1, PartitionBy.DAY, (byte) ',', "ts", null, true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        TestUtils.assertEquals("target table must be empty [table=tab52]", e.getFlyweightMessage());
                    }
                }
        );
    }

    @Test
    public void testImportIsCancelled() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler1, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab43", "test-quotes-big.csv", 1, PartitionBy.DAY, (byte) ',', "ts", null, true, () -> true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), "import cancelled [phase=boundary_check, msg=`Cancelled`]");
                    }
                }
        );
    }

    @Test
    public void testImportNoRowsCsv() throws Exception {
        executeWithPool(
                4, 16, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.setMinChunkSize(10);
                        importer.of(
                                "t",
                                "test-quotes-header-only.csv",
                                1,
                                PartitionBy.MONTH,
                                (byte) ',',
                                "ts",
                                "yyyy-MM-ddTHH:mm:ss.SSSSSSZ",
                                true,
                                null
                        );
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        TestUtils.assertContains(e.getMessage(), "No rows in input file to import.");
                    }
                }
        );
    }

    @Test
    public void testImportTimestampTypFormatMismatch() throws Exception {
        try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
            importer.of("timestamp_test", "test-timestamps.csv", 1, PartitionBy.DAY, (byte) ',', "ts_ns", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", true);
            importer.process(AllowAllSecurityContext.INSTANCE);
            Assert.fail();
        } catch (TextImportException e) {
            Assert.assertEquals("All rows were skipped. Possible reasons: timestamp format mismatch or rows exceed maximum line length (65k).", e.getMessage());
        }
    }

    @Test
    public void testImportTimestampTypeToExist1() throws Exception {
        executeWithPool(4, 8, (engine, compiler, sqlExecutionContext) -> testImportTimestampIntoExisting(
                engine, compiler, sqlExecutionContext, "ts"
        ));
    }

    @Test
    public void testImportTimestampTypeToExist2() throws Exception {
        executeWithPool(4, 8, (engine, compiler, sqlExecutionContext) -> testImportTimestampIntoExisting(
                engine, compiler, sqlExecutionContext, "ts_ns"
        ));
    }

    @Test
    public void testImportTimestampTypeToExistWithTypeMismatch1() throws Exception {
        executeWithPool(4, 8, (engine, compiler, sqlExecutionContext) -> testImportTimestampIntoExistingWithTypeMismatch(
                engine, compiler, sqlExecutionContext, "ts"
        ));
    }

    @Test
    public void testImportTimestampTypeToExistWithTypeMismatch2() throws Exception {
        executeWithPool(4, 8, (engine, compiler, sqlExecutionContext) -> testImportTimestampIntoExistingWithTypeMismatch(
                engine, compiler, sqlExecutionContext, "ts_ns"
        ));
    }

    @Test
    public void testImportTimestampTypeToExistWithTypeMismatch3_specifyTimestampFormat() throws Exception {
        execute(
                "CREATE TABLE 'timestamp_test' ( \n" +
                        "    id INT,\n" +
                        "    ts TIMESTAMP_NS,\n" +
                        "    ts_ns TIMESTAMP\n" +
                        ") timestamp(ts_ns) PARTITION BY DAY",
                sqlExecutionContext
        );
        try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
            importer.of("timestamp_test", "test-timestamps.csv", 1, PartitionBy.DAY, (byte) ',', "ts_ns", "yyyy-MM-ddTHH:mm:ss.SSSSSSNNNZ", true);
            importer.process(AllowAllSecurityContext.INSTANCE);
        }

        refreshTablesInBaseEngine();
        assertQueryNoLeakCheck(
                "id\tts\tts_ns\n" +
                        "1\t2025-08-05T00:00:00.000001000Z\t2025-08-05T00:00:00.000000Z\n" +
                        "2\t2025-08-06T00:00:00.000002000Z\t2025-08-06T00:00:00.000000Z\n" +
                        "3\t2025-08-07T00:00:00.000003000Z\t2025-08-07T00:00:00.000000Z\n" +
                        "4\t2025-08-08T00:00:00.000004000Z\t2025-08-08T00:00:00.000000Z\n" +
                        "5\t2025-08-09T00:00:00.000005000Z\t2025-08-09T00:00:00.000000Z\n" +
                        "6\t2025-08-10T00:00:00.000006000Z\t2025-08-10T00:00:00.000000Z\n" +
                        "7\t2025-08-11T00:00:00.000007000Z\t2025-08-11T00:00:00.000000Z\n" +
                        "8\t2025-08-12T00:00:00.000008000Z\t2025-08-12T00:00:00.000000Z\n" +
                        "9\t2025-08-13T00:00:00.000009000Z\t2025-08-13T00:00:00.000000Z\n",
                "select * from timestamp_test",
                "ts_ns",
                true,
                false,
                true
        );
    }

    @Test
    public void testImportTimestampTypeToNew1() throws Exception {
        executeWithPool(4, 8, (engine, compiler, sqlExecutionContext) -> testImportTimestampTypeToNew(
                engine, compiler, sqlExecutionContext, "ts"
        ));
    }

    @Test
    public void testImportTimestampTypeToNew2() throws Exception {
        executeWithPool(4, 8, (engine, compiler, sqlExecutionContext) -> testImportTimestampTypeToNew(
                engine, compiler, sqlExecutionContext, "ts_ns"
        ));
    }

    @Test
    public void testImportTooSmallFileBufferURing() throws Exception {
        Assume.assumeTrue(configuration.getIOURingFacade().isAvailable());
        testImportTooSmallFileBuffer0("tab32");
    }

    @Test
    public void testImportTooSmallFileBufferVanilla() throws Exception {
        ioURingFacade = new IOURingFacadeImpl() {
            @Override
            public boolean isAvailable() {
                return false;
            }
        };
        testImportTooSmallFileBuffer0("tab31");
    }

    @Test
    public void testImportURingEnqueueFails() throws Exception {
        Assume.assumeTrue(configuration.getIOURingFacade().isAvailable());

        class TestIOURing extends IOURingImpl {
            private final Rnd rnd = new Rnd();

            public TestIOURing(IOURingFacade facade, int capacity) {
                super(facade, capacity);
            }

            @Override
            public long enqueueRead(long fd, long offset, long bufAddr, int len) {
                if (rnd.nextBoolean()) {
                    return super.enqueueRead(fd, offset, bufAddr, len);
                }
                return -1;
            }
        }
        ioURingFacade = new IOURingFacadeImpl() {
            @Override
            public IOURing newInstance(int capacity) {
                return new TestIOURing(this, capacity);
            }
        };

        executeWithPool(
                2, 16, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    final String tableName = "tab29";
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 2)) {
                        importer.setMinChunkSize(10);
                        importer.of(tableName, "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "io_uring error");
                    }
                }
        );
    }

    @Test
    public void testImportURingReadFails() throws Exception {

        Assume.assumeTrue(configuration.getIOURingFacade().isAvailable());

        class TestIOURing extends IOURingImpl {
            private final Rnd rnd = new Rnd();

            public TestIOURing(IOURingFacade facade, int capacity) {
                super(facade, capacity);
            }

            @Override
            public int getCqeRes() {
                return rnd.nextBoolean() ? super.getCqeRes() : -1;
            }
        }
        ioURingFacade = new IOURingFacadeImpl() {
            @Override
            public IOURing newInstance(int capacity) {
                return new TestIOURing(this, capacity);
            }
        };

        executeWithPool(
                2, 16, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    final String tableName = "tab30";
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 2)) {
                        importer.setMinChunkSize(10);
                        importer.of(tableName, "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not read from file");
                    }
                }
        );
    }

    @Test
    public void testImportURingShuffleCqe() throws Exception {

        Assume.assumeTrue(configuration.getIOURingFacade().isAvailable());

        class TestIOURing extends IOURingImpl {
            private final Rnd rnd = new Rnd();
            private final LongList stuff = new LongList();
            private int stuffIndex = 0;
            private int stuffMax = 0;

            public TestIOURing(IOURingFacade facade, int capacity) {
                super(facade, capacity);
            }

            @Override
            public long getCqeId() {
                int index = stuffIndex;
                return stuff.getQuick(index * 2);
            }

            @Override
            public int getCqeRes() {
                int index = stuffIndex;
                return (int) stuff.getQuick(index * 2 + 1);
            }

            @Override
            public boolean nextCqe() {
                if (++stuffIndex < stuffMax) {
                    return true;
                }

                boolean next = super.nextCqe();
                if (!next) {
                    return false;
                }

                stuff.clear();

                do {
                    stuff.add(super.getCqeId(), super.getCqeRes());
                } while (super.nextCqe());

                stuff.shuffle(rnd, 1);

                stuffIndex = 0;
                stuffMax = stuff.size() / 2;

                return true;
            }
        }
        ioURingFacade = new IOURingFacadeImpl() {
            @Override
            public IOURing newInstance(int capacity) {
                return new TestIOURing(this, capacity);
            }
        };

        executeWithPool(
                2, 16, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    final String tableName = "tab28";
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 2)) {
                        importer.setMinChunkSize(10);
                        importer.of(tableName, "test-quotes-big-reverseorder.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }
                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "line\tts\td\tdescription\n" +
                                    "line10\t1972-09-18T00:00:00.000000Z\t0.928671996857\tdesc 10\n" +
                                    "line9\t1972-09-19T00:00:00.000000Z\t0.123847438134\tdesc 9\n" +
                                    "line8\t1972-09-20T00:00:00.000000Z\t0.450854040396\tdesc 8\n" +
                                    "line7\t1972-09-21T00:00:00.000000Z\t0.207871778557\tdesc 7\n" +
                                    "line6\t1972-09-22T00:00:00.000000Z\t0.341597834365\tdesc 6\n" +
                                    "line5\t1972-09-23T00:00:00.000000Z\t0.5071712972\tdesc 5\n" +
                                    "line4\t1972-09-24T00:00:00.000000Z\t0.426072974125\tdesc 4\n" +
                                    "line3\t1972-09-25T00:00:00.000000Z\t0.525414887561\tdesc 3\n" +
                                    "line2\t1972-09-26T00:00:00.000000Z\t0.105484410855\tdesc 2\n" +
                                    "line1\t1972-09-27T00:00:00.000000Z\t0.490933692472\tdesc 1\n",
                            "select * from " + tableName + " limit -10",
                            "ts", true, false, true
                    );
                }
        );
    }

    @Test
    public void testImportVarcharDoubleQuotes() throws Exception {
        executeWithPool(
                2,
                8,
                (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(
                            compiler,
                            "create table x (\n" +
                                    "  str varchar,\n" +
                                    "  ts timestamp" +
                                    ") timestamp(ts) partition by DAY;", sqlExecutionContext
                    );
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 2)) {
                        importer.of("x", "test-varchar-double-quotes.csv", 1, PartitionBy.DAY, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }

                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "str\tts\n" +
                                    "foobar\t1970-01-02T00:00:00.000000Z\n" +
                                    "foobar foobar foobar foobar\t1970-01-02T00:00:00.000000Z\n" +
                                    "foobar foobar \"foobar\" foobar foobar\t1970-01-02T00:00:00.000000Z\n" +
                                    "\"foobar\" foobar foobar foobar\t1970-01-02T00:00:00.000000Z\n" +
                                    "foobar\"\"\t1970-01-02T00:00:00.000000Z\n" +
                                    "фубар \"фубар\" фубар\t1970-01-02T00:00:00.000000Z\n",
                            "x",
                            "ts",
                            true,
                            false,
                            true
                    );
                }
        );
    }

    @Test
    public void testImportWithSkipAllAtomicityFailsWhenNonTimestampColumnCantBeParsedAtDataImportPhase() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab ( ts timestamp, line string, description double, d double ) timestamp(ts) partition by MONTH;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true, null, Atomicity.SKIP_ALL);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        TestUtils.assertContains(e.getMessage(), "import failed [phase=partition_import, msg=`bad syntax");
                    }
                }
        );
    }

    @Test
    public void testImportWithSkipAllAtomicityFailsWhenTimestampCantBeParsedAtIndexingPhase() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab22", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss", true, null, Atomicity.SKIP_ALL);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        TestUtils.assertContains(e.getMessage(), "import failed [phase=indexing, msg=`could not parse timestamp [line=0, column=1]`]");
                    }
                }
        );
    }

    @Test
    public void testImportWithSkipColumnAtomicityImportsAllRowsExceptOneFailingOnTimestamp() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    compiler.compile(
                            "create table alltypes (\n" +
                                    "  bo boolean,\n" +
                                    "  by byte,\n" +
                                    "  sh short,\n" +
                                    "  ch char,\n" +
                                    "  in_ int,\n" +
                                    "  lo long,\n" +
                                    "  dat date, \n" +
                                    "  tstmp timestamp, \n" +
                                    "  ft float,\n" +
                                    "  db double,\n" +
                                    "  str string,\n" +
                                    "  sym symbol,\n" +
                                    "  l256 long256," +
                                    "  ge geohash(20b)" +
                                    ") timestamp(tstmp) partition by DAY;", sqlExecutionContext
                    );

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("alltypes", "test-errors.csv", 1, PartitionBy.DAY, (byte) ',', "tstmp", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true, null, Atomicity.SKIP_COL);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }

                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "cnt\n13\n",
                            "select count(*) cnt from alltypes", null, false, false, true
                    );
                }
        );
    }

    @Test
    public void testImportWithSkipRowAtomicityImportsNoRowsWhenNonTimestampColumnCantBeParsed() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    // the StrSym should be 'symbol' and DoubleCol should be 'double'
                    // we intentionally create these columns with a wrong type so the ParallelCsvFileImporter fails to parse these columns
                    // the subsequent assert checks no row was imported - as the atomicity level is set to SKIP_ROW
                    execute(compiler, "create table tab23 (StrSym int, Int symbol,Int_Col int, DoubleCol int,IsoDate timestamp,Fmt1Date timestamp,Fmt2Date date,Phone string,boolean boolean,long long) timestamp(IsoDate) partition by MONTH;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab23", "test-import.csv", 1, PartitionBy.MONTH, (byte) ',', "IsoDate", "yyyy-MM-ddTHH:mm:ss.SSSZ", false, null, Atomicity.SKIP_ROW);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }

                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck("cnt\n0\n", "select count(*) cnt from tab23", null, false, false, true);
                }
        );
    }

    @Test
    public void testImportWithSkipRowAtomicityImportsOnlyRowsWithNoParseErrors() throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(
                            compiler,
                            "create table alltypes (\n" +
                                    "  bo boolean,\n" +
                                    "  by byte,\n" +
                                    "  sh short,\n" +
                                    "  ch char,\n" +
                                    "  in_ int,\n" +
                                    "  lo long,\n" +
                                    "  dat date, \n" +
                                    "  tstmp timestamp, \n" +
                                    "  ft float,\n" +
                                    "  db double,\n" +
                                    "  str string,\n" +
                                    "  sym symbol,\n" +
                                    "  l256 long256," +
                                    "  ge geohash(20b)" +
                                    ") timestamp(tstmp) partition by DAY;", sqlExecutionContext
                    );

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("alltypes", "test-errors.csv", 1, PartitionBy.DAY, (byte) ',', "tstmp", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true, null, Atomicity.SKIP_ROW);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }

                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck(
                            "bo\tby\tsh\tch\tin_\tlo\tdat\ttstmp\tft\tdb\tstr\tsym\tl256\tge\n" +
                                    "false\t106\t22716\tG\t1\t1\t1970-01-01T00:00:00.000Z\t1970-01-02T00:00:00.000000Z\t1.1\t1.2\ts1\tsy1\t0x0adaa43b7700522b82f4e8d8d7b8c41a985127d17ca3926940533c477c927a33\tu33d\n" +
                                    "false\t29\t8654\tS\t2\t2\t1970-01-02T00:00:00.000Z\t1970-01-03T00:00:00.000000Z\t2.1\t2.2\ts2\tsy2\t0x593c9b7507c60ec943cd1e308a29ac9e645f3f4104fa76983c50b65784d51e37\tu33d\n" +
                                    "false\t105\t-11072\tC\t4\t4\t1970-01-04T00:00:00.000Z\t1970-01-05T00:00:00.000000Z\t4.1\t4.2\ts4\tsy4\t0x64ad74a1e1e5e5897c61daeff695e8be6ab8ea52090049faa3306e2d2440176e\tu33d\n" +
                                    "false\t123\t8110\tC\t5\t5\t1970-01-04T00:00:00.000Z\t1970-01-06T00:00:00.000000Z\t5.1\t5.2\ts5\tsy5\t0x5a86aaa24c707fff785191c8901fd7a16ffa1093e392dc537967b0fb8165c161\tu33d\n" +
                                    "true\t102\t5672\tS\t8\t8\t1970-01-08T00:00:00.000Z\t1970-01-09T00:00:00.000000Z\t8.1\t8.2\ts8\tsy8\t0x6df9f4797b131d69aa4f08d320dde2dc72cb5a65911401598a73264e80123440\tu33d\n", //date format discovery is flawed
                            //"false\t31\t-150\tI\t14\t14\t1970-01-14T00:00:00.000Z\t1970-01-15T00:00:00.000000Z\t14.1000\t14.2\ts13\tsy14\t\tu33d\n",//long256 triggers error for bad values
                            "select * from alltypes", "tstmp", true, false, true
                    );
                }
        );
    }

    @Test
    public void testImportWithZeroLengthQueueReturnsError() throws Exception {
        executeWithPool(
                2, 0, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 2)) {
                        importer.setMinChunkSize(1);
                        importer.of("tab16", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Parallel import queue size cannot be zero");
                    }
                }
        );
    }

    @Test
    public void testImportWithZeroWorkersFails() throws Exception {
        executeWithPool(
                0, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 0)) {
                        importer.setMinChunkSize(1);
                        importer.of("tab15", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Invalid worker count set ");
                    }
                }
        );
    }

    @Test
    public void testIndexChunksInBigCsvByDay() throws Exception { //buffer = 93
        assertIndexChunks(
                4, "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", PartitionBy.DAY, "test-quotes-big.csv",
                chunk("1970-01-02/0_1", 86400000000L, 31), chunk("1970-01-03/0_1", 172800000000L, 94),
                chunk("1970-01-04/0_1", 259200000000L, 157), chunk("1970-01-05/0_1", 345600000000L, 220),
                chunk("1970-01-06/0_1", 432000000000L, 283), chunk("1970-01-07/0_1", 518400000000L, 346),
                chunk("1970-01-08/0_1", 604800000000L, 409), chunk("1970-01-09/0_1", 691200000000L, 472),
                chunk("1970-01-10/0_1", 777600000000L, 535), chunk("1970-01-11/0_1", 864000000000L, 598),
                chunk("1970-01-12/0_1", 950400000000L, 663), chunk("1970-01-13/0_1", 1036800000000L, 728),
                chunk("1970-01-14/0_1", 1123200000000L, 794), chunk("1970-01-15/0_1", 1209600000000L, 859),
                chunk("1970-01-16/0_1", 1296000000000L, 924), chunk("1970-01-17/0_1", 1382400000000L, 989),
                chunk("1970-01-18/0_1", 1468800000000L, 1054), chunk("1970-01-19/0_1", 1555200000000L, 1119),
                chunk("1970-01-20/0_1", 1641600000000L, 1184), chunk("1970-01-21/0_1", 1728000000000L, 1249),
                chunk("1970-01-22/0_1", 1814400000000L, 1315), chunk("1970-01-23/0_1", 1900800000000L, 1380),
                chunk("1970-01-24/0_1", 1987200000000L, 1445), chunk("1970-01-25/0_1", 2073600000000L, 1510),
                chunk("1970-01-26/0_1", 2160000000000L, 1575), chunk("1970-01-27/0_1", 2246400000000L, 1640),
                chunk("1970-01-28/0_1", 2332800000000L, 1705), chunk("1970-01-29/0_1", 2419200000000L, 1771),
                chunk("1970-01-30/0_1", 2505600000000L, 1836), chunk("1970-01-31/0_1", 2592000000000L, 1902),
                chunk("1970-02-01/0_1", 2678400000000L, 1972), chunk("1970-02-02/0_1", 2764800000000L, 2037),
                chunk("1970-02-03/0_1", 2851200000000L, 2102), chunk("1970-02-04/0_1", 2937600000000L, 2168),
                chunk("1970-02-05/0_1", 3024000000000L, 2233), chunk("1970-02-06/0_1", 3110400000000L, 2298),
                chunk("1970-02-07/0_1", 3196800000000L, 2363), chunk("1970-02-08/0_1", 3283200000000L, 2428),
                chunk("1970-02-09/0_1", 3369600000000L, 2493), chunk("1970-02-10/0_1", 3456000000000L, 2558),
                chunk("1970-02-11/0_1", 3542400000000L, 2623), chunk("1970-02-12/0_1", 3628800000000L, 2688),
                chunk("1970-02-13/0_1", 3715200000000L, 2753), chunk("1970-02-14/0_1", 3801600000000L, 2818),
                chunk("1970-02-15/0_1", 3888000000000L, 2883), chunk("1970-02-16/0_1", 3974400000000L, 2948),
                chunk("1970-02-17/0_1", 4060800000000L, 3013), chunk("1970-02-18/0_1", 4147200000000L, 3078),
                chunk("1970-02-19/0_1", 4233600000000L, 3143), chunk("1970-02-20/0_1", 4320000000000L, 3208),
                chunk("1970-02-21/0_1", 4406400000000L, 3274), chunk("1970-02-22/0_1", 4492800000000L, 3339),
                chunk("1970-02-23/0_1", 4579200000000L, 3404), chunk("1970-02-24/0_1", 4665600000000L, 3469),
                chunk("1970-02-25/0_1", 4752000000000L, 3534), chunk("1970-02-26/0_1", 4838400000000L, 3599),
                chunk("1970-02-27/0_1", 4924800000000L, 3664), chunk("1970-02-28/0_1", 5011200000000L, 3729),
                chunk("1970-03-01/0_1", 5097600000000L, 3794), chunk("1970-03-02/0_1", 5184000000000L, 3860),
                chunk("1970-03-03/0_1", 5270400000000L, 3925), chunk("1970-03-04/0_1", 5356800000000L, 3990),
                chunk("1970-03-05/0_1", 5443200000000L, 4055), chunk("1970-03-06/0_1", 5529600000000L, 4120),
                chunk("1970-03-07/0_1", 5616000000000L, 4185), chunk("1970-03-08/0_1", 5702400000000L, 4250),
                chunk("1970-03-09/0_1", 5788800000000L, 4315), chunk("1970-03-10/0_1", 5875200000000L, 4380),
                chunk("1970-03-11/0_1", 5961600000000L, 4445), chunk("1970-03-12/0_1", 6048000000000L, 4510),
                chunk("1970-03-13/0_1", 6134400000000L, 4575), chunk("1970-03-14/0_1", 6220800000000L, 4640),
                chunk("1970-03-15/0_1", 6307200000000L, 4705), chunk("1970-03-16/0_1", 6393600000000L, 4770),
                chunk("1970-03-17/0_1", 6480000000000L, 4835), chunk("1970-03-18/0_1", 6566400000000L, 4900),
                chunk("1970-03-19/0_1", 6652800000000L, 4965), chunk("1970-03-20/0_1", 6739200000000L, 5031),
                chunk("1970-03-21/0_1", 6825600000000L, 5096), chunk("1970-03-22/0_1", 6912000000000L, 5161),
                chunk("1970-03-23/0_1", 6998400000000L, 5226), chunk("1970-03-24/0_1", 7084800000000L, 5291),
                chunk("1970-03-25/0_1", 7171200000000L, 5356), chunk("1970-03-26/0_1", 7257600000000L, 5421),
                chunk("1970-03-27/0_1", 7344000000000L, 5486), chunk("1970-03-28/0_1", 7430400000000L, 5551),
                chunk("1970-03-29/0_1", 7516800000000L, 5616), chunk("1970-03-30/0_1", 7603200000000L, 5681),
                chunk("1970-03-31/0_1", 7689600000000L, 5746), chunk("1970-04-01/0_1", 7776000000000L, 5811),
                chunk("1970-04-02/0_1", 7862400000000L, 5876), chunk("1970-04-03/0_1", 7948800000000L, 5941),
                chunk("1970-04-04/0_1", 8035200000000L, 6006), chunk("1970-04-05/0_1", 8121600000000L, 6071),
                chunk("1970-04-06/0_1", 8208000000000L, 6136), chunk("1970-04-07/0_1", 8294400000000L, 6201),
                chunk("1970-04-08/0_1", 8380800000000L, 6266), chunk("1970-04-09/0_1", 8467200000000L, 6331),
                chunk("1970-04-10/0_1", 8553600000000L, 6396), chunk("1970-04-11/0_1", 8640000000000L, 6461),
                chunk("1970-04-12/0_1", 8726400000000L, 6528), chunk("1970-04-13/0_1", 8812800000000L, 6595),
                chunk("1970-04-14/0_1", 8899200000000L, 6662), chunk("1970-04-15/0_1", 8985600000000L, 6729),
                chunk("1970-04-16/0_1", 9072000000000L, 6796), chunk("1970-04-17/0_1", 9158400000000L, 6863),
                chunk("1970-04-18/0_1", 9244800000000L, 6930), chunk("1970-04-19/0_1", 9331200000000L, 6997),
                chunk("1970-04-20/0_1", 9417600000000L, 7064), chunk("1970-04-21/0_1", 9504000000000L, 7131),
                chunk("1970-04-22/0_1", 9590400000000L, 7198), chunk("1970-04-23/0_1", 9676800000000L, 7265),
                chunk("1970-04-24/0_1", 9763200000000L, 7332), chunk("1970-04-25/0_1", 9849600000000L, 7399),
                chunk("1970-04-26/0_1", 9936000000000L, 7466), chunk("1970-04-27/0_1", 10022400000000L, 7534),
                chunk("1970-04-28/0_1", 10108800000000L, 7601), chunk("1970-04-29/0_1", 10195200000000L, 7668),
                chunk("1970-04-30/0_1", 10281600000000L, 7735), chunk("1970-05-01/0_1", 10368000000000L, 7802),
                chunk("1970-05-02/0_1", 10454400000000L, 7869), chunk("1970-05-03/0_1", 10540800000000L, 7936),
                chunk("1970-05-04/0_1", 10627200000000L, 8003), chunk("1970-05-05/0_1", 10713600000000L, 8070),
                chunk("1970-05-06/0_1", 10800000000000L, 8137), chunk("1970-05-07/0_1", 10886400000000L, 8204),
                chunk("1970-05-08/0_1", 10972800000000L, 8271), chunk("1970-05-09/0_1", 11059200000000L, 8339),
                chunk("1970-05-10/0_1", 11145600000000L, 8406), chunk("1970-05-11/0_1", 11232000000000L, 8473),
                chunk("1970-05-12/0_1", 11318400000000L, 8540), chunk("1970-05-13/0_1", 11404800000000L, 8607),
                chunk("1970-05-14/0_1", 11491200000000L, 8674), chunk("1970-05-15/0_1", 11577600000000L, 8741),
                chunk("1970-05-16/0_1", 11664000000000L, 8808), chunk("1970-05-17/0_1", 11750400000000L, 8875),
                chunk("1970-05-18/0_1", 11836800000000L, 8942), chunk("1970-05-19/0_1", 11923200000000L, 9009),
                chunk("1970-05-20/0_1", 12009600000000L, 9076), chunk("1970-05-21/0_1", 12096000000000L, 9143),
                chunk("1970-05-22/0_1", 12182400000000L, 9210), chunk("1970-05-23/0_1", 12268800000000L, 9278),
                chunk("1970-05-24/0_1", 12355200000000L, 9345), chunk("1970-05-25/0_1", 12441600000000L, 9412),
                chunk("1970-05-26/0_1", 12528000000000L, 9480), chunk("1970-05-27/0_1", 12614400000000L, 9547),
                chunk("1970-05-28/0_1", 12700800000000L, 9614), chunk("1970-05-29/0_1", 12787200000000L, 9681),
                chunk("1970-05-30/0_1", 12873600000000L, 9748), chunk("1970-05-31/0_1", 12960000000000L, 9815),
                chunk("1970-06-01/0_1", 13046400000000L, 9882), chunk("1970-06-02/0_1", 13132800000000L, 9949),
                chunk("1970-06-03/0_1", 13219200000000L, 10017), chunk("1970-06-04/0_1", 13305600000000L, 10084),
                chunk("1970-06-05/0_1", 13392000000000L, 10151), chunk("1970-06-06/0_1", 13478400000000L, 10218),
                chunk("1970-06-07/0_1", 13564800000000L, 10287), chunk("1970-06-08/0_1", 13651200000000L, 10354),
                chunk("1970-06-09/0_1", 13737600000000L, 10421), chunk("1970-06-10/0_1", 13824000000000L, 10488),
                chunk("1970-06-11/0_1", 13910400000000L, 10555), chunk("1970-06-12/0_1", 13996800000000L, 10622),
                chunk("1970-06-13/0_1", 14083200000000L, 10689), chunk("1970-06-14/0_1", 14169600000000L, 10756),
                chunk("1970-06-15/0_1", 14256000000000L, 10823), chunk("1970-06-16/0_1", 14342400000000L, 10890),
                chunk("1970-06-17/0_1", 14428800000000L, 10957), chunk("1970-06-18/0_1", 14515200000000L, 11024),
                chunk("1970-06-19/0_1", 14601600000000L, 11091), chunk("1970-06-20/0_1", 14688000000000L, 11158),
                chunk("1970-06-21/0_1", 14774400000000L, 11226), chunk("1970-06-22/0_1", 14860800000000L, 11293),
                chunk("1970-06-23/0_1", 14947200000000L, 11360), chunk("1970-06-24/0_1", 15033600000000L, 11427),
                chunk("1970-06-25/0_1", 15120000000000L, 11494), chunk("1970-06-26/0_1", 15206400000000L, 11561),
                chunk("1970-06-27/0_1", 15292800000000L, 11628), chunk("1970-06-28/0_1", 15379200000000L, 11695),
                chunk("1970-06-29/0_1", 15465600000000L, 11762), chunk("1970-06-30/0_1", 15552000000000L, 11829),
                chunk("1970-07-01/0_1", 15638400000000L, 11896), chunk("1970-07-02/0_1", 15724800000000L, 11963),
                chunk("1970-07-03/0_1", 15811200000000L, 12030), chunk("1970-07-04/0_1", 15897600000000L, 12097),
                chunk("1970-07-05/0_1", 15984000000000L, 12164), chunk("1970-07-06/0_1", 16070400000000L, 12231),
                chunk("1970-07-07/0_1", 16156800000000L, 12299), chunk("1970-07-08/0_1", 16243200000000L, 12366),
                chunk("1970-07-09/0_1", 16329600000000L, 12434), chunk("1970-07-10/0_1", 16416000000000L, 12501),
                chunk("1970-07-11/0_1", 16502400000000L, 12568), chunk("1970-07-12/0_1", 16588800000000L, 12635),
                chunk("1970-07-13/0_1", 16675200000000L, 12702), chunk("1970-07-14/0_1", 16761600000000L, 12769),
                chunk("1970-07-15/0_1", 16848000000000L, 12836), chunk("1970-07-16/0_1", 16934400000000L, 12903),
                chunk("1970-07-17/0_1", 17020800000000L, 12970), chunk("1970-07-18/0_1", 17107200000000L, 13037),
                chunk("1970-07-19/0_1", 17193600000000L, 13104), chunk("1970-07-20/0_1", 17280000000000L, 13172),
                chunk("1970-07-21/0_1", 17366400000000L, 13240), chunk("1970-07-22/0_1", 17452800000000L, 13307),
                chunk("1970-07-23/0_1", 17539200000000L, 13374), chunk("1970-07-24/0_1", 17625600000000L, 13441),
                chunk("1970-07-25/0_1", 17712000000000L, 13508), chunk("1970-07-26/0_1", 17798400000000L, 13575),
                chunk("1970-07-27/0_1", 17884800000000L, 13642), chunk("1970-07-28/0_1", 17971200000000L, 13710),
                chunk("1970-07-29/0_1", 18057600000000L, 13777), chunk("1970-07-30/0_1", 18144000000000L, 13844),
                chunk("1970-07-31/0_1", 18230400000000L, 13911), chunk("1970-08-01/0_1", 18316800000000L, 13978),
                chunk("1970-08-02/0_1", 18403200000000L, 14045), chunk("1970-08-03/0_1", 18489600000000L, 14112),
                chunk("1970-08-04/0_1", 18576000000000L, 14179), chunk("1970-08-05/0_1", 18662400000000L, 14246),
                chunk("1970-08-06/0_1", 18748800000000L, 14313), chunk("1970-08-07/0_1", 18835200000000L, 14380),
                chunk("1970-08-08/0_1", 18921600000000L, 14447), chunk("1970-08-09/0_1", 19008000000000L, 14514),
                chunk("1970-08-10/0_1", 19094400000000L, 14581), chunk("1970-08-11/0_1", 19180800000000L, 14649),
                chunk("1970-08-12/0_1", 19267200000000L, 14716), chunk("1970-08-13/0_1", 19353600000000L, 14783),
                chunk("1970-08-14/0_1", 19440000000000L, 14851), chunk("1970-08-15/0_1", 19526400000000L, 14918),
                chunk("1970-08-16/0_1", 19612800000000L, 14985), chunk("1970-08-17/0_1", 19699200000000L, 15052),
                chunk("1970-08-18/0_1", 19785600000000L, 15120), chunk("1970-08-19/0_1", 19872000000000L, 15187),
                chunk("1970-08-20/0_1", 19958400000000L, 15254), chunk("1970-08-21/0_1", 20044800000000L, 15321),
                chunk("1970-08-22/0_1", 20131200000000L, 15388), chunk("1970-08-23/0_1", 20217600000000L, 15455),
                chunk("1970-08-24/0_1", 20304000000000L, 15523), chunk("1970-08-25/0_1", 20390400000000L, 15590),
                chunk("1970-08-26/0_1", 20476800000000L, 15657), chunk("1970-08-27/0_1", 20563200000000L, 15725),
                chunk("1970-08-28/0_1", 20649600000000L, 15792), chunk("1970-08-29/0_1", 20736000000000L, 15859),
                chunk("1970-08-30/0_1", 20822400000000L, 15926), chunk("1970-08-31/0_1", 20908800000000L, 15993),
                chunk("1970-09-01/0_1", 20995200000000L, 16060), chunk("1970-09-02/0_1", 21081600000000L, 16127),
                chunk("1970-09-03/0_1", 21168000000000L, 16194), chunk("1970-09-04/0_1", 21254400000000L, 16261),
                chunk("1970-09-05/0_1", 21340800000000L, 16328), chunk("1970-09-06/0_1", 21427200000000L, 16395),
                chunk("1970-09-07/0_1", 21513600000000L, 16462), chunk("1970-09-08/0_1", 21600000000000L, 16529),
                chunk("1970-09-09/0_1", 21686400000000L, 16596), chunk("1970-09-10/0_1", 21772800000000L, 16663),
                chunk("1970-09-11/0_1", 21859200000000L, 16730), chunk("1970-09-12/1_1", 21945600000000L, 16797),
                chunk("1970-09-13/1_1", 22032000000000L, 16864), chunk("1970-09-14/1_1", 22118400000000L, 16931),
                chunk("1970-09-15/1_1", 22204800000000L, 16999), chunk("1970-09-16/1_1", 22291200000000L, 17066),
                chunk("1970-09-17/1_1", 22377600000000L, 17133), chunk("1970-09-18/1_1", 22464000000000L, 17200),
                chunk("1970-09-19/1_1", 22550400000000L, 17267), chunk("1970-09-20/1_1", 22636800000000L, 17334),
                chunk("1970-09-21/1_1", 22723200000000L, 17401), chunk("1970-09-22/1_1", 22809600000000L, 17468),
                chunk("1970-09-23/1_1", 22896000000000L, 17535), chunk("1970-09-24/1_1", 22982400000000L, 17602),
                chunk("1970-09-25/1_1", 23068800000000L, 17669), chunk("1970-09-26/1_1", 23155200000000L, 17736),
                chunk("1970-09-27/1_1", 23241600000000L, 17803), chunk("1970-09-28/1_1", 23328000000000L, 17870),
                chunk("1970-09-29/1_1", 23414400000000L, 17937), chunk("1970-09-30/1_1", 23500800000000L, 18004),
                chunk("1970-10-01/1_1", 23587200000000L, 18071), chunk("1970-10-02/1_1", 23673600000000L, 18138),
                chunk("1970-10-03/1_1", 23760000000000L, 18207), chunk("1970-10-04/1_1", 23846400000000L, 18274),
                chunk("1970-10-05/1_1", 23932800000000L, 18341), chunk("1970-10-06/1_1", 24019200000000L, 18408),
                chunk("1970-10-07/1_1", 24105600000000L, 18475), chunk("1970-10-08/1_1", 24192000000000L, 18542),
                chunk("1970-10-09/1_1", 24278400000000L, 18609), chunk("1970-10-10/1_1", 24364800000000L, 18676),
                chunk("1970-10-11/1_1", 24451200000000L, 18743), chunk("1970-10-12/1_1", 24537600000000L, 18810),
                chunk("1970-10-13/1_1", 24624000000000L, 18878), chunk("1970-10-14/1_1", 24710400000000L, 18945),
                chunk("1970-10-15/1_1", 24796800000000L, 19012), chunk("1970-10-16/1_1", 24883200000000L, 19079),
                chunk("1970-10-17/1_1", 24969600000000L, 19146), chunk("1970-10-18/1_1", 25056000000000L, 19213),
                chunk("1970-10-19/1_1", 25142400000000L, 19280), chunk("1970-10-20/1_1", 25228800000000L, 19347),
                chunk("1970-10-21/1_1", 25315200000000L, 19414), chunk("1970-10-22/1_1", 25401600000000L, 19481),
                chunk("1970-10-23/1_1", 25488000000000L, 19548), chunk("1970-10-24/1_1", 25574400000000L, 19615),
                chunk("1970-10-25/1_1", 25660800000000L, 19682), chunk("1970-10-26/1_1", 25747200000000L, 19749),
                chunk("1970-10-27/1_1", 25833600000000L, 19816), chunk("1970-10-28/1_1", 25920000000000L, 19883),
                chunk("1970-10-29/1_1", 26006400000000L, 19950), chunk("1970-10-30/1_1", 26092800000000L, 20017),
                chunk("1970-10-31/1_1", 26179200000000L, 20084), chunk("1970-11-01/1_1", 26265600000000L, 20151),
                chunk("1970-11-02/1_1", 26352000000000L, 20218), chunk("1970-11-03/1_1", 26438400000000L, 20285),
                chunk("1970-11-04/1_1", 26524800000000L, 20352), chunk("1970-11-05/1_1", 26611200000000L, 20420),
                chunk("1970-11-06/1_1", 26697600000000L, 20487), chunk("1970-11-07/1_1", 26784000000000L, 20554),
                chunk("1970-11-08/1_1", 26870400000000L, 20621), chunk("1970-11-09/1_1", 26956800000000L, 20688),
                chunk("1970-11-10/1_1", 27043200000000L, 20755), chunk("1970-11-11/1_1", 27129600000000L, 20822),
                chunk("1970-11-12/1_1", 27216000000000L, 20889), chunk("1970-11-13/1_1", 27302400000000L, 20956),
                chunk("1970-11-14/1_1", 27388800000000L, 21023), chunk("1970-11-15/1_1", 27475200000000L, 21090),
                chunk("1970-11-16/1_1", 27561600000000L, 21157), chunk("1970-11-17/1_1", 27648000000000L, 21224),
                chunk("1970-11-18/1_1", 27734400000000L, 21291), chunk("1970-11-19/1_1", 27820800000000L, 21358),
                chunk("1970-11-20/1_1", 27907200000000L, 21426), chunk("1970-11-21/1_1", 27993600000000L, 21493),
                chunk("1970-11-22/1_1", 28080000000000L, 21560), chunk("1970-11-23/1_1", 28166400000000L, 21629),
                chunk("1970-11-24/1_1", 28252800000000L, 21696), chunk("1970-11-25/1_1", 28339200000000L, 21763),
                chunk("1970-11-26/1_1", 28425600000000L, 21830), chunk("1970-11-27/1_1", 28512000000000L, 21897),
                chunk("1970-11-28/1_1", 28598400000000L, 21964), chunk("1970-11-29/1_1", 28684800000000L, 22031),
                chunk("1970-11-30/1_1", 28771200000000L, 22100), chunk("1970-12-01/1_1", 28857600000000L, 22167),
                chunk("1970-12-02/1_1", 28944000000000L, 22234), chunk("1970-12-03/1_1", 29030400000000L, 22301),
                chunk("1970-12-04/1_1", 29116800000000L, 22368), chunk("1970-12-05/1_1", 29203200000000L, 22435),
                chunk("1970-12-06/1_1", 29289600000000L, 22502), chunk("1970-12-07/1_1", 29376000000000L, 22569),
                chunk("1970-12-08/1_1", 29462400000000L, 22636), chunk("1970-12-09/1_1", 29548800000000L, 22703),
                chunk("1970-12-10/1_1", 29635200000000L, 22770), chunk("1970-12-11/1_1", 29721600000000L, 22837),
                chunk("1970-12-12/1_1", 29808000000000L, 22904), chunk("1970-12-13/1_1", 29894400000000L, 22971),
                chunk("1970-12-14/1_1", 29980800000000L, 23038), chunk("1970-12-15/1_1", 30067200000000L, 23105),
                chunk("1970-12-16/1_1", 30153600000000L, 23172), chunk("1970-12-17/1_1", 30240000000000L, 23240),
                chunk("1970-12-18/1_1", 30326400000000L, 23307), chunk("1970-12-19/1_1", 30412800000000L, 23374),
                chunk("1970-12-20/1_1", 30499200000000L, 23441), chunk("1970-12-21/1_1", 30585600000000L, 23508),
                chunk("1970-12-22/1_1", 30672000000000L, 23575), chunk("1970-12-23/1_1", 30758400000000L, 23643),
                chunk("1970-12-24/1_1", 30844800000000L, 23711), chunk("1970-12-25/1_1", 30931200000000L, 23778),
                chunk("1970-12-26/1_1", 31017600000000L, 23845), chunk("1970-12-27/1_1", 31104000000000L, 23913),
                chunk("1970-12-28/1_1", 31190400000000L, 23980), chunk("1970-12-29/1_1", 31276800000000L, 24047),
                chunk("1970-12-30/1_1", 31363200000000L, 24115), chunk("1970-12-31/1_1", 31449600000000L, 24182),
                chunk("1971-01-01/1_1", 31536000000000L, 24249), chunk("1971-01-02/1_1", 31622400000000L, 24316),
                chunk("1971-01-03/1_1", 31708800000000L, 24383), chunk("1971-01-04/1_1", 31795200000000L, 24450),
                chunk("1971-01-05/1_1", 31881600000000L, 24517), chunk("1971-01-06/1_1", 31968000000000L, 24584),
                chunk("1971-01-07/1_1", 32054400000000L, 24651), chunk("1971-01-08/1_1", 32140800000000L, 24718),
                chunk("1971-01-09/1_1", 32227200000000L, 24786), chunk("1971-01-10/1_1", 32313600000000L, 24853),
                chunk("1971-01-11/1_1", 32400000000000L, 24920), chunk("1971-01-12/1_1", 32486400000000L, 24987),
                chunk("1971-01-13/1_1", 32572800000000L, 25054), chunk("1971-01-14/1_1", 32659200000000L, 25122),
                chunk("1971-01-15/1_1", 32745600000000L, 25189), chunk("1971-01-16/1_1", 32832000000000L, 25256),
                chunk("1971-01-17/1_1", 32918400000000L, 25323), chunk("1971-01-18/1_1", 33004800000000L, 25390),
                chunk("1971-01-19/1_1", 33091200000000L, 25457), chunk("1971-01-20/1_1", 33177600000000L, 25524),
                chunk("1971-01-21/1_1", 33264000000000L, 25591), chunk("1971-01-22/1_1", 33350400000000L, 25658),
                chunk("1971-01-23/1_1", 33436800000000L, 25725), chunk("1971-01-24/1_1", 33523200000000L, 25792),
                chunk("1971-01-25/1_1", 33609600000000L, 25859), chunk("1971-01-26/1_1", 33696000000000L, 25927),
                chunk("1971-01-27/1_1", 33782400000000L, 25994), chunk("1971-01-28/1_1", 33868800000000L, 26062),
                chunk("1971-01-29/1_1", 33955200000000L, 26129), chunk("1971-01-30/1_1", 34041600000000L, 26196),
                chunk("1971-01-31/1_1", 34128000000000L, 26263), chunk("1971-02-01/1_1", 34214400000000L, 26330),
                chunk("1971-02-02/1_1", 34300800000000L, 26397), chunk("1971-02-03/1_1", 34387200000000L, 26464),
                chunk("1971-02-04/1_1", 34473600000000L, 26531), chunk("1971-02-05/1_1", 34560000000000L, 26598),
                chunk("1971-02-06/1_1", 34646400000000L, 26665), chunk("1971-02-07/1_1", 34732800000000L, 26733),
                chunk("1971-02-08/1_1", 34819200000000L, 26800), chunk("1971-02-09/1_1", 34905600000000L, 26867),
                chunk("1971-02-10/1_1", 34992000000000L, 26934), chunk("1971-02-11/1_1", 35078400000000L, 27001),
                chunk("1971-02-12/1_1", 35164800000000L, 27069), chunk("1971-02-13/1_1", 35251200000000L, 27136),
                chunk("1971-02-14/1_1", 35337600000000L, 27203), chunk("1971-02-15/1_1", 35424000000000L, 27270),
                chunk("1971-02-16/1_1", 35510400000000L, 27337), chunk("1971-02-17/1_1", 35596800000000L, 27405),
                chunk("1971-02-18/1_1", 35683200000000L, 27472), chunk("1971-02-19/1_1", 35769600000000L, 27539),
                chunk("1971-02-20/1_1", 35856000000000L, 27607), chunk("1971-02-21/1_1", 35942400000000L, 27674),
                chunk("1971-02-22/1_1", 36028800000000L, 27741), chunk("1971-02-23/1_1", 36115200000000L, 27808),
                chunk("1971-02-24/1_1", 36201600000000L, 27875), chunk("1971-02-25/1_1", 36288000000000L, 27942),
                chunk("1971-02-26/1_1", 36374400000000L, 28009), chunk("1971-02-27/1_1", 36460800000000L, 28076),
                chunk("1971-02-28/1_1", 36547200000000L, 28143), chunk("1971-03-01/1_1", 36633600000000L, 28210),
                chunk("1971-03-02/1_1", 36720000000000L, 28278), chunk("1971-03-03/1_1", 36806400000000L, 28347),
                chunk("1971-03-04/1_1", 36892800000000L, 28414), chunk("1971-03-05/1_1", 36979200000000L, 28482),
                chunk("1971-03-06/1_1", 37065600000000L, 28549), chunk("1971-03-07/1_1", 37152000000000L, 28616),
                chunk("1971-03-08/1_1", 37238400000000L, 28683), chunk("1971-03-09/1_1", 37324800000000L, 28750),
                chunk("1971-03-10/1_1", 37411200000000L, 28817), chunk("1971-03-11/1_1", 37497600000000L, 28884),
                chunk("1971-03-12/1_1", 37584000000000L, 28951), chunk("1971-03-13/1_1", 37670400000000L, 29018),
                chunk("1971-03-14/1_1", 37756800000000L, 29085), chunk("1971-03-15/1_1", 37843200000000L, 29152),
                chunk("1971-03-16/1_1", 37929600000000L, 29219), chunk("1971-03-17/1_1", 38016000000000L, 29286),
                chunk("1971-03-18/1_1", 38102400000000L, 29353), chunk("1971-03-19/1_1", 38188800000000L, 29421),
                chunk("1971-03-20/1_1", 38275200000000L, 29488), chunk("1971-03-21/1_1", 38361600000000L, 29555),
                chunk("1971-03-22/1_1", 38448000000000L, 29622), chunk("1971-03-23/1_1", 38534400000000L, 29690),
                chunk("1971-03-24/1_1", 38620800000000L, 29758), chunk("1971-03-25/1_1", 38707200000000L, 29825),
                chunk("1971-03-26/1_1", 38793600000000L, 29892), chunk("1971-03-27/1_1", 38880000000000L, 29959),
                chunk("1971-03-28/1_1", 38966400000000L, 30026), chunk("1971-03-29/1_1", 39052800000000L, 30093),
                chunk("1971-03-30/1_1", 39139200000000L, 30160), chunk("1971-03-31/1_1", 39225600000000L, 30229),
                chunk("1971-04-01/1_1", 39312000000000L, 30296), chunk("1971-04-02/1_1", 39398400000000L, 30363),
                chunk("1971-04-03/1_1", 39484800000000L, 30430), chunk("1971-04-04/1_1", 39571200000000L, 30497),
                chunk("1971-04-05/1_1", 39657600000000L, 30564), chunk("1971-04-06/1_1", 39744000000000L, 30631),
                chunk("1971-04-07/1_1", 39830400000000L, 30698), chunk("1971-04-08/1_1", 39916800000000L, 30765),
                chunk("1971-04-09/1_1", 40003200000000L, 30832), chunk("1971-04-10/1_1", 40089600000000L, 30899),
                chunk("1971-04-11/1_1", 40176000000000L, 30966), chunk("1971-04-12/1_1", 40262400000000L, 31033),
                chunk("1971-04-13/1_1", 40348800000000L, 31100), chunk("1971-04-14/1_1", 40435200000000L, 31167),
                chunk("1971-04-15/1_1", 40521600000000L, 31234), chunk("1971-04-16/1_1", 40608000000000L, 31301),
                chunk("1971-04-17/1_1", 40694400000000L, 31368), chunk("1971-04-18/1_1", 40780800000000L, 31435),
                chunk("1971-04-19/1_1", 40867200000000L, 31502), chunk("1971-04-20/1_1", 40953600000000L, 31569),
                chunk("1971-04-21/1_1", 41040000000000L, 31636), chunk("1971-04-22/1_1", 41126400000000L, 31703),
                chunk("1971-04-23/1_1", 41212800000000L, 31770), chunk("1971-04-24/1_1", 41299200000000L, 31838),
                chunk("1971-04-25/1_1", 41385600000000L, 31905), chunk("1971-04-26/1_1", 41472000000000L, 31972),
                chunk("1971-04-27/1_1", 41558400000000L, 32039), chunk("1971-04-28/1_1", 41644800000000L, 32106),
                chunk("1971-04-29/1_1", 41731200000000L, 32173), chunk("1971-04-30/1_1", 41817600000000L, 32240),
                chunk("1971-05-01/1_1", 41904000000000L, 32307), chunk("1971-05-02/1_1", 41990400000000L, 32374),
                chunk("1971-05-03/1_1", 42076800000000L, 32441), chunk("1971-05-04/1_1", 42163200000000L, 32508),
                chunk("1971-05-05/1_1", 42249600000000L, 32575), chunk("1971-05-06/1_1", 42336000000000L, 32643),
                chunk("1971-05-07/1_1", 42422400000000L, 32710), chunk("1971-05-08/1_1", 42508800000000L, 32777),
                chunk("1971-05-09/1_1", 42595200000000L, 32844), chunk("1971-05-10/1_1", 42681600000000L, 32911),
                chunk("1971-05-11/1_1", 42768000000000L, 32978), chunk("1971-05-12/1_1", 42854400000000L, 33045),
                chunk("1971-05-13/1_1", 42940800000000L, 33112), chunk("1971-05-14/1_1", 43027200000000L, 33179),
                chunk("1971-05-15/1_1", 43113600000000L, 33246), chunk("1971-05-16/1_1", 43200000000000L, 33313),
                chunk("1971-05-17/1_1", 43286400000000L, 33380), chunk("1971-05-18/1_1", 43372800000000L, 33447),
                chunk("1971-05-19/2_1", 43459200000000L, 33514), chunk("1971-05-20/2_1", 43545600000000L, 33581),
                chunk("1971-05-21/2_1", 43632000000000L, 33648), chunk("1971-05-22/2_1", 43718400000000L, 33715),
                chunk("1971-05-23/2_1", 43804800000000L, 33782), chunk("1971-05-24/2_1", 43891200000000L, 33849),
                chunk("1971-05-25/2_1", 43977600000000L, 33916), chunk("1971-05-26/2_1", 44064000000000L, 33983),
                chunk("1971-05-27/2_1", 44150400000000L, 34050), chunk("1971-05-28/2_1", 44236800000000L, 34117),
                chunk("1971-05-29/2_1", 44323200000000L, 34184), chunk("1971-05-30/2_1", 44409600000000L, 34251),
                chunk("1971-05-31/2_1", 44496000000000L, 34318), chunk("1971-06-01/2_1", 44582400000000L, 34385),
                chunk("1971-06-02/2_1", 44668800000000L, 34452), chunk("1971-06-03/2_1", 44755200000000L, 34519),
                chunk("1971-06-04/2_1", 44841600000000L, 34586), chunk("1971-06-05/2_1", 44928000000000L, 34653),
                chunk("1971-06-06/2_1", 45014400000000L, 34720), chunk("1971-06-07/2_1", 45100800000000L, 34787),
                chunk("1971-06-08/2_1", 45187200000000L, 34854), chunk("1971-06-09/2_1", 45273600000000L, 34921),
                chunk("1971-06-10/2_1", 45360000000000L, 34989), chunk("1971-06-11/2_1", 45446400000000L, 35056),
                chunk("1971-06-12/2_1", 45532800000000L, 35123), chunk("1971-06-13/2_1", 45619200000000L, 35190),
                chunk("1971-06-14/2_1", 45705600000000L, 35257), chunk("1971-06-15/2_1", 45792000000000L, 35324),
                chunk("1971-06-16/2_1", 45878400000000L, 35391), chunk("1971-06-17/2_1", 45964800000000L, 35458),
                chunk("1971-06-18/2_1", 46051200000000L, 35525), chunk("1971-06-19/2_1", 46137600000000L, 35592),
                chunk("1971-06-20/2_1", 46224000000000L, 35659), chunk("1971-06-21/2_1", 46310400000000L, 35726),
                chunk("1971-06-22/2_1", 46396800000000L, 35793), chunk("1971-06-23/2_1", 46483200000000L, 35860),
                chunk("1971-06-24/2_1", 46569600000000L, 35928), chunk("1971-06-25/2_1", 46656000000000L, 35995),
                chunk("1971-06-26/2_1", 46742400000000L, 36062), chunk("1971-06-27/2_1", 46828800000000L, 36129),
                chunk("1971-06-28/2_1", 46915200000000L, 36196), chunk("1971-06-29/2_1", 47001600000000L, 36263),
                chunk("1971-06-30/2_1", 47088000000000L, 36330), chunk("1971-07-01/2_1", 47174400000000L, 36397),
                chunk("1971-07-02/2_1", 47260800000000L, 36464), chunk("1971-07-03/2_1", 47347200000000L, 36531),
                chunk("1971-07-04/2_1", 47433600000000L, 36599), chunk("1971-07-05/2_1", 47520000000000L, 36666),
                chunk("1971-07-06/2_1", 47606400000000L, 36733), chunk("1971-07-07/2_1", 47692800000000L, 36801),
                chunk("1971-07-08/2_1", 47779200000000L, 36868), chunk("1971-07-09/2_1", 47865600000000L, 36935),
                chunk("1971-07-10/2_1", 47952000000000L, 37002), chunk("1971-07-11/2_1", 48038400000000L, 37069),
                chunk("1971-07-12/2_1", 48124800000000L, 37137), chunk("1971-07-13/2_1", 48211200000000L, 37205),
                chunk("1971-07-14/2_1", 48297600000000L, 37272), chunk("1971-07-15/2_1", 48384000000000L, 37339),
                chunk("1971-07-16/2_1", 48470400000000L, 37406), chunk("1971-07-17/2_1", 48556800000000L, 37473),
                chunk("1971-07-18/2_1", 48643200000000L, 37540), chunk("1971-07-19/2_1", 48729600000000L, 37607),
                chunk("1971-07-20/2_1", 48816000000000L, 37674), chunk("1971-07-21/2_1", 48902400000000L, 37741),
                chunk("1971-07-22/2_1", 48988800000000L, 37808), chunk("1971-07-23/2_1", 49075200000000L, 37875),
                chunk("1971-07-24/2_1", 49161600000000L, 37942), chunk("1971-07-25/2_1", 49248000000000L, 38009),
                chunk("1971-07-26/2_1", 49334400000000L, 38076), chunk("1971-07-27/2_1", 49420800000000L, 38143),
                chunk("1971-07-28/2_1", 49507200000000L, 38210), chunk("1971-07-29/2_1", 49593600000000L, 38277),
                chunk("1971-07-30/2_1", 49680000000000L, 38344), chunk("1971-07-31/2_1", 49766400000000L, 38411),
                chunk("1971-08-01/2_1", 49852800000000L, 38478), chunk("1971-08-02/2_1", 49939200000000L, 38545),
                chunk("1971-08-03/2_1", 50025600000000L, 38613), chunk("1971-08-04/2_1", 50112000000000L, 38680),
                chunk("1971-08-05/2_1", 50198400000000L, 38747), chunk("1971-08-06/2_1", 50284800000000L, 38814),
                chunk("1971-08-07/2_1", 50371200000000L, 38881), chunk("1971-08-08/2_1", 50457600000000L, 38948),
                chunk("1971-08-09/2_1", 50544000000000L, 39016), chunk("1971-08-10/2_1", 50630400000000L, 39083),
                chunk("1971-08-11/2_1", 50716800000000L, 39150), chunk("1971-08-12/2_1", 50803200000000L, 39217),
                chunk("1971-08-13/2_1", 50889600000000L, 39284), chunk("1971-08-14/2_1", 50976000000000L, 39351),
                chunk("1971-08-15/2_1", 51062400000000L, 39418), chunk("1971-08-16/2_1", 51148800000000L, 39485),
                chunk("1971-08-17/2_1", 51235200000000L, 39552), chunk("1971-08-18/2_1", 51321600000000L, 39619),
                chunk("1971-08-19/2_1", 51408000000000L, 39687), chunk("1971-08-20/2_1", 51494400000000L, 39754),
                chunk("1971-08-21/2_1", 51580800000000L, 39821), chunk("1971-08-22/2_1", 51667200000000L, 39888),
                chunk("1971-08-23/2_1", 51753600000000L, 39956), chunk("1971-08-24/2_1", 51840000000000L, 40023),
                chunk("1971-08-25/2_1", 51926400000000L, 40090), chunk("1971-08-26/2_1", 52012800000000L, 40157),
                chunk("1971-08-27/2_1", 52099200000000L, 40224), chunk("1971-08-28/2_1", 52185600000000L, 40291),
                chunk("1971-08-29/2_1", 52272000000000L, 40358), chunk("1971-08-30/2_1", 52358400000000L, 40425),
                chunk("1971-08-31/2_1", 52444800000000L, 40492), chunk("1971-09-01/2_1", 52531200000000L, 40559),
                chunk("1971-09-02/2_1", 52617600000000L, 40626), chunk("1971-09-03/2_1", 52704000000000L, 40693),
                chunk("1971-09-04/2_1", 52790400000000L, 40760), chunk("1971-09-05/2_1", 52876800000000L, 40827),
                chunk("1971-09-06/2_1", 52963200000000L, 40894), chunk("1971-09-07/2_1", 53049600000000L, 40962),
                chunk("1971-09-08/2_1", 53136000000000L, 41029), chunk("1971-09-09/2_1", 53222400000000L, 41096),
                chunk("1971-09-10/2_1", 53308800000000L, 41163), chunk("1971-09-11/2_1", 53395200000000L, 41230),
                chunk("1971-09-12/2_1", 53481600000000L, 41297), chunk("1971-09-13/2_1", 53568000000000L, 41364),
                chunk("1971-09-14/2_1", 53654400000000L, 41431), chunk("1971-09-15/2_1", 53740800000000L, 41498),
                chunk("1971-09-16/2_1", 53827200000000L, 41565), chunk("1971-09-17/2_1", 53913600000000L, 41632),
                chunk("1971-09-18/2_1", 54000000000000L, 41699), chunk("1971-09-19/2_1", 54086400000000L, 41767),
                chunk("1971-09-20/2_1", 54172800000000L, 41834), chunk("1971-09-21/2_1", 54259200000000L, 41901),
                chunk("1971-09-22/2_1", 54345600000000L, 41968), chunk("1971-09-23/2_1", 54432000000000L, 42035),
                chunk("1971-09-24/2_1", 54518400000000L, 42102), chunk("1971-09-25/2_1", 54604800000000L, 42169),
                chunk("1971-09-26/2_1", 54691200000000L, 42236), chunk("1971-09-27/2_1", 54777600000000L, 42303),
                chunk("1971-09-28/2_1", 54864000000000L, 42370), chunk("1971-09-29/2_1", 54950400000000L, 42437),
                chunk("1971-09-30/2_1", 55036800000000L, 42504), chunk("1971-10-01/2_1", 55123200000000L, 42571),
                chunk("1971-10-02/2_1", 55209600000000L, 42638), chunk("1971-10-03/2_1", 55296000000000L, 42705),
                chunk("1971-10-04/2_1", 55382400000000L, 42772), chunk("1971-10-05/2_1", 55468800000000L, 42839),
                chunk("1971-10-06/2_1", 55555200000000L, 42906), chunk("1971-10-07/2_1", 55641600000000L, 42973),
                chunk("1971-10-08/2_1", 55728000000000L, 43040), chunk("1971-10-09/2_1", 55814400000000L, 43107),
                chunk("1971-10-10/2_1", 55900800000000L, 43174), chunk("1971-10-11/2_1", 55987200000000L, 43241),
                chunk("1971-10-12/2_1", 56073600000000L, 43308), chunk("1971-10-13/2_1", 56160000000000L, 43375),
                chunk("1971-10-14/2_1", 56246400000000L, 43442), chunk("1971-10-15/2_1", 56332800000000L, 43509),
                chunk("1971-10-16/2_1", 56419200000000L, 43576), chunk("1971-10-17/2_1", 56505600000000L, 43643),
                chunk("1971-10-18/2_1", 56592000000000L, 43710), chunk("1971-10-19/2_1", 56678400000000L, 43777),
                chunk("1971-10-20/2_1", 56764800000000L, 43844), chunk("1971-10-21/2_1", 56851200000000L, 43911),
                chunk("1971-10-22/2_1", 56937600000000L, 43978), chunk("1971-10-23/2_1", 57024000000000L, 44046),
                chunk("1971-10-24/2_1", 57110400000000L, 44113), chunk("1971-10-25/2_1", 57196800000000L, 44180),
                chunk("1971-10-26/2_1", 57283200000000L, 44247), chunk("1971-10-27/2_1", 57369600000000L, 44314),
                chunk("1971-10-28/2_1", 57456000000000L, 44381), chunk("1971-10-29/2_1", 57542400000000L, 44448),
                chunk("1971-10-30/2_1", 57628800000000L, 44516), chunk("1971-10-31/2_1", 57715200000000L, 44583),
                chunk("1971-11-01/2_1", 57801600000000L, 44650), chunk("1971-11-02/2_1", 57888000000000L, 44717),
                chunk("1971-11-03/2_1", 57974400000000L, 44784), chunk("1971-11-04/2_1", 58060800000000L, 44851),
                chunk("1971-11-05/2_1", 58147200000000L, 44918), chunk("1971-11-06/2_1", 58233600000000L, 44985),
                chunk("1971-11-07/2_1", 58320000000000L, 45052), chunk("1971-11-08/2_1", 58406400000000L, 45119),
                chunk("1971-11-09/2_1", 58492800000000L, 45186), chunk("1971-11-10/2_1", 58579200000000L, 45253),
                chunk("1971-11-11/2_1", 58665600000000L, 45320), chunk("1971-11-12/2_1", 58752000000000L, 45387),
                chunk("1971-11-13/2_1", 58838400000000L, 45454), chunk("1971-11-14/2_1", 58924800000000L, 45521),
                chunk("1971-11-15/2_1", 59011200000000L, 45588), chunk("1971-11-16/2_1", 59097600000000L, 45655),
                chunk("1971-11-17/2_1", 59184000000000L, 45722), chunk("1971-11-18/2_1", 59270400000000L, 45789),
                chunk("1971-11-19/2_1", 59356800000000L, 45856), chunk("1971-11-20/2_1", 59443200000000L, 45923),
                chunk("1971-11-21/2_1", 59529600000000L, 45990), chunk("1971-11-22/2_1", 59616000000000L, 46058),
                chunk("1971-11-23/2_1", 59702400000000L, 46125), chunk("1971-11-24/2_1", 59788800000000L, 46192),
                chunk("1971-11-25/2_1", 59875200000000L, 46259), chunk("1971-11-26/2_1", 59961600000000L, 46326),
                chunk("1971-11-27/2_1", 60048000000000L, 46393), chunk("1971-11-28/2_1", 60134400000000L, 46460),
                chunk("1971-11-29/2_1", 60220800000000L, 46527), chunk("1971-11-30/2_1", 60307200000000L, 46594),
                chunk("1971-12-01/2_1", 60393600000000L, 46661), chunk("1971-12-02/2_1", 60480000000000L, 46728),
                chunk("1971-12-03/2_1", 60566400000000L, 46795), chunk("1971-12-04/2_1", 60652800000000L, 46862),
                chunk("1971-12-05/2_1", 60739200000000L, 46929), chunk("1971-12-06/2_1", 60825600000000L, 46996),
                chunk("1971-12-07/2_1", 60912000000000L, 47063), chunk("1971-12-08/2_1", 60998400000000L, 47130),
                chunk("1971-12-09/2_1", 61084800000000L, 47197), chunk("1971-12-10/2_1", 61171200000000L, 47264),
                chunk("1971-12-11/2_1", 61257600000000L, 47331), chunk("1971-12-12/2_1", 61344000000000L, 47398),
                chunk("1971-12-13/2_1", 61430400000000L, 47465), chunk("1971-12-14/2_1", 61516800000000L, 47532),
                chunk("1971-12-15/2_1", 61603200000000L, 47599), chunk("1971-12-16/2_1", 61689600000000L, 47666),
                chunk("1971-12-17/2_1", 61776000000000L, 47733), chunk("1971-12-18/2_1", 61862400000000L, 47800),
                chunk("1971-12-19/2_1", 61948800000000L, 47867), chunk("1971-12-20/2_1", 62035200000000L, 47934),
                chunk("1971-12-21/2_1", 62121600000000L, 48001), chunk("1971-12-22/2_1", 62208000000000L, 48068),
                chunk("1971-12-23/2_1", 62294400000000L, 48135), chunk("1971-12-24/2_1", 62380800000000L, 48202),
                chunk("1971-12-25/2_1", 62467200000000L, 48269), chunk("1971-12-26/2_1", 62553600000000L, 48336),
                chunk("1971-12-27/2_1", 62640000000000L, 48403), chunk("1971-12-28/2_1", 62726400000000L, 48470),
                chunk("1971-12-29/2_1", 62812800000000L, 48537), chunk("1971-12-30/2_1", 62899200000000L, 48604),
                chunk("1971-12-31/2_1", 62985600000000L, 48671), chunk("1972-01-01/2_1", 63072000000000L, 48738),
                chunk("1972-01-02/2_1", 63158400000000L, 48806), chunk("1972-01-03/2_1", 63244800000000L, 48873),
                chunk("1972-01-04/2_1", 63331200000000L, 48940), chunk("1972-01-05/2_1", 63417600000000L, 49007),
                chunk("1972-01-06/2_1", 63504000000000L, 49074), chunk("1972-01-07/2_1", 63590400000000L, 49142),
                chunk("1972-01-08/2_1", 63676800000000L, 49209), chunk("1972-01-09/2_1", 63763200000000L, 49276),
                chunk("1972-01-10/2_1", 63849600000000L, 49344), chunk("1972-01-11/2_1", 63936000000000L, 49411),
                chunk("1972-01-12/2_1", 64022400000000L, 49478), chunk("1972-01-13/2_1", 64108800000000L, 49545),
                chunk("1972-01-14/2_1", 64195200000000L, 49612), chunk("1972-01-15/2_1", 64281600000000L, 49679),
                chunk("1972-01-16/2_1", 64368000000000L, 49746), chunk("1972-01-17/2_1", 64454400000000L, 49813),
                chunk("1972-01-18/2_1", 64540800000000L, 49881), chunk("1972-01-19/2_1", 64627200000000L, 49948),
                chunk("1972-01-20/2_1", 64713600000000L, 50015), chunk("1972-01-21/2_1", 64800000000000L, 50082),
                chunk("1972-01-22/2_1", 64886400000000L, 50149), chunk("1972-01-23/3_1", 64972800000000L, 50216),
                chunk("1972-01-24/3_1", 65059200000000L, 50283), chunk("1972-01-25/3_1", 65145600000000L, 50350),
                chunk("1972-01-26/3_1", 65232000000000L, 50417), chunk("1972-01-27/3_1", 65318400000000L, 50484),
                chunk("1972-01-28/3_1", 65404800000000L, 50551), chunk("1972-01-29/3_1", 65491200000000L, 50618),
                chunk("1972-01-30/3_1", 65577600000000L, 50685), chunk("1972-01-31/3_1", 65664000000000L, 50752),
                chunk("1972-02-01/3_1", 65750400000000L, 50819), chunk("1972-02-02/3_1", 65836800000000L, 50886),
                chunk("1972-02-03/3_1", 65923200000000L, 50953), chunk("1972-02-04/3_1", 66009600000000L, 51020),
                chunk("1972-02-05/3_1", 66096000000000L, 51087), chunk("1972-02-06/3_1", 66182400000000L, 51154),
                chunk("1972-02-07/3_1", 66268800000000L, 51221), chunk("1972-02-08/3_1", 66355200000000L, 51288),
                chunk("1972-02-09/3_1", 66441600000000L, 51355), chunk("1972-02-10/3_1", 66528000000000L, 51422),
                chunk("1972-02-11/3_1", 66614400000000L, 51489), chunk("1972-02-12/3_1", 66700800000000L, 51556),
                chunk("1972-02-13/3_1", 66787200000000L, 51623), chunk("1972-02-14/3_1", 66873600000000L, 51691),
                chunk("1972-02-15/3_1", 66960000000000L, 51758), chunk("1972-02-16/3_1", 67046400000000L, 51825),
                chunk("1972-02-17/3_1", 67132800000000L, 51892), chunk("1972-02-18/3_1", 67219200000000L, 51959),
                chunk("1972-02-19/3_1", 67305600000000L, 52026), chunk("1972-02-20/3_1", 67392000000000L, 52093),
                chunk("1972-02-21/3_1", 67478400000000L, 52160), chunk("1972-02-22/3_1", 67564800000000L, 52227),
                chunk("1972-02-23/3_1", 67651200000000L, 52294), chunk("1972-02-24/3_1", 67737600000000L, 52361),
                chunk("1972-02-25/3_1", 67824000000000L, 52429), chunk("1972-02-26/3_1", 67910400000000L, 52497),
                chunk("1972-02-27/3_1", 67996800000000L, 52564), chunk("1972-02-28/3_1", 68083200000000L, 52631),
                chunk("1972-02-29/3_1", 68169600000000L, 52698), chunk("1972-03-01/3_1", 68256000000000L, 52765),
                chunk("1972-03-02/3_1", 68342400000000L, 52832), chunk("1972-03-03/3_1", 68428800000000L, 52899),
                chunk("1972-03-04/3_1", 68515200000000L, 52966), chunk("1972-03-05/3_1", 68601600000000L, 53033),
                chunk("1972-03-06/3_1", 68688000000000L, 53100), chunk("1972-03-07/3_1", 68774400000000L, 53167),
                chunk("1972-03-08/3_1", 68860800000000L, 53234), chunk("1972-03-09/3_1", 68947200000000L, 53301),
                chunk("1972-03-10/3_1", 69033600000000L, 53368), chunk("1972-03-11/3_1", 69120000000000L, 53435),
                chunk("1972-03-12/3_1", 69206400000000L, 53502), chunk("1972-03-13/3_1", 69292800000000L, 53569),
                chunk("1972-03-14/3_1", 69379200000000L, 53636), chunk("1972-03-15/3_1", 69465600000000L, 53703),
                chunk("1972-03-16/3_1", 69552000000000L, 53770), chunk("1972-03-17/3_1", 69638400000000L, 53837),
                chunk("1972-03-18/3_1", 69724800000000L, 53904), chunk("1972-03-19/3_1", 69811200000000L, 53972),
                chunk("1972-03-20/3_1", 69897600000000L, 54039), chunk("1972-03-21/3_1", 69984000000000L, 54106),
                chunk("1972-03-22/3_1", 70070400000000L, 54173), chunk("1972-03-23/3_1", 70156800000000L, 54240),
                chunk("1972-03-24/3_1", 70243200000000L, 54307), chunk("1972-03-25/3_1", 70329600000000L, 54374),
                chunk("1972-03-26/3_1", 70416000000000L, 54441), chunk("1972-03-27/3_1", 70502400000000L, 54508),
                chunk("1972-03-28/3_1", 70588800000000L, 54575), chunk("1972-03-29/3_1", 70675200000000L, 54642),
                chunk("1972-03-30/3_1", 70761600000000L, 54709), chunk("1972-03-31/3_1", 70848000000000L, 54776),
                chunk("1972-04-01/3_1", 70934400000000L, 54843), chunk("1972-04-02/3_1", 71020800000000L, 54910),
                chunk("1972-04-03/3_1", 71107200000000L, 54977), chunk("1972-04-04/3_1", 71193600000000L, 55044),
                chunk("1972-04-05/3_1", 71280000000000L, 55111), chunk("1972-04-06/3_1", 71366400000000L, 55178),
                chunk("1972-04-07/3_1", 71452800000000L, 55246), chunk("1972-04-08/3_1", 71539200000000L, 55313),
                chunk("1972-04-09/3_1", 71625600000000L, 55380), chunk("1972-04-10/3_1", 71712000000000L, 55447),
                chunk("1972-04-11/3_1", 71798400000000L, 55514), chunk("1972-04-12/3_1", 71884800000000L, 55581),
                chunk("1972-04-13/3_1", 71971200000000L, 55648), chunk("1972-04-14/3_1", 72057600000000L, 55715),
                chunk("1972-04-15/3_1", 72144000000000L, 55782), chunk("1972-04-16/3_1", 72230400000000L, 55849),
                chunk("1972-04-17/3_1", 72316800000000L, 55916), chunk("1972-04-18/3_1", 72403200000000L, 55983),
                chunk("1972-04-19/3_1", 72489600000000L, 56050), chunk("1972-04-20/3_1", 72576000000000L, 56117),
                chunk("1972-04-21/3_1", 72662400000000L, 56184), chunk("1972-04-22/3_1", 72748800000000L, 56251),
                chunk("1972-04-23/3_1", 72835200000000L, 56318), chunk("1972-04-24/3_1", 72921600000000L, 56385),
                chunk("1972-04-25/3_1", 73008000000000L, 56452), chunk("1972-04-26/3_1", 73094400000000L, 56519),
                chunk("1972-04-27/3_1", 73180800000000L, 56586), chunk("1972-04-28/3_1", 73267200000000L, 56653),
                chunk("1972-04-29/3_1", 73353600000000L, 56720), chunk("1972-04-30/3_1", 73440000000000L, 56787),
                chunk("1972-05-01/3_1", 73526400000000L, 56854), chunk("1972-05-02/3_1", 73612800000000L, 56921),
                chunk("1972-05-03/3_1", 73699200000000L, 56988), chunk("1972-05-04/3_1", 73785600000000L, 57055),
                chunk("1972-05-05/3_1", 73872000000000L, 57122), chunk("1972-05-06/3_1", 73958400000000L, 57189),
                chunk("1972-05-07/3_1", 74044800000000L, 57256), chunk("1972-05-08/3_1", 74131200000000L, 57323),
                chunk("1972-05-09/3_1", 74217600000000L, 57390), chunk("1972-05-10/3_1", 74304000000000L, 57457),
                chunk("1972-05-11/3_1", 74390400000000L, 57524), chunk("1972-05-12/3_1", 74476800000000L, 57591),
                chunk("1972-05-13/3_1", 74563200000000L, 57658), chunk("1972-05-14/3_1", 74649600000000L, 57725),
                chunk("1972-05-15/3_1", 74736000000000L, 57792), chunk("1972-05-16/3_1", 74822400000000L, 57859),
                chunk("1972-05-17/3_1", 74908800000000L, 57926), chunk("1972-05-18/3_1", 74995200000000L, 57993),
                chunk("1972-05-19/3_1", 75081600000000L, 58060), chunk("1972-05-20/3_1", 75168000000000L, 58127),
                chunk("1972-05-21/3_1", 75254400000000L, 58194), chunk("1972-05-22/3_1", 75340800000000L, 58261),
                chunk("1972-05-23/3_1", 75427200000000L, 58328), chunk("1972-05-24/3_1", 75513600000000L, 58395),
                chunk("1972-05-25/3_1", 75600000000000L, 58462), chunk("1972-05-26/3_1", 75686400000000L, 58529),
                chunk("1972-05-27/3_1", 75772800000000L, 58596), chunk("1972-05-28/3_1", 75859200000000L, 58663),
                chunk("1972-05-29/3_1", 75945600000000L, 58731), chunk("1972-05-30/3_1", 76032000000000L, 58798),
                chunk("1972-05-31/3_1", 76118400000000L, 58865), chunk("1972-06-01/3_1", 76204800000000L, 58933),
                chunk("1972-06-02/3_1", 76291200000000L, 59000), chunk("1972-06-03/3_1", 76377600000000L, 59068),
                chunk("1972-06-04/3_1", 76464000000000L, 59135), chunk("1972-06-05/3_1", 76550400000000L, 59202),
                chunk("1972-06-06/3_1", 76636800000000L, 59269), chunk("1972-06-07/3_1", 76723200000000L, 59336),
                chunk("1972-06-08/3_1", 76809600000000L, 59403), chunk("1972-06-09/3_1", 76896000000000L, 59470),
                chunk("1972-06-10/3_1", 76982400000000L, 59537), chunk("1972-06-11/3_1", 77068800000000L, 59604),
                chunk("1972-06-12/3_1", 77155200000000L, 59671), chunk("1972-06-13/3_1", 77241600000000L, 59738),
                chunk("1972-06-14/3_1", 77328000000000L, 59805), chunk("1972-06-15/3_1", 77414400000000L, 59872),
                chunk("1972-06-16/3_1", 77500800000000L, 59939), chunk("1972-06-17/3_1", 77587200000000L, 60007),
                chunk("1972-06-18/3_1", 77673600000000L, 60074), chunk("1972-06-19/3_1", 77760000000000L, 60142),
                chunk("1972-06-20/3_1", 77846400000000L, 60209), chunk("1972-06-21/3_1", 77932800000000L, 60276),
                chunk("1972-06-22/3_1", 78019200000000L, 60343), chunk("1972-06-23/3_1", 78105600000000L, 60410),
                chunk("1972-06-24/3_1", 78192000000000L, 60477), chunk("1972-06-25/3_1", 78278400000000L, 60544),
                chunk("1972-06-26/3_1", 78364800000000L, 60611), chunk("1972-06-27/3_1", 78451200000000L, 60678),
                chunk("1972-06-28/3_1", 78537600000000L, 60745), chunk("1972-06-29/3_1", 78624000000000L, 60812),
                chunk("1972-06-30/3_1", 78710400000000L, 60879), chunk("1972-07-01/3_1", 78796800000000L, 60946),
                chunk("1972-07-02/3_1", 78883200000000L, 61013), chunk("1972-07-03/3_1", 78969600000000L, 61080),
                chunk("1972-07-04/3_1", 79056000000000L, 61147), chunk("1972-07-05/3_1", 79142400000000L, 61215),
                chunk("1972-07-06/3_1", 79228800000000L, 61282), chunk("1972-07-07/3_1", 79315200000000L, 61349),
                chunk("1972-07-08/3_1", 79401600000000L, 61416), chunk("1972-07-09/3_1", 79488000000000L, 61483),
                chunk("1972-07-10/3_1", 79574400000000L, 61550), chunk("1972-07-11/3_1", 79660800000000L, 61617),
                chunk("1972-07-12/3_1", 79747200000000L, 61684), chunk("1972-07-13/3_1", 79833600000000L, 61751),
                chunk("1972-07-14/3_1", 79920000000000L, 61818), chunk("1972-07-15/3_1", 80006400000000L, 61885),
                chunk("1972-07-16/3_1", 80092800000000L, 61952), chunk("1972-07-17/3_1", 80179200000000L, 62019),
                chunk("1972-07-18/3_1", 80265600000000L, 62086), chunk("1972-07-19/3_1", 80352000000000L, 62153),
                chunk("1972-07-20/3_1", 80438400000000L, 62220), chunk("1972-07-21/3_1", 80524800000000L, 62287),
                chunk("1972-07-22/3_1", 80611200000000L, 62354), chunk("1972-07-23/3_1", 80697600000000L, 62421),
                chunk("1972-07-24/3_1", 80784000000000L, 62489), chunk("1972-07-25/3_1", 80870400000000L, 62556),
                chunk("1972-07-26/3_1", 80956800000000L, 62625), chunk("1972-07-27/3_1", 81043200000000L, 62692),
                chunk("1972-07-28/3_1", 81129600000000L, 62759), chunk("1972-07-29/3_1", 81216000000000L, 62826),
                chunk("1972-07-30/3_1", 81302400000000L, 62893), chunk("1972-07-31/3_1", 81388800000000L, 62960),
                chunk("1972-08-01/3_1", 81475200000000L, 63027), chunk("1972-08-02/3_1", 81561600000000L, 63094),
                chunk("1972-08-03/3_1", 81648000000000L, 63161), chunk("1972-08-04/3_1", 81734400000000L, 63228),
                chunk("1972-08-05/3_1", 81820800000000L, 63295), chunk("1972-08-06/3_1", 81907200000000L, 63362),
                chunk("1972-08-07/3_1", 81993600000000L, 63429), chunk("1972-08-08/3_1", 82080000000000L, 63496),
                chunk("1972-08-09/3_1", 82166400000000L, 63563), chunk("1972-08-10/3_1", 82252800000000L, 63630),
                chunk("1972-08-11/3_1", 82339200000000L, 63699), chunk("1972-08-12/3_1", 82425600000000L, 63766),
                chunk("1972-08-13/3_1", 82512000000000L, 63833), chunk("1972-08-14/3_1", 82598400000000L, 63900),
                chunk("1972-08-15/3_1", 82684800000000L, 63967), chunk("1972-08-16/3_1", 82771200000000L, 64034),
                chunk("1972-08-17/3_1", 82857600000000L, 64101), chunk("1972-08-18/3_1", 82944000000000L, 64168),
                chunk("1972-08-19/3_1", 83030400000000L, 64236), chunk("1972-08-20/3_1", 83116800000000L, 64303),
                chunk("1972-08-21/3_1", 83203200000000L, 64371), chunk("1972-08-22/3_1", 83289600000000L, 64438),
                chunk("1972-08-23/3_1", 83376000000000L, 64505), chunk("1972-08-24/3_1", 83462400000000L, 64572),
                chunk("1972-08-25/3_1", 83548800000000L, 64639), chunk("1972-08-26/3_1", 83635200000000L, 64706),
                chunk("1972-08-27/3_1", 83721600000000L, 64774), chunk("1972-08-28/3_1", 83808000000000L, 64841),
                chunk("1972-08-29/3_1", 83894400000000L, 64908), chunk("1972-08-30/3_1", 83980800000000L, 64975),
                chunk("1972-08-31/3_1", 84067200000000L, 65042), chunk("1972-09-01/3_1", 84153600000000L, 65109),
                chunk("1972-09-02/3_1", 84240000000000L, 65176), chunk("1972-09-03/3_1", 84326400000000L, 65244),
                chunk("1972-09-04/3_1", 84412800000000L, 65311), chunk("1972-09-05/3_1", 84499200000000L, 65378),
                chunk("1972-09-06/3_1", 84585600000000L, 65445), chunk("1972-09-07/3_1", 84672000000000L, 65512),
                chunk("1972-09-08/3_1", 84758400000000L, 65579), chunk("1972-09-09/3_1", 84844800000000L, 65646),
                chunk("1972-09-10/3_1", 84931200000000L, 65714), chunk("1972-09-11/3_1", 85017600000000L, 65781),
                chunk("1972-09-12/3_1", 85104000000000L, 65848), chunk("1972-09-13/3_1", 85190400000000L, 65915),
                chunk("1972-09-14/3_1", 85276800000000L, 65982), chunk("1972-09-15/3_1", 85363200000000L, 66049),
                chunk("1972-09-16/3_1", 85449600000000L, 66116), chunk("1972-09-17/3_1", 85536000000000L, 66183),
                chunk("1972-09-18/3_1", 85622400000000L, 66250), chunk("1972-09-19/3_1", 85708800000000L, 66317),
                chunk("1972-09-20/3_1", 85795200000000L, 66384), chunk("1972-09-21/3_1", 85881600000000L, 66452),
                chunk("1972-09-22/3_1", 85968000000000L, 66519), chunk("1972-09-23/3_1", 86054400000000L, 66586),
                chunk("1972-09-24/3_1", 86140800000000L, 66653), chunk("1972-09-25/3_1", 86227200000000L, 66720),
                chunk("1972-09-26/3_1", 86313600000000L, 66787), chunk("1972-09-27/3_1", 86400000000000L, 66854)
        );
    }

    @Test
    public void testIndexChunksInBigCsvByYear() throws Exception { //buffer = 93
        assertIndexChunks(
                4, "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", PartitionBy.YEAR, "test-quotes-big.csv",
                chunk(
                        "1970/0_1", 86400000000L, 31L, 172800000000L, 94L, 259200000000L, 157L, 345600000000L, 220L, 432000000000L, 283L, 518400000000L, 346L, 604800000000L, 409L, 691200000000L, 472L, 777600000000L, 535L, 864000000000L, 598L, 950400000000L, 663L, 1036800000000L, 728L, 1123200000000L, 794L, 1209600000000L, 859L, 1296000000000L, 924L, 1382400000000L, 989L, 1468800000000L, 1054L, 1555200000000L, 1119L, 1641600000000L, 1184L, 1728000000000L, 1249L, 1814400000000L, 1315L, 1900800000000L, 1380L, 1987200000000L, 1445L, 2073600000000L, 1510L, 2160000000000L, 1575L, 2246400000000L, 1640L, 2332800000000L, 1705L, 2419200000000L, 1771L, 2505600000000L, 1836L, 2592000000000L, 1902L, 2678400000000L, 1972L, 2764800000000L, 2037L, 2851200000000L, 2102L, 2937600000000L, 2168L, 3024000000000L, 2233L, 3110400000000L, 2298L, 3196800000000L, 2363L, 3283200000000L, 2428L, 3369600000000L, 2493L, 3456000000000L, 2558L, 3542400000000L, 2623L, 3628800000000L, 2688L, 3715200000000L, 2753L, 3801600000000L, 2818L, 3888000000000L, 2883L, 3974400000000L, 2948L, 4060800000000L, 3013L, 4147200000000L, 3078L, 4233600000000L, 3143L, 4320000000000L, 3208L, 4406400000000L, 3274L, 4492800000000L, 3339L, 4579200000000L, 3404L, 4665600000000L, 3469L, 4752000000000L, 3534L, 4838400000000L, 3599L, 4924800000000L, 3664L, 5011200000000L, 3729L, 5097600000000L, 3794L, 5184000000000L, 3860L, 5270400000000L, 3925L, 5356800000000L, 3990L, 5443200000000L, 4055L, 5529600000000L, 4120L, 5616000000000L, 4185L, 5702400000000L, 4250L, 5788800000000L, 4315L, 5875200000000L, 4380L, 5961600000000L, 4445L, 6048000000000L, 4510L, 6134400000000L, 4575L, 6220800000000L, 4640L, 6307200000000L, 4705L, 6393600000000L, 4770L, 6480000000000L, 4835L, 6566400000000L, 4900L, 6652800000000L, 4965L, 6739200000000L, 5031L, 6825600000000L, 5096L, 6912000000000L, 5161L, 6998400000000L, 5226L, 7084800000000L, 5291L, 7171200000000L, 5356L, 7257600000000L, 5421L, 7344000000000L, 5486L, 7430400000000L, 5551L, 7516800000000L, 5616L, 7603200000000L, 5681L, 7689600000000L, 5746L, 7776000000000L, 5811L, 7862400000000L, 5876L, 7948800000000L, 5941L, 8035200000000L, 6006L, 8121600000000L, 6071L, 8208000000000L, 6136L, 8294400000000L, 6201L, 8380800000000L, 6266L, 8467200000000L, 6331L, 8553600000000L, 6396L, 8640000000000L, 6461L, 8726400000000L, 6528L, 8812800000000L, 6595L, 8899200000000L, 6662L, 8985600000000L, 6729L, 9072000000000L, 6796L, 9158400000000L, 6863L, 9244800000000L, 6930L, 9331200000000L, 6997L, 9417600000000L, 7064L, 9504000000000L, 7131L, 9590400000000L, 7198L, 9676800000000L, 7265L, 9763200000000L, 7332L, 9849600000000L, 7399L, 9936000000000L, 7466L, 10022400000000L, 7534L, 10108800000000L, 7601L, 10195200000000L, 7668L, 10281600000000L, 7735L, 10368000000000L, 7802L, 10454400000000L, 7869L, 10540800000000L, 7936L, 10627200000000L, 8003L, 10713600000000L, 8070L, 10800000000000L, 8137L, 10886400000000L, 8204L, 10972800000000L, 8271L, 11059200000000L, 8339L, 11145600000000L, 8406L, 11232000000000L, 8473L, 11318400000000L, 8540L, 11404800000000L, 8607L, 11491200000000L, 8674L, 11577600000000L, 8741L, 11664000000000L, 8808L, 11750400000000L, 8875L, 11836800000000L, 8942L, 11923200000000L, 9009L, 12009600000000L, 9076L, 12096000000000L, 9143L, 12182400000000L, 9210L, 12268800000000L, 9278L, 12355200000000L, 9345L, 12441600000000L, 9412L, 12528000000000L, 9480L, 12614400000000L, 9547L, 12700800000000L, 9614L, 12787200000000L, 9681L, 12873600000000L, 9748L, 12960000000000L, 9815L, 13046400000000L, 9882L, 13132800000000L, 9949L, 13219200000000L, 10017L, 13305600000000L, 10084L, 13392000000000L, 10151L, 13478400000000L, 10218L, 13564800000000L, 10287L, 13651200000000L, 10354L, 13737600000000L, 10421L, 13824000000000L, 10488L, 13910400000000L, 10555L, 13996800000000L, 10622L, 14083200000000L, 10689L, 14169600000000L, 10756L, 14256000000000L, 10823L, 14342400000000L, 10890L, 14428800000000L, 10957L, 14515200000000L, 11024L, 14601600000000L, 11091L, 14688000000000L, 11158L, 14774400000000L, 11226L, 14860800000000L, 11293L, 14947200000000L, 11360L, 15033600000000L, 11427L, 15120000000000L, 11494L, 15206400000000L, 11561L, 15292800000000L, 11628L, 15379200000000L, 11695L, 15465600000000L, 11762L, 15552000000000L, 11829L, 15638400000000L, 11896L, 15724800000000L, 11963L, 15811200000000L, 12030L, 15897600000000L, 12097L, 15984000000000L, 12164L, 16070400000000L, 12231L, 16156800000000L, 12299L, 16243200000000L, 12366L, 16329600000000L, 12434L, 16416000000000L, 12501L, 16502400000000L, 12568L, 16588800000000L, 12635L, 16675200000000L, 12702L, 16761600000000L, 12769L, 16848000000000L, 12836L, 16934400000000L, 12903L, 17020800000000L, 12970L, 17107200000000L, 13037L, 17193600000000L, 13104L, 17280000000000L, 13172L, 17366400000000L, 13240L, 17452800000000L, 13307L, 17539200000000L, 13374L, 17625600000000L, 13441L, 17712000000000L, 13508L, 17798400000000L, 13575L, 17884800000000L, 13642L, 17971200000000L, 13710L, 18057600000000L, 13777L, 18144000000000L, 13844L, 18230400000000L, 13911L, 18316800000000L, 13978L, 18403200000000L, 14045L, 18489600000000L, 14112L, 18576000000000L, 14179L, 18662400000000L, 14246L, 18748800000000L, 14313L, 18835200000000L, 14380L, 18921600000000L, 14447L, 19008000000000L, 14514L, 19094400000000L, 14581L, 19180800000000L, 14649L, 19267200000000L, 14716L, 19353600000000L, 14783L, 19440000000000L, 14851L, 19526400000000L, 14918L, 19612800000000L, 14985L, 19699200000000L, 15052L, 19785600000000L, 15120L, 19872000000000L, 15187L, 19958400000000L, 15254L, 20044800000000L, 15321L, 20131200000000L, 15388L, 20217600000000L, 15455L, 20304000000000L, 15523L, 20390400000000L, 15590L, 20476800000000L, 15657L, 20563200000000L, 15725L, 20649600000000L, 15792L, 20736000000000L, 15859L, 20822400000000L, 15926L, 20908800000000L, 15993L, 20995200000000L, 16060L, 21081600000000L, 16127L, 21168000000000L, 16194L, 21254400000000L, 16261L, 21340800000000L, 16328L, 21427200000000L, 16395L, 21513600000000L, 16462L, 21600000000000L, 16529L, 21686400000000L, 16596L, 21772800000000L, 16663L, 21859200000000L, 16730
                ),
                chunk(
                        "1970/1_1", 21945600000000L, 16797L, 22032000000000L, 16864L, 22118400000000L, 16931L, 22204800000000L, 16999L, 22291200000000L, 17066L, 22377600000000L, 17133L, 22464000000000L, 17200L, 22550400000000L, 17267L, 22636800000000L, 17334L, 22723200000000L, 17401L, 22809600000000L, 17468L, 22896000000000L, 17535L, 22982400000000L, 17602L, 23068800000000L, 17669L, 23155200000000L, 17736L, 23241600000000L, 17803L, 23328000000000L, 17870L, 23414400000000L, 17937L, 23500800000000L, 18004L, 23587200000000L, 18071L, 23673600000000L, 18138L, 23760000000000L, 18207L, 23846400000000L, 18274L, 23932800000000L, 18341L, 24019200000000L, 18408L, 24105600000000L, 18475L, 24192000000000L, 18542L, 24278400000000L, 18609L, 24364800000000L, 18676L, 24451200000000L, 18743L, 24537600000000L, 18810L, 24624000000000L, 18878L, 24710400000000L, 18945L, 24796800000000L, 19012L, 24883200000000L, 19079L, 24969600000000L, 19146L, 25056000000000L, 19213L, 25142400000000L, 19280L, 25228800000000L, 19347L, 25315200000000L, 19414L, 25401600000000L, 19481L, 25488000000000L, 19548L, 25574400000000L, 19615L, 25660800000000L, 19682L, 25747200000000L, 19749L, 25833600000000L, 19816L, 25920000000000L, 19883L, 26006400000000L, 19950L, 26092800000000L, 20017L, 26179200000000L, 20084L, 26265600000000L, 20151L, 26352000000000L, 20218L, 26438400000000L, 20285L, 26524800000000L, 20352L, 26611200000000L, 20420L, 26697600000000L, 20487L, 26784000000000L, 20554L, 26870400000000L, 20621L, 26956800000000L, 20688L, 27043200000000L, 20755L, 27129600000000L, 20822L, 27216000000000L, 20889L, 27302400000000L, 20956L, 27388800000000L, 21023L, 27475200000000L, 21090L, 27561600000000L, 21157L, 27648000000000L, 21224L, 27734400000000L, 21291L, 27820800000000L, 21358L, 27907200000000L, 21426L, 27993600000000L, 21493L, 28080000000000L, 21560L, 28166400000000L, 21629L, 28252800000000L, 21696L, 28339200000000L, 21763L, 28425600000000L, 21830L, 28512000000000L, 21897L, 28598400000000L, 21964L, 28684800000000L, 22031L, 28771200000000L, 22100L, 28857600000000L, 22167L, 28944000000000L, 22234L, 29030400000000L, 22301L, 29116800000000L, 22368L, 29203200000000L, 22435L, 29289600000000L, 22502L, 29376000000000L, 22569L, 29462400000000L, 22636L, 29548800000000L, 22703L, 29635200000000L, 22770L, 29721600000000L, 22837L, 29808000000000L, 22904L, 29894400000000L, 22971L, 29980800000000L, 23038L, 30067200000000L, 23105L, 30153600000000L, 23172L, 30240000000000L, 23240L, 30326400000000L, 23307L, 30412800000000L, 23374L, 30499200000000L, 23441L, 30585600000000L, 23508L, 30672000000000L, 23575L, 30758400000000L, 23643L, 30844800000000L, 23711L, 30931200000000L, 23778L, 31017600000000L, 23845L, 31104000000000L, 23913L, 31190400000000L, 23980L, 31276800000000L, 24047L, 31363200000000L, 24115L, 31449600000000L, 24182
                ),
                chunk(
                        "1971/1_1", 31536000000000L, 24249L, 31622400000000L, 24316L, 31708800000000L, 24383L, 31795200000000L, 24450L, 31881600000000L, 24517L, 31968000000000L, 24584L, 32054400000000L, 24651L, 32140800000000L, 24718L, 32227200000000L, 24786L, 32313600000000L, 24853L, 32400000000000L, 24920L, 32486400000000L, 24987L, 32572800000000L, 25054L, 32659200000000L, 25122L, 32745600000000L, 25189L, 32832000000000L, 25256L, 32918400000000L, 25323L, 33004800000000L, 25390L, 33091200000000L, 25457L, 33177600000000L, 25524L, 33264000000000L, 25591L, 33350400000000L, 25658L, 33436800000000L, 25725L, 33523200000000L, 25792L, 33609600000000L, 25859L, 33696000000000L, 25927L, 33782400000000L, 25994L, 33868800000000L, 26062L, 33955200000000L, 26129L, 34041600000000L, 26196L, 34128000000000L, 26263L, 34214400000000L, 26330L, 34300800000000L, 26397L, 34387200000000L, 26464L, 34473600000000L, 26531L, 34560000000000L, 26598L, 34646400000000L, 26665L, 34732800000000L, 26733L, 34819200000000L, 26800L, 34905600000000L, 26867L, 34992000000000L, 26934L, 35078400000000L, 27001L, 35164800000000L, 27069L, 35251200000000L, 27136L, 35337600000000L, 27203L, 35424000000000L, 27270L, 35510400000000L, 27337L, 35596800000000L, 27405L, 35683200000000L, 27472L, 35769600000000L, 27539L, 35856000000000L, 27607L, 35942400000000L, 27674L, 36028800000000L, 27741L, 36115200000000L, 27808L, 36201600000000L, 27875L, 36288000000000L, 27942L, 36374400000000L, 28009L, 36460800000000L, 28076L, 36547200000000L, 28143L, 36633600000000L, 28210L, 36720000000000L, 28278L, 36806400000000L, 28347L, 36892800000000L, 28414L, 36979200000000L, 28482L, 37065600000000L, 28549L, 37152000000000L, 28616L, 37238400000000L, 28683L, 37324800000000L, 28750L, 37411200000000L, 28817L, 37497600000000L, 28884L, 37584000000000L, 28951L, 37670400000000L, 29018L, 37756800000000L, 29085L, 37843200000000L, 29152L, 37929600000000L, 29219L, 38016000000000L, 29286L, 38102400000000L, 29353L, 38188800000000L, 29421L, 38275200000000L, 29488L, 38361600000000L, 29555L, 38448000000000L, 29622L, 38534400000000L, 29690L, 38620800000000L, 29758L, 38707200000000L, 29825L, 38793600000000L, 29892L, 38880000000000L, 29959L, 38966400000000L, 30026L, 39052800000000L, 30093L, 39139200000000L, 30160L, 39225600000000L, 30229L, 39312000000000L, 30296L, 39398400000000L, 30363L, 39484800000000L, 30430L, 39571200000000L, 30497L, 39657600000000L, 30564L, 39744000000000L, 30631L, 39830400000000L, 30698L, 39916800000000L, 30765L, 40003200000000L, 30832L, 40089600000000L, 30899L, 40176000000000L, 30966L, 40262400000000L, 31033L, 40348800000000L, 31100L, 40435200000000L, 31167L, 40521600000000L, 31234L, 40608000000000L, 31301L, 40694400000000L, 31368L, 40780800000000L, 31435L, 40867200000000L, 31502L, 40953600000000L, 31569L, 41040000000000L, 31636L, 41126400000000L, 31703L, 41212800000000L, 31770L, 41299200000000L, 31838L, 41385600000000L, 31905L, 41472000000000L, 31972L, 41558400000000L, 32039L, 41644800000000L, 32106L, 41731200000000L, 32173L, 41817600000000L, 32240L, 41904000000000L, 32307L, 41990400000000L, 32374L, 42076800000000L, 32441L, 42163200000000L, 32508L, 42249600000000L, 32575L, 42336000000000L, 32643L, 42422400000000L, 32710L, 42508800000000L, 32777L, 42595200000000L, 32844L, 42681600000000L, 32911L, 42768000000000L, 32978L, 42854400000000L, 33045L, 42940800000000L, 33112L, 43027200000000L, 33179L, 43113600000000L, 33246L, 43200000000000L, 33313L, 43286400000000L, 33380L, 43372800000000L, 33447
                ),
                chunk(
                        "1971/2_1", 43459200000000L, 33514L, 43545600000000L, 33581L, 43632000000000L, 33648L, 43718400000000L, 33715L, 43804800000000L, 33782L, 43891200000000L, 33849L, 43977600000000L, 33916L, 44064000000000L, 33983L, 44150400000000L, 34050L, 44236800000000L, 34117L, 44323200000000L, 34184L, 44409600000000L, 34251L, 44496000000000L, 34318L, 44582400000000L, 34385L, 44668800000000L, 34452L, 44755200000000L, 34519L, 44841600000000L, 34586L, 44928000000000L, 34653L, 45014400000000L, 34720L, 45100800000000L, 34787L, 45187200000000L, 34854L, 45273600000000L, 34921L, 45360000000000L, 34989L, 45446400000000L, 35056L, 45532800000000L, 35123L, 45619200000000L, 35190L, 45705600000000L, 35257L, 45792000000000L, 35324L, 45878400000000L, 35391L, 45964800000000L, 35458L, 46051200000000L, 35525L, 46137600000000L, 35592L, 46224000000000L, 35659L, 46310400000000L, 35726L, 46396800000000L, 35793L, 46483200000000L, 35860L, 46569600000000L, 35928L, 46656000000000L, 35995L, 46742400000000L, 36062L, 46828800000000L, 36129L, 46915200000000L, 36196L, 47001600000000L, 36263L, 47088000000000L, 36330L, 47174400000000L, 36397L, 47260800000000L, 36464L, 47347200000000L, 36531L, 47433600000000L, 36599L, 47520000000000L, 36666L, 47606400000000L, 36733L, 47692800000000L, 36801L, 47779200000000L, 36868L, 47865600000000L, 36935L, 47952000000000L, 37002L, 48038400000000L, 37069L, 48124800000000L, 37137L, 48211200000000L, 37205L, 48297600000000L, 37272L, 48384000000000L, 37339L, 48470400000000L, 37406L, 48556800000000L, 37473L, 48643200000000L, 37540L, 48729600000000L, 37607L, 48816000000000L, 37674L, 48902400000000L, 37741L, 48988800000000L, 37808L, 49075200000000L, 37875L, 49161600000000L, 37942L, 49248000000000L, 38009L, 49334400000000L, 38076L, 49420800000000L, 38143L, 49507200000000L, 38210L, 49593600000000L, 38277L, 49680000000000L, 38344L, 49766400000000L, 38411L, 49852800000000L, 38478L, 49939200000000L, 38545L, 50025600000000L, 38613L, 50112000000000L, 38680L, 50198400000000L, 38747L, 50284800000000L, 38814L, 50371200000000L, 38881L, 50457600000000L, 38948L, 50544000000000L, 39016L, 50630400000000L, 39083L, 50716800000000L, 39150L, 50803200000000L, 39217L, 50889600000000L, 39284L, 50976000000000L, 39351L, 51062400000000L, 39418L, 51148800000000L, 39485L, 51235200000000L, 39552L, 51321600000000L, 39619L, 51408000000000L, 39687L, 51494400000000L, 39754L, 51580800000000L, 39821L, 51667200000000L, 39888L, 51753600000000L, 39956L, 51840000000000L, 40023L, 51926400000000L, 40090L, 52012800000000L, 40157L, 52099200000000L, 40224L, 52185600000000L, 40291L, 52272000000000L, 40358L, 52358400000000L, 40425L, 52444800000000L, 40492L, 52531200000000L, 40559L, 52617600000000L, 40626L, 52704000000000L, 40693L, 52790400000000L, 40760L, 52876800000000L, 40827L, 52963200000000L, 40894L, 53049600000000L, 40962L, 53136000000000L, 41029L, 53222400000000L, 41096L, 53308800000000L, 41163L, 53395200000000L, 41230L, 53481600000000L, 41297L, 53568000000000L, 41364L, 53654400000000L, 41431L, 53740800000000L, 41498L, 53827200000000L, 41565L, 53913600000000L, 41632L, 54000000000000L, 41699L, 54086400000000L, 41767L, 54172800000000L, 41834L, 54259200000000L, 41901L, 54345600000000L, 41968L, 54432000000000L, 42035L, 54518400000000L, 42102L, 54604800000000L, 42169L, 54691200000000L, 42236L, 54777600000000L, 42303L, 54864000000000L, 42370L, 54950400000000L, 42437L, 55036800000000L, 42504L, 55123200000000L, 42571L, 55209600000000L, 42638L, 55296000000000L, 42705L, 55382400000000L, 42772L, 55468800000000L, 42839L, 55555200000000L, 42906L, 55641600000000L, 42973L, 55728000000000L, 43040L, 55814400000000L, 43107L, 55900800000000L, 43174L, 55987200000000L, 43241L, 56073600000000L, 43308L, 56160000000000L, 43375L, 56246400000000L, 43442L, 56332800000000L, 43509L, 56419200000000L, 43576L, 56505600000000L, 43643L, 56592000000000L, 43710L, 56678400000000L, 43777L, 56764800000000L, 43844L, 56851200000000L, 43911L, 56937600000000L, 43978L, 57024000000000L, 44046L, 57110400000000L, 44113L, 57196800000000L, 44180L, 57283200000000L, 44247L, 57369600000000L, 44314L, 57456000000000L, 44381L, 57542400000000L, 44448L, 57628800000000L, 44516L, 57715200000000L, 44583L, 57801600000000L, 44650L, 57888000000000L, 44717L, 57974400000000L, 44784L, 58060800000000L, 44851L, 58147200000000L, 44918L, 58233600000000L, 44985L, 58320000000000L, 45052L, 58406400000000L, 45119L, 58492800000000L, 45186L, 58579200000000L, 45253L, 58665600000000L, 45320L, 58752000000000L, 45387L, 58838400000000L, 45454L, 58924800000000L, 45521L, 59011200000000L, 45588L, 59097600000000L, 45655L, 59184000000000L, 45722L, 59270400000000L, 45789L, 59356800000000L, 45856L, 59443200000000L, 45923L, 59529600000000L, 45990L, 59616000000000L, 46058L, 59702400000000L, 46125L, 59788800000000L, 46192L, 59875200000000L, 46259L, 59961600000000L, 46326L, 60048000000000L, 46393L, 60134400000000L, 46460L, 60220800000000L, 46527L, 60307200000000L, 46594L, 60393600000000L, 46661L, 60480000000000L, 46728L, 60566400000000L, 46795L, 60652800000000L, 46862L, 60739200000000L, 46929L, 60825600000000L, 46996L, 60912000000000L, 47063L, 60998400000000L, 47130L, 61084800000000L, 47197L, 61171200000000L, 47264L, 61257600000000L, 47331L, 61344000000000L, 47398L, 61430400000000L, 47465L, 61516800000000L, 47532L, 61603200000000L, 47599L, 61689600000000L, 47666L, 61776000000000L, 47733L, 61862400000000L, 47800L, 61948800000000L, 47867L, 62035200000000L, 47934L, 62121600000000L, 48001L, 62208000000000L, 48068L, 62294400000000L, 48135L, 62380800000000L, 48202L, 62467200000000L, 48269L, 62553600000000L, 48336L, 62640000000000L, 48403L, 62726400000000L, 48470L, 62812800000000L, 48537L, 62899200000000L, 48604L, 62985600000000L, 48671
                ),
                chunk("1972/2_1", 63072000000000L, 48738L, 63158400000000L, 48806L, 63244800000000L, 48873L, 63331200000000L, 48940L, 63417600000000L, 49007L, 63504000000000L, 49074L, 63590400000000L, 49142L, 63676800000000L, 49209L, 63763200000000L, 49276L, 63849600000000L, 49344L, 63936000000000L, 49411L, 64022400000000L, 49478L, 64108800000000L, 49545L, 64195200000000L, 49612L, 64281600000000L, 49679L, 64368000000000L, 49746L, 64454400000000L, 49813L, 64540800000000L, 49881L, 64627200000000L, 49948L, 64713600000000L, 50015L, 64800000000000L, 50082L, 64886400000000L, 50149),
                chunk(
                        "1972/3_1", 64972800000000L, 50216L, 65059200000000L, 50283L, 65145600000000L, 50350L, 65232000000000L, 50417L, 65318400000000L, 50484L, 65404800000000L, 50551L, 65491200000000L, 50618L, 65577600000000L, 50685L, 65664000000000L, 50752L, 65750400000000L, 50819L, 65836800000000L, 50886L, 65923200000000L, 50953L, 66009600000000L, 51020L, 66096000000000L, 51087L, 66182400000000L, 51154L, 66268800000000L, 51221L, 66355200000000L, 51288L, 66441600000000L, 51355L, 66528000000000L, 51422L, 66614400000000L, 51489L, 66700800000000L, 51556L, 66787200000000L, 51623L, 66873600000000L, 51691L, 66960000000000L, 51758L, 67046400000000L, 51825L, 67132800000000L, 51892L, 67219200000000L, 51959L, 67305600000000L, 52026L, 67392000000000L, 52093L, 67478400000000L, 52160L, 67564800000000L, 52227L, 67651200000000L, 52294L, 67737600000000L, 52361L, 67824000000000L, 52429L, 67910400000000L, 52497L, 67996800000000L, 52564L, 68083200000000L, 52631L, 68169600000000L, 52698L, 68256000000000L, 52765L, 68342400000000L, 52832L, 68428800000000L, 52899L, 68515200000000L, 52966L, 68601600000000L, 53033L, 68688000000000L, 53100L, 68774400000000L, 53167L, 68860800000000L, 53234L, 68947200000000L, 53301L, 69033600000000L, 53368L, 69120000000000L, 53435L, 69206400000000L, 53502L, 69292800000000L, 53569L, 69379200000000L, 53636L, 69465600000000L, 53703L, 69552000000000L, 53770L, 69638400000000L, 53837L, 69724800000000L, 53904L, 69811200000000L, 53972L, 69897600000000L, 54039L, 69984000000000L, 54106L, 70070400000000L, 54173L, 70156800000000L, 54240L, 70243200000000L, 54307L, 70329600000000L, 54374L, 70416000000000L, 54441L, 70502400000000L, 54508L, 70588800000000L, 54575L, 70675200000000L, 54642L, 70761600000000L, 54709L, 70848000000000L, 54776L, 70934400000000L, 54843L, 71020800000000L, 54910L, 71107200000000L, 54977L, 71193600000000L, 55044L, 71280000000000L, 55111L, 71366400000000L, 55178L, 71452800000000L, 55246L, 71539200000000L, 55313L, 71625600000000L, 55380L, 71712000000000L, 55447L, 71798400000000L, 55514L, 71884800000000L, 55581L, 71971200000000L, 55648L, 72057600000000L, 55715L, 72144000000000L, 55782L, 72230400000000L, 55849L, 72316800000000L, 55916L, 72403200000000L, 55983L, 72489600000000L, 56050L, 72576000000000L, 56117L, 72662400000000L, 56184L, 72748800000000L, 56251L, 72835200000000L, 56318L, 72921600000000L, 56385L, 73008000000000L, 56452L, 73094400000000L, 56519L, 73180800000000L, 56586L, 73267200000000L, 56653L, 73353600000000L, 56720L, 73440000000000L, 56787L, 73526400000000L, 56854L, 73612800000000L, 56921L, 73699200000000L, 56988L, 73785600000000L, 57055L, 73872000000000L, 57122L, 73958400000000L, 57189L, 74044800000000L, 57256L, 74131200000000L, 57323L, 74217600000000L, 57390L, 74304000000000L, 57457L, 74390400000000L, 57524L, 74476800000000L, 57591L, 74563200000000L, 57658L, 74649600000000L, 57725L, 74736000000000L, 57792L, 74822400000000L, 57859L, 74908800000000L, 57926L, 74995200000000L, 57993L, 75081600000000L, 58060L, 75168000000000L, 58127L, 75254400000000L, 58194L, 75340800000000L, 58261L, 75427200000000L, 58328L, 75513600000000L, 58395L, 75600000000000L, 58462L, 75686400000000L, 58529L, 75772800000000L, 58596L, 75859200000000L, 58663L, 75945600000000L, 58731L, 76032000000000L, 58798L, 76118400000000L, 58865L, 76204800000000L, 58933L, 76291200000000L, 59000L, 76377600000000L, 59068L, 76464000000000L, 59135L, 76550400000000L, 59202L, 76636800000000L, 59269L, 76723200000000L, 59336L, 76809600000000L, 59403L, 76896000000000L, 59470L, 76982400000000L, 59537L, 77068800000000L, 59604L, 77155200000000L, 59671L, 77241600000000L, 59738L, 77328000000000L, 59805L, 77414400000000L, 59872L, 77500800000000L, 59939L, 77587200000000L, 60007L, 77673600000000L, 60074L, 77760000000000L, 60142L, 77846400000000L, 60209L, 77932800000000L, 60276L, 78019200000000L, 60343L, 78105600000000L, 60410L, 78192000000000L, 60477L, 78278400000000L, 60544L, 78364800000000L, 60611L, 78451200000000L, 60678L, 78537600000000L, 60745L, 78624000000000L, 60812L, 78710400000000L, 60879L, 78796800000000L, 60946L, 78883200000000L, 61013L, 78969600000000L, 61080L, 79056000000000L, 61147L, 79142400000000L, 61215L, 79228800000000L, 61282L, 79315200000000L, 61349L, 79401600000000L, 61416L, 79488000000000L, 61483L, 79574400000000L, 61550L, 79660800000000L, 61617L, 79747200000000L, 61684L, 79833600000000L, 61751L, 79920000000000L, 61818L, 80006400000000L, 61885L, 80092800000000L, 61952L, 80179200000000L, 62019L, 80265600000000L, 62086L, 80352000000000L, 62153L, 80438400000000L, 62220L, 80524800000000L, 62287L, 80611200000000L, 62354L, 80697600000000L, 62421L, 80784000000000L, 62489L, 80870400000000L, 62556L, 80956800000000L, 62625L, 81043200000000L, 62692L, 81129600000000L, 62759L, 81216000000000L, 62826L, 81302400000000L, 62893L, 81388800000000L, 62960L, 81475200000000L, 63027L, 81561600000000L, 63094L, 81648000000000L, 63161L, 81734400000000L, 63228L, 81820800000000L, 63295L, 81907200000000L, 63362L, 81993600000000L, 63429L, 82080000000000L, 63496L, 82166400000000L, 63563L, 82252800000000L, 63630L, 82339200000000L, 63699L, 82425600000000L, 63766L, 82512000000000L, 63833L, 82598400000000L, 63900L, 82684800000000L, 63967L, 82771200000000L, 64034L, 82857600000000L, 64101L, 82944000000000L, 64168L, 83030400000000L, 64236L, 83116800000000L, 64303L, 83203200000000L, 64371L, 83289600000000L, 64438L, 83376000000000L, 64505L, 83462400000000L, 64572L, 83548800000000L, 64639L, 83635200000000L, 64706L, 83721600000000L, 64774L, 83808000000000L, 64841L, 83894400000000L, 64908L, 83980800000000L, 64975L, 84067200000000L, 65042L, 84153600000000L, 65109L, 84240000000000L, 65176L, 84326400000000L, 65244L, 84412800000000L, 65311L, 84499200000000L, 65378L, 84585600000000L, 65445L, 84672000000000L, 65512L, 84758400000000L, 65579L, 84844800000000L, 65646L, 84931200000000L, 65714L, 85017600000000L, 65781L, 85104000000000L, 65848L, 85190400000000L, 65915L, 85276800000000L, 65982L, 85363200000000L, 66049L, 85449600000000L, 66116L, 85536000000000L, 66183L, 85622400000000L, 66250L, 85708800000000L, 66317L, 85795200000000L, 66384L, 85881600000000L, 66452L, 85968000000000L, 66519L, 86054400000000L, 66586L, 86140800000000L, 66653L, 86227200000000L, 66720L, 86313600000000L, 66787L, 86400000000000L, 66854
                )
        );
    }

    @Test
    public void testIndexChunksInReverseOrderBigCsvByYear() throws Exception {
        assertIndexChunks(
                4, "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", PartitionBy.YEAR, "test-quotes-big-reverseorder.csv",
                chunk(
                        "1970/2_1", 21600000000000L, 50149, 21686400000000L, 50082, 21772800000000L, 50015, 21859200000000L, 49948, 21945600000000L, 49881, 22032000000000L, 49813, 22118400000000L, 49746, 22204800000000L, 49679, 22291200000000L, 49612, 22377600000000L, 49545, 22464000000000L, 49478, 22550400000000L, 49411, 22636800000000L, 49344, 22723200000000L, 49276, 22809600000000L, 49209, 22896000000000L, 49142, 22982400000000L, 49074, 23068800000000L, 49007, 23155200000000L, 48940, 23241600000000L, 48873, 23328000000000L, 48806, 23414400000000L, 48738, 23500800000000L, 48671, 23587200000000L, 48604, 23673600000000L, 48537, 23760000000000L, 48470, 23846400000000L, 48403, 23932800000000L, 48336, 24019200000000L, 48269, 24105600000000L, 48202, 24192000000000L, 48135, 24278400000000L, 48068, 24364800000000L, 48001, 24451200000000L, 47934, 24537600000000L, 47867, 24624000000000L, 47800, 24710400000000L, 47733, 24796800000000L, 47666, 24883200000000L, 47599, 24969600000000L, 47532, 25056000000000L, 47465, 25142400000000L, 47398, 25228800000000L, 47331, 25315200000000L, 47264, 25401600000000L, 47197, 25488000000000L, 47130, 25574400000000L, 47063, 25660800000000L, 46996, 25747200000000L, 46929, 25833600000000L, 46862, 25920000000000L, 46795, 26006400000000L, 46728, 26092800000000L, 46661, 26179200000000L, 46594, 26265600000000L, 46527, 26352000000000L, 46460, 26438400000000L, 46393, 26524800000000L, 46326, 26611200000000L, 46259, 26697600000000L, 46192, 26784000000000L, 46125, 26870400000000L, 46058, 26956800000000L, 45990, 27043200000000L, 45923, 27129600000000L, 45856, 27216000000000L, 45789, 27302400000000L, 45722, 27388800000000L, 45655, 27475200000000L, 45588, 27561600000000L, 45521, 27648000000000L, 45454, 27734400000000L, 45387, 27820800000000L, 45320, 27907200000000L, 45253, 27993600000000L, 45186, 28080000000000L, 45119, 28166400000000L, 45052, 28252800000000L, 44985, 28339200000000L, 44918, 28425600000000L, 44851, 28512000000000L, 44784, 28598400000000L, 44717, 28684800000000L, 44650, 28771200000000L, 44583, 28857600000000L, 44516, 28944000000000L, 44448, 29030400000000L, 44381, 29116800000000L, 44314, 29203200000000L, 44247, 29289600000000L, 44180, 29376000000000L, 44113, 29462400000000L, 44046, 29548800000000L, 43978, 29635200000000L, 43911, 29721600000000L, 43844, 29808000000000L, 43777, 29894400000000L, 43710, 29980800000000L, 43643, 30067200000000L, 43576, 30153600000000L, 43509, 30240000000000L, 43442, 30326400000000L, 43375, 30412800000000L, 43308, 30499200000000L, 43241, 30585600000000L, 43174, 30672000000000L, 43107, 30758400000000L, 43040, 30844800000000L, 42973, 30931200000000L, 42906, 31017600000000L, 42839, 31104000000000L, 42772, 31190400000000L, 42705, 31276800000000L, 42638, 31363200000000L, 42571, 31449600000000L, 42504
                ),
                chunk(
                        "1970/3_1", 86400000000L, 66854, 172800000000L, 66787, 259200000000L, 66720, 345600000000L, 66653, 432000000000L, 66586, 518400000000L, 66519, 604800000000L, 66452, 691200000000L, 66384, 777600000000L, 66317, 864000000000L, 66250, 950400000000L, 66183, 1036800000000L, 66116, 1123200000000L, 66049, 1209600000000L, 65982, 1296000000000L, 65915, 1382400000000L, 65848, 1468800000000L, 65781, 1555200000000L, 65714, 1641600000000L, 65646, 1728000000000L, 65579, 1814400000000L, 65512, 1900800000000L, 65445, 1987200000000L, 65378, 2073600000000L, 65311, 2160000000000L, 65244, 2246400000000L, 65176, 2332800000000L, 65109, 2419200000000L, 65042, 2505600000000L, 64975, 2592000000000L, 64908, 2678400000000L, 64841, 2764800000000L, 64774, 2851200000000L, 64706, 2937600000000L, 64639, 3024000000000L, 64572, 3110400000000L, 64505, 3196800000000L, 64438, 3283200000000L, 64371, 3369600000000L, 64303, 3456000000000L, 64236, 3542400000000L, 64168, 3628800000000L, 64101, 3715200000000L, 64034, 3801600000000L, 63967, 3888000000000L, 63900, 3974400000000L, 63833, 4060800000000L, 63766, 4147200000000L, 63699, 4233600000000L, 63630, 4320000000000L, 63563, 4406400000000L, 63496, 4492800000000L, 63429, 4579200000000L, 63362, 4665600000000L, 63295, 4752000000000L, 63228, 4838400000000L, 63161, 4924800000000L, 63094, 5011200000000L, 63027, 5097600000000L, 62960, 5184000000000L, 62893, 5270400000000L, 62826, 5356800000000L, 62759, 5443200000000L, 62692, 5529600000000L, 62625, 5616000000000L, 62556, 5702400000000L, 62489, 5788800000000L, 62421, 5875200000000L, 62354, 5961600000000L, 62287, 6048000000000L, 62220, 6134400000000L, 62153, 6220800000000L, 62086, 6307200000000L, 62019, 6393600000000L, 61952, 6480000000000L, 61885, 6566400000000L, 61818, 6652800000000L, 61751, 6739200000000L, 61684, 6825600000000L, 61617, 6912000000000L, 61550, 6998400000000L, 61483, 7084800000000L, 61416, 7171200000000L, 61349, 7257600000000L, 61282, 7344000000000L, 61215, 7430400000000L, 61147, 7516800000000L, 61080, 7603200000000L, 61013, 7689600000000L, 60946, 7776000000000L, 60879, 7862400000000L, 60812, 7948800000000L, 60745, 8035200000000L, 60678, 8121600000000L, 60611, 8208000000000L, 60544, 8294400000000L, 60477, 8380800000000L, 60410, 8467200000000L, 60343, 8553600000000L, 60276, 8640000000000L, 60209, 8726400000000L, 60142, 8812800000000L, 60074, 8899200000000L, 60007, 8985600000000L, 59939, 9072000000000L, 59872, 9158400000000L, 59805, 9244800000000L, 59738, 9331200000000L, 59671, 9417600000000L, 59604, 9504000000000L, 59537, 9590400000000L, 59470, 9676800000000L, 59403, 9763200000000L, 59336, 9849600000000L, 59269, 9936000000000L, 59202, 10022400000000L, 59135, 10108800000000L, 59068, 10195200000000L, 59000L, 10281600000000L, 58933, 10368000000000L, 58865, 10454400000000L, 58798, 10540800000000L, 58731, 10627200000000L, 58663, 10713600000000L, 58596, 10800000000000L, 58529, 10886400000000L, 58462, 10972800000000L, 58395, 11059200000000L, 58328, 11145600000000L, 58261, 11232000000000L, 58194, 11318400000000L, 58127, 11404800000000L, 58060, 11491200000000L, 57993, 11577600000000L, 57926, 11664000000000L, 57859, 11750400000000L, 57792, 11836800000000L, 57725, 11923200000000L, 57658, 12009600000000L, 57591, 12096000000000L, 57524, 12182400000000L, 57457, 12268800000000L, 57390, 12355200000000L, 57323, 12441600000000L, 57256, 12528000000000L, 57189, 12614400000000L, 57122, 12700800000000L, 57055, 12787200000000L, 56988, 12873600000000L, 56921, 12960000000000L, 56854, 13046400000000L, 56787, 13132800000000L, 56720, 13219200000000L, 56653, 13305600000000L, 56586, 13392000000000L, 56519, 13478400000000L, 56452, 13564800000000L, 56385, 13651200000000L, 56318, 13737600000000L, 56251, 13824000000000L, 56184, 13910400000000L, 56117, 13996800000000L, 56050, 14083200000000L, 55983, 14169600000000L, 55916, 14256000000000L, 55849, 14342400000000L, 55782, 14428800000000L, 55715, 14515200000000L, 55648, 14601600000000L, 55581, 14688000000000L, 55514, 14774400000000L, 55447, 14860800000000L, 55380, 14947200000000L, 55313, 15033600000000L, 55246, 15120000000000L, 55178, 15206400000000L, 55111, 15292800000000L, 55044, 15379200000000L, 54977, 15465600000000L, 54910, 15552000000000L, 54843, 15638400000000L, 54776, 15724800000000L, 54709, 15811200000000L, 54642, 15897600000000L, 54575, 15984000000000L, 54508, 16070400000000L, 54441, 16156800000000L, 54374, 16243200000000L, 54307, 16329600000000L, 54240, 16416000000000L, 54173, 16502400000000L, 54106, 16588800000000L, 54039, 16675200000000L, 53972, 16761600000000L, 53904, 16848000000000L, 53837, 16934400000000L, 53770, 17020800000000L, 53703, 17107200000000L, 53636, 17193600000000L, 53569, 17280000000000L, 53502, 17366400000000L, 53435, 17452800000000L, 53368, 17539200000000L, 53301, 17625600000000L, 53234, 17712000000000L, 53167, 17798400000000L, 53100, 17884800000000L, 53033, 17971200000000L, 52966, 18057600000000L, 52899, 18144000000000L, 52832, 18230400000000L, 52765, 18316800000000L, 52698, 18403200000000L, 52631, 18489600000000L, 52564, 18576000000000L, 52497, 18662400000000L, 52429, 18748800000000L, 52361, 18835200000000L, 52294, 18921600000000L, 52227, 19008000000000L, 52160, 19094400000000L, 52093, 19180800000000L, 52026, 19267200000000L, 51959, 19353600000000L, 51892, 19440000000000L, 51825, 19526400000000L, 51758, 19612800000000L, 51691, 19699200000000L, 51623, 19785600000000L, 51556, 19872000000000L, 51489, 19958400000000L, 51422, 20044800000000L, 51355, 20131200000000L, 51288, 20217600000000L, 51221, 20304000000000L, 51154, 20390400000000L, 51087, 20476800000000L, 51020, 20563200000000L, 50953, 20649600000000L, 50886, 20736000000000L, 50819, 20822400000000L, 50752, 20908800000000L, 50685, 20995200000000L, 50618, 21081600000000L, 50551, 21168000000000L, 50484, 21254400000000L, 50417, 21340800000000L, 50350, 21427200000000L, 50283, 21513600000000L, 50216
                ),
                chunk(
                        "1971/1_1", 43113600000000L, 33447, 43200000000000L, 33380, 43286400000000L, 33313, 43372800000000L, 33246, 43459200000000L, 33179, 43545600000000L, 33112, 43632000000000L, 33045, 43718400000000L, 32978, 43804800000000L, 32911, 43891200000000L, 32844, 43977600000000L, 32777, 44064000000000L, 32710, 44150400000000L, 32643, 44236800000000L, 32575, 44323200000000L, 32508, 44409600000000L, 32441, 44496000000000L, 32374, 44582400000000L, 32307, 44668800000000L, 32240, 44755200000000L, 32173, 44841600000000L, 32106, 44928000000000L, 32039, 45014400000000L, 31972, 45100800000000L, 31905, 45187200000000L, 31838, 45273600000000L, 31770, 45360000000000L, 31703, 45446400000000L, 31636, 45532800000000L, 31569, 45619200000000L, 31502, 45705600000000L, 31435, 45792000000000L, 31368, 45878400000000L, 31301, 45964800000000L, 31234, 46051200000000L, 31167, 46137600000000L, 31100, 46224000000000L, 31033, 46310400000000L, 30966, 46396800000000L, 30899, 46483200000000L, 30832, 46569600000000L, 30765, 46656000000000L, 30698, 46742400000000L, 30631, 46828800000000L, 30564, 46915200000000L, 30497, 47001600000000L, 30430, 47088000000000L, 30363, 47174400000000L, 30296, 47260800000000L, 30229, 47347200000000L, 30160, 47433600000000L, 30093, 47520000000000L, 30026, 47606400000000L, 29959, 47692800000000L, 29892, 47779200000000L, 29825, 47865600000000L, 29758, 47952000000000L, 29690, 48038400000000L, 29622, 48124800000000L, 29555, 48211200000000L, 29488, 48297600000000L, 29421, 48384000000000L, 29353, 48470400000000L, 29286, 48556800000000L, 29219, 48643200000000L, 29152, 48729600000000L, 29085, 48816000000000L, 29018, 48902400000000L, 28951, 48988800000000L, 28884, 49075200000000L, 28817, 49161600000000L, 28750, 49248000000000L, 28683, 49334400000000L, 28616, 49420800000000L, 28549, 49507200000000L, 28482, 49593600000000L, 28414, 49680000000000L, 28347, 49766400000000L, 28278, 49852800000000L, 28210, 49939200000000L, 28143, 50025600000000L, 28076, 50112000000000L, 28009, 50198400000000L, 27942, 50284800000000L, 27875, 50371200000000L, 27808, 50457600000000L, 27741, 50544000000000L, 27674, 50630400000000L, 27607, 50716800000000L, 27539, 50803200000000L, 27472, 50889600000000L, 27405, 50976000000000L, 27337, 51062400000000L, 27270, 51148800000000L, 27203, 51235200000000L, 27136, 51321600000000L, 27069, 51408000000000L, 27001, 51494400000000L, 26934, 51580800000000L, 26867, 51667200000000L, 26800, 51753600000000L, 26733, 51840000000000L, 26665, 51926400000000L, 26598, 52012800000000L, 26531, 52099200000000L, 26464, 52185600000000L, 26397, 52272000000000L, 26330, 52358400000000L, 26263, 52444800000000L, 26196, 52531200000000L, 26129, 52617600000000L, 26062, 52704000000000L, 25994, 52790400000000L, 25927, 52876800000000L, 25859, 52963200000000L, 25792, 53049600000000L, 25725, 53136000000000L, 25658, 53222400000000L, 25591, 53308800000000L, 25524, 53395200000000L, 25457, 53481600000000L, 25390, 53568000000000L, 25323, 53654400000000L, 25256, 53740800000000L, 25189, 53827200000000L, 25122, 53913600000000L, 25054, 54000000000000L, 24987, 54086400000000L, 24920, 54172800000000L, 24853, 54259200000000L, 24786, 54345600000000L, 24718, 54432000000000L, 24651, 54518400000000L, 24584, 54604800000000L, 24517, 54691200000000L, 24450, 54777600000000L, 24383, 54864000000000L, 24316, 54950400000000L, 24249, 55036800000000L, 24182, 55123200000000L, 24115, 55209600000000L, 24047, 55296000000000L, 23980, 55382400000000L, 23913, 55468800000000L, 23845, 55555200000000L, 23778, 55641600000000L, 23711, 55728000000000L, 23643, 55814400000000L, 23575, 55900800000000L, 23508, 55987200000000L, 23441, 56073600000000L, 23374, 56160000000000L, 23307, 56246400000000L, 23240, 56332800000000L, 23172, 56419200000000L, 23105, 56505600000000L, 23038, 56592000000000L, 22971, 56678400000000L, 22904, 56764800000000L, 22837, 56851200000000L, 22770, 56937600000000L, 22703, 57024000000000L, 22636, 57110400000000L, 22569, 57196800000000L, 22502, 57283200000000L, 22435, 57369600000000L, 22368, 57456000000000L, 22301, 57542400000000L, 22234, 57628800000000L, 22167, 57715200000000L, 22100, 57801600000000L, 22031, 57888000000000L, 21964, 57974400000000L, 21897, 58060800000000L, 21830, 58147200000000L, 21763, 58233600000000L, 21696, 58320000000000L, 21629, 58406400000000L, 21560, 58492800000000L, 21493, 58579200000000L, 21426, 58665600000000L, 21358, 58752000000000L, 21291, 58838400000000L, 21224, 58924800000000L, 21157, 59011200000000L, 21090, 59097600000000L, 21023, 59184000000000L, 20956, 59270400000000L, 20889, 59356800000000L, 20822, 59443200000000L, 20755, 59529600000000L, 20688, 59616000000000L, 20621, 59702400000000L, 20554, 59788800000000L, 20487, 59875200000000L, 20420, 59961600000000L, 20352, 60048000000000L, 20285, 60134400000000L, 20218, 60220800000000L, 20151, 60307200000000L, 20084, 60393600000000L, 20017, 60480000000000L, 19950, 60566400000000L, 19883, 60652800000000L, 19816, 60739200000000L, 19749, 60825600000000L, 19682, 60912000000000L, 19615, 60998400000000L, 19548, 61084800000000L, 19481, 61171200000000L, 19414, 61257600000000L, 19347, 61344000000000L, 19280, 61430400000000L, 19213, 61516800000000L, 19146, 61603200000000L, 19079, 61689600000000L, 19012, 61776000000000L, 18945, 61862400000000L, 18878, 61948800000000L, 18810, 62035200000000L, 18743, 62121600000000L, 18676, 62208000000000L, 18609, 62294400000000L, 18542, 62380800000000L, 18475, 62467200000000L, 18408, 62553600000000L, 18341, 62640000000000L, 18274, 62726400000000L, 18207, 62812800000000L, 18138, 62899200000000L, 18071, 62985600000000L, 18004
                ),
                chunk(
                        "1971/2_1", 31536000000000L, 42437, 31622400000000L, 42370, 31708800000000L, 42303, 31795200000000L, 42236, 31881600000000L, 42169, 31968000000000L, 42102, 32054400000000L, 42035, 32140800000000L, 41968, 32227200000000L, 41901, 32313600000000L, 41834, 32400000000000L, 41767, 32486400000000L, 41699, 32572800000000L, 41632, 32659200000000L, 41565, 32745600000000L, 41498, 32832000000000L, 41431, 32918400000000L, 41364, 33004800000000L, 41297, 33091200000000L, 41230, 33177600000000L, 41163, 33264000000000L, 41096, 33350400000000L, 41029, 33436800000000L, 40962, 33523200000000L, 40894, 33609600000000L, 40827, 33696000000000L, 40760, 33782400000000L, 40693, 33868800000000L, 40626, 33955200000000L, 40559, 34041600000000L, 40492, 34128000000000L, 40425, 34214400000000L, 40358, 34300800000000L, 40291, 34387200000000L, 40224, 34473600000000L, 40157, 34560000000000L, 40090, 34646400000000L, 40023, 34732800000000L, 39956, 34819200000000L, 39888, 34905600000000L, 39821, 34992000000000L, 39754, 35078400000000L, 39687, 35164800000000L, 39619, 35251200000000L, 39552, 35337600000000L, 39485, 35424000000000L, 39418, 35510400000000L, 39351, 35596800000000L, 39284, 35683200000000L, 39217, 35769600000000L, 39150, 35856000000000L, 39083, 35942400000000L, 39016, 36028800000000L, 38948, 36115200000000L, 38881, 36201600000000L, 38814, 36288000000000L, 38747, 36374400000000L, 38680, 36460800000000L, 38613, 36547200000000L, 38545, 36633600000000L, 38478, 36720000000000L, 38411, 36806400000000L, 38344, 36892800000000L, 38277, 36979200000000L, 38210, 37065600000000L, 38143, 37152000000000L, 38076, 37238400000000L, 38009, 37324800000000L, 37942, 37411200000000L, 37875, 37497600000000L, 37808, 37584000000000L, 37741, 37670400000000L, 37674, 37756800000000L, 37607, 37843200000000L, 37540, 37929600000000L, 37473, 38016000000000L, 37406, 38102400000000L, 37339, 38188800000000L, 37272, 38275200000000L, 37205, 38361600000000L, 37137, 38448000000000L, 37069, 38534400000000L, 37002, 38620800000000L, 36935, 38707200000000L, 36868, 38793600000000L, 36801, 38880000000000L, 36733, 38966400000000L, 36666, 39052800000000L, 36599, 39139200000000L, 36531, 39225600000000L, 36464, 39312000000000L, 36397, 39398400000000L, 36330, 39484800000000L, 36263, 39571200000000L, 36196, 39657600000000L, 36129, 39744000000000L, 36062, 39830400000000L, 35995, 39916800000000L, 35928, 40003200000000L, 35860, 40089600000000L, 35793, 40176000000000L, 35726, 40262400000000L, 35659, 40348800000000L, 35592, 40435200000000L, 35525, 40521600000000L, 35458, 40608000000000L, 35391, 40694400000000L, 35324, 40780800000000L, 35257, 40867200000000L, 35190, 40953600000000L, 35123, 41040000000000L, 35056, 41126400000000L, 34989, 41212800000000L, 34921, 41299200000000L, 34854, 41385600000000L, 34787, 41472000000000L, 34720, 41558400000000L, 34653, 41644800000000L, 34586, 41731200000000L, 34519, 41817600000000L, 34452, 41904000000000L, 34385, 41990400000000L, 34318, 42076800000000L, 34251, 42163200000000L, 34184, 42249600000000L, 34117, 42336000000000L, 34050, 42422400000000L, 33983, 42508800000000L, 33916, 42595200000000L, 33849, 42681600000000L, 33782, 42768000000000L, 33715, 42854400000000L, 33648, 42940800000000L, 33581, 43027200000000L, 33514
                ),
                chunk(
                        "1972/0_1", 64627200000000L, 16730, 64713600000000L, 16663, 64800000000000L, 16596, 64886400000000L, 16529, 64972800000000L, 16462, 65059200000000L, 16395, 65145600000000L, 16328, 65232000000000L, 16261, 65318400000000L, 16194, 65404800000000L, 16127, 65491200000000L, 16060, 65577600000000L, 15993, 65664000000000L, 15926, 65750400000000L, 15859, 65836800000000L, 15792, 65923200000000L, 15725, 66009600000000L, 15657, 66096000000000L, 15590, 66182400000000L, 15523, 66268800000000L, 15455, 66355200000000L, 15388, 66441600000000L, 15321, 66528000000000L, 15254, 66614400000000L, 15187, 66700800000000L, 15120, 66787200000000L, 15052, 66873600000000L, 14985, 66960000000000L, 14918, 67046400000000L, 14851, 67132800000000L, 14783, 67219200000000L, 14716, 67305600000000L, 14649, 67392000000000L, 14581, 67478400000000L, 14514, 67564800000000L, 14447, 67651200000000L, 14380, 67737600000000L, 14313, 67824000000000L, 14246, 67910400000000L, 14179, 67996800000000L, 14112, 68083200000000L, 14045, 68169600000000L, 13978, 68256000000000L, 13911, 68342400000000L, 13844, 68428800000000L, 13777, 68515200000000L, 13710, 68601600000000L, 13642, 68688000000000L, 13575, 68774400000000L, 13508, 68860800000000L, 13441, 68947200000000L, 13374, 69033600000000L, 13307, 69120000000000L, 13240, 69206400000000L, 13172, 69292800000000L, 13104, 69379200000000L, 13037, 69465600000000L, 12970, 69552000000000L, 12903, 69638400000000L, 12836, 69724800000000L, 12769, 69811200000000L, 12702, 69897600000000L, 12635, 69984000000000L, 12568, 70070400000000L, 12501, 70156800000000L, 12434, 70243200000000L, 12366, 70329600000000L, 12299, 70416000000000L, 12231, 70502400000000L, 12164, 70588800000000L, 12097, 70675200000000L, 12030, 70761600000000L, 11963, 70848000000000L, 11896, 70934400000000L, 11829, 71020800000000L, 11762, 71107200000000L, 11695, 71193600000000L, 11628, 71280000000000L, 11561, 71366400000000L, 11494, 71452800000000L, 11427, 71539200000000L, 11360, 71625600000000L, 11293, 71712000000000L, 11226, 71798400000000L, 11158, 71884800000000L, 11091, 71971200000000L, 11024, 72057600000000L, 10957, 72144000000000L, 10890, 72230400000000L, 10823, 72316800000000L, 10756, 72403200000000L, 10689, 72489600000000L, 10622, 72576000000000L, 10555, 72662400000000L, 10488, 72748800000000L, 10421, 72835200000000L, 10354, 72921600000000L, 10287, 73008000000000L, 10218, 73094400000000L, 10151, 73180800000000L, 10084, 73267200000000L, 10017, 73353600000000L, 9949, 73440000000000L, 9882, 73526400000000L, 9815, 73612800000000L, 9748, 73699200000000L, 9681, 73785600000000L, 9614, 73872000000000L, 9547, 73958400000000L, 9480, 74044800000000L, 9412, 74131200000000L, 9345, 74217600000000L, 9278, 74304000000000L, 9210, 74390400000000L, 9143, 74476800000000L, 9076, 74563200000000L, 9009, 74649600000000L, 8942, 74736000000000L, 8875, 74822400000000L, 8808, 74908800000000L, 8741, 74995200000000L, 8674, 75081600000000L, 8607, 75168000000000L, 8540, 75254400000000L, 8473, 75340800000000L, 8406, 75427200000000L, 8339, 75513600000000L, 8271, 75600000000000L, 8204, 75686400000000L, 8137, 75772800000000L, 8070, 75859200000000L, 8003, 75945600000000L, 7936, 76032000000000L, 7869, 76118400000000L, 7802, 76204800000000L, 7735, 76291200000000L, 7668, 76377600000000L, 7601, 76464000000000L, 7534, 76550400000000L, 7466, 76636800000000L, 7399, 76723200000000L, 7332, 76809600000000L, 7265, 76896000000000L, 7198, 76982400000000L, 7131, 77068800000000L, 7064, 77155200000000L, 6997, 77241600000000L, 6930, 77328000000000L, 6863, 77414400000000L, 6796, 77500800000000L, 6729, 77587200000000L, 6662, 77673600000000L, 6595, 77760000000000L, 6528, 77846400000000L, 6461, 77932800000000L, 6396, 78019200000000L, 6331, 78105600000000L, 6266, 78192000000000L, 6201, 78278400000000L, 6136, 78364800000000L, 6071, 78451200000000L, 6006, 78537600000000L, 5941, 78624000000000L, 5876, 78710400000000L, 5811, 78796800000000L, 5746, 78883200000000L, 5681, 78969600000000L, 5616, 79056000000000L, 5551, 79142400000000L, 5486, 79228800000000L, 5421, 79315200000000L, 5356, 79401600000000L, 5291, 79488000000000L, 5226, 79574400000000L, 5161, 79660800000000L, 5096, 79747200000000L, 5031, 79833600000000L, 4965, 79920000000000L, 4900, 80006400000000L, 4835, 80092800000000L, 4770, 80179200000000L, 4705, 80265600000000L, 4640, 80352000000000L, 4575, 80438400000000L, 4510, 80524800000000L, 4445, 80611200000000L, 4380, 80697600000000L, 4315, 80784000000000L, 4250, 80870400000000L, 4185, 80956800000000L, 4120, 81043200000000L, 4055, 81129600000000L, 3990, 81216000000000L, 3925, 81302400000000L, 3860, 81388800000000L, 3794, 81475200000000L, 3729, 81561600000000L, 3664, 81648000000000L, 3599, 81734400000000L, 3534, 81820800000000L, 3469, 81907200000000L, 3404, 81993600000000L, 3339, 82080000000000L, 3274, 82166400000000L, 3208, 82252800000000L, 3143, 82339200000000L, 3078, 82425600000000L, 3013, 82512000000000L, 2948, 82598400000000L, 2883, 82684800000000L, 2818, 82771200000000L, 2753, 82857600000000L, 2688, 82944000000000L, 2623, 83030400000000L, 2558, 83116800000000L, 2493, 83203200000000L, 2428, 83289600000000L, 2363, 83376000000000L, 2298, 83462400000000L, 2233, 83548800000000L, 2168, 83635200000000L, 2102, 83721600000000L, 2037, 83808000000000L, 1972, 83894400000000L, 1902, 83980800000000L, 1836, 84067200000000L, 1771, 84153600000000L, 1705, 84240000000000L, 1640, 84326400000000L, 1575, 84412800000000L, 1510, 84499200000000L, 1445, 84585600000000L, 1380, 84672000000000L, 1315, 84758400000000L, 1249, 84844800000000L, 1184, 84931200000000L, 1119, 85017600000000L, 1054, 85104000000000L, 989, 85190400000000L, 924, 85276800000000L, 859, 85363200000000L, 794, 85449600000000L, 728, 85536000000000L, 663, 85622400000000L, 598, 85708800000000L, 535, 85795200000000L, 472, 85881600000000L, 409, 85968000000000L, 346, 86054400000000L, 283, 86140800000000L, 220, 86227200000000L, 157, 86313600000000L, 94, 86400000000000L, 31
                ),
                chunk("1972/1_1", 63072000000000L, 17937, 63158400000000L, 17870, 63244800000000L, 17803, 63331200000000L, 17736, 63417600000000L, 17669, 63504000000000L, 17602, 63590400000000L, 17535, 63676800000000L, 17468, 63763200000000L, 17401, 63849600000000L, 17334, 63936000000000L, 17267, 64022400000000L, 17200, 64108800000000L, 17133, 64195200000000L, 17066, 64281600000000L, 16999, 64368000000000L, 16931, 64454400000000L, 16864, 64540800000000L, 16797)
        );
    }

    @Test
    public void testIndexChunksInSingleLineCsvWithPool() throws Exception {
        assertIndexChunks(
                4, "test-quotes-oneline.csv",
                chunk("2022-05-14/0_1", 1652529121000000L, 18L)
        );
    }

    @Test
    public void testIndexChunksInSmallCsvWith1Worker() throws Exception {
        assertIndexChunks(
                1, "test-quotes-small.csv",
                chunk("2022-05-10/0_1", 1652183520000000L, 15L),
                chunk("2022-05-11/0_1", 1652269920000000L, 90L, 1652269920001000L, 185L)
        );
    }

    @Test
    public void testIndexChunksInSmallCsvWith2Workers() throws Exception {
        assertIndexChunks(
                2, "test-quotes-small.csv",
                chunk("2022-05-10/0_1", 1652183520000000L, 15L),
                chunk("2022-05-11/0_1", 1652269920000000L, 90L),
                chunk("2022-05-11/1_1", 1652269920001000L, 185L)
        );
    }

    @Test
    public void testIndexChunksInSmallCsvWith3Workers() throws Exception {
        assertIndexChunks(
                3, "test-quotes-small.csv",
                chunk("2022-05-10/0_1", 1652183520000000L, 15L),
                chunk("2022-05-11/1_1", 1652269920000000L, 90L),
                chunk("2022-05-11/2_1", 1652269920001000L, 185L)
        );
    }

    @Test
    public void testIndexChunksInSmallCsvWith4Workers() throws Exception {
        assertIndexChunks(
                4, "test-quotes-small.csv",
                chunk("2022-05-10/0_1", 1652183520000000L, 15L),
                chunk("2022-05-11/1_1", 1652269920000000L, 90L),
                chunk("2022-05-11/2_1", 1652269920001000L, 185L)
        );
    }

    @Test
    public void testParallelCopyProcessingQueueCapacityZero() throws Exception {
        executeWithPool(
                1, 0, TestFilesFacadeImpl.INSTANCE, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try {
                        executeCopy(compiler, sqlExecutionContext);
                        engine.getCopyContext().clear();
                        executeCopy(compiler, sqlExecutionContext);
                        Assert.fail();
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), "Unable to process the import request. Another import request may be in progress.");
                    }
                }
        );
    }

    @Test
    public void testRunManyImportsSequentially() throws Exception {
        int run = 0;

        for (int workers = 2; workers < 3; workers++) {
            for (int queueSize = 1; queueSize < 9; queueSize <<= 1) {
                LOG.info().$("run [no=").$(run++).$(",workers=").$(workers).$(",queueSize=").$(queueSize).I$();

                int workerCount = workers;
                executeWithPool(
                        workers, queueSize, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext context) -> {
                            try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, workerCount)) {
                                importer.setMinChunkSize(1);

                                importAndCleanupTable(
                                        importer,
                                        context,
                                        compiler,
                                        "tab17",
                                        "test-quotes-big.csv",
                                        PartitionBy.YEAR,
                                        "ts",
                                        "yyyy-MM-ddTHH:mm:ss.SSSSSSZ",
                                        true,
                                        1000
                                );

                                importAndCleanupTable(
                                        importer,
                                        context,
                                        compiler,
                                        "alltypes",
                                        "test-alltypes-with-gaps.csv",
                                        PartitionBy.MONTH,
                                        "tstmp",
                                        "yyyy-MM-ddTHH:mm:ss.SSSUUUZ",
                                        true,
                                        10
                                );

                                importAndCleanupTable(
                                        importer,
                                        context,
                                        compiler,
                                        "alltypes_errors",
                                        "test-errors.csv",
                                        PartitionBy.HOUR,
                                        "tstmp",
                                        "yyyy-MM-ddTHH:mm:ss.SSSSSSZ",
                                        true,
                                        13
                                );

                                compiler.compile("create table testimport (StrSym symbol index,Int symbol,Int_Col int,DoubleCol double,IsoDate timestamp,Fmt1Date timestamp,Fmt2Date date,Phone string,boolean boolean,long long) timestamp(IsoDate) partition by DAY;", sqlExecutionContext);

                                importAndCleanupTable(
                                        importer,
                                        context,
                                        compiler,
                                        "testimport",
                                        "test-import.csv",
                                        PartitionBy.DAY,
                                        "IsoDate",
                                        "yyyy-MM-ddTHH:mm:ss.SSSZ",
                                        false,
                                        128
                                );
                            }
                        }
                );
            }
        }
    }

    @Test
    public void testWhenImportFailsWhenAttachingPartitionsThenPreExistingTableIsStillEmpty() throws Exception {
        FilesFacade brokenFf = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ path) {
                if (Utf8s.endsWithAscii(path, "1972-09" + configuration.getAttachPartitionSuffix() + File.separator + "ts.d")) {
                    return -1;
                }
                return super.openRO(path);
            }
        };

        executeWithPool(
                4, 8, brokenFf, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab21 ( line symbol, ts timestamp, d double, description string) timestamp(ts) partition by MONTH;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.setMinChunkSize(1);
                        importer.of("tab21", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), "could not attach [partition='1972-09'");
                    }

                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck("cnt\n0\n", "select count(*) cnt from tab21", null, false, false, true);
                }
        );
    }

    @Test
    public void testWhenImportFailsWhenMovingPartitionsThenPreExistingTableIsStillEmpty() throws Exception {
        FilesFacade brokenFf = new TestFilesFacadeImpl() {
            @Override
            public int copy(LPSZ from, LPSZ to) {
                return -1;
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (Utf8s.endsWithAscii(from, "1972-09" + File.separator)) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };

        executeWithPool(
                4, 8, brokenFf, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table tab20 ( line symbol, ts timestamp, d double, description string) timestamp(ts) partition by MONTH;", sqlExecutionContext);

                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.setMinChunkSize(1);
                        importer.of("tab20", "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), "could not copy partition file");
                    }

                    refreshTablesInBaseEngine();
                    assertQueryNoLeakCheck("cnt\n0\n", "select count(*) cnt from tab20", null, false, false, true);
                }
        );
    }

    @Test
    public void testWhenImportFailsWhileAttachingPartitionThenNewlyCreatedTableIsRemoved() throws Exception {
        FilesFacade brokenFf = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ path) {
                if (Utf8s.endsWithAscii(path, "1972-09" + configuration.getAttachPartitionSuffix() + File.separator + "ts.d")) {
                    return -1;
                }
                return super.openRO(path);
            }
        };

        assertImportFailsWith("tab19", brokenFf, "could not attach [partition='1972-09'");
    }

    @Test
    public void testWhenImportFailsWhileMovingPartitionThenNewlyCreatedTableIsRemoved() throws Exception {
        FilesFacade brokenFf = new TestFilesFacadeImpl() {
            @Override
            public int copy(LPSZ from, LPSZ to) {
                return -1;
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (Utf8s.endsWithAscii(from, "1972-09" + File.separator)) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };

        assertImportFailsWith("tab18", brokenFf, "could not copy partition fil");
    }

    @Test
    public void testWhenRenameBreaksBecauseTempFilesAreOnDifferentFSThanDbDirThenImportStillWorks() throws Exception {
        AtomicInteger counter = new AtomicInteger(0);
        FilesFacade brokenRename = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (counter.incrementAndGet() < 11) {
                    return Files.FILES_RENAME_ERR_EXDEV;
                }
                return super.rename(from, to);
            }
        };
        executeWithPool(4, 8, brokenRename, this::importAllIntoNew);
    }

    private static boolean stackContains(String klass) {
        return Arrays.stream(new Exception().getStackTrace())
                .anyMatch(stackTraceElement -> stackTraceElement.getClassName().endsWith(klass));
    }

    private void assertChunkBoundariesFor(String fileName, LongList expectedBoundaries, int workerCount) throws TextImportException {
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path().of(inputRoot).slash().concat(fileName);
             ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, workerCount)) {
            importer.setMinChunkSize(1);
            importer.of("table", fileName, 1, PartitionBy.DAY, (byte) ',', "unknown", null, false);

            long fd = ff.openRO(path.$());
            long length = ff.length(fd);
            Assert.assertTrue(fd > -1);

            try {
                LongList actualBoundaries = importer.phaseBoundaryCheck(length);
                Assert.assertEquals(expectedBoundaries, actualBoundaries);
            } finally {
                ff.close(fd);
            }
        }
    }

    private void assertColumnNameException(String fileName, boolean forceHeader, String message) throws Exception {
        executeWithPool(
                4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.of("tab60", fileName, 1, PartitionBy.DAY, (byte) ',', "ts", null, forceHeader);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), message);
                    }
                }
        );
    }

    private void assertImportFailsInPhase(String tableName, FilesFacade brokenFf, String phase) throws Exception {
        executeWithPool(
                4, 8, brokenFf, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    execute(compiler, "create table " + tableName + " ( line symbol index, ts timestamp, d double, description string) timestamp(ts) partition by YEAR;", sqlExecutionContext);
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.setMinChunkSize(1);
                        importer.of(tableName, "test-quotes-big.csv", 1, PartitionBy.YEAR, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), "import failed [phase=" + phase);
                    }
                }
        );
    }

    private void assertImportFailsWith(String tableName, FilesFacade brokenFf, String expectedError) throws Exception {
        executeWithPool(
                4, 8, brokenFf, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.setMinChunkSize(1);
                        importer.of(tableName, "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), expectedError);
                    }

                    try {
                        compiler.compile("select count(*) from " + tableName + ";", sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getMessage(), "table does not exist");
                    }
                }
        );
    }

    private void assertIndexChunks(int workerCount, String fileName, IndexChunk... expectedChunks) throws Exception {
        assertIndexChunks(workerCount, "yyyy-MM-ddTHH:mm:ss.SSSZ", PartitionBy.DAY, fileName, expectedChunks);
    }

    private void assertIndexChunks(int workerCount, String dateFormat, int partitionBy, String fileName, IndexChunk... expectedChunks) throws Exception {
        executeWithPool(
                workerCount, 8,
                (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) ->
                        assertIndexChunksFor(sqlExecutionContext, workerCount, dateFormat, partitionBy, fileName, expectedChunks)
        );
    }

    private void assertIndexChunksFor(
            SqlExecutionContext sqlExecutionContext, int workerCount, String format, int partitionBy,
            String fileName, IndexChunk... expectedChunks
    ) {
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        inputRoot = TestUtils.getCsvRoot();

        try (
                Path path = new Path().of(inputRoot).concat(fileName);
                ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, workerCount)
        ) {
            importer.setMinChunkSize(1);
            importer.of("tableName", fileName, 1, partitionBy, (byte) ',', "ts", format, false);

            long fd = TableUtils.openRO(ff, path.$(), LOG);
            try {
                importer.parseStructure(fd, sqlExecutionContext.getSecurityContext());
                long length = ff.length(fd);
                importer.phaseBoundaryCheck(length);
                importer.phaseIndexing();
            } finally {
                ff.close(fd);
            }
        }

        TableToken tableToken = engine.verifyTableName("tableName");
        ObjList<IndexChunk> actualChunks = readIndexChunks(new File(inputWorkRoot, tableToken.getDirName()));
        Assert.assertEquals(list(expectedChunks), actualChunks);
    }

    private void executeCopy(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        CompiledQuery cq = compiler.compile(
                "copy xy from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' partition by MONTH on error ABORT; ",
                sqlExecutionContext
        );
        try (RecordCursor cursor = cq.getRecordCursorFactory().getCursor(sqlExecutionContext)) {
            Assert.assertTrue(cursor.hasNext());
        }
    }

    private void importAllIntoExisting(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException, TextImportException {
        execute(
                compiler,
                "create table alltypes (\n" +
                        "  bo boolean,\n" +
                        "  by byte,\n" +
                        "  sh short,\n" +
                        "  ch char,\n" +
                        "  in_ int,\n" +
                        "  lo long,\n" +
                        "  dat date, \n" +
                        "  tstmp timestamp, \n" +
                        "  ft float,\n" +
                        "  db double,\n" +
                        "  str string,\n" +
                        "  sym symbol,\n" +
                        "  l256 long256," +
                        "  ge geohash(20b)," +
                        "  uid uuid" +
                        ") timestamp(tstmp) partition by DAY;", sqlExecutionContext
        );
        try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
            importer.of("alltypes", "test-alltypes.csv", 1, PartitionBy.DAY, (byte) ',', "tstmp", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", true);
            importer.process(AllowAllSecurityContext.INSTANCE);
        }

        refreshTablesInBaseEngine();
        assertQueryNoLeakCheck(
                "bo\tby\tsh\tch\tin_\tlo\tdat\ttstmp\tft\tdb\tstr\tsym\tl256\tge\tuid\n" +
                        "false\t106\t22716\tG\t1\t1\t1970-01-02T00:00:00.000Z\t1970-01-02T00:00:00.000000Z\t1.1\t1.2\ts1\tsy1\t0x0adaa43b7700522b82f4e8d8d7b8c41a985127d17ca3926940533c477c927a33\tu33d\t11111111-1111-1111-1111-111111111111\n" +
                        "false\t29\t8654\tS\t2\t2\t1970-01-03T00:00:00.000Z\t1970-01-03T00:00:00.000000Z\t2.1\t2.2\ts2\tsy2\t0x593c9b7507c60ec943cd1e308a29ac9e645f3f4104fa76983c50b65784d51e37\tu33d\t11111111-1111-1111-2222-111111111111\n" +
                        "false\t104\t11600\tT\t3\t3\t1970-01-04T00:00:00.000Z\t1970-01-04T00:00:00.000000Z\t3.1\t3.2\ts3\tsy3\t0x30cb58d11566e857a87063d9dba8961195ddd1458f633b7f285307c11a7072d1\tu33d\t11111111-1111-1111-3333-111111111111\n" +
                        "false\t105\t31772\tC\t4\t4\t1970-01-05T00:00:00.000Z\t1970-01-05T00:00:00.000000Z\t4.1\t4.2\ts4\tsy4\t0x64ad74a1e1e5e5897c61daeff695e8be6ab8ea52090049faa3306e2d2440176e\tu33d\t11111111-1111-1111-4444-111111111111\n" +
                        "false\t123\t8110\tE\t5\t5\t1970-01-06T00:00:00.000Z\t1970-01-06T00:00:00.000000Z\t5.1\t5.2\ts5\tsy5\t0x5a86aaa24c707fff785191c8901fd7a16ffa1093e392dc537967b0fb8165c161\tu33d\t11111111-1111-1111-5555-111111111111\n" +
                        "false\t98\t25729\tM\t6\t6\t1970-01-07T00:00:00.000Z\t1970-01-07T00:00:00.000000Z\t6.1\t6.2\ts6\tsy6\t0x8fbdd90a38ecfaa89b71e0b7a1d088ada82ff4bad36b72c47056f3fabd4cfeed\tu33d\t11111111-1111-1111-6666-111111111111\n" +
                        "false\t44\t-19823\tU\t7\t7\t1970-01-08T00:00:00.000Z\t1970-01-08T00:00:00.000000Z\t7.1\t7.2\ts7\tsy7\t0xfb87e052526d72b5faf2f76f0f4bd855bc983a6991a2e7c78c671857b35a8755\tu33d\t11111111-1111-1111-7777-111111111111\n" +
                        "true\t102\t5672\tS\t8\t8\t1970-01-09T00:00:00.000Z\t1970-01-09T00:00:00.000000Z\t8.1\t8.2\ts8\tsy8\t0x6df9f4797b131d69aa4f08d320dde2dc72cb5a65911401598a73264e80123440\tu33d\t11111111-1111-1111-8888-111111111111\n" +
                        "false\t73\t-5962\tE\t9\t9\t1970-01-10T00:00:00.000Z\t1970-01-10T00:00:00.000000Z\t9.1\t9.2\ts9\tsy9\t0xdc33dd2e6ea8cc86a6ef5e562486cceb67886eea99b9dd07ba84e3fba7f66cd6\tu33d\t11111111-1111-1111-9999-111111111111\n" +
                        "true\t61\t-17553\tD\t10\t10\t1970-01-11T00:00:00.000Z\t1970-01-11T00:00:00.000000Z\t10.1\t10.2\ts10\tsy10\t0x83e9d33db60120e69ba3fb676e3280ed6a6e16373be3139063343d28d3738449\tu33d\t11111111-1111-1111-0000-111111111111\n",
                "select * from alltypes", "tstmp", true, false, true
        );
    }

    private void importAllIntoNew(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws Exception {
        try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 1)) {
            importer.of("alltypes", "test-alltypes.csv", 1, PartitionBy.DAY, (byte) ',', "tstmp", "yyyy-MM-ddTHH:mm:ss.SSSUUUZ", true);
            importer.process(AllowAllSecurityContext.INSTANCE);
        }

        refreshTablesInBaseEngine();

        assertQuery(
                "bo\tby\tsh\tch\tin_\tlo\tdat\ttstmp\tft\tdb\tstr\tsym\tl256\tge\tuid\n" +
                        "false\t106\t22716\tG\t1\t1\t1970-01-02T00:00:00.000Z\t1970-01-02T00:00:00.000000Z\t1.1\t1.2\ts1\tsy1\t0x0adaa43b7700522b82f4e8d8d7b8c41a985127d17ca3926940533c477c927a33\tu33d\t11111111-1111-1111-1111-111111111111\n" +
                        "false\t29\t8654\tS\t2\t2\t1970-01-03T00:00:00.000Z\t1970-01-03T00:00:00.000000Z\t2.1\t2.2\ts2\tsy2\t0x593c9b7507c60ec943cd1e308a29ac9e645f3f4104fa76983c50b65784d51e37\tu33d\t11111111-1111-1111-2222-111111111111\n" +
                        "false\t104\t11600\tT\t3\t3\t1970-01-04T00:00:00.000Z\t1970-01-04T00:00:00.000000Z\t3.1\t3.2\ts3\tsy3\t0x30cb58d11566e857a87063d9dba8961195ddd1458f633b7f285307c11a7072d1\tu33d\t11111111-1111-1111-3333-111111111111\n" +
                        "false\t105\t31772\tC\t4\t4\t1970-01-05T00:00:00.000Z\t1970-01-05T00:00:00.000000Z\t4.1\t4.2\ts4\tsy4\t0x64ad74a1e1e5e5897c61daeff695e8be6ab8ea52090049faa3306e2d2440176e\tu33d\t11111111-1111-1111-4444-111111111111\n" +
                        "false\t123\t8110\tE\t5\t5\t1970-01-06T00:00:00.000Z\t1970-01-06T00:00:00.000000Z\t5.1\t5.2\ts5\tsy5\t0x5a86aaa24c707fff785191c8901fd7a16ffa1093e392dc537967b0fb8165c161\tu33d\t11111111-1111-1111-5555-111111111111\n" +
                        "false\t98\t25729\tM\t6\t6\t1970-01-07T00:00:00.000Z\t1970-01-07T00:00:00.000000Z\t6.1\t6.2\ts6\tsy6\t0x8fbdd90a38ecfaa89b71e0b7a1d088ada82ff4bad36b72c47056f3fabd4cfeed\tu33d\t11111111-1111-1111-6666-111111111111\n" +
                        "false\t44\t-19823\tU\t7\t7\t1970-01-08T00:00:00.000Z\t1970-01-08T00:00:00.000000Z\t7.1\t7.2\ts7\tsy7\t0xfb87e052526d72b5faf2f76f0f4bd855bc983a6991a2e7c78c671857b35a8755\tu33d\t11111111-1111-1111-7777-111111111111\n" +
                        "true\t102\t5672\tS\t8\t8\t1970-01-09T00:00:00.000Z\t1970-01-09T00:00:00.000000Z\t8.1\t8.2\ts8\tsy8\t0x6df9f4797b131d69aa4f08d320dde2dc72cb5a65911401598a73264e80123440\tu33d\t11111111-1111-1111-8888-111111111111\n" +
                        "false\t73\t-5962\tE\t9\t9\t1970-01-10T00:00:00.000Z\t1970-01-10T00:00:00.000000Z\t9.1\t9.2\ts9\tsy9\t0xdc33dd2e6ea8cc86a6ef5e562486cceb67886eea99b9dd07ba84e3fba7f66cd6\tu33d\t11111111-1111-1111-9999-111111111111\n" +
                        "true\t61\t-17553\tD\t10\t10\t1970-01-11T00:00:00.000Z\t1970-01-11T00:00:00.000000Z\t10.1\t10.2\ts10\tsy10\t0x83e9d33db60120e69ba3fb676e3280ed6a6e16373be3139063343d28d3738449\tu33d\t11111111-1111-1111-0000-111111111111\n",
                "select * from alltypes",
                "tstmp",
                true,
                true
        );

        assertQueryNoLeakCheck(
                compiler,
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                        "bo\tBOOLEAN\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                        "by\tINT\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                        "sh\tINT\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                        "ch\tCHAR\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                        "in_\tINT\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                        "lo\tINT\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                        "dat\tDATE\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                        "tstmp\tTIMESTAMP\tfalse\t256\tfalse\t0\t0\ttrue\tfalse\n" +
                        "ft\tDOUBLE\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                        "db\tDOUBLE\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                        "str\t" + stringTypeName + "\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                        "sym\t" + stringTypeName + "\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                        "l256\tLONG256\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                        "ge\t" + stringTypeName + "\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                        "uid\tUUID\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n",
                "show columns from alltypes",
                null,
                sqlExecutionContext,
                false,
                false
        );
    }

    private void importAndCleanupTable(
            ParallelCsvFileImporter importer,
            SqlExecutionContext context,
            SqlCompiler compiler,
            String tableName,
            String inputFileName,
            int partitionBy,
            String timestampColumn,
            String timestampFormat,
            boolean forceHeader,
            int expectedCount
    ) throws Exception {
        importer.of(
                tableName,
                inputFileName,
                1,
                partitionBy,
                (byte) ',',
                timestampColumn,
                timestampFormat,
                forceHeader,
                null,
                Atomicity.SKIP_COL
        );
        importer.process(AllowAllSecurityContext.INSTANCE);
        importer.clear();
        assertQueryNoLeakCheck(
                compiler,
                "cnt\n" + expectedCount + "\n",
                "select count(*) cnt from " + tableName,
                null,
                false,
                context,
                true
        );
        CairoEngine cairoEngine = context.getCairoEngine();
        cairoEngine.execute("drop table " + tableName, context);
    }

    private LongList list(long... values) {
        LongList result = new LongList();
        for (long l : values) {
            result.add(l);
        }
        return result;
    }

    private void testImportCsvIntoNewTable0(String tableName) throws Exception {
        executeWithPool(
                16, 16, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 16)) {
                        importer.setMinChunkSize(10);
                        importer.of(tableName, "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }

                    refreshTablesInBaseEngine();
                    assertQuery(
                            "cnt\n" +
                                    "1000\n",
                            "select count(*) cnt from " + tableName,
                            null, false, true
                    );
                    assertQueryNoLeakCheck(
                            "line\tts\td\tdescription\n" +
                                    "line991\t1972-09-18T00:00:00.000000Z\t0.744582123075\tdesc 991\n" +
                                    "line992\t1972-09-19T00:00:00.000000Z\t0.107142280151\tdesc 992\n" +
                                    "line993\t1972-09-20T00:00:00.000000Z\t0.0974353165713\tdesc 993\n" +
                                    "line994\t1972-09-21T00:00:00.000000Z\t0.81272025622\tdesc 994\n" +
                                    "line995\t1972-09-22T00:00:00.000000Z\t0.566736320714\tdesc 995\n" +
                                    "line996\t1972-09-23T00:00:00.000000Z\t0.415739766699\tdesc 996\n" +
                                    "line997\t1972-09-24T00:00:00.000000Z\t0.378956184893\tdesc 997\n" +
                                    "line998\t1972-09-25T00:00:00.000000Z\t0.736755687844\tdesc 998\n" +
                                    "line999\t1972-09-26T00:00:00.000000Z\t0.910141500002\tdesc 999\n" +
                                    "line1000\t1972-09-27T00:00:00.000000Z\t0.918270255022\tdesc 1000\n",
                            "select * from " + tableName + " limit -10",
                            "ts", true, false, true
                    );
                }
        );
    }

    private void testImportThrowsException(String tableName, String fileName, int partitionBy, String tsCol, String tsFormat, String expectedError) throws Exception {
        testImportThrowsException(TestFilesFacadeImpl.INSTANCE, tableName, fileName, partitionBy, tsCol, tsFormat, expectedError);
    }

    private void testImportThrowsException(FilesFacade ff, String tableName, String fileName, int partitionBy, String tsCol, String tsFormat, String expectedError) throws Exception {
        executeWithPool(
                4, 8, ff, (CairoEngine engine1, SqlCompiler compiler1, SqlExecutionContext sqlExecutionContext1) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine1, 4)) {
                        importer.of(tableName, fileName, 1, partitionBy, (byte) ',', tsCol, tsFormat, true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail("exception expected");
                    } catch (Exception e) {
                        TestUtils.assertContains(e.getMessage(), expectedError);
                    }
                }
        );
    }

    private void testImportTimestampIntoExisting(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampColumn) throws SqlException {
        execute(
                compiler,
                "CREATE TABLE 'timestamp_test' ( \n" +
                        "    id INT,\n" +
                        "    ts TIMESTAMP,\n" +
                        "    ts_ns TIMESTAMP_NS\n" +
                        ") timestamp(" + timestampColumn + ") PARTITION BY DAY",
                sqlExecutionContext
        );
        try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
            importer.of("timestamp_test", "test-timestamps.csv", 1, PartitionBy.DAY, (byte) ',', timestampColumn, null, true);
            importer.process(AllowAllSecurityContext.INSTANCE);
        }

        refreshTablesInBaseEngine();
        assertQueryNoLeakCheck(
                "id\tts\tts_ns\n" +
                        "1\t2025-08-05T00:00:00.000001Z\t2025-08-05T00:00:00.000000001Z\n" +
                        "2\t2025-08-06T00:00:00.000002Z\t2025-08-06T00:00:00.000000002Z\n" +
                        "3\t2025-08-07T00:00:00.000003Z\t2025-08-07T00:00:00.000000003Z\n" +
                        "4\t2025-08-08T00:00:00.000004Z\t2025-08-08T00:00:00.000000004Z\n" +
                        "5\t2025-08-09T00:00:00.000005Z\t2025-08-09T00:00:00.000000005Z\n" +
                        "6\t2025-08-10T00:00:00.000006Z\t2025-08-10T00:00:00.000000006Z\n" +
                        "7\t2025-08-11T00:00:00.000007Z\t2025-08-11T00:00:00.000000007Z\n" +
                        "8\t2025-08-12T00:00:00.000008Z\t2025-08-12T00:00:00.000000008Z\n" +
                        "9\t2025-08-13T00:00:00.000009Z\t2025-08-13T00:00:00.000000009Z\n",
                "select * from timestamp_test",
                timestampColumn,
                true,
                false,
                true
        );
    }

    private void testImportTimestampIntoExistingWithTypeMismatch(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampColumn) throws SqlException {
        execute(
                compiler,
                "CREATE TABLE 'timestamp_test' ( \n" +
                        "    id INT,\n" +
                        "    ts TIMESTAMP_NS,\n" +
                        "    ts_ns TIMESTAMP\n" +
                        ") timestamp(" + timestampColumn + ") PARTITION BY DAY",
                sqlExecutionContext
        );
        try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
            importer.of("timestamp_test", "test-timestamps.csv", 1, PartitionBy.DAY, (byte) ',', timestampColumn, null, true);
            importer.process(AllowAllSecurityContext.INSTANCE);
        }

        refreshTablesInBaseEngine();
        assertQueryNoLeakCheck(
                "id\tts\tts_ns\n" +
                        "1\t2025-08-05T00:00:00.000001000Z\t2025-08-05T00:00:00.000000Z\n" +
                        "2\t2025-08-06T00:00:00.000002000Z\t2025-08-06T00:00:00.000000Z\n" +
                        "3\t2025-08-07T00:00:00.000003000Z\t2025-08-07T00:00:00.000000Z\n" +
                        "4\t2025-08-08T00:00:00.000004000Z\t2025-08-08T00:00:00.000000Z\n" +
                        "5\t2025-08-09T00:00:00.000005000Z\t2025-08-09T00:00:00.000000Z\n" +
                        "6\t2025-08-10T00:00:00.000006000Z\t2025-08-10T00:00:00.000000Z\n" +
                        "7\t2025-08-11T00:00:00.000007000Z\t2025-08-11T00:00:00.000000Z\n" +
                        "8\t2025-08-12T00:00:00.000008000Z\t2025-08-12T00:00:00.000000Z\n" +
                        "9\t2025-08-13T00:00:00.000009000Z\t2025-08-13T00:00:00.000000Z\n",
                "select * from timestamp_test",
                timestampColumn,
                true,
                false,
                true
        );
    }

    private void testImportTimestampTypeToNew(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampColumn) throws Exception {
        try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 1)) {
            importer.of("timestamp_test", "test-timestamps.csv", 1, PartitionBy.DAY, (byte) ',', timestampColumn, null, true);
            importer.process(AllowAllSecurityContext.INSTANCE);
        }

        refreshTablesInBaseEngine();

        assertQuery(
                "id\tts\tts_ns\n" +
                        "1\t2025-08-05T00:00:00.000001Z\t2025-08-05T00:00:00.000000001Z\n" +
                        "2\t2025-08-06T00:00:00.000002Z\t2025-08-06T00:00:00.000000002Z\n" +
                        "3\t2025-08-07T00:00:00.000003Z\t2025-08-07T00:00:00.000000003Z\n" +
                        "4\t2025-08-08T00:00:00.000004Z\t2025-08-08T00:00:00.000000004Z\n" +
                        "5\t2025-08-09T00:00:00.000005Z\t2025-08-09T00:00:00.000000005Z\n" +
                        "6\t2025-08-10T00:00:00.000006Z\t2025-08-10T00:00:00.000000006Z\n" +
                        "7\t2025-08-11T00:00:00.000007Z\t2025-08-11T00:00:00.000000007Z\n" +
                        "8\t2025-08-12T00:00:00.000008Z\t2025-08-12T00:00:00.000000008Z\n" +
                        "9\t2025-08-13T00:00:00.000009Z\t2025-08-13T00:00:00.000000009Z\n",
                "select * from timestamp_test",
                timestampColumn,
                true,
                true
        );

        assertQueryNoLeakCheck(
                compiler,
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                        "id\tINT\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\n" +
                        "ts\tTIMESTAMP\tfalse\t256\tfalse\t0\t0\t" + (timestampColumn.equals("ts") ? "true" : "false") + "\tfalse\n" +
                        "ts_ns\tTIMESTAMP_NS\tfalse\t256\tfalse\t0\t0\t" + (timestampColumn.equals("ts_ns") ? "true" : "false") + "\tfalse\n",
                "show columns from timestamp_test",
                null,
                sqlExecutionContext,
                false,
                false
        );
    }

    private void testImportTooSmallFileBuffer0(String tableName) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COPY_BUFFER_SIZE, 50);
        executeWithPool(
                2,
                2,
                (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 4)) {
                        importer.setMinChunkSize(10);
                        importer.of(tableName, "test-quotes-big.csv", 1, PartitionBy.MONTH, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.process(AllowAllSecurityContext.INSTANCE);
                        Assert.fail();
                    } catch (TextImportException e) {
                        TestUtils.assertContains(e.getMessage(), "buffer overflow");
                    }
                }
        );
    }

    private void testStatusLogCleanup(int daysToKeep) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            String backlogTableName = configuration.getSystemTableNamePrefix() + "text_import_log";
            execute(
                    compiler, "create table \"" + backlogTableName + "\" as " +
                            "(" +
                            "select" +
                            " timestamp_sequence(0, 100000000000) ts," +
                            " rnd_symbol(5,4,4,3) table_name," +
                            " rnd_symbol(5,4,4,3) file," +
                            " rnd_symbol(5,4,4,3) phase," +
                            " rnd_symbol(5,4,4,3) status," +
                            " rnd_str(5,4,4,3) message," +
                            " rnd_long() rows_handled," +
                            " rnd_long() rows_imported," +
                            " rnd_long() errors" +
                            " from" +
                            " long_sequence(5)" +
                            ") timestamp(ts) partition by DAY", sqlExecutionContext
            );

            node1.setProperty(PropertyKey.CAIRO_SQL_COPY_LOG_RETENTION_DAYS, daysToKeep);
            new CopyRequestJob(engine, 1).close();
            assertQuery(
                    "count\n" + daysToKeep + "\n",
                    "select count() from " + backlogTableName,
                    null,
                    false,
                    true
            );
            engine.execute("drop table \"" + backlogTableName + "\"", sqlExecutionContext);
        }
    }

    static IndexChunk chunk(String path, long... data) {
        return new IndexChunk(path, data);
    }

    protected static void execute(
            @Nullable WorkerPool pool,
            TextImportRunnable runnable,
            CairoConfiguration configuration
    ) throws Exception {
        final int workerCount = pool == null ? 1 : pool.getWorkerCount();
        try (
                final CairoEngine engine = new CairoEngine(configuration);
                final SqlCompiler compiler = engine.getSqlCompiler()
        ) {

            try (final SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine, workerCount)) {
                try {
                    if (pool != null) {
                        CopyJob.assignToPool(engine.getMessageBus(), pool);
                        pool.start(LOG);
                    }

                    runnable.run(engine, compiler, sqlExecutionContext);
                    Assert.assertEquals("busy writer", 0, engine.getBusyWriterCount());
                    Assert.assertEquals("busy reader", 0, engine.getBusyReaderCount());
                } finally {
                    if (pool != null) {
                        pool.halt();
                    }
                }
            }
        }
    }

    static List<File> findAllFilesIn(File root) {
        List<File> result = new ArrayList<>();
        File[] files = root.listFiles();
        if (files == null) {
            return result;
        }
        for (File f : files) {
            if (f.isDirectory()) {
                result.addAll(findAllFilesIn(f));
            } else if (f.isFile()) {
                result.add(f);
            }
        }

        return result;
    }

    static ObjList<IndexChunk> list(IndexChunk... chunks) {
        ObjList<IndexChunk> result = new ObjList<>(chunks.length);
        for (IndexChunk chunk : chunks) {
            result.add(chunk);
        }
        return result;
    }

    static ObjList<IndexChunk> readIndexChunks(File root) {
        List<File> indexChunks = findAllFilesIn(root);
        indexChunks.sort(Comparator.comparing(File::getAbsolutePath));

        Path p = new Path();
        MemoryCMARWImpl memory = new MemoryCMARWImpl();
        ObjList<IndexChunk> result = new ObjList<>();

        try {
            long MASK = ~((255L) << 56 | (255L) << 48);

            for (File chunk : indexChunks) {
                p.of(chunk.getAbsolutePath()).$();
                memory.smallFile(engine.getConfiguration().getFilesFacade(), p.$(), MemoryTag.NATIVE_DEFAULT);
                long[] data = new long[(int) chunk.length() / Long.BYTES];

                for (int i = 0; i < data.length; i++) {
                    long val = memory.getLong(i * Long.BYTES);
                    if ((i & 1) == 1) {
                        val = val & MASK; // ignore length packed in offset for time being
                    }
                    data[i] = val;
                }

                // we use '/' as the path separator on all OSes to simply test code
                result.add(new IndexChunk(chunk.getParentFile().getName() + "/" + chunk.getName(), data));
            }
        } finally {
            p.close();
            memory.close(false);
        }

        return result;
    }

    protected void executeWithPool(
            int workerCount,
            int queueCapacity,
            TextImportRunnable runnable
    ) throws Exception {
        executeWithPool(workerCount, queueCapacity, TestFilesFacadeImpl.INSTANCE, runnable);
    }

    protected void executeWithPool(
            int workerCount,
            int queueCapacity,
            FilesFacade ff,
            TextImportRunnable runnable
    ) throws Exception {
        // we need to create entire engine
        assertMemoryLeak(() -> {
            if (workerCount > 0) {
                final CairoConfiguration configuration1 = new DefaultTestCairoConfiguration(root) {
                    @Override
                    public @NotNull FilesFacade getFilesFacade() {
                        return ff;
                    }

                    @Override
                    public @NotNull IOURingFacade getIOURingFacade() {
                        return ioURingFacade;
                    }

                    @Override
                    public int getSqlCopyBufferSize() {
                        return configuration.getSqlCopyBufferSize();
                    }

                    @Override
                    public CharSequence getSqlCopyInputRoot() {
                        return ParallelCsvFileImporterTest.inputRoot;
                    }

                    @Override
                    public CharSequence getSqlCopyInputWorkRoot() {
                        return ParallelCsvFileImporterTest.inputWorkRoot;
                    }

                    @Override
                    public int getSqlCopyQueueCapacity() {
                        return queueCapacity;
                    }
                };
                WorkerPool pool = new WorkerPool(() -> workerCount);
                execute(pool, runnable, configuration1);
            } else {
                // we need to create entire engine
                final CairoConfiguration configuration1 = new DefaultTestCairoConfiguration(root) {
                    @Override
                    public @NotNull FilesFacade getFilesFacade() {
                        return ff;
                    }

                    @Override
                    public @NotNull IOURingFacade getIOURingFacade() {
                        return ioURingFacade;
                    }

                    @Override
                    public int getSqlCopyBufferSize() {
                        return configuration.getSqlCopyBufferSize();
                    }

                    @Override
                    public int getSqlCopyQueueCapacity() {
                        return queueCapacity;
                    }
                };
                execute(null, runnable, configuration1);
            }
        });
    }

    @FunctionalInterface
    interface TextImportRunnable {
        void run(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws Exception;
    }

    static class IndexChunk {
        long[] data; // timestamp+offset pairs
        String path;

        IndexChunk(String path, long... data) {
            this.path = path;
            this.data = data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IndexChunk that = (IndexChunk) o;
            return path.equals(that.path) && Arrays.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(path);
            result = 31 * result + Arrays.hashCode(data);
            return result;
        }

        @Override
        public String toString() {
            return "{" +
                    "path='" + path + '\'' +
                    ", data=" + Arrays.toString(data) +
                    '}';
        }
    }
}
