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

import io.questdb.PropertyKey;
import io.questdb.cairo.AttachDetachStatus;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.idx.IndexFactory;
import io.questdb.cairo.idx.IndexWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.FindVisitor;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.questdb.cairo.AttachDetachStatus.*;
import static io.questdb.cairo.TableUtils.*;

@RunWith(Parameterized.class)
public class AlterTableDetachPartitionTest extends AbstractAlterTableAttachPartitionTest {
    // how assertAttachPartitionReadsCurrentVersionOfUpdatedColumn() attaches the partition
    private static final int ATTACH_WITHOUT_COLUMN_VERSION_FILE = 1;
    private static final int ATTACH_WITH_COLUMN_VERSION_FILE = 0;
    private static final int ATTACH_WITH_KNOWN_SIZE = 2;
    private static O3PartitionPurgeJob purgeJob;
    private final TestTimestampType timestampType;

    public AlterTableDetachPartitionTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
        purgeJob = new O3PartitionPurgeJob(engine, 1);
    }

    @AfterClass
    public static void tearDownStatic() {
        purgeJob = Misc.free(purgeJob);
        AbstractCairoTest.tearDownStatic();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Override
    public void setUp() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_INDEX_TYPE, TestUtils.randomSymbolIndexTypeName(rnd));
        super.setUp();
    }

    @Test
    public void testAlreadyDetached1() throws Exception {
        assertFailure(
                "tab143",
                "ALTER TABLE tab143 DETACH PARTITION LIST '2022-06-03', '2022-06-03'",
                "could not detach partition [table=tab143, detachStatus=DETACH_ERR_MISSING_PARTITION"
        );
    }

    @Test
    public void testAlreadyDetached2() throws Exception {
        assertFailure(
                null,
                "tab143",
                "ALTER TABLE tab143 DETACH PARTITION LIST '2022-06-03'",
                "could not detach partition [table=tab143, detachStatus=DETACH_ERR_ALREADY_DETACHED",
                () -> {
                    TableToken tableToken = engine.verifyTableName("tab143");
                    path.of(configuration.getDbRoot())
                            .concat(tableToken)
                            .concat("2022-06-03")
                            .put(DETACHED_DIR_MARKER)
                            .slash$();
                    Assert.assertEquals(0, TestFilesFacadeImpl.INSTANCE.mkdirs(path, 509));
                }
        );
    }

    @Test
    public void testAttachCannotCopy() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_ATTACH_PARTITION_COPY, true);
            String tableName = "tabDetachAttachMissingMeta";
            node1.setProperty(
                    PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX,
                    DETACHED_DIR_MARKER
            );
            ff = new TestFilesFacadeImpl() {
                @Override
                public int copyRecursive(Path src, Path dst, int dirMode) {
                    if (Utf8s.containsAscii(src, DETACHED_DIR_MARKER)) {
                        return 1000;
                    }
                    return 0;
                }
            };

            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    10,
                    "2022-06-01",
                    3
            );
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'");
            assertQuery("select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix("""
                            first\tts
                            2022-06-03T02:23:59.300000Z\t2022-06-03T02:23:59.300000Z
                            """));

            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'",
                    ATTACH_ERR_COPY.name()
            );
        });
    }

    @Test
    public void testAttachFailsCopyMeta() throws Exception {
        FilesFacadeImpl ff1 = new TestFilesFacadeImpl() {
            int i = 0;

            @Override
            public int copy(LPSZ src, LPSZ dest) {
                if (Utf8s.containsAscii(dest, META_FILE_NAME)) {
                    i++;
                    if (i == 3) {
                        return -1;
                    }
                }
                return super.copy(src, dest);
            }
        };
        node1.setProperty(
                PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX,
                DETACHED_DIR_MARKER
        );
        assertMemoryLeak(ff1, () -> {
            String tableName = "tab";

            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    10,
                    "2022-06-01",
                    3
            );
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'");
            assertQuery("select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix("""
                            first\tts
                            2022-06-03T02:23:59.300000Z\t2022-06-03T02:23:59.300000Z
                            """));

            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'");
            assertFailure("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'", DETACH_ERR_COPY_META.name());
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'");
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'");

            assertQuery("select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix("""
                            first\tts
                            2022-06-01T07:11:59.900000Z\t2022-06-01T07:11:59.900000Z
                            2022-06-02T11:59:59.500000Z\t2022-06-02T07:11:59.900000Z
                            2022-06-03T09:35:59.200000Z\t2022-06-03T07:11:59.900000Z
                            """));
        });
    }

    @Test
    public void testAttachFailsRetried() throws Exception {
        FilesFacadeImpl ff1 = new TestFilesFacadeImpl() {
            int i = 0;

            @Override
            public int rename(LPSZ src, LPSZ dst) {
                if (Utf8s.containsAscii(src, DETACHED_DIR_MARKER) && i++ < 2) {
                    return -1;
                }
                super.rename(src, dst);
                return 0;
            }
        };
        assertMemoryLeak(ff1, () -> {
            String tableName = "tabDetachAttachMissingMeta";
            node1.setProperty(
                    PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX,
                    DETACHED_DIR_MARKER
            );

            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    10,
                    "2022-06-01",
                    3
            );
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'");
            assertQuery("select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix("""
                            first\tts
                            2022-06-03T02:23:59.300000Z\t2022-06-03T02:23:59.300000Z
                            """));

            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'",
                    "could not attach partition"
            );
            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'",
                    "could not attach partition"
            );

            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'");
            assertQuery("select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix("""
                            first\tts
                            2022-06-01T07:11:59.900000Z\t2022-06-01T07:11:59.900000Z
                            2022-06-02T11:59:59.500000Z\t2022-06-02T07:11:59.900000Z
                            2022-06-03T09:35:59.200000Z\t2022-06-03T07:11:59.900000Z
                            """));
        });
    }

    @Test
    public void testAttachNativePartitionAfterTruncateRestoresSymbolNullFlag() throws Exception {
        assertAttachPartitionAfterTruncateRestoresSymbolNullFlag(false, true, true);
    }

    @Test
    public void testAttachRestoresDetachedDirWhenScanOfRenamedSymbolFails() throws Exception {
        assertAttachRestoresDetachedDirWhenSymbolScanFails(false, false, true);
    }

    @Test
    public void testAttachRestoresDetachedDirWhenScanOfRenamedSymbolFailsAfterWriterRecreation() throws Exception {
        assertAttachRestoresDetachedDirWhenSymbolScanFails(false, true, true);
    }

    @Test
    public void testAttachRestoresDetachedDirWhenScanOfRenamedSymbolFailsInCopyMode() throws Exception {
        assertAttachRestoresDetachedDirWhenSymbolScanFails(true, false, true);
    }

    @Test
    public void testAttachRestoresDetachedDirWhenSymbolScanFails() throws Exception {
        assertAttachRestoresDetachedDirWhenSymbolScanFails(false, false, false);
    }

    @Test
    public void testAttachRestoresDetachedDirWhenSymbolScanFailsAfterWriterRecreation() throws Exception {
        assertAttachRestoresDetachedDirWhenSymbolScanFails(false, true, false);
    }

    @Test
    public void testAttachRestoresDetachedDirWhenSymbolScanFailsInCopyMode() throws Exception {
        assertAttachRestoresDetachedDirWhenSymbolScanFails(true, false, false);
    }

    @Test
    public void testAttachRetryAfterFailedLink() throws Exception {
        assertAttachRetryAfterFailedLink(false);
    }

    @Test
    public void testAttachRetryAfterFailedLinkWhenLeftoverStays() throws Exception {
        assertAttachRetryAfterFailedLink(true);
    }

    /**
     * Outside ADAPTIVE an ATTACH renames .attachable into the partition's directory. A failure after the rename whose
     * rename back fails too leaves the whole partition, the only copy, under the txn name, and no .attachable. The
     * file system reports no entry types, so no writer open purges it. The retry's message must name the step that
     * keeps the data: rename it back, not remove it.
     */
    @Test
    public void testAttachRetryAfterFailedRenameBack() throws Exception {
        // ADAPTIVE links the source; the other modes rename it
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        final AtomicBoolean isEntryTypeUnknown = new AtomicBoolean();
        final AtomicBoolean isNullFlagReadFailing = new AtomicBoolean();
        final AtomicBoolean isRenameBackFailing = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int findType(long findPtr) {
                return isEntryTypeUnknown.get() ? Files.DT_UNKNOWN : super.findType(findPtr);
            }

            @Override
            public long openRO(LPSZ name) {
                // The ATTACH reads the symbol column of the renamed partition for the column's null flag
                if (isNullFlagReadFailing.get() && isAttachDestination(name) && Utf8s.endsWithAscii(name, Files.SEPARATOR + "sym.d")) {
                    isNullFlagReadFailing.set(false);
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (isRenameBackFailing.get() && isAttachDestination(from) && Utf8s.endsWithAscii(to, configuration.getAttachPartitionSuffix())) {
                    isRenameBackFailing.set(false);
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO tab VALUES
                        ('2024-01-01T00:00:00Z', 1, 'A'),
                        ('2024-01-01T01:00:00Z', 2, 'B'),
                        ('2024-01-02T00:00:00Z', 3, 'C')
                    """);
            drainWalQueue();
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            renameDetachedToAttachable("tab", "2024-01-01");
            final TableToken tableToken = engine.verifyTableName("tab");
            final java.nio.file.Path tableDir = java.nio.file.Path.of(configuration.getDbRoot().toString(), tableToken.getDirName());
            final String attachableName = "2024-01-01" + configuration.getAttachPartitionSuffix();

            isEntryTypeUnknown.set(true);
            try {
                isNullFlagReadFailing.set(true);
                isRenameBackFailing.set(true);
                execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
                drainWalQueue();
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
                final String leftover = findAttachDestination(tableToken);
                Assert.assertNotNull(leftover);
                Assert.assertFalse(java.nio.file.Files.exists(tableDir.resolve(attachableName)));

                execute("ALTER TABLE tab RESUME WAL");
                drainWalQueue();
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
                final String error = engine.getTableSequencerAPI().getTxnTracker(tableToken).getErrorMessage();
                TestUtils.assertContains(error, ATTACH_ERR_DIR_EXISTS.name());
                TestUtils.assertContains(error, tableToken.getDirName() + Files.SEPARATOR + leftover + " may hold the partition");
                TestUtils.assertContains(error, "rename it to ");
                TestUtils.assertContains(error, tableToken.getDirName() + Files.SEPARATOR + attachableName);
                TestUtils.assertContains(error, "then RESUME WAL");

                // The step the message names
                java.nio.file.Files.move(tableDir.resolve(leftover), tableDir.resolve(attachableName));
                execute("ALTER TABLE tab RESUME WAL");
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));

                execute("INSERT INTO tab VALUES ('2024-01-03T00:00:00Z', 4, 'D')");
                drainWalQueue();
                engine.releaseAllWriters();
                execute("INSERT INTO tab VALUES ('2024-01-04T00:00:00Z', 5, 'E')");
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
                assertQuery("SELECT x, sym FROM tab")
                        .noLeakCheck().expectSize().inferRandomAccess().returns("x\tsym\n1\tA\n2\tB\n3\tC\n4\tD\n5\tE\n");
            } finally {
                isEntryTypeUnknown.set(false);
            }
        });
    }

    /**
     * An adaptive ATTACH of a soft-linked source (a partition kept on other storage) makes the partition a soft link
     * to the same target and fails after that. Unlink fails until the end: the cleanup, the writer-open purge on
     * RESUME and the retry must leave the link, never remove through it, so the target keeps every file.
     */
    @Test
    public void testAttachRetryAfterFailedSoftLinkUnlink() throws Exception {
        // Windows does not detect a directory soft link (isSoftLink is false), so the ATTACH hard-links a copy of the
        // source instead of soft-linking it, its cleanup removes that copy, and the retry attaches
        Assume.assumeFalse(Os.isWindows());
        // Only ADAPTIVE links the source; the other modes rename it
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final AtomicBoolean isNullFlagReadFailing = new AtomicBoolean();
        final AtomicBoolean isUnlinkFailing = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // The ATTACH reads the symbol column of the linked partition for the column's null flag
                if (isNullFlagReadFailing.get() && isAttachDestination(name) && Utf8s.endsWithAscii(name, Files.SEPARATOR + "sym.d")) {
                    isNullFlagReadFailing.set(false);
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public int unlink(LPSZ softLink) {
                if (isUnlinkFailing.get() && isAttachDestination(softLink)) {
                    return -1;
                }
                return super.unlink(softLink);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO tab VALUES
                        ('2024-01-01T00:00:00Z', 1, 'A'),
                        ('2024-01-01T01:00:00Z', 2, 'B'),
                        ('2024-01-02T00:00:00Z', 3, 'C')
                    """);
            drainWalQueue();
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("tab");
            final java.nio.file.Path tableDir = java.nio.file.Path.of(configuration.getDbRoot().toString(), tableToken.getDirName());
            final java.nio.file.Path target = java.nio.file.Path.of(root, "cold", "2024-01-01" + DETACHED_DIR_MARKER);
            java.nio.file.Files.createDirectories(target.getParent());
            java.nio.file.Files.move(tableDir.resolve("2024-01-01" + DETACHED_DIR_MARKER), target);
            java.nio.file.Files.createSymbolicLink(tableDir.resolve("2024-01-01" + configuration.getAttachPartitionSuffix()), target);
            final List<String> targetFiles = listFileNames(target);

            isUnlinkFailing.set(true);
            isNullFlagReadFailing.set(true);
            execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(targetFiles, listFileNames(target));

            // RESUME reopens the writer, whose purge meets the link, then retries the ATTACH
            execute("ALTER TABLE tab RESUME WAL");
            drainWalQueue();
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(targetFiles, listFileNames(target));
            final String leftover = findAttachDestination(tableToken);
            Assert.assertNotNull(leftover);
            final String error = engine.getTableSequencerAPI().getTxnTracker(tableToken).getErrorMessage();
            TestUtils.assertContains(error, ATTACH_ERR_DIR_EXISTS.name());
            TestUtils.assertContains(error, "remove only the soft link ");
            TestUtils.assertContains(error, tableToken.getDirName() + Files.SEPARATOR + leftover + " (not its target)");
            TestUtils.assertContains(error, "then RESUME WAL");

            isUnlinkFailing.set(false);
            execute("ALTER TABLE tab RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            execute("INSERT INTO tab VALUES ('2024-01-03T00:00:00Z', 4, 'D')");
            drainWalQueue();
            engine.releaseAllWriters();
            execute("INSERT INTO tab VALUES ('2024-01-04T00:00:00Z', 5, 'E')");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertQuery("SELECT x, sym FROM tab")
                    .noLeakCheck().expectSize().inferRandomAccess().returns("x\tsym\n1\tA\n2\tB\n3\tC\n4\tD\n5\tE\n");
            Assert.assertEquals(targetFiles, listFileNames(target));
        });
    }

    @Test
    public void testAttachParquetNullRecoveryKeepsNonNullSiblingFalse() throws Exception {
        assertAttachParquetNullRecoveryKeepsNonNullSiblingFalse(true);
    }

    @Test
    public void testAttachParquetNullRecoveryKeepsNonNullSiblingFalseWithoutStatistics() throws Exception {
        assertAttachParquetNullRecoveryKeepsNonNullSiblingFalse(false);
    }

    @Test
    public void testAttachParquetNullRecoveryUsesStatisticsOnlyForNoNulls() throws Exception {
        assertAttachParquetWithoutNulls(true);
    }

    @Test
    public void testAttachParquetWithoutStatisticsWithoutNulls() throws Exception {
        assertAttachParquetWithoutNulls(false);
    }

    @Test
    public void testAttachParquetPartitionAfterColumnsRenamedAwayAndBack() throws Exception {
        assertAttachPartitionAfterColumnsRenamedAwayAndBack(false, true, null);
    }

    @Test
    public void testAttachParquetPartitionAfterColumnsRenamedAwayAndBackWal() throws Exception {
        assertAttachPartitionAfterColumnsRenamedAwayAndBack(true, true, null);
    }

    @Test
    public void testAttachParquetPartitionAfterTruncateRestoresSymbolNullFlag() throws Exception {
        assertAttachPartitionAfterTruncateRestoresSymbolNullFlag(true, true, true);
    }

    @Test
    public void testAttachParquetPartitionDetachedBeforeTruncateReadsAddedColumnsAsNull() throws Exception {
        assertAttachPartitionDetachedBeforeTruncateReadsAddedColumnsAsNull(false, true);
    }

    @Test
    public void testAttachParquetPartitionDetachedBeforeTruncateReadsAddedColumnsAsNullWal() throws Exception {
        assertAttachPartitionDetachedBeforeTruncateReadsAddedColumnsAsNull(true, true);
    }

    @Test
    public void testAttachParquetPartitionRestoresSymbolNullFlagsAcrossRowGroupsAndColumnIds() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 2);
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", dropped INT, x INT, s SYMBOL, t SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO tab VALUES
                        ('2024-01-01T00:00:00Z', 0, 1, NULL, NULL),
                        ('2024-01-01T01:00:00Z', 0, 2, NULL, NULL),
                        ('2024-01-01T02:00:00Z', 0, 3, 'A', 'B'),
                        ('2024-01-01T03:00:00Z', 0, 4, 'A', 'B'),
                        ('2024-01-02T00:00:00Z', 0, 5, 'A', 'B')
                    """);
            execute("ALTER TABLE tab DROP COLUMN dropped");
            execute("ALTER TABLE tab CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            execute("ALTER TABLE tab RENAME COLUMN s TO sym");
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            execute("TRUNCATE TABLE tab");
            execute("INSERT INTO tab VALUES ('2024-01-02T00:00:00Z', 6, 'A', 'B')");
            Assert.assertFalse(containsSymbolNullValue("tab", "sym"));
            Assert.assertFalse(containsSymbolNullValue("tab", "t"));
            renameDetachedToAttachable("tab", "2024-01-01");
            execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
            engine.clear();
            assertQuery("SELECT x, sym, t FROM tab")
                    .noLeakCheck().expectSize().inferRandomAccess().returns("x\tsym\tt\n1\t\t\n2\t\t\n3\tA\tB\n4\tA\tB\n6\tA\tB\n");
            assertQuery("SELECT x, sym FROM tab LATEST ON ts PARTITION BY sym")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\tsym\n2\t\n6\tA\n");
            assertQuery("SELECT x, t FROM tab LATEST ON ts PARTITION BY t")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\tt\n2\t\n6\tB\n");
            Assert.assertTrue(containsSymbolNullValue("tab", "sym"));
            Assert.assertTrue(containsSymbolNullValue("tab", "t"));
        });
    }

    @Test
    public void testAttachParquetPartitionWithNullsOnWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO tab VALUES
                        ('2024-01-01T00:00:00Z', 1, NULL),
                        ('2024-01-01T01:00:00Z', 2, NULL),
                        ('2024-01-02T00:00:00Z', 3, 'A')
                    """);
            drainWalQueue();
            execute("ALTER TABLE tab CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            execute("TRUNCATE TABLE tab");
            drainWalQueue();
            renameDetachedToAttachable("tab", "2024-01-01");
            execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            engine.clear();
            try (TableReader reader = getReader("tab")) {
                Assert.assertTrue(reader.getTxFile().isPartitionParquet(0));
            }
            assertQuery("SELECT x, s FROM tab LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n2\t\n");
            Assert.assertTrue(containsSymbolNullValue("tab", "s"));
        });
    }

    @Test
    public void testAttachParquetPartitionWithoutMetadataAfterTruncateRestoresSymbolNullFlag() throws Exception {
        assertAttachPartitionAfterTruncateRestoresSymbolNullFlag(true, false, true);
    }

    @Test
    public void testAttachParquetPartitionWithoutNullsPreservesSymbolNullFlag() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 2);
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO tab VALUES
                        ('2024-01-01T00:00:00Z', 1, 'A'),
                        ('2024-01-01T01:00:00Z', 2, 'A'),
                        ('2024-01-01T02:00:00Z', 3, 'B'),
                        ('2024-01-01T03:00:00Z', 4, 'B'),
                        ('2024-01-02T00:00:00Z', 5, 'C')
                    """);
            execute("ALTER TABLE tab CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            execute("TRUNCATE TABLE tab KEEP SYMBOL MAPS");
            Assert.assertFalse(containsSymbolNullValue("tab", "s"));
            renameDetachedToAttachable("tab", "2024-01-01");
            execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
            engine.clear();
            Assert.assertFalse(containsSymbolNullValue("tab", "s"));
            assertQuery("SELECT x, s FROM tab LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n2\tA\n4\tB\n");
        });
    }

    @Test
    public void testAttachParquetPartitionWithoutStatisticsAfterTruncateRestoresSymbolNullFlag() throws Exception {
        assertAttachPartitionAfterTruncateRestoresSymbolNullFlag(true, true, false);
    }

    @Test
    public void testAttachPartitionAfterColumnsRenamedAwayAndBack() throws Exception {
        assertAttachPartitionAfterColumnsRenamedAwayAndBack(false, false, null);
    }

    @Test
    public void testAttachPartitionAfterColumnsRenamedAwayAndBackAndNewestDropped() throws Exception {
        assertAttachPartitionAfterColumnsRenamedAwayAndBack(false, false, "ALTER TABLE tab DROP PARTITION LIST '2024-01-03'");
    }

    @Test
    public void testAttachPartitionAfterColumnsRenamedAwayAndBackAndNewestDroppedWal() throws Exception {
        assertAttachPartitionAfterColumnsRenamedAwayAndBack(true, false, "ALTER TABLE tab DROP PARTITION LIST '2024-01-03'");
    }

    @Test
    public void testAttachPartitionAfterColumnsRenamedAwayAndBackAndTruncated() throws Exception {
        // KEEP SYMBOL MAPS, as a WAL table truncates anyway: the attached rows read the symbols they were written with
        assertAttachPartitionAfterColumnsRenamedAwayAndBack(false, false, "TRUNCATE TABLE tab KEEP SYMBOL MAPS");
    }

    @Test
    public void testAttachPartitionAfterColumnsRenamedAwayAndBackAndTruncatedWal() throws Exception {
        assertAttachPartitionAfterColumnsRenamedAwayAndBack(true, false, "TRUNCATE TABLE tab KEEP SYMBOL MAPS");
    }

    @Test
    public void testAttachPartitionAfterColumnsRenamedAwayAndBackMatchesTable() throws Exception {
        // ref takes the same statements as tab, but keeps 2024-01-02 attached throughout
        assertMemoryLeak(() -> {
            createTableWithColumnsToRename("ref", false);
            renameColumnsAwayAndBackAndDropIndex("ref", false);
            insertIntoAttachedPartition("ref", false);

            createTableWithColumnsToRename("tab", false);
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-02'");
            renameColumnsAwayAndBackAndDropIndex("tab", false);
            renameDetachedToAttachable("tab", "2024-01-02");
            execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-02'");
            insertIntoAttachedPartition("tab", false);
            assertSqlCursors("ref", "tab");
        });
    }

    @Test
    public void testAttachPartitionAfterColumnsRenamedAwayAndBackWal() throws Exception {
        assertAttachPartitionAfterColumnsRenamedAwayAndBack(true, false, null);
    }

    @Test
    public void testAttachPartitionAfterSymbolsRenamedAwayAndBackAndTruncatedRestoresSymbolNullFlag() throws Exception {
        // A plain TRUNCATE of a non-WAL table empties the symbol maps and unsets their null flags, so the attach reads
        // the partition's symbols for NULLs. They hold only NULLs there, which the emptied maps read the same.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            final String[] symbols = {"s", "sb", "sp"};
            // added after the first row, the symbols take column name txns above 0
            execute("ALTER TABLE tab ADD COLUMN s SYMBOL");
            execute("ALTER TABLE tab ADD COLUMN sb SYMBOL INDEX TYPE BITMAP");
            execute("ALTER TABLE tab ADD COLUMN sp SYMBOL INDEX TYPE POSTING");
            execute("""
                    INSERT INTO tab VALUES
                        ('2024-01-02T00:00:00.000000Z', 2, NULL, NULL, NULL),
                        ('2024-01-02T01:00:00.000000Z', 3, NULL, NULL, NULL),
                        ('2024-01-03T00:00:00.000000Z', 4, 'A', 'A', 'A')
                    """);
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-02'");
            execute("TRUNCATE TABLE tab");
            for (String symbol : symbols) {
                execute("ALTER TABLE tab RENAME COLUMN " + symbol + " TO " + symbol + "_away");
                execute("ALTER TABLE tab RENAME COLUMN " + symbol + "_away TO " + symbol);
                Assert.assertFalse(containsSymbolNullValue("tab", symbol));
            }
            renameDetachedToAttachable("tab", "2024-01-02");
            execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-02'");
            for (String symbol : symbols) {
                Assert.assertTrue(symbol, containsSymbolNullValue("tab", symbol));
                assertQuery("SELECT x FROM tab WHERE " + symbol + " IS NULL").noLeakCheck().returns("x\n2\n3\n");
            }
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(replaceTimestampSuffix("""
                    ts\tx\ts\tsb\tsp
                    2024-01-02T00:00:00.000000Z\t2\t\t\t
                    2024-01-02T01:00:00.000000Z\t3\t\t\t
                    """));
        });
    }

    @Test
    public void testAttachPartitionAfterTruncate() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    src.col("sym", ColumnType.SYMBOL).timestamp("ts", timestampType.getTimestampType()),
                    3,
                    "2020-01-01",
                    1
            );

            execute("insert into " + tableName + " values ('foobar', '2020-01-02T23:59:59')");

            assertQuery("select first(ts), sym from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            """
                                    first\tsym
                                    2020-01-01T07:59:59.666666Z\tCPSW
                                    2020-01-01T15:59:59.333332Z\tHYRX
                                    2020-01-01T23:59:58.999998Z\t
                                    2020-01-02T23:59:59.000000Z\tfoobar
                                    """
                            : """
                            first\tsym
                            2020-01-01T07:59:59.666666666Z\tCPSW
                            2020-01-01T15:59:59.333333332Z\tHYRX
                            2020-01-01T23:59:58.999999998Z\t
                            2020-01-02T23:59:59.000000000Z\tfoobar
                            """);

            execute("alter table " + tableName + " detach partition list '2020-01-01'");

            execute("truncate table " + tableName);

            renameDetachedToAttachable(tableName, "2020-01-01");
            execute("alter table " + tableName + " attach partition list '2020-01-01'");

            // No symbols are present.
            assertQuery("select first(ts), sym from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            """
                                    first\tsym
                                    2020-01-01T07:59:59.666666Z\t
                                    2020-01-01T15:59:59.333332Z\t
                                    2020-01-01T23:59:58.999998Z\t
                                    """
                            : """
                            first\tsym
                            2020-01-01T07:59:59.666666666Z\t
                            2020-01-01T15:59:59.333333332Z\t
                            2020-01-01T23:59:58.999999998Z\t
                            """);
        });
    }

    @Test
    public void testAttachPartitionAfterTruncateKeepSymbolTables() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "testAttachPartitionAfterTruncateKeepSymbolTables";
            TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY);
            // It's important to have a symbol column here to make sure
            // that we don't wipe symbol tables on TRUNCATE.
            createPopulateTable(
                    src.col("sym", ColumnType.SYMBOL).timestamp("ts", timestampType.getTimestampType()),
                    3,
                    "2020-01-01",
                    1
            );

            execute("insert into " + tableName + " values ('foobar', '2020-01-02T23:59:59')");

            assertQuery("select first(ts), sym from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            """
                                    first\tsym
                                    2020-01-01T07:59:59.666666Z\tCPSW
                                    2020-01-01T15:59:59.333332Z\tHYRX
                                    2020-01-01T23:59:58.999998Z\t
                                    2020-01-02T23:59:59.000000Z\tfoobar
                                    """
                            : """
                            first\tsym
                            2020-01-01T07:59:59.666666666Z\tCPSW
                            2020-01-01T15:59:59.333333332Z\tHYRX
                            2020-01-01T23:59:58.999999998Z\t
                            2020-01-02T23:59:59.000000000Z\tfoobar
                            """);

            execute("alter table " + tableName + " detach partition list '2020-01-01'");

            execute("truncate table " + tableName + " keep symbol maps");

            renameDetachedToAttachable(tableName, "2020-01-01");
            execute("alter table " + tableName + " attach partition list '2020-01-01'");

            // All symbols are kept.
            assertQuery("select first(ts), sym from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? """
                            first\tsym
                            2020-01-01T07:59:59.666666Z\tCPSW
                            2020-01-01T15:59:59.333332Z\tHYRX
                            2020-01-01T23:59:58.999998Z\t
                            """ :
                            """
                                    first\tsym
                                    2020-01-01T07:59:59.666666666Z\tCPSW
                                    2020-01-01T15:59:59.333333332Z\tHYRX
                                    2020-01-01T23:59:58.999999998Z\t
                                    """);
        });
    }

    @Test
    public void testAttachPartitionDetachedBeforeNewestDroppedReadsAddedColumnsAsNull() throws Exception {
        assertAttachPartitionReadsAddedColumnsAsNull(false, false, "ALTER TABLE tab DROP PARTITION LIST '2024-01-04'");
    }

    @Test
    public void testAttachPartitionDetachedBeforeNewestDroppedReadsAddedColumnsAsNullWal() throws Exception {
        assertAttachPartitionReadsAddedColumnsAsNull(true, false, "ALTER TABLE tab DROP PARTITION LIST '2024-01-04'");
    }

    @Test
    public void testAttachPartitionDetachedBeforeTruncateKeepsColumnTop() throws Exception {
        // The partition holds the added column from its third row on: an O3 write gave it an explicit column
        // top in _cv, which the detached _cv carries and which the attach has to keep after the TRUNCATE.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (price DOUBLE, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO tab VALUES
                        (1.0, '2024-01-01T01:00:00.000000Z'),
                        (2.0, '2024-01-01T02:00:00.000000Z'),
                        (4.0, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE tab ADD COLUMN qty LONG");
            execute("INSERT INTO tab VALUES (3.0, '2024-01-01T03:00:00.000000Z', 30)");
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            execute("TRUNCATE TABLE tab");
            renameDetachedToAttachable("tab", "2024-01-01");
            execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
            final String expected = replaceTimestampSuffix("""
                    price\tts\tqty
                    1.0\t2024-01-01T01:00:00.000000Z\tnull
                    2.0\t2024-01-01T02:00:00.000000Z\tnull
                    3.0\t2024-01-01T03:00:00.000000Z\t30
                    """);
            assertQuery("SELECT price, ts, qty FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(expected);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT price, ts, qty FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(expected);
        });
    }

    @Test
    public void testAttachPartitionDetachedBeforeTruncateKeepsDroppedColumnDropped() throws Exception {
        // A column dropped before the DETACH is gone from both the detached metadata and the table. The attach still
        // takes the presence of the column added after the partition was written from the detached _cv.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (price DOUBLE, gone INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO tab VALUES
                        (1.0, 10, '2024-01-01T01:00:00.000000Z'),
                        (2.0, 20, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE tab ADD COLUMN qty LONG");
            execute("INSERT INTO tab VALUES (3.0, 30, '2024-01-02T02:00:00.000000Z', 300)");
            execute("ALTER TABLE tab DROP COLUMN gone");
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            execute("TRUNCATE TABLE tab");
            renameDetachedToAttachable("tab", "2024-01-01");
            execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
            execute("INSERT INTO tab VALUES (4.0, '2024-01-03T01:00:00.000000Z', 400)");
            final String expected = replaceTimestampSuffix("""
                    price\tts\tqty
                    1.0\t2024-01-01T01:00:00.000000Z\tnull
                    4.0\t2024-01-03T01:00:00.000000Z\t400
                    """);
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(expected);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(expected);
        });
    }

    @Test
    public void testAttachPartitionDetachedBeforeTruncateReadsAddedColumnsAsNull() throws Exception {
        assertAttachPartitionDetachedBeforeTruncateReadsAddedColumnsAsNull(false, false);
    }

    @Test
    public void testAttachPartitionDetachedBeforeTruncateReadsAddedColumnsAsNullWal() throws Exception {
        assertAttachPartitionDetachedBeforeTruncateReadsAddedColumnsAsNull(true, false);
    }

    @Test
    public void testAttachPartitionReadsCurrentVersionOfUpdatedColumn() throws Exception {
        assertAttachPartitionReadsCurrentVersionOfUpdatedColumn(false, ATTACH_WITH_COLUMN_VERSION_FILE);
    }

    @Test
    public void testAttachPartitionReadsCurrentVersionOfUpdatedColumnWal() throws Exception {
        assertAttachPartitionReadsCurrentVersionOfUpdatedColumn(true, ATTACH_WITH_COLUMN_VERSION_FILE);
    }

    @Test
    public void testAttachPartitionWithoutColumnVersionFileReadsNewestVersionOfUpdatedColumn() throws Exception {
        assertAttachPartitionReadsCurrentVersionOfUpdatedColumn(false, ATTACH_WITHOUT_COLUMN_VERSION_FILE);
    }

    @Test
    public void testAttachPartitionWithKnownSizeReadsNewestVersionOfUpdatedColumn() throws Exception {
        assertAttachPartitionReadsCurrentVersionOfUpdatedColumn(false, ATTACH_WITH_KNOWN_SIZE);
    }

    @Test
    public void testAttachPartitionDetachedWithoutTruncateReadsAddedColumnsAsNull() throws Exception {
        assertAttachPartitionReadsAddedColumnsAsNull(false, false, null);
    }

    @Test
    public void testAttachPartitionDetachedWithoutTruncateReadsAddedColumnsAsNullWal() throws Exception {
        assertAttachPartitionReadsAddedColumnsAsNull(true, false, null);
    }

    @Test
    public void testAttachPartitionDetachedAfterColumnDroppedAndAddedBack() throws Exception {
        assertAttachPartitionDetachedAfterColumnReplaced(false, false, false);
    }

    @Test
    public void testAttachPartitionDetachedAfterColumnDroppedAndAddedBackParquet() throws Exception {
        assertAttachPartitionDetachedAfterColumnReplaced(false, true, false);
    }

    @Test
    public void testAttachPartitionDetachedAfterColumnDroppedAndAddedBackParquetWal() throws Exception {
        assertAttachPartitionDetachedAfterColumnReplaced(true, true, false);
    }

    @Test
    public void testAttachPartitionDetachedAfterColumnDroppedAndAddedBackWal() throws Exception {
        assertAttachPartitionDetachedAfterColumnReplaced(true, false, false);
    }

    @Test
    public void testAttachPartitionDetachedAfterTypeChange() throws Exception {
        assertAttachPartitionDetachedAfterColumnReplaced(false, false, true);
    }

    @Test
    public void testAttachPartitionDetachedAfterTypeChangeParquet() throws Exception {
        assertAttachPartitionDetachedAfterColumnReplaced(false, true, true);
    }

    @Test
    public void testAttachPartitionDetachedAfterTypeChangeParquetWal() throws Exception {
        assertAttachPartitionDetachedAfterColumnReplaced(true, true, true);
    }

    @Test
    public void testAttachPartitionDetachedAfterTypeChangeWal() throws Exception {
        assertAttachPartitionDetachedAfterColumnReplaced(true, false, true);
    }

    @Test
    public void testAttachPartitionDetachedBeforeColumnChanges() throws Exception {
        // While the partition is detached, y is dropped and z added: the partition takes neither, and reads z as NULL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, y INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00.000000Z', 1, 10), ('2024-01-02T00:00:00.000000Z', 3, 30)");
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            execute("ALTER TABLE tab DROP COLUMN y");
            execute("ALTER TABLE tab ADD COLUMN z INT");
            execute("INSERT INTO tab VALUES ('2024-01-02T01:00:00.000000Z', 4, 40)");
            renameDetachedToAttachable("tab", "2024-01-01");
            execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
            final String expected = replaceTimestampSuffix("""
                    ts\tx\tz
                    2024-01-01T00:00:00.000000Z\t1\tnull
                    2024-01-02T00:00:00.000000Z\t3\tnull
                    2024-01-02T01:00:00.000000Z\t4\t40
                    """);
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(expected);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(expected);
        });
    }

    @Test
    public void testAttachPartitionDetachedBeforeTypeChangeFails() throws Exception {
        // The partition holds x as INT, while the table now reads it as LONG: the attach still refuses it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, y INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00.000000Z', 1, 10), ('2024-01-02T00:00:00.000000Z', 3, 30)");
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            execute("ALTER TABLE tab ALTER COLUMN x TYPE LONG");
            renameDetachedToAttachable("tab", "2024-01-01");
            assertFailure(
                    "ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'",
                    "Detached column [index=3, name=x, attribute=name] does not match current table metadata"
            );
            assertQuery("SELECT ts, x, y FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(replaceTimestampSuffix("""
                    ts\tx\ty
                    2024-01-02T00:00:00.000000Z\t3\t30
                    """));
            // the table takes writes after the refused attach
            execute("INSERT INTO tab VALUES ('2024-01-03T00:00:00.000000Z', 5, 50)");
            assertQuery("SELECT ts, x, y FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(replaceTimestampSuffix("""
                    ts\tx\ty
                    2024-01-02T00:00:00.000000Z\t3\t30
                    2024-01-03T00:00:00.000000Z\t5\t50
                    """));
        });
    }

    @Test
    public void testAttachPartitionReadsSymbolNullFlagFromIndex() throws Exception {
        final SymbolDataMapFailingFilesFacade ff = new SymbolDataMapFailingFilesFacade();
        assertMemoryLeak(ff, () -> {
            for (byte indexType : new byte[]{IndexType.BITMAP, IndexType.POSTING}) {
                for (boolean hasNulls : new boolean[]{true, false}) {
                    execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, sym SYMBOL INDEX TYPE " + IndexType.nameOf(indexType)
                            + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                    execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00Z', 1, 'A'), ('2024-01-01T01:00:00Z', 2, " + (hasNulls ? "NULL" : "'B'")
                            + "), ('2024-01-01T02:00:00Z', 3, 'A'), ('2024-01-02T00:00:00Z', 4, 'C')");
                    execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
                    unsetSymbolNullFlag("tab", "sym");
                    renameDetachedToAttachable("tab", "2024-01-01");
                    ff.isFailing = true;
                    try {
                        execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
                    } finally {
                        ff.isFailing = false;
                    }
                    Assert.assertEquals(hasNulls, containsSymbolNullValue("tab", "sym"));
                    assertQuery("SELECT x, sym FROM tab LATEST ON ts PARTITION BY sym")
                            .noLeakCheck().inferRandomAccess().sizeMayVary()
                            .returns(hasNulls ? "x\tsym\n2\t\n3\tA\n4\tC\n" : "x\tsym\n2\tB\n3\tA\n4\tC\n");
                    execute("DROP TABLE tab");
                }
            }
        });
    }

    @Test
    public void testAttachPartitionScansSymbolDataWhenIndexIsIncomplete() throws Exception {
        assertMemoryLeak(() -> {
            for (byte indexType : new byte[]{IndexType.BITMAP, IndexType.POSTING}) {
                execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, sym SYMBOL INDEX TYPE " + IndexType.nameOf(indexType)
                        + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("""
                        INSERT INTO tab VALUES
                            ('2024-01-01T00:00:00Z', 1, 'A'),
                            ('2024-01-01T01:00:00Z', 2, 'B'),
                            ('2024-01-01T02:00:00Z', 3, NULL),
                            ('2024-01-02T00:00:00Z', 4, 'C')
                        """);
                execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
                unsetSymbolNullFlag("tab", "sym");
                try (
                        Path path = new Path();
                        IndexWriter indexWriter = IndexFactory.createWriter(indexType, configuration)
                ) {
                    path.of(configuration.getDbRoot()).concat(engine.verifyTableName("tab")).concat("2024-01-01").put(DETACHED_DIR_MARKER);
                    indexWriter.of(path, "sym", COLUMN_NAME_TXN_NONE);
                    indexWriter.rollbackValues(1);
                }
                renameDetachedToAttachable("tab", "2024-01-01");
                execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
                Assert.assertTrue(containsSymbolNullValue("tab", "sym"));
                assertQuery("SELECT x, sym FROM tab")
                        .noLeakCheck().expectSize().inferRandomAccess().returns("x\tsym\n1\tA\n2\tB\n3\t\n4\tC\n");
                execute("DROP TABLE tab");
            }
        });
    }

    @Test
    public void testAttachPartitionScansSymbolDataWhenIndexIsUnreadable() throws Exception {
        final SymbolIndexOpenFailingFilesFacade ff = new SymbolIndexOpenFailingFilesFacade();
        assertMemoryLeak(ff, () -> {
            for (byte indexType : new byte[]{IndexType.BITMAP, IndexType.POSTING}) {
                execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, sym SYMBOL INDEX TYPE " + IndexType.nameOf(indexType)
                        + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("""
                        INSERT INTO tab VALUES
                            ('2024-01-01T00:00:00Z', 1, 'A'),
                            ('2024-01-01T01:00:00Z', 2, NULL),
                            ('2024-01-01T02:00:00Z', 3, 'A'),
                            ('2024-01-02T00:00:00Z', 4, 'C')
                        """);
                execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
                unsetSymbolNullFlag("tab", "sym");
                renameDetachedToAttachable("tab", "2024-01-01");
                ff.isFailing = true;
                try {
                    execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
                } finally {
                    ff.isFailing = false;
                }
                Assert.assertTrue(containsSymbolNullValue("tab", "sym"));
                assertQuery("SELECT x, sym FROM tab LATEST ON ts PARTITION BY sym")
                        .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\tsym\n2\t\n3\tA\n4\tC\n");
                execute("DROP TABLE tab");
            }
        });
    }

    @Test
    public void testAttachPartitionSetsSymbolNullFlagForColumnTopRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00Z', 1)");
            execute("ALTER TABLE tab ADD COLUMN s SYMBOL");
            execute("INSERT INTO tab VALUES ('2024-01-02T00:00:00Z', 2, 'A')");
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            unsetSymbolNullFlag("tab", "s");
            renameDetachedToAttachable("tab", "2024-01-01");
            execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
            Assert.assertTrue(containsSymbolNullValue("tab", "s"));
            assertQuery("SELECT x, s FROM tab LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n1\t\n2\tA\n");
        });
    }

    @Test
    public void testAttachPartitionSetsSymbolNullFlagForNullKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00Z', 1, NULL), ('2024-01-02T00:00:00Z', 2, 'A')");
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            unsetSymbolNullFlag("tab", "s");
            renameDetachedToAttachable("tab", "2024-01-01");
            execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
            Assert.assertTrue(containsSymbolNullValue("tab", "s"));
            assertQuery("SELECT x, s FROM tab LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n1\t\n2\tA\n");
        });
    }

    @Test
    public void testAttachPartitionWithKnownSizeSkipsSymbolDataScan() throws Exception {
        final SymbolDataOpenCountingFilesFacade ff = new SymbolDataOpenCountingFilesFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00Z', 1, 'A'), ('2024-01-01T01:00:00Z', 2, 'B'), ('2024-01-02T00:00:00Z', 3, 'C')");
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            renameDetachedToAttachable("tab", "2024-01-01");
            try (TableWriter writer = getWriter("tab")) {
                ff.isCounting = true;
                try {
                    Assert.assertEquals(AttachDetachStatus.OK, writer.attachPartition(timestampType.getDriver().parseFloorLiteral("2024-01-01"), 2));
                } finally {
                    ff.isCounting = false;
                }
            }
            Assert.assertEquals(0, ff.dataOpenCount);
            Assert.assertFalse(containsSymbolNullValue("tab", "sym"));
            assertQuery("SELECT x, sym FROM tab")
                    .noLeakCheck().expectSize().inferRandomAccess().returns("x\tsym\n1\tA\n2\tB\n3\tC\n");
        });
    }

    @Test
    public void testAttachPartitionWithoutMetadataReadsSymbolDataOnce() throws Exception {
        final SymbolDataOpenCountingFilesFacade ff = new SymbolDataOpenCountingFilesFacade();
        assertMemoryLeak(ff, () -> {
            final String[][] attachedSymbols = {{"'A'", "NULL"}, {"'A'", "'B'"}, {"NULL", "NULL"}};
            final String[] expected = {"x\tsym\n1\tA\n2\t\n3\tC\n", "x\tsym\n1\tA\n2\tB\n3\tC\n", "x\tsym\n2\t\n3\tC\n"};
            for (int i = 0; i < attachedSymbols.length; i++) {
                final boolean hasNulls = attachedSymbols[i][1].equals("NULL");
                execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00Z', 1, " + attachedSymbols[i][0] + "), ('2024-01-01T01:00:00Z', 2, "
                        + attachedSymbols[i][1] + "), ('2024-01-02T00:00:00Z', 3, 'C')");
                execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
                unsetSymbolNullFlag("tab", "sym");
                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(engine.verifyTableName("tab")).concat("2024-01-01").put(DETACHED_DIR_MARKER).concat(META_FILE_NAME);
                    Assert.assertTrue(TestUtils.remove(path.$()));
                }
                renameDetachedToAttachable("tab", "2024-01-01");
                ff.dataOpenCount = 0;
                ff.isCounting = true;
                try {
                    execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
                } finally {
                    ff.isCounting = false;
                }
                Assert.assertEquals(1, ff.dataOpenCount);
                Assert.assertEquals(hasNulls, containsSymbolNullValue("tab", "sym"));
                assertQuery("SELECT x, sym FROM tab LATEST ON ts PARTITION BY sym")
                        .noLeakCheck().inferRandomAccess().sizeMayVary()
                        .returns(expected[i]);
                execute("DROP TABLE tab");
            }
        });
    }

    @Test
    public void testAttachPartitionCommits() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    5,
                    "2022-06-01",
                    4
            );

            String timestampDay = "2022-06-02";
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);

            renameDetachedToAttachable(tableName, timestampDay);

            TimestampDriver driver = timestampType.getDriver();
            try (TableWriter writer = getWriter(tableName)) {
                // structural change
                writer.addColumn("new_column", ColumnType.INT, AllowAllSecurityContext.INSTANCE);

                TableWriter.Row row = writer.newRow(driver.parseFloorLiteral("2022-06-03T12:00:00.000000Z"));
                row.putLong(0, 33L);
                row.putInt(1, 33);
                row.append();

                Assert.assertEquals(AttachDetachStatus.OK, writer.attachPartition(driver.parseFloorLiteral(timestampDay)));
            }

            assertContent(
                    """
                            l\ti\tts\tnew_column
                            1\t1\t2022-06-01T19:11:59.800000Z\tnull
                            2\t2\t2022-06-02T14:23:59.600000Z\tnull
                            3\t3\t2022-06-03T09:35:59.400000Z\tnull
                            33\t33\t2022-06-03T12:00:00.000000Z\tnull
                            4\t4\t2022-06-04T04:47:59.200000Z\tnull
                            5\t5\t2022-06-04T23:59:59.000000Z\tnull
                            """,
                    tableName
            );
        });
    }

    @Test
    public void testAttachPartitionCommitsToSamePartition() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .col("vch", ColumnType.VARCHAR)
                            .timestamp("ts", timestampType.getTimestampType()),
                    5,
                    "2022-06-01",
                    4
            );

            String timestampDay = "2022-06-02";
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);

            renameDetachedToAttachable(tableName, timestampDay);

            TimestampDriver driver = timestampType.getDriver();
            long timestamp = driver.parseFloorLiteral(timestampDay + "T22:00:00.000000Z");
            try (TableWriter writer = getWriter(tableName)) {
                // structural change
                writer.addColumn("new_column", ColumnType.INT, AllowAllSecurityContext.INSTANCE);

                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong(0, 33L);
                row.putInt(1, 33);
                row.append();

                Assert.assertEquals(AttachDetachStatus.ATTACH_ERR_PARTITION_EXISTS, writer.attachPartition(driver.parseFloorLiteral(timestampDay)));
            }

            assertContent(
                    """
                            l\ti\tvch\tts\tnew_column
                            1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T19:11:59.800000Z\tnull
                            33\t33\t\t2022-06-02T22:00:00.000000Z\tnull
                            3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-03T09:35:59.400000Z\tnull
                            4\t4\t\t2022-06-04T04:47:59.200000Z\tnull
                            5\t5\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-04T23:59:59.000000Z\tnull
                            """,
                    tableName
            );
        });
    }

    @Test
    public void testAttachPartitionWithColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY);

            createPopulateTable(
                    src.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    100,
                    "2020-01-01",
                    2
            );

            execute("alter table " + tableName + " add column str string");
            execute("alter table " + tableName + " add column vch varchar");

            execute("insert into " + tableName +
                    " select x, rnd_int(), timestamp_sequence('2020-01-02T23:59:59', 1000000L * 60 * 20), rnd_str('a', 'b', 'c', null), rnd_varchar('a', 'b', 'c', null)" +
                    " from long_sequence(100)");

            execute("alter table " + tableName + " detach partition list '2020-01-02', '2020-01-03'");

            assertQuery("select first(ts), str from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(replaceTimestampSuffix("""
                            first\tstr
                            2020-01-01T00:28:47.990000Z\t
                            2020-01-04T00:19:59.000000Z\tb
                            2020-01-04T00:39:59.000000Z\ta
                            2020-01-04T00:59:59.000000Z\tb
                            2020-01-04T01:39:59.000000Z\t
                            2020-01-04T05:19:59.000000Z\tc
                            """));

            renameDetachedToAttachable(tableName, "2020-01-02", "2020-01-03");
            execute("alter table " + tableName + " attach partition list '2020-01-02', '2020-01-03'");

            assertQuery("select first(ts), str from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(replaceTimestampSuffix("""
                            first\tstr
                            2020-01-01T00:28:47.990000Z\t
                            2020-01-02T00:57:35.480000Z\t
                            2020-01-02T23:59:59.000000Z\tc
                            2020-01-03T00:19:59.000000Z\tb
                            2020-01-03T00:39:59.000000Z\t
                            2020-01-03T00:59:59.000000Z\ta
                            2020-01-03T01:59:59.000000Z\tc
                            2020-01-03T05:39:59.000000Z\tb
                            2020-01-04T00:39:59.000000Z\ta
                            2020-01-04T00:59:59.000000Z\tb
                            2020-01-04T01:39:59.000000Z\t
                            2020-01-04T05:19:59.000000Z\tc
                            """));
        });
    }

    @Test
    public void testAttachWillFailIfThePartitionWasRecreated() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabTimeTravel2";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            String timestampDay = "2022-06-01";
            createPopulateTable(
                    tab
                            .col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .col("vch", ColumnType.VARCHAR)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    timestampDay,
                    4
            );
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            """
                                    l\ti\tvch\tts
                                    1\t1\t1\t2022-06-01T07:59:59.916666Z
                                    2\t2\t2\t2022-06-01T15:59:59.833332Z
                                    3\t3\t3\t2022-06-01T23:59:59.749998Z
                                    4\t4\t4\t2022-06-02T07:59:59.666664Z
                                    5\t5\t5\t2022-06-02T15:59:59.583330Z
                                    6\t6\t6\t2022-06-02T23:59:59.499996Z
                                    7\t7\t7\t2022-06-03T07:59:59.416662Z
                                    8\t8\t8\t2022-06-03T15:59:59.333328Z
                                    9\t9\t9\t2022-06-03T23:59:59.249994Z
                                    10\t10\t10\t2022-06-04T07:59:59.166660Z
                                    11\t11\t11\t2022-06-04T15:59:59.083326Z
                                    12\t12\t12\t2022-06-04T23:59:58.999992Z
                                    """
                            : """
                            l\ti\tvch\tts
                            1\t1\t1\t2022-06-01T07:59:59.916666666Z
                            2\t2\t2\t2022-06-01T15:59:59.833333332Z
                            3\t3\t3\t2022-06-01T23:59:59.749999998Z
                            4\t4\t4\t2022-06-02T07:59:59.666666664Z
                            5\t5\t5\t2022-06-02T15:59:59.583333330Z
                            6\t6\t6\t2022-06-02T23:59:59.499999996Z
                            7\t7\t7\t2022-06-03T07:59:59.416666662Z
                            8\t8\t8\t2022-06-03T15:59:59.333333328Z
                            9\t9\t9\t2022-06-03T23:59:59.249999994Z
                            10\t10\t10\t2022-06-04T07:59:59.166666660Z
                            11\t11\t11\t2022-06-04T15:59:59.083333326Z
                            12\t12\t12\t2022-06-04T23:59:58.999999992Z
                            """,
                    tableName
            );

            // drop the partition
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);

            // insert data, which will create the partition again
            engine.clear();
            TimestampDriver driver = timestampType.getDriver();
            long timestamp = driver.parseFloorLiteral(timestampDay + "T09:59:59.999999Z");
            try (TableWriter writer = getWriter(tableName)) {
                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong(0, 137L);
                row.putInt(1, 137);
                row.append();
                writer.commit();
            }
            String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? """
                    l\ti\tvch\tts
                    137\t137\t\t2022-06-01T09:59:59.999999Z
                    4\t4\t4\t2022-06-02T07:59:59.666664Z
                    5\t5\t5\t2022-06-02T15:59:59.583330Z
                    6\t6\t6\t2022-06-02T23:59:59.499996Z
                    7\t7\t7\t2022-06-03T07:59:59.416662Z
                    8\t8\t8\t2022-06-03T15:59:59.333328Z
                    9\t9\t9\t2022-06-03T23:59:59.249994Z
                    10\t10\t10\t2022-06-04T07:59:59.166660Z
                    11\t11\t11\t2022-06-04T15:59:59.083326Z
                    12\t12\t12\t2022-06-04T23:59:58.999992Z
                    """
                    : """
                    l\ti\tvch\tts
                    137\t137\t\t2022-06-01T09:59:59.999999000Z
                    4\t4\t4\t2022-06-02T07:59:59.666666664Z
                    5\t5\t5\t2022-06-02T15:59:59.583333330Z
                    6\t6\t6\t2022-06-02T23:59:59.499999996Z
                    7\t7\t7\t2022-06-03T07:59:59.416666662Z
                    8\t8\t8\t2022-06-03T15:59:59.333333328Z
                    9\t9\t9\t2022-06-03T23:59:59.249999994Z
                    10\t10\t10\t2022-06-04T07:59:59.166666660Z
                    11\t11\t11\t2022-06-04T15:59:59.083333326Z
                    12\t12\t12\t2022-06-04T23:59:58.999999992Z
                    """;
            assertQuery(tableName)
                    .ddl(null)
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected);
            renameDetachedToAttachable(tableName, timestampDay);
            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'",
                    "could not attach partition [table=tabTimeTravel2, detachStatus=ATTACH_ERR_PARTITION_EXISTS"
            );
            assertQuery(tableName)
                    .ddl(null)
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected);
        });
    }

    /**
     * Attaching a parquet partition whose {@code _pm} metadata file is absent regenerates it durably, and the
     * rows come back. A converted partition already carries {@code _pm}, so the test removes it first. Both
     * fsyncs on this path ({@code _pm} after {@code ParquetMetadataWriter.generate}, then the partition
     * directory) stay eager under adaptive too, because no durable epoch re-derives an attach.
     */
    @Test
    public void testCanAttachParquetPartitionMissingMetadataFile() throws Exception {
        // The two durability gates this test reaches are guarded by `effectiveCommitMode != CommitMode.NOSYNC`,
        // so under nosync the row counts below pass while neither fsync runs. The test skips nosync rather
        // than pinning one mode, because sync, async and adaptive all exercise the gates.
        Overrides.assumeDurableCommitMode();
        assertMemoryLeak(() -> {
            String tableName = "tabAttachParquetOk";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG),
                    10,
                    "2022-06-01",
                    2
            );
            final long before = countRows(tableName);
            Assert.assertTrue("precondition: the table must hold rows to lose", before > 0);

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2022-06-01'", sqlExecutionContext);
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
            final long afterDetach = countRows(tableName);
            Assert.assertTrue("precondition: detaching must actually remove rows, otherwise the"
                    + " re-attach below proves nothing", afterDetach < before);

            renameDetachedToAttachable(tableName, "2022-06-01");

            // Remove _pm, keeping data.parquet: the inverse of the sibling test's removal, and the only
            // shape that reaches the regenerate-and-fsync block.
            TableToken tableToken = engine.verifyTableName(tableName);
            path.of(configuration.getDbRoot()).concat(tableToken)
                    .concat("2022-06-01").put(configuration.getAttachPartitionSuffix())
                    .concat(PARQUET_METADATA_FILE_NAME).$();
            Assert.assertTrue("precondition: conversion must have produced _pm for us to evict",
                    Files.exists(path.$()));
            Assert.assertTrue(Files.remove(path.$()));

            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);

            // The attach must regenerate _pm, not merely tolerate its absence. The attached directory
            // carries a name-txn suffix, so resolve the path the way the engine does instead of assuming
            // the bare partition name.
            try (TableReader reader = engine.getReader(tableToken)) {
                int parquetPartitions = 0;
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (!reader.getTxFile().isPartitionParquet(i)) {
                        continue;
                    }
                    parquetPartitions++;
                    path.of(configuration.getDbRoot()).concat(tableToken);
                    TableUtils.setPathForParquetPartitionMetadata(
                            path,
                            timestampType.getTimestampType(),
                            PartitionBy.DAY,
                            reader.getTxFile().getPartitionTimestampByIndex(i),
                            reader.getTxFile().getPartitionNameTxn(i)
                    );
                    Assert.assertTrue("attach must regenerate the missing _pm [path=" + path + "]",
                            Files.exists(path.$()));
                }
                Assert.assertEquals("the re-attached partition must still be parquet",
                        1, parquetPartitions);
            }

            Assert.assertEquals("every detached row must come back after re-attaching the parquet"
                    + " partition", before, countRows(tableName));
        });
    }

    private long countRows(String tableName) {
        try (io.questdb.cairo.sql.RecordCursorFactory f = select("select count() from " + tableName)) {
            try (io.questdb.cairo.sql.RecordCursor c = f.getCursor(sqlExecutionContext)) {
                return c.hasNext() ? c.getRecord().getLong(0) : 0L;
            }
        } catch (io.questdb.griffin.SqlException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCannotAttachParquetPartitionMissingDataFile() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabAttachNoParquetData";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG),
                    10,
                    "2022-06-01",
                    2
            );
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2022-06-01'", sqlExecutionContext);
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
            renameDetachedToAttachable(tableName, "2022-06-01");

            // Evict the parquet data file, leaving only _pm, to mirror a remotely-served partition.
            TableToken tableToken = engine.verifyTableName(tableName);
            path.of(configuration.getDbRoot()).concat(tableToken)
                    .concat("2022-06-01").put(configuration.getAttachPartitionSuffix())
                    .concat(PARQUET_PARTITION_NAME).$();
            Assert.assertTrue(Files.exists(path.$()));
            Assert.assertTrue(Files.remove(path.$()));

            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'",
                    "could not attach partition [table=" + tableName + ", detachStatus=ATTACH_ERR_MISSING_PARQUET_DATA"
            );
        });
    }

    @Test
    public void testCannotAttachPartitionWithDelta() throws Exception {
        // DETACH refuses a delta-active partition, so a _delta folder marks a directory ATTACH refuses.
        assertMemoryLeak(() -> {
            String tableName = "tabAttachDelta";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG),
                    10,
                    "2022-06-01",
                    2
            );
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
            renameDetachedToAttachable(tableName, "2022-06-01");

            final FilesFacade ff = configuration.getFilesFacade();
            TableToken tableToken = engine.verifyTableName(tableName);
            path.of(configuration.getDbRoot()).concat(tableToken)
                    .concat("2022-06-01").put(configuration.getAttachPartitionSuffix())
                    .concat(DELTA_DIR_NAME).slash$();
            Assert.assertEquals(0, ff.mkdirs(path, configuration.getMkDirMode()));
            final int deltaDirLen = path.size();
            Assert.assertTrue(ff.touch(path.concat(DELTA_CATALOG_FILE_NAME).$()));

            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'",
                    "could not attach partition [table=" + tableName + ", detachStatus=ATTACH_ERR_DELTA"
            );
            assertQuery("SELECT count() FROM " + tableName)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n5\n");
            Assert.assertTrue(ff.exists(path.$()));

            // Without _delta the directory attaches as an ordinary partition.
            Assert.assertTrue(ff.rmdir(path.trimTo(deltaDirLen)));
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
            assertQuery("SELECT count() FROM " + tableName)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n10\n");
        });
    }

    @Test
    public void testCannotCopyColumnVersions() throws Exception {
        assertCannotCopyMeta("testCannotCopyColumnVersions", 2);
    }

    @Test
    public void testCannotCopyMeta() throws Exception {
        assertCannotCopyMeta("testCannotCopyMeta", 1);
    }

    @Test
    public void testCannotCopyTxn() throws Exception {
        assertCannotCopyMeta("tab", 3);
    }

    @Test
    public void testCannotDetachActivePartition() throws Exception {
        assertFailure(
                "tab17",
                "ALTER TABLE tab17 DETACH PARTITION LIST '2022-06-05'",
                "could not detach partition [table=tab17, detachStatus=DETACH_ERR_ACTIVE"
        );
    }

    @Test
    public void testCannotDetachDeltaActivePartition() throws Exception {
        // The Delta catalog changes in place, so DETACH refuses the partition, at SQL time and in the writer.
        assertMemoryLeak(() -> {
            String tableName = "tabDetachDelta";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG),
                    10,
                    "2022-06-01",
                    2
            );
            final long timestamp = timestampType.getDriver().parseFloorLiteral("2022-06-01");
            try (TableWriter writer = getWriter(tableName)) {
                writer.getTxWriter().setPartitionDeltaActiveByTimestamp(timestamp);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }

            assertFailure(
                    "ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'",
                    "cannot detach partition, partition is delta-active [table=" + tableName + ", partition=2022-06-01]"
            );
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertEquals(DETACH_ERR_DELTA, writer.detachPartition(timestamp));
                Assert.assertTrue(writer.getTxWriter().isPartitionDeltaActive(0));
            }
            assertQuery("SELECT count() FROM " + tableName)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n10\n");
            path.of(configuration.getDbRoot()).concat(engine.verifyTableName(tableName))
                    .concat("2022-06-01").put(DETACHED_DIR_MARKER).$();
            Assert.assertFalse(configuration.getFilesFacade().exists(path.$()));
        });
    }

    @Test
    public void testCannotDetachRemotelyServedPartition() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabDetachRemote";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG),
                    10,
                    "2022-06-01",
                    2
            );
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2022-06-01'", sqlExecutionContext);

            try (TableWriter writer = getWriter(tableName)) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertTrue(tx.isPartitionParquet(0));
                tx.setPartitionParquetGenerated(0, false);
                tx.setPartitionRemote(0, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
                Assert.assertTrue(tx.isPartitionRemotelyServed(0));
            }

            assertFailure(
                    "ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'",
                    "could not detach partition [table=" + tableName + ", detachStatus=DETACH_ERR_REMOTE"
            );

            // The rejection left the partition attached and still remotely served.
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.getTxWriter().isPartitionRemotelyServed(0));
            }
        });
    }

    @Test
    public void testCannotRemoveFolder() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean rmdir(Path path, boolean lazy) {
                // The partition directory, not the temporary name the detached copy leaves.
                if (Utf8s.containsAscii(path, "2022-06-03") && !Utf8s.containsAscii(path, TableUtils.DETACHING_DIR_MARKER)) {
                    if (counter.getAndIncrement() == 0) {
                        return false;
                    }
                }
                return super.rmdir(path, lazy);
            }
        };

        assertMemoryLeak(ff, () -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    100,
                    "2022-06-01",
                    5
            );


            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-03'");

            Assert.assertEquals(1, counter.get());
            runPartitionPurgeJobs();
            Assert.assertEquals(2, counter.get());
        });
    }

    @Test
    public void testCannotUndoRenameAfterBrokenCopyMeta() throws Exception {
        FilesFacadeImpl ff1 = new TestFilesFacadeImpl() {
            private boolean copyCalled = false;

            @Override
            public int copy(LPSZ from, LPSZ to) {
                copyCalled = true;
                return -1;
            }

            @Override
            public boolean rmdir(Path path, boolean lazy) {
                if (!copyCalled) {
                    return super.rmdir(path, lazy);
                }
                return false;
            }
        };
        assertMemoryLeak(
                ff1,
                () -> {
                    String tableName = "tabUndoRenameCopyMeta";
                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
                    createPopulateTable(
                            tab
                                    .timestamp("ts", timestampType.getTimestampType())
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("vch", ColumnType.VARCHAR),
                            10,
                            "2022-06-01",
                            4
                    );

                    engine.clear();
                    TimestampDriver driver = timestampType.getDriver();
                    long timestamp = driver.parseFloorLiteral("2022-06-01T00:00:00.000000Z");
                    try (TableWriter writer = getWriter(tableName)) {
                        AttachDetachStatus attachDetachStatus = writer.detachPartition(timestamp);
                        Assert.assertEquals(DETACH_ERR_COPY_META, attachDetachStatus);
                    }
                    assertContent("""
                            ts\ti\tl\tvch
                            2022-06-01T09:35:59.900000Z\t1\t1\t1
                            2022-06-01T19:11:59.800000Z\t2\t2\t2
                            2022-06-02T04:47:59.700000Z\t3\t3\t3
                            2022-06-02T14:23:59.600000Z\t4\t4\t4
                            2022-06-02T23:59:59.500000Z\t5\t5\t5
                            2022-06-03T09:35:59.400000Z\t6\t6\t6
                            2022-06-03T19:11:59.300000Z\t7\t7\t7
                            2022-06-04T04:47:59.200000Z\t8\t8\t8
                            2022-06-04T14:23:59.100000Z\t9\t9\t9
                            2022-06-04T23:59:59.000000Z\t10\t10\t10
                            """, tableName);
                }
        );
    }

    @Test
    public void testDetachAttachAnotherDrive() throws Exception {
        FilesFacadeImpl ff1 = new TestFilesFacadeImpl() {
            @Override
            public int hardLinkDirRecursive(Path src, Path dst, int dirMode) {
                return 100018;
            }

            public boolean isCrossDeviceCopyError(int errno) {
                return true;
            }
        };

        assertMemoryLeak(
                ff1,
                () -> {
                    node1.setProperty(PropertyKey.CAIRO_ATTACH_PARTITION_COPY, true);
                    String tableName = "tab";
                    node1.setProperty(
                            PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX,
                            DETACHED_DIR_MARKER
                    );

                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
                    createPopulateTable(
                            tab
                                    .timestamp("ts", timestampType.getTimestampType())
                                    .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s2", ColumnType.SYMBOL),
                            10,
                            "2022-06-01",
                            3
                    );
                    execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'");
                    assertQuery("select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION")
                            .noLeakCheck()
                            .noRandomAccess()
                            .timestamp("ts")
                            .returns(replaceTimestampSuffix("""
                                    first\tts
                                    2022-06-03T02:23:59.300000Z\t2022-06-03T02:23:59.300000Z
                                    """));

                    for (int i = 0; i < 2; i++) {
                        execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'");
                        assertQuery("select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION")
                                .noLeakCheck()
                                .noRandomAccess()
                                .timestamp("ts")
                                .returns(replaceTimestampSuffix("""
                                        first\tts
                                        2022-06-01T07:11:59.900000Z\t2022-06-01T07:11:59.900000Z
                                        2022-06-02T11:59:59.500000Z\t2022-06-02T07:11:59.900000Z
                                        2022-06-03T09:35:59.200000Z\t2022-06-03T07:11:59.900000Z
                                        """));
                        execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-06-01', '2022-06-02'");

                    }
                }
        );
    }

    @Test
    public void testDetachAttachAnotherDriveFailsToCopy() throws Exception {
        FilesFacadeImpl ff1 = new TestFilesFacadeImpl() {
            @Override
            public int copyRecursive(Path src, Path dst, int dirMode) {
                return 100018;
            }

            @Override
            public int hardLinkDirRecursive(Path src, Path dst, int dirMode) {
                return 100018;
            }

            public boolean isCrossDeviceCopyError(int errno) {
                return true;
            }
        };

        assertMemoryLeak(
                ff1,
                () -> {
                    node1.setProperty(PropertyKey.CAIRO_ATTACH_PARTITION_COPY, true);
                    String tableName = "testDetachAttachAnotherDriveFailsToCopy";
                    node1.setProperty(
                            PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX,
                            DETACHED_DIR_MARKER
                    );

                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
                    createPopulateTable(tab
                                    .timestamp("ts", timestampType.getTimestampType())
                                    .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s2", ColumnType.SYMBOL)
                                    .col("vch", ColumnType.VARCHAR),
                            10,
                            "2022-06-01",
                            3
                    );
                    assertFailure(
                            "ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'",
                            DETACH_ERR_COPY.name()
                    );

                }
        );
    }

    @Test
    public void testDetachAttachAnotherDriveFailsToHardLink() throws Exception {
        FilesFacadeImpl ff1 = new TestFilesFacadeImpl() {
            @Override
            public int hardLinkDirRecursive(Path src, Path dst, int dirMode) {
                return 100018;
            }
        };

        assertMemoryLeak(
                ff1,
                () -> {
                    String tableName = "tab";
                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
                    createPopulateTable(
                            tab
                                    .timestamp("ts", timestampType.getTimestampType())
                                    .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s2", ColumnType.SYMBOL)
                                    .col("vch", ColumnType.VARCHAR),
                            10,
                            "2022-06-01",
                            3
                    );
                    assertFailure(
                            "ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'",
                            DETACH_ERR_HARD_LINK.name()
                    );

                }
        );
    }

    @Test
    public void testDetachAttachParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.timestamp("ts", timestampType.getTimestampType())
                            .col("si", ColumnType.SYMBOL).indexed(true, 250)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    10,
                    "2022-06-01",
                    2
            );

            String expected = """
                    ts\tsi\ti\tl\ts\tvch
                    2022-06-01T04:47:59.900000Z\tPEHN\t1\t1\tSXUX\t擉q\uDAE2\uDC5E͛
                    2022-06-01T09:35:59.800000Z\t\t2\t2\t\t蝰L➤~2\uDAC6\uDED3ڎBH
                    2022-06-01T14:23:59.700000Z\tVTJW\t3\t3\tRXGZ\t
                    2022-06-01T19:11:59.600000Z\tVTJW\t4\t4\tGPGW\t:}w?5J8A.mS+F~W
                    2022-06-01T23:59:59.500000Z\tCPSW\t5\t5\tGPGW\td^Z
                    2022-06-02T04:47:59.400000Z\tPEHN\t6\t6\tGPGW\t篸{\uD9D7\uDFE5\uDAE9\uDF46OF
                    2022-06-02T09:35:59.300000Z\tVTJW\t7\t7\t\t
                    2022-06-02T14:23:59.200000Z\tVTJW\t8\t8\t\t䒭ܲ\u0379軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً
                    2022-06-02T19:11:59.100000Z\t\t9\t9\tGPGW\tK\uD8E2\uDE25ӽ-\uDBED\uDC98
                    2022-06-02T23:59:59.000000Z\t\t10\t10\t\ty\u0086W
                    """;

            assertContent(expected, tableName);

            engine.clear();
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
            renameDetachedToAttachable(tableName, "2022-06-01");
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);

            engine.clear();
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2022-06-01'", sqlExecutionContext);
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
            renameDetachedToAttachable(tableName, "2022-06-01");
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);

            assertContent(expected, tableName);
        });
    }

    @Test
    public void testDetachAttachParquetPartitionAfterAddColumn() throws Exception {
        // y, added while the partition is detached, has no Parquet column: attach skips it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO tab VALUES
                        ('2024-01-01T00:00:00.000000Z', 1),
                        ('2024-01-01T12:00:00.000000Z', 2),
                        ('2024-01-02T00:00:00.000000Z', 3)
                    """);
            execute("ALTER TABLE tab CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            execute("ALTER TABLE tab ADD COLUMN y INT");
            renameDetachedToAttachable("tab", "2024-01-01");
            execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
            assertQuery("SELECT * FROM tab").timestamp("ts").expectSize().returns(replaceTimestampSuffix("""
                    ts\tx\ty
                    2024-01-01T00:00:00.000000Z\t1\tnull
                    2024-01-01T12:00:00.000000Z\t2\tnull
                    2024-01-02T00:00:00.000000Z\t3\tnull
                    """));
        });
    }

    @Test
    public void testDetachAttachParquetPartitionAfterRoundTrip() throws Exception {
        // Reattach a parquet partition, then round-trip it through native and
        // back to parquet. Both writes rewrite the _pm; the test asserts the
        // final _pm resolves cleanly against the post-rewrite txn-encoded
        // parquet file size and that the row content is preserved end-to-end.
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG),
                    10,
                    "2022-06-01",
                    2
            );
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2022-06-01'", sqlExecutionContext);
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
            renameDetachedToAttachable(tableName, "2022-06-01");
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);

            // Edit step: convert back to native and forward to parquet again.
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO NATIVE LIST '2022-06-01'", sqlExecutionContext);
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2022-06-01'", sqlExecutionContext);

            assertParquetPmResolvesAgainstTxn(tableName);

            assertQuery("select count() from " + tableName)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n10\n");
        });
    }

    @Test
    public void testDetachAttachParquetPartitionResolvesFooterAgainstTxn() throws Exception {
        // The basic detach/reattach survival check is at testDetachAttachParquetPartition;
        // this one tightens the contract by opening the post-attach _pm
        // through the production JNI path and calling resolveFooter with the
        // exact parquet file size encoded in the post-attach _txn (not
        // Long.MAX_VALUE). A regression that mishandles the MVCC chain across
        // detach would surface here as a failed match rather than a row-count
        // mismatch.
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG),
                    10,
                    "2022-06-01",
                    2
            );
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2022-06-01'", sqlExecutionContext);
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
            renameDetachedToAttachable(tableName, "2022-06-01");
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);

            assertParquetPmResolvesAgainstTxn(tableName);
        });
    }

    @Test
    public void testDetachAttachParquetPartitionWithMvccChain() throws Exception {
        // After conversion to parquet, append O3 rows into the converted
        // partition to extend the _pm MVCC chain (the writer appends a new
        // footer rather than rewriting the file in place). Detach and
        // reattach; the resolveFooter call against the latest txn-encoded
        // parquet size must still succeed, proving the entire chain travels
        // with the partition directory.
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG),
                    20,
                    "2022-06-01",
                    2
            );
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2022-06-01'", sqlExecutionContext);

            // O3 insert into the parquet partition forces a chain extension.
            execute("INSERT INTO " + tableName + "(ts, i, l) VALUES('2022-06-01T00:00:00.000000Z', 999, 999)", sqlExecutionContext);

            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
            renameDetachedToAttachable(tableName, "2022-06-01");
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);

            assertParquetPmResolvesAgainstTxn(tableName);

            // Row content survived chain + detach + reattach.
            assertQuery("select count() from " + tableName)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n21\n");
        });
    }

    @Test
    public void testDetachAttachPartition() throws Exception {
        assertMemoryLeak(
                () -> {
                    String tableName = "tab";
                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
                    createPopulateTable(
                            1,
                            tab.timestamp("ts", timestampType.getTimestampType())
                                    .col("si", ColumnType.SYMBOL).indexed(true, 250)
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s", ColumnType.SYMBOL)
                                    .col("vch", ColumnType.VARCHAR),
                            10,
                            "2022-06-01",
                            2
                    );

                    String expected = """
                            ts\tsi\ti\tl\ts\tvch
                            2022-06-01T04:47:59.900000Z\tPEHN\t1\t1\tSXUX\t擉q\uDAE2\uDC5E͛
                            2022-06-01T09:35:59.800000Z\t\t2\t2\t\t蝰L➤~2\uDAC6\uDED3ڎBH
                            2022-06-01T14:23:59.700000Z\tVTJW\t3\t3\tRXGZ\t
                            2022-06-01T19:11:59.600000Z\tVTJW\t4\t4\tGPGW\t:}w?5J8A.mS+F~W
                            2022-06-01T23:59:59.500000Z\tCPSW\t5\t5\tGPGW\td^Z
                            2022-06-02T04:47:59.400000Z\tPEHN\t6\t6\tGPGW\t篸{\uD9D7\uDFE5\uDAE9\uDF46OF
                            2022-06-02T09:35:59.300000Z\tVTJW\t7\t7\t\t
                            2022-06-02T14:23:59.200000Z\tVTJW\t8\t8\t\t䒭ܲ\u0379軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً
                            2022-06-02T19:11:59.100000Z\t\t9\t9\tGPGW\tK\uD8E2\uDE25ӽ-\uDBED\uDC98
                            2022-06-02T23:59:59.000000Z\t\t10\t10\t\ty\u0086W
                            """;

                    assertContent(expected, tableName);

                    engine.clear();
                    execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
                    renameDetachedToAttachable(tableName, "2022-06-01");
                    execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);

                    engine.clear();
                    execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
                    renameDetachedToAttachable(tableName, "2022-06-01");
                    execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);

                    assertContent(expected, tableName);
                });
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataColumnType() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabBrokenColType";
            String brokenTableName = "tabBrokenColType2";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            TableModel brokenMeta = new TableModel(configuration, brokenTableName, PartitionBy.DAY);
            try (
                    MemoryMARW mem = Vm.getCMARWInstance()
            ) {
                String timestampDay = "2022-06-01";
                createPopulateTable(
                        1,
                        tab.timestamp("ts", timestampType.getTimestampType())
                                .col("s1", ColumnType.SYMBOL).indexed(true, 256)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
                        10,
                        timestampDay,
                        3
                );
                TestUtils.createTable(
                        engine,
                        mem,
                        path,
                        brokenMeta.timestamp("ts", timestampType.getTimestampType())
                                .col("s1", ColumnType.SYMBOL).indexed(true, 256)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL),
                        1,
                        brokenMeta.getTableName()
                );
                execute("INSERT INTO " + brokenMeta.getName() + " SELECT * FROM " + tab.getName());

                engine.clear();
                execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "'");
                execute("ALTER TABLE " + brokenTableName + " DETACH PARTITION LIST '" + timestampDay + "'");

                engine.clear();
                TableToken tableToken = engine.verifyTableName(brokenTableName);
                TableToken tableToken1 = engine.verifyTableName(tableName);

                path.of(configuration.getDbRoot()).concat(tableToken).concat(timestampDay).put(DETACHED_DIR_MARKER).$();
                other.of(configuration.getDbRoot()).concat(tableToken1).concat(timestampDay).put(configuration.getAttachPartitionSuffix()).$();

                Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);

                // attempt to reattach
                assertFailure(
                        "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'",
                        "Detached column [index=3, name=l, attribute=type] does not match current table metadata"
                );
            }
        });
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataTableId() throws Exception {
        assertFailedAttachBecauseOfMetadata(
                2,
                "tabBrokenTableId",
                "tabBrokenTableId2",
                brokenMeta -> brokenMeta
                        .timestamp("ts", timestampType.getTimestampType())
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG),
                "insert into tabBrokenTableId2 " +
                        "select " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts, " +
                        "cast(x as int) i, " +
                        "x l " +
                        "from long_sequence(100)",
                "ALTER TABLE tabBrokenTableId2 ADD COLUMN s SHORT",
                "Detached partition metadata [table_id] is not compatible with current table metadata"
        );
    }

    @Test
    public void testDetachAttachPartitionBrokenMetadataTimestampIndex() throws Exception {
        assertFailedAttachBecauseOfMetadata(
                1,
                "tabBrokenTimestampIdx",
                "tabBrokenTimestampIdx2",
                brokenMeta -> brokenMeta
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG)
                        .timestamp("ts", timestampType.getTimestampType()),
                "insert into tabBrokenTimestampIdx2 " +
                        "select " +
                        "cast(x as int) i, " +
                        "x l, " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts " +
                        "from long_sequence(100)",
                "ALTER TABLE tabBrokenTimestampIdx2 ADD COLUMN s SHORT",
                "Detached partition metadata [timestamp_index] is not compatible with current table metadata"
        );
    }

    @Test
    public void testDetachAttachPartitionFailsYouDidNotRenameTheFolderToAttachable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabDetachAttachNotAttachable";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("si", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s", ColumnType.SYMBOL),
                    10,
                    "2022-06-01",
                    3
            );

            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'",
                    "could not attach partition [table=tabDetachAttachNotAttachable, detachStatus=ATTACH_ERR_MISSING_PARTITION"
            );
        });
    }

    @Test
    public void testDetachAttachPartitionMissingMetadata() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabDetachAttachMissingMeta";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    10,
                    "2022-06-01",
                    3
            );
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);

            // remove _meta.detached simply prevents metadata checking, all else is the same
            TableToken tableToken = engine.verifyTableName(tableName);
            path.of(configuration.getDbRoot())
                    .concat(tableToken)
                    .concat("2022-06-02")
                    .put(DETACHED_DIR_MARKER)
                    .concat(META_FILE_NAME)
                    .$();
            Assert.assertTrue(TestUtils.remove(path.$()));
            path.parent().concat(COLUMN_VERSION_FILE_NAME).$();
            Assert.assertTrue(TestUtils.remove(path.$()));
            renameDetachedToAttachable(tableName, "2022-06-01", "2022-06-02");
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'", sqlExecutionContext);
            assertContent(
                    """
                            ts\ts1\ti\tl\ts2\tvch
                            2022-06-01T07:11:59.900000Z\tPEHN\t1\t1\tSXUX\t1
                            2022-06-01T14:23:59.800000Z\tVTJW\t2\t2\t\t2
                            2022-06-01T21:35:59.700000Z\t\t3\t3\tSXUX\t3
                            2022-06-02T04:47:59.600000Z\t\t4\t4\t\t4
                            2022-06-02T11:59:59.500000Z\t\t5\t5\tGPGW\t5
                            2022-06-02T19:11:59.400000Z\tPEHN\t6\t6\tRXGZ\t6
                            2022-06-03T02:23:59.300000Z\tCPSW\t7\t7\t\t7
                            2022-06-03T09:35:59.200000Z\t\t8\t8\t\t8
                            2022-06-03T16:47:59.100000Z\tPEHN\t9\t9\tRXGZ\t9
                            2022-06-03T23:59:59.000000Z\tVTJW\t10\t10\tIBBT\t10
                            """,
                    tableName
            );
        });
    }

    @Test
    public void testDetachAttachPartitionPingPongConcurrent() throws Throwable {
        ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
        assertMemoryLeak(() -> {
            String tableName = "tabPingPong";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab.timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    10,
                    "2022-06-01",
                    3
            );


            CyclicBarrier start = new CyclicBarrier(2);
            CountDownLatch end = new CountDownLatch(2);
            AtomicBoolean isLive = new AtomicBoolean(true);
            AtomicInteger failureCounter = new AtomicInteger();
            Set<Long> detachedPartitionTimestamps = new ConcurrentSkipListSet<>();
            AtomicInteger detachedCount = new AtomicInteger();
            AtomicInteger attachedCount = new AtomicInteger();
            Rnd rnd = TestUtils.generateRandom(LOG);

            Thread detThread = new Thread(() -> {
                try {
                    TestUtils.unchecked(() -> {
                        start.await();
                        TimestampDriver driver = timestampType.getDriver();
                        while (isLive.get()) {
                            try (TableWriter writer = getWriter(tableName)) {
                                long partitionTimestamp = (rnd.nextInt() % writer.getPartitionCount()) * driver.fromDays(1);
                                if (!detachedPartitionTimestamps.contains(partitionTimestamp)) {
                                    writer.detachPartition(partitionTimestamp);
                                    detachedCount.incrementAndGet();
                                    detachedPartitionTimestamps.add(partitionTimestamp);
                                }
                            } catch (Throwable e) {
                                exceptions.add(e);
                                LOG.error().$(e).$();
                            }
                            TimeUnit.MILLISECONDS.sleep(rnd.nextLong(15L));
                        }
                    }, failureCounter);
                } finally {
                    Path.clearThreadLocals();
                    end.countDown();
                }
            });
            detThread.start();

            Thread attThread = new Thread(() -> {
                try {
                    TestUtils.unchecked(() -> {
                        start.await();
                        while (isLive.get() || !detachedPartitionTimestamps.isEmpty()) {
                            if (!detachedPartitionTimestamps.isEmpty()) {
                                Iterator<Long> timestamps = detachedPartitionTimestamps.iterator();
                                if (timestamps.hasNext()) {
                                    long partitionTimestamp = timestamps.next();
                                    try (TableWriter writer = getWriter(tableName)) {
                                        renameDetachedToAttachable(tableName, TableUtils.getTimestampType(tab), partitionTimestamp);
                                        writer.attachPartition(partitionTimestamp);
                                        timestamps.remove();
                                        attachedCount.incrementAndGet();
                                    } catch (Throwable e) {
                                        exceptions.add(e);
                                        TimeUnit.MILLISECONDS.sleep(rnd.nextLong(11L));
                                        LOG.error().$(e).$();
                                    }
                                }
                            }
                            TimeUnit.MILLISECONDS.sleep(rnd.nextLong(10L));
                        }
                    }, failureCounter);
                } finally {
                    Path.clearThreadLocals();
                    end.countDown();
                }
            });
            attThread.start();
            isLive.set(false);
            end.await();
            for (int i = 0, limit = exceptions.size(); i < limit; i++) {
                Assert.assertTrue(exceptions.poll() instanceof EntryUnavailableException);
            }
            Assert.assertEquals(detachedCount.get(), attachedCount.get() + detachedPartitionTimestamps.size());
        });
    }

    @Test
    public void testDetachAttachSameDrive() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_ATTACH_PARTITION_COPY, true);
            String tableName = "tabDetachAttachMissingMeta";
            node1.setProperty(
                    PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX,
                    DETACHED_DIR_MARKER
            );
            ff = new TestFilesFacadeImpl() {
                @Override
                public int hardLinkDirRecursive(Path src, Path dst, int dirMode) {
                    return 100018;
                }

                public boolean isCrossDeviceCopyError(int errno) {
                    return true;
                }
            };

            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL),
                    10,
                    "2022-06-01",
                    3
            );
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'");
            assertQuery("select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix("""
                            first\tts
                            2022-06-03T02:23:59.300000Z\t2022-06-03T02:23:59.300000Z
                            """));

            for (int i = 0; i < 2; i++) {
                execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01', '2022-06-02'");
                assertQuery("select first(ts), ts from " + tableName + " sample by 1d ALIGN TO FIRST OBSERVATION")
                        .noLeakCheck()
                        .noRandomAccess()
                        .timestamp("ts")
                        .returns(replaceTimestampSuffix("""
                                first\tts
                                2022-06-01T07:11:59.900000Z\t2022-06-01T07:11:59.900000Z
                                2022-06-02T11:59:59.500000Z\t2022-06-02T07:11:59.900000Z
                                2022-06-03T09:35:59.200000Z\t2022-06-03T07:11:59.900000Z
                                """));
                execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-06-01', '2022-06-02'");

            }
        });
    }

    @Test
    public void testDetachAttachSplitPartition() throws Exception {
        assertMemoryLeak(
                () -> {
                    String tableName = "testDetachAttachSplitPartition";
                    Overrides overrides = node1.getConfigurationOverrides();
                    overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 300);
                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
                    TableToken token = createPopulateTable(
                            1,
                            tab.timestamp("ts", timestampType.getTimestampType())
                                    .col("si", ColumnType.SYMBOL).indexed(true, 250)
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s", ColumnType.SYMBOL),
                            1000,
                            "2022-06-01",
                            2
                    );

                    assertQuery("select count(1), min(ts), max(ts) from " + tableName + " where ts in '2022-06-01'")
                            .noLeakCheck()
                            .expectSize()
                            .noRandomAccess()
                            .returns(replaceTimestampSuffix("""
                                    count\tmin\tmax
                                    500\t2022-06-01T00:02:52.799000Z\t2022-06-01T23:59:59.500000Z
                                    """));
                    assertQuery("select count(1), min(ts), max(ts) from " + tableName + " where ts in '2022-06-02'")
                            .noLeakCheck()
                            .expectSize()
                            .noRandomAccess()
                            .returns(replaceTimestampSuffix("""
                                    count\tmin\tmax
                                    500\t2022-06-02T00:02:52.299000Z\t2022-06-02T23:59:59.000000Z
                                    """));

                    try (TableReader ignore = getReader(token)) {
                        // Split partition by committing O3 to "2022-06-01"
                        execute("insert into " + tableName + "(ts) select ts + 20 * 60 * 60 * " + (ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "1000000L" : "1000000000L") + " from " + tableName, sqlExecutionContext);

                        Path path = Path.getThreadLocal(configuration.getDbRoot()).concat(token).concat(ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "2022-06-01T200057-183001.1" : "2022-06-01T200057-183000001.1").concat("ts.d");
                        FilesFacade ff = configuration.getFilesFacade();
                        Assert.assertTrue(ff.exists(path.$()));

                        execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
                    }

                    renameDetachedToAttachable(tableName, "2022-06-01");
                    execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-01'", sqlExecutionContext);
                    assertQuery("select min(ts) from " + tableName)
                            .noLeakCheck()
                            .expectSize()
                            .timestamp("min")
                            .returns(replaceTimestampSuffix("""
                                    min
                                    2022-06-01T00:02:52.799000Z
                                    """));
                });
    }

    /**
     * A DETACH links the partition into a temporary copy named after its txn. A link that fails part-way used to leave
     * that copy behind; once a later commit passed the txn, the next writer open finished the failed DETACH and gave
     * the partial copy the .detached name, next to the partition that is still attached.
     */
    @Test
    public void testDetachFailedLinkLeavesNoDetachedCopy() throws Exception {
        final AtomicInteger linksBeforeFailure = new AtomicInteger(Integer.MAX_VALUE);
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int hardLink(LPSZ src, LPSZ hardLink) {
                if (Utf8s.containsAscii(hardLink, DETACHING_DIR_MARKER) && linksBeforeFailure.getAndDecrement() <= 0) {
                    return -1;
                }
                return super.hardLink(src, hardLink);
            }

            @Override
            public boolean isCrossDeviceCopyError(int errno) {
                return false;
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO tab VALUES
                        ('2024-01-01T00:00:00Z', 1, 'A'),
                        ('2024-01-01T01:00:00Z', 2, 'B'),
                        ('2024-01-02T00:00:00Z', 3, 'C')
                    """);
            linksBeforeFailure.set(1);
            assertFailure("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'", DETACH_ERR_HARD_LINK.name());
            linksBeforeFailure.set(Integer.MAX_VALUE);

            // A commit passes the failed DETACH's txn, then the writer opens again
            execute("INSERT INTO tab VALUES ('2024-01-02T01:00:00Z', 4, 'D')");
            engine.releaseAllWriters();
            execute("INSERT INTO tab VALUES ('2024-01-02T02:00:00Z', 5, 'E')");

            final java.nio.file.Path tablePath = java.nio.file.Path.of(configuration.getDbRoot().toString(), engine.verifyTableName("tab").getDirName());
            try (Stream<java.nio.file.Path> entries = java.nio.file.Files.list(tablePath)) {
                Assert.assertEquals(
                        List.of(),
                        entries.map(entry -> entry.getFileName().toString())
                                .filter(name -> name.endsWith(DETACHED_DIR_MARKER) || name.endsWith(DETACHING_DIR_MARKER))
                                .collect(Collectors.toList())
                );
            }
            assertQuery("SELECT x, sym FROM tab")
                    .noLeakCheck().expectSize().inferRandomAccess().returns("x\tsym\n1\tA\n2\tB\n3\tC\n4\tD\n5\tE\n");
        });
    }

    /**
     * The rename that gives a committed DETACH its .detached name fails with a Windows-style errno, and the statement
     * says so. Nothing retries it in the statement's writer; the next writer open gives the partition its name.
     */
    @Test
    public void testDetachNamingFailureFinishedByWriterOpen() throws Exception {
        final DetachedNameFailingFacade ff = new DetachedNameFailingFacade(5);
        assertMemoryLeak(ff, () -> {
            final java.nio.file.Path tableDir = createDetachNamingTable(false);
            ff.isFailing = true;
            try {
                execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
                Assert.fail("DETACH must report that the partition is detached under its temporary name");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "partition is detached, its files are in ");
            } finally {
                ff.disarm();
            }
            final List<String> files = listFileNames(tableDir.resolve(assertTemporaryName(tableDir)));
            engine.releaseAllWriters();
            try (TableWriter writer = getWriter("tab")) {
                Assert.assertNotNull(writer);
            }
            assertDetachNamed(tableDir, files);
            assertAttachedBack(tableDir, false);
        });
    }

    /**
     * The DETACH commits before the partition gets its .detached name, so a failed rename cannot undo it: the
     * statement fails at the partition literal, says the partition is detached and where its files are, and a
     * repeated DETACH in the same writer finishes the name. The same fault fails the link-based naming this replaced.
     */
    @Test
    public void testDetachNamingFailureIsReportedAndRetried() throws Exception {
        final DetachedNameFailingFacade ff = new DetachedNameFailingFacade(28);
        assertMemoryLeak(ff, () -> {
            final java.nio.file.Path tableDir = createDetachNamingTable(false);
            final String sql = "ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'";
            ff.isFailing = true;
            try {
                execute(sql);
                Assert.fail("DETACH must report that the partition is detached under its temporary name");
            } catch (CairoException e) {
                Assert.assertEquals(sql.indexOf('\''), e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "partition is detached, its files are in ");
                TestUtils.assertContains(e.getFlyweightMessage(), DETACHING_DIR_MARKER);
                TestUtils.assertContains(e.getFlyweightMessage(), "when the DETACH is repeated");
            } finally {
                ff.disarm();
            }
            final List<String> files = listFileNames(tableDir.resolve(assertTemporaryName(tableDir)));

            // The statement's writer is still pooled (a query would release it): the repeat renames
            execute(sql);
            assertDetachNamed(tableDir, files);
            assertQuery("SELECT count() FROM tab").noRandomAccess().expectSize().returns("count\n2\n");
            assertAttachedBack(tableDir, false);
        });
    }

    @Test
    public void testDetachNamingFailureKeepsNosyncWalTableSuspended() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertDetachNamingFailureKeepsWalTableSuspended(CommitMode.NOSYNC, false, false);
    }

    @Test
    public void testDetachNamingFailureKeepsSyncWalTableSuspended() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        assertDetachNamingFailureKeepsWalTableSuspended(CommitMode.SYNC, false, false);
    }

    @Test
    public void testDetachNamingFailureKeepsWalTableSuspendedAfterWriterRelease() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertDetachNamingFailureKeepsWalTableSuspended(CommitMode.NOSYNC, false, true);
    }

    @Test
    public void testDetachNamingFailureSuspendsAdaptiveWalTable() throws Exception {
        // ADAPTIVE renames once a save point covers the DETACH; the rename failing there suspends the table too
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertDetachNamingFailureSuspendsWalTable(true);
    }

    @Test
    public void testDetachNamingFailureSuspendsNosyncWalTable() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertDetachNamingFailureSuspendsWalTable(false);
    }

    @Test
    public void testDetachNamingRefusalKeepsWalTableSuspended() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertDetachNamingFailureKeepsWalTableSuspended(CommitMode.NOSYNC, true, false);
    }

    /**
     * A directory that takes the .detached name while a DETACH runs, a user's copy for one, is never replaced or
     * merged into: the statement reports the partition under its temporary name, and so does every writer open until
     * the name is free.
     */
    @Test
    public void testDetachNamingRefusesExistingDetachedDirectory() throws Exception {
        final DetachedNameFailingFacade ff = new DetachedNameFailingFacade(28);
        assertMemoryLeak(ff, () -> {
            final java.nio.file.Path tableDir = createDetachNamingTable(false);
            final java.nio.file.Path userCopy = tableDir.resolve("2024-01-01" + DETACHED_DIR_MARKER);
            final String sql = "ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'";
            ff.isUserCopyArmed = true;
            try {
                execute(sql);
                Assert.fail("DETACH must not name the partition over an existing directory");
            } catch (CairoException e) {
                Assert.assertEquals(sql.indexOf('\''), e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "partition is detached, its files are in ");
                TestUtils.assertContains(e.getFlyweightMessage(), "already exists, move it away");
            }
            final String temporaryName = assertTemporaryName(tableDir);
            final List<String> files = listFileNames(tableDir.resolve(temporaryName));
            Assert.assertEquals(List.of("user-file"), listFileNames(userCopy));

            engine.releaseAllWriters();
            try (TableWriter writer = getWriter("tab")) {
                Assert.assertNotNull(writer);
            }
            Assert.assertEquals(temporaryName, assertTemporaryName(tableDir));
            Assert.assertEquals(List.of("user-file"), listFileNames(userCopy));

            // The user moves the copy away; the next writer open names the partition
            java.nio.file.Files.delete(userCopy.resolve("user-file"));
            java.nio.file.Files.delete(userCopy);
            engine.releaseAllWriters();
            try (TableWriter writer = getWriter("tab")) {
                Assert.assertNotNull(writer);
            }
            assertDetachNamed(tableDir, files);
            assertAttachedBack(tableDir, false);
        });
    }

    @Test
    public void testDetachNonPartitionedNotAllowed() throws Exception {
        assertMemoryLeak(
                () -> {
                    String tableName = "tab";
                    TableModel tab = new TableModel(configuration, tableName, PartitionBy.NONE);
                    createPopulateTable(
                            tab
                                    .timestamp("ts", timestampType.getTimestampType())
                                    .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                    .col("i", ColumnType.INT)
                                    .col("l", ColumnType.LONG)
                                    .col("s2", ColumnType.SYMBOL),
                            10,
                            "2022-06-01",
                            1
                    );
                    assertFailure(
                            "ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-01', '2022-06-02'",
                            "table is not partitioned"
                    );
                });
    }

    @Test
    public void testDetachPartitionCommits() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .col("vch", ColumnType.VARCHAR)
                            .timestamp("ts", timestampType.getTimestampType()),
                    5,
                    "2022-06-01",
                    4
            );

            TimestampDriver driver = timestampType.getDriver();
            String timestampDay = "2022-06-02";
            try (TableWriter writer = getWriter(tableName)) {
                // structural change
                writer.addColumn("new_column", ColumnType.INT, AllowAllSecurityContext.INSTANCE);

                TableWriter.Row row = writer.newRow(driver.parseFloorLiteral("2022-05-03T12:00:00.000000Z"));
                row.putLong(0, 33L);
                row.putInt(1, 33);
                row.append();

                Assert.assertEquals(AttachDetachStatus.OK, writer.detachPartition((driver.parseFloorLiteral(timestampDay))));
            }

            renameDetachedToAttachable(tableName, timestampDay);
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);

            // attach the partition
            assertContent(
                    """
                            l\ti\tvch\tts\tnew_column
                            33\t33\t\t2022-05-03T12:00:00.000000Z\tnull
                            1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T19:11:59.800000Z\tnull
                            2\t2\t\t2022-06-02T14:23:59.600000Z\tnull
                            3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-03T09:35:59.400000Z\tnull
                            4\t4\t\t2022-06-04T04:47:59.200000Z\tnull
                            5\t5\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-04T23:59:59.000000Z\tnull
                            """,
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionIndexFilesGetIndexed() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabIndexFilesIndex";
            String brokenTableName = "tabIndexFilesIndex2";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            TableModel brokenMeta = new TableModel(configuration, brokenTableName, PartitionBy.DAY);
            try (
                    MemoryMARW mem = Vm.getCMARWInstance()
            ) {
                String timestampDay = "2022-06-01";
                createPopulateTable(
                        1,
                        tab.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 512)
                                .col("vch", ColumnType.VARCHAR)
                                .timestamp("ts", timestampType.getTimestampType()),
                        12,
                        timestampDay,
                        4
                );

                TestUtils.createTable(
                        engine,
                        mem,
                        path,
                        brokenMeta.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL)
                                .col("vch", ColumnType.VARCHAR)
                                .timestamp("ts", timestampType.getTimestampType()),
                        1,
                        brokenMeta.getTableName()
                );
                execute("INSERT INTO " + brokenMeta.getName() + " SELECT * FROM " + tab.getName());

                TimestampDriver driver = timestampType.getDriver();
                long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
                try (TableWriter writer = getWriter(brokenTableName)) {
                    writer.detachPartition(timestamp);
                }
                try (TableWriter writer = getWriter(tableName)) {
                    writer.detachPartition(timestamp);
                }
                TableToken tableToken = engine.verifyTableName(brokenTableName);
                TableToken tableToken1 = engine.verifyTableName(tableName);

                path.of(configuration.getDbRoot()).concat(tableToken).concat(timestampDay).put(DETACHED_DIR_MARKER).$();
                other.of(configuration.getDbRoot()).concat(tableToken1).concat(timestampDay).put(configuration.getAttachPartitionSuffix()).$();

                Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);

                try (TableWriter writer = getWriter(tableName)) {
                    writer.attachPartition(timestamp);
                }

                assertContent(
                        ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                                """
                                        l\ti\ts\tvch\tts
                                        1\t1\tCPSW\těȞ鼷G\uD991\uDE7E\t2022-06-01T07:59:59.916666Z
                                        2\t2\t\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-01T15:59:59.833332Z
                                        3\t3\tPEHN\t\uF2C1ӍKB\t2022-06-01T23:59:59.749998Z
                                        4\t4\tPEHN\tK䰭\t2022-06-02T07:59:59.666664Z
                                        5\t5\tHYRX\tѱʜ\uDB8D\uDE4Eᯤ\\篸{\t2022-06-02T15:59:59.583330Z
                                        6\t6\tCPSW\tl";&=RON\t2022-06-02T23:59:59.499996Z
                                        7\t7\tVTJW\t\t2022-06-03T07:59:59.416662Z
                                        8\t8\tPEHN\t\uDBAE\uDD12ɜ|\\軦۽\t2022-06-03T15:59:59.333328Z
                                        9\t9\tPEHN\t7=\t2022-06-03T23:59:59.249994Z
                                        10\t10\tHYRX\t\t2022-06-04T07:59:59.166660Z
                                        11\t11\tPEHN\txL?49M\t2022-06-04T15:59:59.083326Z
                                        12\t12\tCPSW\t鳓\t2022-06-04T23:59:58.999992Z
                                        """
                                : """
                                l\ti\ts\tvch\tts
                                1\t1\tCPSW\těȞ鼷G\uD991\uDE7E\t2022-06-01T07:59:59.916666666Z
                                2\t2\t\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-01T15:59:59.833333332Z
                                3\t3\tPEHN\t\uF2C1ӍKB\t2022-06-01T23:59:59.749999998Z
                                4\t4\tPEHN\tK䰭\t2022-06-02T07:59:59.666666664Z
                                5\t5\tHYRX\tѱʜ\uDB8D\uDE4Eᯤ\\篸{\t2022-06-02T15:59:59.583333330Z
                                6\t6\tCPSW\tl";&=RON\t2022-06-02T23:59:59.499999996Z
                                7\t7\tVTJW\t\t2022-06-03T07:59:59.416666662Z
                                8\t8\tPEHN\t\uDBAE\uDD12ɜ|\\軦۽\t2022-06-03T15:59:59.333333328Z
                                9\t9\tPEHN\t7=\t2022-06-03T23:59:59.249999994Z
                                10\t10\tHYRX\t\t2022-06-04T07:59:59.166666660Z
                                11\t11\tPEHN\txL?49M\t2022-06-04T15:59:59.083333326Z
                                12\t12\tCPSW\t鳓\t2022-06-04T23:59:58.999999992Z
                                """,
                        tableName
                );
            }
        });
    }

    @Test
    public void testDetachPartitionIndexFilesGetReIndexed() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabIndexFilesReIndex";
            String brokenTableName = "tabIndexFilesReIndex2";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            TableModel brokenMeta = new TableModel(configuration, brokenTableName, PartitionBy.DAY);

            try (
                    MemoryMARW mem = Vm.getCMARWInstance()
            ) {
                String timestampDay = "2022-06-01";
                createPopulateTable(
                        1,
                        tab.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 512)
                                .col("vch", ColumnType.VARCHAR)
                                .timestamp("ts", timestampType.getTimestampType()),
                        12,
                        timestampDay,
                        4
                );


                TestUtils.createTable(
                        engine,
                        mem,
                        path,
                        brokenMeta.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 32)
                                .col("vch", ColumnType.VARCHAR)
                                .timestamp("ts", timestampType.getTimestampType()),
                        1,
                        brokenMeta.getTableName()
                );
                execute("INSERT INTO " + brokenMeta.getName() + " SELECT * FROM " + tab.getName());

                TimestampDriver driver = timestampType.getDriver();
                long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
                try (TableWriter writer = getWriter(brokenTableName)) {
                    writer.detachPartition(timestamp);
                }
                try (TableWriter writer = getWriter(tableName)) {
                    writer.detachPartition(timestamp);
                }

                TableToken tableToken = engine.verifyTableName(brokenTableName);
                TableToken tableToken1 = engine.verifyTableName(tableName);

                path.of(configuration.getDbRoot()).concat(tableToken).concat(timestampDay).put(DETACHED_DIR_MARKER).$();
                other.of(configuration.getDbRoot()).concat(tableToken1).concat(timestampDay).put(configuration.getAttachPartitionSuffix()).$();

                Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);

                try (TableWriter writer = getWriter(tableName)) {
                    writer.attachPartition(timestamp);
                }

                assertContent(
                        ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                                """
                                        l\ti\ts\tvch\tts
                                        1\t1\tCPSW\těȞ鼷G\uD991\uDE7E\t2022-06-01T07:59:59.916666Z
                                        2\t2\t\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-01T15:59:59.833332Z
                                        3\t3\tPEHN\t\uF2C1ӍKB\t2022-06-01T23:59:59.749998Z
                                        4\t4\tPEHN\tK䰭\t2022-06-02T07:59:59.666664Z
                                        5\t5\tHYRX\tѱʜ\uDB8D\uDE4Eᯤ\\篸{\t2022-06-02T15:59:59.583330Z
                                        6\t6\tCPSW\tl";&=RON\t2022-06-02T23:59:59.499996Z
                                        7\t7\tVTJW\t\t2022-06-03T07:59:59.416662Z
                                        8\t8\tPEHN\t\uDBAE\uDD12ɜ|\\軦۽\t2022-06-03T15:59:59.333328Z
                                        9\t9\tPEHN\t7=\t2022-06-03T23:59:59.249994Z
                                        10\t10\tHYRX\t\t2022-06-04T07:59:59.166660Z
                                        11\t11\tPEHN\txL?49M\t2022-06-04T15:59:59.083326Z
                                        12\t12\tCPSW\t鳓\t2022-06-04T23:59:58.999992Z
                                        """
                                : """
                                l\ti\ts\tvch\tts
                                1\t1\tCPSW\těȞ鼷G\uD991\uDE7E\t2022-06-01T07:59:59.916666666Z
                                2\t2\t\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-01T15:59:59.833333332Z
                                3\t3\tPEHN\t\uF2C1ӍKB\t2022-06-01T23:59:59.749999998Z
                                4\t4\tPEHN\tK䰭\t2022-06-02T07:59:59.666666664Z
                                5\t5\tHYRX\tѱʜ\uDB8D\uDE4Eᯤ\\篸{\t2022-06-02T15:59:59.583333330Z
                                6\t6\tCPSW\tl";&=RON\t2022-06-02T23:59:59.499999996Z
                                7\t7\tVTJW\t\t2022-06-03T07:59:59.416666662Z
                                8\t8\tPEHN\t\uDBAE\uDD12ɜ|\\軦۽\t2022-06-03T15:59:59.333333328Z
                                9\t9\tPEHN\t7=\t2022-06-03T23:59:59.249999994Z
                                10\t10\tHYRX\t\t2022-06-04T07:59:59.166666660Z
                                11\t11\tPEHN\txL?49M\t2022-06-04T15:59:59.083333326Z
                                12\t12\tCPSW\t鳓\t2022-06-04T23:59:58.999999992Z
                                """,
                        tableName
                );
            }
        });
    }

    @Test
    public void testDetachPartitionIndexFilesGetRemoved() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabIndexFiles";
            String brokenTableName = "tabIndexFiles2";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            TableModel brokenMeta = new TableModel(configuration, brokenTableName, PartitionBy.DAY);

            try (
                    MemoryMARW mem = Vm.getCMARWInstance()
            ) {
                String timestampDay = "2022-06-01";
                createPopulateTable(
                        1,
                        tab.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL)
                                .timestamp("ts", timestampType.getTimestampType()),
                        12,
                        timestampDay,
                        4
                );

                TestUtils.createTable(
                        engine,
                        mem,
                        path,
                        brokenMeta.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 32)
                                .timestamp("ts", timestampType.getTimestampType()),
                        3,
                        brokenMeta.getTableName()
                );

                execute("INSERT INTO " + brokenMeta.getName() + " SELECT * FROM " + tab.getName());

                String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? """
                        l\ti\ts\tts
                        1\t1\tCPSW\t2022-06-01T07:59:59.916666Z
                        2\t2\tHYRX\t2022-06-01T15:59:59.833332Z
                        3\t3\t\t2022-06-01T23:59:59.749998Z
                        4\t4\tVTJW\t2022-06-02T07:59:59.666664Z
                        5\t5\tPEHN\t2022-06-02T15:59:59.583330Z
                        6\t6\t\t2022-06-02T23:59:59.499996Z
                        7\t7\tVTJW\t2022-06-03T07:59:59.416662Z
                        8\t8\t\t2022-06-03T15:59:59.333328Z
                        9\t9\tCPSW\t2022-06-03T23:59:59.249994Z
                        10\t10\t\t2022-06-04T07:59:59.166660Z
                        11\t11\tPEHN\t2022-06-04T15:59:59.083326Z
                        12\t12\tCPSW\t2022-06-04T23:59:58.999992Z
                        """
                        : """
                        l\ti\ts\tts
                        1\t1\tCPSW\t2022-06-01T07:59:59.916666666Z
                        2\t2\tHYRX\t2022-06-01T15:59:59.833333332Z
                        3\t3\t\t2022-06-01T23:59:59.749999998Z
                        4\t4\tVTJW\t2022-06-02T07:59:59.666666664Z
                        5\t5\tPEHN\t2022-06-02T15:59:59.583333330Z
                        6\t6\t\t2022-06-02T23:59:59.499999996Z
                        7\t7\tVTJW\t2022-06-03T07:59:59.416666662Z
                        8\t8\t\t2022-06-03T15:59:59.333333328Z
                        9\t9\tCPSW\t2022-06-03T23:59:59.249999994Z
                        10\t10\t\t2022-06-04T07:59:59.166666660Z
                        11\t11\tPEHN\t2022-06-04T15:59:59.083333326Z
                        12\t12\tCPSW\t2022-06-04T23:59:58.999999992Z
                        """;

                assertContent(expected, tableName);
                assertContent(expected, brokenTableName);

                TimestampDriver driver = timestampType.getDriver();
                long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
                try (TableWriter writer = getWriter(brokenTableName)) {
                    writer.detachPartition(timestamp);
                }
                try (TableWriter writer = getWriter(tableName)) {
                    writer.detachPartition(timestamp);
                }

                TableToken tableToken = engine.verifyTableName(tableName);
                TableToken brokenTableToken = engine.verifyTableName(brokenTableName);

                path.of(configuration.getDbRoot()).concat(brokenTableToken).concat(timestampDay).put(DETACHED_DIR_MARKER).$();
                other.of(configuration.getDbRoot()).concat(tableToken).concat(timestampDay).put(configuration.getAttachPartitionSuffix()).$();
                Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);

                // Change table id in the metadata file in the partition
                other.concat(META_FILE_NAME).$();
                try (MemoryCMARW mem2 = Vm.getCMARWInstance()) {
                    mem2.smallFile(configuration.getFilesFacade(), other.$(), MemoryTag.NATIVE_DEFAULT);
                    mem2.putInt(META_OFFSET_TABLE_ID, tableToken.getTableId());
                }

                try (TableWriter writer = getWriter(tableName)) {
                    writer.attachPartition(timestamp);
                }

                Assert.assertFalse(Files.exists(other.of(configuration.getDbRoot()).concat(tableToken).concat(timestampDay).concat("s.k").$()));
                Assert.assertFalse(Files.exists(other.parent().concat("s.v").$()));

                assertContent(expected, tableName);
            }
        });
    }

    @Test
    public void testDetachPartitionLongerName() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    5,
                    "2022-06-01",
                    4
            );

            TimestampDriver driver = timestampType.getDriver();
            String timestampDay = "2022-06-02";
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertEquals(AttachDetachStatus.OK, writer.detachPartition((driver.parseFloorLiteral(timestampDay))));
            }
            renameDetachedToAttachable(tableName, timestampDay);
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "T23:59:59.000000Z'", sqlExecutionContext);
            assertContent(
                    """
                            l\ti\tts
                            1\t1\t2022-06-01T19:11:59.800000Z
                            2\t2\t2022-06-02T14:23:59.600000Z
                            3\t3\t2022-06-03T09:35:59.400000Z
                            4\t4\t2022-06-04T04:47:59.200000Z
                            5\t5\t2022-06-04T23:59:59.000000Z
                            """,
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionsColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabColumnTops";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .col("vch", ColumnType.VARCHAR)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    "2022-06-01",
                    4
            );

            TimestampDriver driver = timestampType.getDriver();
            String timestampDay = "2022-06-02";
            long timestamp = driver.parseFloorLiteral(timestampDay + "T22:00:00.000000Z");
            Utf8StringSink utf8sink = new Utf8StringSink();
            utf8sink.put("33");
            try (TableWriter writer = getWriter(tableName)) {
                // structural change
                writer.addColumn("new_column", ColumnType.INT, AllowAllSecurityContext.INSTANCE);

                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong(0, 33L);
                row.putInt(1, 33);
                row.putVarchar(2, utf8sink);
                row.putInt(4, 33);
                row.append();

                writer.commit();
            }
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? """
                            l\ti\tvch\tts\tnew_column
                            1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666Z\tnull
                            2\t2\t\t2022-06-01T15:59:59.833332Z\tnull
                            3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749998Z\tnull
                            4\t4\t\t2022-06-02T07:59:59.666664Z\tnull
                            5\t5\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-02T15:59:59.583330Z\tnull
                            33\t33\t33\t2022-06-02T22:00:00.000000Z\t33
                            6\t6\tṟ\u1AD3ڎB\t2022-06-02T23:59:59.499996Z\tnull
                            7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416662Z\tnull
                            8\t8\t䇜"\t2022-06-03T15:59:59.333328Z\tnull
                            9\t9\t\t2022-06-03T23:59:59.249994Z\tnull
                            10\t10\t\t2022-06-04T07:59:59.166660Z\tnull
                            11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083326Z\tnull
                            12\t12\t"+zM\t2022-06-04T23:59:58.999992Z\tnull
                            """
                            : """
                            l\ti\tvch\tts\tnew_column
                            1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666666Z\tnull
                            2\t2\t\t2022-06-01T15:59:59.833333332Z\tnull
                            3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749999998Z\tnull
                            4\t4\t\t2022-06-02T07:59:59.666666664Z\tnull
                            5\t5\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-02T15:59:59.583333330Z\tnull
                            33\t33\t33\t2022-06-02T22:00:00.000000Z\t33
                            6\t6\tṟ\u1AD3ڎB\t2022-06-02T23:59:59.499999996Z\tnull
                            7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416666662Z\tnull
                            8\t8\t䇜"\t2022-06-03T15:59:59.333333328Z\tnull
                            9\t9\t\t2022-06-03T23:59:59.249999994Z\tnull
                            10\t10\t\t2022-06-04T07:59:59.166666660Z\tnull
                            11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083333326Z\tnull
                            12\t12\t"+zM\t2022-06-04T23:59:58.999999992Z\tnull
                            """,
                    tableName
            );

            // detach the partition
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);

            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType())
                            ? """
                            l\ti\tvch\tts\tnew_column
                            1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666Z\tnull
                            2\t2\t\t2022-06-01T15:59:59.833332Z\tnull
                            3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749998Z\tnull
                            7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416662Z\tnull
                            8\t8\t䇜"\t2022-06-03T15:59:59.333328Z\tnull
                            9\t9\t\t2022-06-03T23:59:59.249994Z\tnull
                            10\t10\t\t2022-06-04T07:59:59.166660Z\tnull
                            11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083326Z\tnull
                            12\t12\t"+zM\t2022-06-04T23:59:58.999992Z\tnull
                            """
                            : """
                            l\ti\tvch\tts\tnew_column
                            1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666666Z\tnull
                            2\t2\t\t2022-06-01T15:59:59.833333332Z\tnull
                            3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749999998Z\tnull
                            7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416666662Z\tnull
                            8\t8\t䇜"\t2022-06-03T15:59:59.333333328Z\tnull
                            9\t9\t\t2022-06-03T23:59:59.249999994Z\tnull
                            10\t10\t\t2022-06-04T07:59:59.166666660Z\tnull
                            11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083333326Z\tnull
                            12\t12\t"+zM\t2022-06-04T23:59:58.999999992Z\tnull
                            """,
                    tableName
            );

            // insert data, which will create the partition again
            engine.clear();
            utf8sink.clear();
            utf8sink.put("25160");
            try (TableWriter writer = getWriter(tableName)) {
                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong(0, 25160L);
                row.putInt(1, 25160);
                row.putVarchar(2, utf8sink);
                row.putInt(4, 25160);
                row.append();
                writer.commit();
            }
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? """
                            l\ti\tvch\tts\tnew_column
                            1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666Z\tnull
                            2\t2\t\t2022-06-01T15:59:59.833332Z\tnull
                            3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749998Z\tnull
                            25160\t25160\t25160\t2022-06-02T22:00:00.000000Z\t25160
                            7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416662Z\tnull
                            8\t8\t䇜"\t2022-06-03T15:59:59.333328Z\tnull
                            9\t9\t\t2022-06-03T23:59:59.249994Z\tnull
                            10\t10\t\t2022-06-04T07:59:59.166660Z\tnull
                            11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083326Z\tnull
                            12\t12\t"+zM\t2022-06-04T23:59:58.999992Z\tnull
                            """
                            : """
                            l\ti\tvch\tts\tnew_column
                            1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666666Z\tnull
                            2\t2\t\t2022-06-01T15:59:59.833333332Z\tnull
                            3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749999998Z\tnull
                            25160\t25160\t25160\t2022-06-02T22:00:00.000000Z\t25160
                            7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416666662Z\tnull
                            8\t8\t䇜"\t2022-06-03T15:59:59.333333328Z\tnull
                            9\t9\t\t2022-06-03T23:59:59.249999994Z\tnull
                            10\t10\t\t2022-06-04T07:59:59.166666660Z\tnull
                            11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083333326Z\tnull
                            12\t12\t"+zM\t2022-06-04T23:59:58.999999992Z\tnull
                            """,
                    tableName
            );

            dropCurrentVersionOfPartition(tableName, timestampDay);
            renameDetachedToAttachable(tableName, timestampDay);

            // reattach old version
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? """
                            l\ti\tvch\tts\tnew_column
                            1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666Z\tnull
                            2\t2\t\t2022-06-01T15:59:59.833332Z\tnull
                            3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749998Z\tnull
                            4\t4\t\t2022-06-02T07:59:59.666664Z\tnull
                            5\t5\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-02T15:59:59.583330Z\tnull
                            33\t33\t33\t2022-06-02T22:00:00.000000Z\t33
                            6\t6\tṟ\u1AD3ڎB\t2022-06-02T23:59:59.499996Z\tnull
                            7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416662Z\tnull
                            8\t8\t䇜"\t2022-06-03T15:59:59.333328Z\tnull
                            9\t9\t\t2022-06-03T23:59:59.249994Z\tnull
                            10\t10\t\t2022-06-04T07:59:59.166660Z\tnull
                            11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083326Z\tnull
                            12\t12\t"+zM\t2022-06-04T23:59:58.999992Z\tnull
                            """
                            : """
                            l\ti\tvch\tts\tnew_column
                            1\t1\t&\uDA1F\uDE98|\uD924\uDE04\t2022-06-01T07:59:59.916666666Z\tnull
                            2\t2\t\t2022-06-01T15:59:59.833333332Z\tnull
                            3\t3\těȞ鼷G\uD991\uDE7E\t2022-06-01T23:59:59.749999998Z\tnull
                            4\t4\t\t2022-06-02T07:59:59.666666664Z\tnull
                            5\t5\t͛Ԉ龘и\uDA89\uDFA4~\t2022-06-02T15:59:59.583333330Z\tnull
                            33\t33\t33\t2022-06-02T22:00:00.000000Z\t33
                            6\t6\tṟ\u1AD3ڎB\t2022-06-02T23:59:59.499999996Z\tnull
                            7\t7\tqK䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\篸\t2022-06-03T07:59:59.416666662Z\tnull
                            8\t8\t䇜"\t2022-06-03T15:59:59.333333328Z\tnull
                            9\t9\t\t2022-06-03T23:59:59.249999994Z\tnull
                            10\t10\t\t2022-06-04T07:59:59.166666660Z\tnull
                            11\t11\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t2022-06-04T15:59:59.083333326Z\tnull
                            12\t12\t"+zM\t2022-06-04T23:59:58.999999992Z\tnull
                            """,
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionsIndexCapacityDiffers() throws Exception {
        assertFailedAttachBecauseOfMetadata(
                2,
                "tabBrokenIndexCapacity",
                "tabBrokenIndexCapacity2",
                brokenMeta -> brokenMeta
                        .timestamp("ts", timestampType.getTimestampType())
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG),
                "insert into tabBrokenIndexCapacity2 " +
                        "select " +
                        "CAST(1654041600000000L AS TIMESTAMP) + x * 3455990000  ts, " +
                        "cast(x as int) i, " +
                        "x l " +
                        "from long_sequence(100)",
                "ALTER TABLE tabBrokenIndexCapacity2 ADD COLUMN s SHORT",
                "Detached partition metadata [table_id] is not compatible with current table metadata"
        );
    }

    @Test
    public void testDetachPartitionsTableAddColumn() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabInAddColumn";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    "2022-06-01",
                    4
            );

            engine.clear();
            TimestampDriver driver = timestampType.getDriver();
            String timestampDay = "2022-06-01";
            long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
            try (TableWriter writer = getWriter(tableName)) {
                // structural change
                writer.addColumn("new_column", ColumnType.INT, AllowAllSecurityContext.INSTANCE);
                writer.detachPartition(timestamp);
                Assert.assertEquals(9, writer.size());
            }

            renameDetachedToAttachable(tableName, timestampDay);

            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            """
                                    l\ti\tts\tnew_column
                                    1\t1\t2022-06-01T07:59:59.916666Z\tnull
                                    2\t2\t2022-06-01T15:59:59.833332Z\tnull
                                    3\t3\t2022-06-01T23:59:59.749998Z\tnull
                                    4\t4\t2022-06-02T07:59:59.666664Z\tnull
                                    5\t5\t2022-06-02T15:59:59.583330Z\tnull
                                    6\t6\t2022-06-02T23:59:59.499996Z\tnull
                                    7\t7\t2022-06-03T07:59:59.416662Z\tnull
                                    8\t8\t2022-06-03T15:59:59.333328Z\tnull
                                    9\t9\t2022-06-03T23:59:59.249994Z\tnull
                                    10\t10\t2022-06-04T07:59:59.166660Z\tnull
                                    11\t11\t2022-06-04T15:59:59.083326Z\tnull
                                    12\t12\t2022-06-04T23:59:58.999992Z\tnull
                                    """
                            : """
                            l\ti\tts\tnew_column
                            1\t1\t2022-06-01T07:59:59.916666666Z\tnull
                            2\t2\t2022-06-01T15:59:59.833333332Z\tnull
                            3\t3\t2022-06-01T23:59:59.749999998Z\tnull
                            4\t4\t2022-06-02T07:59:59.666666664Z\tnull
                            5\t5\t2022-06-02T15:59:59.583333330Z\tnull
                            6\t6\t2022-06-02T23:59:59.499999996Z\tnull
                            7\t7\t2022-06-03T07:59:59.416666662Z\tnull
                            8\t8\t2022-06-03T15:59:59.333333328Z\tnull
                            9\t9\t2022-06-03T23:59:59.249999994Z\tnull
                            10\t10\t2022-06-04T07:59:59.166666660Z\tnull
                            11\t11\t2022-06-04T15:59:59.083333326Z\tnull
                            12\t12\t2022-06-04T23:59:58.999999992Z\tnull
                            """,
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionsTableAddColumn2() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabInAddColumn2";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    "2022-06-01",
                    4
            );

            engine.clear();
            TimestampDriver driver = timestampType.getDriver();
            String timestampDay = "2022-06-01";
            long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
            try (TableWriter writer = getWriter(tableName)) {
                writer.detachPartition(timestamp);
                // structural change
                writer.addColumn("new_column", ColumnType.INT, AllowAllSecurityContext.INSTANCE);
                Assert.assertEquals(9, writer.size());
            }

            renameDetachedToAttachable(tableName, timestampDay);

            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            """
                                    l\ti\tts\tnew_column
                                    1\t1\t2022-06-01T07:59:59.916666Z\tnull
                                    2\t2\t2022-06-01T15:59:59.833332Z\tnull
                                    3\t3\t2022-06-01T23:59:59.749998Z\tnull
                                    4\t4\t2022-06-02T07:59:59.666664Z\tnull
                                    5\t5\t2022-06-02T15:59:59.583330Z\tnull
                                    6\t6\t2022-06-02T23:59:59.499996Z\tnull
                                    7\t7\t2022-06-03T07:59:59.416662Z\tnull
                                    8\t8\t2022-06-03T15:59:59.333328Z\tnull
                                    9\t9\t2022-06-03T23:59:59.249994Z\tnull
                                    10\t10\t2022-06-04T07:59:59.166660Z\tnull
                                    11\t11\t2022-06-04T15:59:59.083326Z\tnull
                                    12\t12\t2022-06-04T23:59:58.999992Z\tnull
                                    """
                            : """
                            l\ti\tts\tnew_column
                            1\t1\t2022-06-01T07:59:59.916666666Z\tnull
                            2\t2\t2022-06-01T15:59:59.833333332Z\tnull
                            3\t3\t2022-06-01T23:59:59.749999998Z\tnull
                            4\t4\t2022-06-02T07:59:59.666666664Z\tnull
                            5\t5\t2022-06-02T15:59:59.583333330Z\tnull
                            6\t6\t2022-06-02T23:59:59.499999996Z\tnull
                            7\t7\t2022-06-03T07:59:59.416666662Z\tnull
                            8\t8\t2022-06-03T15:59:59.333333328Z\tnull
                            9\t9\t2022-06-03T23:59:59.249999994Z\tnull
                            10\t10\t2022-06-04T07:59:59.166666660Z\tnull
                            11\t11\t2022-06-04T15:59:59.083333326Z\tnull
                            12\t12\t2022-06-04T23:59:58.999999992Z\tnull
                            """,
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionsTableAddColumnAndData() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabInAddColumnAndData";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    "2022-06-01",
                    4
            );

            engine.clear();
            TimestampDriver driver = timestampType.getDriver();
            String timestampDay = "2022-06-01";
            long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
            try (TableWriter writer = getWriter(tableName)) {
                // structural change
                writer.addColumn("new_column", ColumnType.INT, AllowAllSecurityContext.INSTANCE);
                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong(0, 33L);
                row.putInt(1, 33);
                row.append();
                writer.detachPartition(timestamp);
                Assert.assertEquals(9, writer.size());
            }

            renameDetachedToAttachable(tableName, timestampDay);

            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            """
                                    l\ti\tts\tnew_column
                                    33\t33\t2022-06-01T00:00:00.000000Z\tnull
                                    1\t1\t2022-06-01T07:59:59.916666Z\tnull
                                    2\t2\t2022-06-01T15:59:59.833332Z\tnull
                                    3\t3\t2022-06-01T23:59:59.749998Z\tnull
                                    4\t4\t2022-06-02T07:59:59.666664Z\tnull
                                    5\t5\t2022-06-02T15:59:59.583330Z\tnull
                                    6\t6\t2022-06-02T23:59:59.499996Z\tnull
                                    7\t7\t2022-06-03T07:59:59.416662Z\tnull
                                    8\t8\t2022-06-03T15:59:59.333328Z\tnull
                                    9\t9\t2022-06-03T23:59:59.249994Z\tnull
                                    10\t10\t2022-06-04T07:59:59.166660Z\tnull
                                    11\t11\t2022-06-04T15:59:59.083326Z\tnull
                                    12\t12\t2022-06-04T23:59:58.999992Z\tnull
                                    """
                            : """
                            l\ti\tts\tnew_column
                            33\t33\t2022-06-01T00:00:00.000000Z\tnull
                            1\t1\t2022-06-01T07:59:59.916666666Z\tnull
                            2\t2\t2022-06-01T15:59:59.833333332Z\tnull
                            3\t3\t2022-06-01T23:59:59.749999998Z\tnull
                            4\t4\t2022-06-02T07:59:59.666666664Z\tnull
                            5\t5\t2022-06-02T15:59:59.583333330Z\tnull
                            6\t6\t2022-06-02T23:59:59.499999996Z\tnull
                            7\t7\t2022-06-03T07:59:59.416666662Z\tnull
                            8\t8\t2022-06-03T15:59:59.333333328Z\tnull
                            9\t9\t2022-06-03T23:59:59.249999994Z\tnull
                            10\t10\t2022-06-04T07:59:59.166666660Z\tnull
                            11\t11\t2022-06-04T15:59:59.083333326Z\tnull
                            12\t12\t2022-06-04T23:59:58.999999992Z\tnull
                            """,
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionsTableAddColumnAndData2() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabInAddColumn2";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    "2022-06-01",
                    4
            );

            engine.clear();
            TimestampDriver driver = timestampType.getDriver();
            String timestampDay = "2022-06-01";
            long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
            try (TableWriter writer = getWriter(tableName)) {
                writer.detachPartition(timestamp);

                // structural change
                writer.addColumn("new_column", ColumnType.INT, AllowAllSecurityContext.INSTANCE);

                TableWriter.Row row = writer.newRow(driver.parseFloorLiteral("2022-06-02T00:00:00.000000Z"));
                row.putLong(0, 33L);
                row.putInt(1, 33);
                row.putInt(3, 333);
                row.append();

                Assert.assertEquals(10, writer.size());
                writer.commit();
            }

            renameDetachedToAttachable(tableName, timestampDay);

            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            """
                                    l\ti\tts\tnew_column
                                    1\t1\t2022-06-01T07:59:59.916666Z\tnull
                                    2\t2\t2022-06-01T15:59:59.833332Z\tnull
                                    3\t3\t2022-06-01T23:59:59.749998Z\tnull
                                    33\t33\t2022-06-02T00:00:00.000000Z\t333
                                    4\t4\t2022-06-02T07:59:59.666664Z\tnull
                                    5\t5\t2022-06-02T15:59:59.583330Z\tnull
                                    6\t6\t2022-06-02T23:59:59.499996Z\tnull
                                    7\t7\t2022-06-03T07:59:59.416662Z\tnull
                                    8\t8\t2022-06-03T15:59:59.333328Z\tnull
                                    9\t9\t2022-06-03T23:59:59.249994Z\tnull
                                    10\t10\t2022-06-04T07:59:59.166660Z\tnull
                                    11\t11\t2022-06-04T15:59:59.083326Z\tnull
                                    12\t12\t2022-06-04T23:59:58.999992Z\tnull
                                    """ :
                            """
                                    l\ti\tts\tnew_column
                                    1\t1\t2022-06-01T07:59:59.916666666Z\tnull
                                    2\t2\t2022-06-01T15:59:59.833333332Z\tnull
                                    3\t3\t2022-06-01T23:59:59.749999998Z\tnull
                                    33\t33\t2022-06-02T00:00:00.000000Z\t333
                                    4\t4\t2022-06-02T07:59:59.666666664Z\tnull
                                    5\t5\t2022-06-02T15:59:59.583333330Z\tnull
                                    6\t6\t2022-06-02T23:59:59.499999996Z\tnull
                                    7\t7\t2022-06-03T07:59:59.416666662Z\tnull
                                    8\t8\t2022-06-03T15:59:59.333333328Z\tnull
                                    9\t9\t2022-06-03T23:59:59.249999994Z\tnull
                                    10\t10\t2022-06-04T07:59:59.166666660Z\tnull
                                    11\t11\t2022-06-04T15:59:59.083333326Z\tnull
                                    12\t12\t2022-06-04T23:59:58.999999992Z\tnull
                                    """,
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionsTimeTravel() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tabTimeTravel";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            String timestampDay = "2022-06-01";
            createPopulateTable(
                    tab
                            .col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    timestampDay,
                    4
            );
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                            """
                                    l\ti\tts
                                    1\t1\t2022-06-01T07:59:59.916666Z
                                    2\t2\t2022-06-01T15:59:59.833332Z
                                    3\t3\t2022-06-01T23:59:59.749998Z
                                    4\t4\t2022-06-02T07:59:59.666664Z
                                    5\t5\t2022-06-02T15:59:59.583330Z
                                    6\t6\t2022-06-02T23:59:59.499996Z
                                    7\t7\t2022-06-03T07:59:59.416662Z
                                    8\t8\t2022-06-03T15:59:59.333328Z
                                    9\t9\t2022-06-03T23:59:59.249994Z
                                    10\t10\t2022-06-04T07:59:59.166660Z
                                    11\t11\t2022-06-04T15:59:59.083326Z
                                    12\t12\t2022-06-04T23:59:58.999992Z
                                    """
                            : """
                            l\ti\tts
                            1\t1\t2022-06-01T07:59:59.916666666Z
                            2\t2\t2022-06-01T15:59:59.833333332Z
                            3\t3\t2022-06-01T23:59:59.749999998Z
                            4\t4\t2022-06-02T07:59:59.666666664Z
                            5\t5\t2022-06-02T15:59:59.583333330Z
                            6\t6\t2022-06-02T23:59:59.499999996Z
                            7\t7\t2022-06-03T07:59:59.416666662Z
                            8\t8\t2022-06-03T15:59:59.333333328Z
                            9\t9\t2022-06-03T23:59:59.249999994Z
                            10\t10\t2022-06-04T07:59:59.166666660Z
                            11\t11\t2022-06-04T15:59:59.083333326Z
                            12\t12\t2022-06-04T23:59:58.999999992Z
                            """,
                    tableName
            );

            // drop the partition
            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "'", sqlExecutionContext);

            // insert data, which will create the partition again
            engine.clear();
            TimestampDriver driver = timestampType.getDriver();
            long timestamp = driver.parseFloorLiteral(timestampDay + "T00:00:00.000000Z");
            long timestamp2 = driver.parseFloorLiteral("2022-06-01T09:59:59.999999Z");
            try (TableWriter writer = getWriter(tableName)) {

                TableWriter.Row row = writer.newRow(timestamp2);
                row.putLong(0, 137L);
                row.putInt(1, 137);
                row.append(); // O3 append

                row = writer.newRow(timestamp);
                row.putLong(0, 2802L);
                row.putInt(1, 2802);
                row.append(); // O3 append

                writer.commit();
            }
            assertQuery(tableName)
                    .ddl(null)
                    .timestamp("ts")
                    .expectSize()
                    .returns(ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? """
                            l\ti\tts
                            2802\t2802\t2022-06-01T00:00:00.000000Z
                            137\t137\t2022-06-01T09:59:59.999999Z
                            4\t4\t2022-06-02T07:59:59.666664Z
                            5\t5\t2022-06-02T15:59:59.583330Z
                            6\t6\t2022-06-02T23:59:59.499996Z
                            7\t7\t2022-06-03T07:59:59.416662Z
                            8\t8\t2022-06-03T15:59:59.333328Z
                            9\t9\t2022-06-03T23:59:59.249994Z
                            10\t10\t2022-06-04T07:59:59.166660Z
                            11\t11\t2022-06-04T15:59:59.083326Z
                            12\t12\t2022-06-04T23:59:58.999992Z
                            """
                            : """
                            l\ti\tts
                            2802\t2802\t2022-06-01T00:00:00.000000000Z
                            137\t137\t2022-06-01T09:59:59.999999000Z
                            4\t4\t2022-06-02T07:59:59.666666664Z
                            5\t5\t2022-06-02T15:59:59.583333330Z
                            6\t6\t2022-06-02T23:59:59.499999996Z
                            7\t7\t2022-06-03T07:59:59.416666662Z
                            8\t8\t2022-06-03T15:59:59.333333328Z
                            9\t9\t2022-06-03T23:59:59.249999994Z
                            10\t10\t2022-06-04T07:59:59.166666660Z
                            11\t11\t2022-06-04T15:59:59.083333326Z
                            12\t12\t2022-06-04T23:59:58.999999992Z
                            """);

            dropCurrentVersionOfPartition(tableName, timestampDay);
            renameDetachedToAttachable(tableName, timestampDay);

            // reattach old version
            execute("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'");
            assertContent(
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? """
                            l\ti\tts
                            1\t1\t2022-06-01T07:59:59.916666Z
                            2\t2\t2022-06-01T15:59:59.833332Z
                            3\t3\t2022-06-01T23:59:59.749998Z
                            4\t4\t2022-06-02T07:59:59.666664Z
                            5\t5\t2022-06-02T15:59:59.583330Z
                            6\t6\t2022-06-02T23:59:59.499996Z
                            7\t7\t2022-06-03T07:59:59.416662Z
                            8\t8\t2022-06-03T15:59:59.333328Z
                            9\t9\t2022-06-03T23:59:59.249994Z
                            10\t10\t2022-06-04T07:59:59.166660Z
                            11\t11\t2022-06-04T15:59:59.083326Z
                            12\t12\t2022-06-04T23:59:58.999992Z
                            """
                            : """
                            l\ti\tts
                            1\t1\t2022-06-01T07:59:59.916666666Z
                            2\t2\t2022-06-01T15:59:59.833333332Z
                            3\t3\t2022-06-01T23:59:59.749999998Z
                            4\t4\t2022-06-02T07:59:59.666666664Z
                            5\t5\t2022-06-02T15:59:59.583333330Z
                            6\t6\t2022-06-02T23:59:59.499999996Z
                            7\t7\t2022-06-03T07:59:59.416666662Z
                            8\t8\t2022-06-03T15:59:59.333333328Z
                            9\t9\t2022-06-03T23:59:59.249999994Z
                            10\t10\t2022-06-04T07:59:59.166666660Z
                            11\t11\t2022-06-04T15:59:59.083333326Z
                            12\t12\t2022-06-04T23:59:58.999999992Z
                            """,
                    tableName
            );
        });
    }

    @Test
    public void testDetachPartitionsTimestampColumnTooShort() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    "2022-06-01",
                    4
            );

            String timestampDay = "2022-06-01";
            String timestampWrongDay2 = "2022-06-02";

            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "','" + timestampWrongDay2 + "'");
            renameDetachedToAttachable(tableName, timestampDay);

            TableToken tableToken = engine.verifyTableName(tableName);
            Path src = Path.PATH.get().of(configuration.getDbRoot()).concat(tableToken).concat(timestampDay).put(configuration.getAttachPartitionSuffix()).slash();
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            dFile(src, "ts", -1);
            long fd = TableUtils.openRW(ff, src.$(), LOG, configuration.getWriterFileOpenOpts());
            try {
                ff.truncate(fd, 8);
            } finally {
                ff.close(fd);
            }

            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampDay + "'",
                    "cannot read min, max timestamp from the"
            );
        });
    }

    @Test
    public void testDetachPartitionsWrongFolderName() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab";
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    1,
                    tab.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts", timestampType.getTimestampType()),
                    12,
                    "2022-06-01",
                    4
            );

            String timestampDay = "2022-06-01";
            String timestampWrongDay2 = "2022-06-02";

            execute("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + timestampDay + "','" + timestampWrongDay2 + "'");
            renameDetachedToAttachable(tableName, timestampDay);

            String timestampWrongDay = "2021-06-01";

            // Partition does not exist in copied _dtxn
            TableToken tableToken = engine.verifyTableName(tableName);
            Path src = Path.PATH.get().of(configuration.getDbRoot()).concat(tableToken).concat(timestampDay).put(configuration.getAttachPartitionSuffix()).slash();
            Path dst = Path.PATH2.get().of(configuration.getDbRoot()).concat(tableToken).concat(timestampWrongDay).put(configuration.getAttachPartitionSuffix()).slash();

            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            Assert.assertEquals(0, ff.rename(src.$(), dst.$()));
            assertFailure("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampWrongDay + "'", "partition is not preset in detached txn file");

            // Existing partition but wrong folder name
            dst = Path.PATH2.get().of(configuration.getDbRoot()).concat(tableToken).concat(timestampWrongDay).put(configuration.getAttachPartitionSuffix()).slash();
            Path dst2 = Path.PATH.get().of(configuration.getDbRoot()).concat(tableToken).concat(timestampWrongDay2).put(configuration.getAttachPartitionSuffix()).slash();
            Assert.assertEquals(0, ff.rename(dst.$(), dst2.$()));

            assertFailure(
                    "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + timestampWrongDay2 + "'",
                    "invalid timestamp data in detached partition, data does not match partition directory name"
            );
        });
    }

    /**
     * A crash between a DETACH's commit and the rename that gives the partition its {@code .detached} name leaves the
     * partition under its temporary name; the next writer open renames it. On a file system that reports no entry
     * types (XFS without ftype, some FUSE and network file systems) the DETACH must still link the partition and the
     * open's scan must still find the temporary name.
     */
    @Test
    public void testFinishDetachAfterCrashWhenEntryTypesAreUnknown() throws Exception {
        final AtomicBoolean isEntryTypeUnknown = new AtomicBoolean();
        ff = new TestFilesFacadeImpl() {
            @Override
            public int findType(long findPtr) {
                return isEntryTypeUnknown.get() ? Files.DT_UNKNOWN : super.findType(findPtr);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("create table tab (ts " + timestampType.getTypeName() + ", v long) timestamp(ts) partition by day bypass wal");
            execute("insert into tab values ('2024-01-01T00:00:00.000000Z', 1), ('2024-01-02T00:00:00.000000Z', 2)");
            // Every listing from the DETACH to the end reports unknown types: the DETACH's link, the writer open's
            // scan and finish after the simulated crash, the ATTACH and the query.
            isEntryTypeUnknown.set(true);
            execute("alter table tab detach partition list '2024-01-01'");
            final TableToken tt = engine.verifyTableName("tab");
            final long detachTxn;
            try (TableWriter writer = getWriter(tt)) {
                detachTxn = writer.getTxn();
            }
            engine.releaseAllWriters();

            final java.nio.file.Path tableDir = java.nio.file.Path.of(configuration.getDbRoot().toString(), tt.getDirName());
            final java.nio.file.Path detached = tableDir.resolve("2024-01-01" + TableUtils.DETACHED_DIR_MARKER);
            final java.nio.file.Path detaching = tableDir.resolve("2024-01-01." + detachTxn + TableUtils.DETACHING_DIR_MARKER);
            java.nio.file.Files.move(detached, detaching);
            final List<String> files;
            try (Stream<java.nio.file.Path> list = java.nio.file.Files.list(detaching)) {
                files = list.map(p -> p.getFileName().toString()).sorted().collect(Collectors.toList());
            }
            Assert.assertFalse("the temporary name must hold the partition's files", files.isEmpty());

            try (TableWriter writer = getWriter(tt)) {
                Assert.assertNotNull(writer);
            }
            engine.releaseAllWriters();
            Assert.assertFalse("the open must finish the detach", java.nio.file.Files.exists(detaching));
            try (Stream<java.nio.file.Path> list = java.nio.file.Files.list(detached)) {
                Assert.assertEquals(files, list.map(p -> p.getFileName().toString()).sorted().collect(Collectors.toList()));
            }

            java.nio.file.Files.move(detached, tableDir.resolve("2024-01-01" + configuration.getAttachPartitionSuffix()));
            execute("alter table tab attach partition list '2024-01-01'");
            assertQuery("select count(), sum(v) from tab").noRandomAccess().expectSize().returns("count\tsum\n2\t3\n");
            isEntryTypeUnknown.set(false);
        });
    }

    @Test
    public void testNoDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tab = new TableModel(configuration, "tab0", PartitionBy.DAY);
            AbstractCairoTest.create(tab
                    .col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG)
            );
            try {
                execute("ALTER TABLE tab0 DETACH PARTITION LIST '2022-06-27'", sqlExecutionContext);
                Assert.fail();
            } catch (AssertionError e) {
                Assert.assertEquals(-1, tab.getTimestampIndex());
            }
        });
    }

    @Test
    public void testNotPartitioned() throws Exception {
        TableModel tableModel = new TableModel(configuration, "tab", PartitionBy.NONE).timestamp();
        AbstractSqlParserTest.assertSyntaxError(
                "ALTER TABLE tab DETACH PARTITION LIST '2022-06-27'",
                23,
                "table is not partitioned",
                tableModel
        );
    }

    @Test
    public void testPartitionEmpty() throws Exception {
        assertFailure(
                "tab11",
                "ALTER TABLE tab11 DETACH PARTITION LIST '2022-06-06'",
                "could not detach partition [table=tab11, detachStatus=DETACH_ERR_MISSING_PARTITION"
        );
    }

    @Test
    public void testPartitionFolderDoesNotExist() throws Exception {
        assertFailure(
                new TestFilesFacadeImpl() {
                    @Override
                    public boolean exists(LPSZ path) {
                        if (Utf8s.endsWithAscii(path, "2022-06-03")) {
                            return false;
                        }
                        return super.exists(path);
                    }
                },
                "tab111",
                "ALTER TABLE tab111 DETACH PARTITION LIST '2022-06-03'",
                "could not detach partition [table=tab111, detachStatus=DETACH_ERR_MISSING_PARTITION_DIR"
        );
    }

    @Test
    public void testSyntaxErrorListOrWhereExpected() throws Exception {
        TableModel tableModel = new TableModel(configuration, "tab", PartitionBy.DAY).timestamp();
        AbstractSqlParserTest.assertSyntaxError(
                "ALTER TABLE tab DETACH PARTITION",
                32,
                "'list' or 'where' expected",
                tableModel
        );
    }

    @Test
    public void testSyntaxErrorPartitionMissing() throws Exception {
        TableModel tableModel = new TableModel(configuration, "tab", PartitionBy.DAY).timestamp();
        AbstractSqlParserTest.assertSyntaxError(
                "ALTER TABLE tab DETACH '2022-06-27'",
                23,
                "'partition' expected",
                tableModel
        );
    }

    @Test
    public void testSyntaxErrorPartitionNameExpected() throws Exception {
        TableModel tableModel = new TableModel(configuration, "tab", PartitionBy.DAY).timestamp();
        AbstractSqlParserTest.assertSyntaxError(
                "ALTER TABLE tab DETACH PARTITION LIST",
                37,
                "partition name expected",
                tableModel
        );
    }

    @Test
    public void testSyntaxErrorUnknownKeyword() throws Exception {
        TableModel tableModel = new TableModel(configuration, "tab", PartitionBy.DAY).timestamp();
        AbstractSqlParserTest.assertSyntaxError(
                "ALTER TABLE tab foobar",
                16,
                SqlCompilerImpl.ALTER_TABLE_EXPECTED_TOKEN_DESCR,
                tableModel
        );
    }

    // The detached partition renamed to .attachable and attached again brings back all its rows
    private void assertAttachedBack(java.nio.file.Path tableDir, boolean isWal) throws Exception {
        java.nio.file.Files.move(
                tableDir.resolve("2024-01-01" + DETACHED_DIR_MARKER),
                tableDir.resolve("2024-01-01" + configuration.getAttachPartitionSuffix())
        );
        execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
        if (isWal) {
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("tab")));
        }
        assertQuery("SELECT count() FROM tab").noRandomAccess().expectSize().returns("count\n3\n");
    }

    /**
     * The fault that keeps a committed DETACH from its .detached name holds across RESUME WAL: a WAL table must stay
     * suspended with the error, applying nothing after the DETACH, until the rename succeeds. The pending rename lives
     * on the disk only (the apply job closes a writer whose apply failed), so every writer open retries it.
     */
    private void assertDetachNamingFailureKeepsWalTableSuspended(int commitMode, boolean isNameTaken, boolean isWriterReleased) throws Exception {
        // A disk-full errno, which the suspension's error tag must still show after the failed writer open
        final DetachedNameFailingFacade ff = new DetachedNameFailingFacade(Os.isWindows() ? 112 : 28);
        assertMemoryLeak(ff, () -> {
            Assert.assertEquals(commitMode, configuration.getCommitMode());
            final java.nio.file.Path tableDir = createDetachNamingTable(true);
            final java.nio.file.Path userCopy = tableDir.resolve("2024-01-01" + DETACHED_DIR_MARKER);
            final TableToken tableToken = engine.verifyTableName("tab");
            final String expectedError = isNameTaken ? "already exists, move it away" : "QuestDB renames them to ";
            if (isNameTaken) {
                ff.isUserCopyArmed = true;
            } else {
                ff.isFailing = true;
            }
            try {
                execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
                drainWalQueue();
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
                TestUtils.assertContains(engine.getTableSequencerAPI().getTxnTracker(tableToken).getErrorMessage(), expectedError);
                final String temporaryName = assertTemporaryName(tableDir);
                execute("INSERT INTO tab VALUES ('2024-01-03T01:00:00Z', 4)");
                if (isWriterReleased) {
                    engine.releaseAllWriters();
                }

                execute("ALTER TABLE tab RESUME WAL");
                drainWalQueue();
                Assert.assertTrue("a WAL table must not apply on while its detached partition has no .detached name",
                        engine.getTableSequencerAPI().isSuspended(tableToken));
                final String error = engine.getTableSequencerAPI().getTxnTracker(tableToken).getErrorMessage();
                TestUtils.assertContains(error, "partition is detached, its files are in ");
                TestUtils.assertContains(error, temporaryName);
                TestUtils.assertContains(error, expectedError);
                Assert.assertEquals(
                        isNameTaken ? ErrorTag.NONE : ErrorTag.DISK_FULL,
                        engine.getTableSequencerAPI().getTxnTracker(tableToken).getErrorTag()
                );
                Assert.assertEquals(temporaryName, assertTemporaryName(tableDir));
                if (isNameTaken) {
                    Assert.assertEquals(List.of("user-file"), listFileNames(userCopy));
                }
            } finally {
                ff.disarm();
            }
            final List<String> files = listFileNames(tableDir.resolve(assertTemporaryName(tableDir)));
            if (isNameTaken) {
                // The user moves the copy away
                java.nio.file.Files.delete(userCopy.resolve("user-file"));
                java.nio.file.Files.delete(userCopy);
            }

            execute("ALTER TABLE tab RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertDetachNamed(tableDir, files);
            assertQuery("SELECT count() FROM tab").noRandomAccess().expectSize().returns("count\n3\n");
        });
    }

    private void assertDetachNamingFailureSuspendsWalTable(boolean isDeferred) throws Exception {
        final DetachedNameFailingFacade ff = new DetachedNameFailingFacade(28);
        assertMemoryLeak(ff, () -> {
            Assert.assertEquals(isDeferred ? CommitMode.ADAPTIVE : CommitMode.NOSYNC, configuration.getCommitMode());
            final java.nio.file.Path tableDir = createDetachNamingTable(true);
            final TableToken tableToken = engine.verifyTableName("tab");
            try (TableWriter writer = getWriter(tableToken)) {
                Assert.assertEquals(isDeferred, writer.isEnrolledAdaptive());
            }
            ff.isFailing = true;
            try {
                execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
                drainWalQueue();
            } finally {
                ff.disarm();
            }
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            final String error = engine.getTableSequencerAPI().getTxnTracker(tableToken).getErrorMessage();
            TestUtils.assertContains(error, "partition is detached, its files are in ");
            TestUtils.assertContains(error, "RESUME WAL");
            final List<String> files = listFileNames(tableDir.resolve(assertTemporaryName(tableDir)));

            execute("ALTER TABLE tab RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertDetachNamed(tableDir, files);
            assertQuery("SELECT count() FROM tab").noRandomAccess().expectSize().returns("count\n2\n");
            assertAttachedBack(tableDir, true);
        });
    }

    private void assertCannotCopyMeta(String tableName, int copyCallId) throws Exception {
        assertMemoryLeak(() -> {
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("i", ColumnType.INT)
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL)
                            .col("vch", ColumnType.VARCHAR),
                    10,
                    "2022-06-01",
                    4
            );
            String expected = """
                    ts\ti\ts1\tl\ts2\tvch
                    2022-06-01T09:35:59.900000Z\t1\tPEHN\t1\tSXUX\t1
                    2022-06-01T19:11:59.800000Z\t2\tVTJW\t2\t\t2
                    2022-06-02T04:47:59.700000Z\t3\t\t3\tSXUX\t3
                    2022-06-02T14:23:59.600000Z\t4\t\t4\t\t4
                    2022-06-02T23:59:59.500000Z\t5\t\t5\tGPGW\t5
                    2022-06-03T09:35:59.400000Z\t6\tPEHN\t6\tRXGZ\t6
                    2022-06-03T19:11:59.300000Z\t7\tCPSW\t7\t\t7
                    2022-06-04T04:47:59.200000Z\t8\t\t8\t\t8
                    2022-06-04T14:23:59.100000Z\t9\tPEHN\t9\tRXGZ\t9
                    2022-06-04T23:59:59.000000Z\t10\tVTJW\t10\tIBBT\t10
                    """;
            assertContent(expected, tableName);

            AbstractCairoTest.ff = new TestFilesFacadeImpl() {
                private int copyCallCount = 0;

                public int copy(LPSZ from, LPSZ to) {
                    return ++copyCallCount == copyCallId ? -1 : super.copy(from, to);
                }
            };
            engine.clear(); // to recreate the writer with the new ff
            TimestampDriver driver = timestampType.getDriver();
            long timestamp = driver.parseFloorLiteral("2022-06-01T00:00:00.000000Z");
            try (TableWriter writer = getWriter(tableName)) {
                AttachDetachStatus attachDetachStatus = writer.detachPartition(timestamp);
                Assert.assertEquals(DETACH_ERR_COPY_META, attachDetachStatus);
            }

            assertContent(expected, tableName);

            // check no metadata files were left behind
            TableToken tableToken = engine.verifyTableName(tableName);
            path.of(configuration.getDbRoot())
                    .concat(tableToken)
                    .concat("2022-06-01").concat(META_FILE_NAME)
                    .$();
            Assert.assertFalse(Files.exists(path.$()));
            path.parent().concat(COLUMN_VERSION_FILE_NAME).$();
            Assert.assertFalse(Files.exists(path.$()));
        });
    }

    private void assertContent(String expected, String tableName) throws Exception {
        engine.clear();
        expected = replaceTimestampSuffix1(expected, timestampType.getTypeName());
        assertQuery(tableName)
                .ddl(null)
                .timestamp("ts")
                .expectSize()
                .returns(expected);
    }

    private void assertFailedAttachBecauseOfMetadata(
            int brokenMetaId,
            String tableName,
            String brokenTableName,
            Function<TableModel, TableModel> brokenMetaTransform,
            String insertStmt,
            String finalStmt,
            String expectedErrorMessage
    ) throws Exception {
        assertMemoryLeak(() -> {
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            TableModel brokenMeta = new TableModel(configuration, brokenTableName, PartitionBy.DAY);
            try (
                    MemoryMARW mem = Vm.getCMARWInstance()
            ) {
                createPopulateTable(
                        1,
                        tab.timestamp("ts", timestampType.getTimestampType())
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL)
                                .col("vch", ColumnType.VARCHAR),
                        10,
                        "2022-06-01",
                        3
                );
                // create populate broken metadata table
                TestUtils.createTable(
                        engine,
                        mem,
                        path,
                        brokenMetaTransform.apply(brokenMeta),
                        brokenMetaId,
                        brokenMeta.getTableName()
                );
                if (insertStmt != null) {
                    execute(insertStmt, sqlExecutionContext);
                }
                if (finalStmt != null) {
                    execute(finalStmt, sqlExecutionContext);
                }

                // detach partitions and override detached metadata with broken metadata
                engine.clear();
                execute(
                        "ALTER TABLE " + tableName + " DETACH PARTITION LIST '2022-06-02'",
                        sqlExecutionContext
                );
                execute(
                        "ALTER TABLE " + brokenTableName + " DETACH PARTITION LIST '2022-06-02'",
                        sqlExecutionContext
                );
                engine.clear();
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(configuration.getDbRoot())
                        .concat(tableToken)
                        .concat("2022-06-02")
                        .put(DETACHED_DIR_MARKER)
                        .concat(META_FILE_NAME)
                        .$();
                Assert.assertTrue(TestUtils.remove(path.$()));
                other.of(configuration.getDbRoot())
                        .concat(engine.verifyTableName(brokenTableName))
                        .concat("2022-06-02")
                        .put(DETACHED_DIR_MARKER)
                        .concat(META_FILE_NAME)
                        .$();
                Assert.assertEquals(Files.FILES_RENAME_OK, Files.rename(other.$(), path.$()));

                renameDetachedToAttachable(tableName, "2022-06-02");

                // attempt to reattach
                assertFailure(
                        "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2022-06-02'",
                        expectedErrorMessage
                );
            }
        });
    }

    /**
     * @param isSymbolRenamedAwayAndBack whether sym is renamed away and back while the partition is detached: the
     *                                   symbol scan then has to read the version of sym the partition holds
     */
    private void assertAttachRestoresDetachedDirWhenSymbolScanFails(boolean isCopied, boolean isWriterRecreated, boolean isSymbolRenamedAwayAndBack) throws Exception {
        final SymbolDataMapFailingFilesFacade ff = new SymbolDataMapFailingFilesFacade();
        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_ATTACH_PARTITION_COPY, Boolean.toString(isCopied));
            if (isSymbolRenamedAwayAndBack) {
                // sym joins the empty table after another column, so that its files carry a column name txn above 0,
                // and its null flag stays unset while it takes no NULL
                execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("ALTER TABLE tab ADD COLUMN y INT");
                execute("ALTER TABLE tab ADD COLUMN sym SYMBOL");
            } else {
                execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            }
            execute("""
                    INSERT INTO tab (ts, x, sym) VALUES
                        ('2024-01-01T00:00:00Z', 1, 'A'),
                        ('2024-01-01T01:00:00Z', 2, 'B'),
                        ('2024-01-02T00:00:00Z', 3, 'C')
                    """);
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            if (isSymbolRenamedAwayAndBack) {
                final long symNameTxn = defaultColumnNameTxn("tab", "sym");
                Assert.assertTrue(symNameTxn > 0);
                // the version of sym the detached partition holds
                ff.dataFileName = "sym.d." + symNameTxn;
                execute("ALTER TABLE tab RENAME COLUMN sym TO sym_away");
                execute("ALTER TABLE tab RENAME COLUMN sym_away TO sym");
            }
            renameDetachedToAttachable("tab", "2024-01-01");
            ff.isFailing = true;
            try {
                assertFailure("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'", "could not mmap");
            } finally {
                ff.isFailing = false;
            }
            assertOnlyAttachableDir("tab", "2024-01-01");
            // the failed attach leaves the table as it was
            assertQuery("SELECT x, sym FROM tab")
                    .noLeakCheck().expectSize().inferRandomAccess().returns("x\tsym\n3\tC\n");
            if (isWriterRecreated) {
                engine.releaseAllWriters();
            }
            execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
            assertQuery("SELECT x, sym FROM tab")
                    .noLeakCheck().expectSize().inferRandomAccess().returns("x\tsym\n1\tA\n2\tB\n3\tC\n");
        });
    }

    /**
     * An adaptive WAL ATTACH links .attachable into the partition's directory and fails after the link. The file
     * system reports no entry types, so no writer open purges what the failure leaves. The retry used to find that
     * directory, skip the ATTACH silently and leave the writer's path inside it: the next new partition was created
     * in it, and its rows were lost at the next writer open.
     */
    private void assertAttachRetryAfterFailedLink(boolean isLeftoverStuck) throws Exception {
        // Only ADAPTIVE links the source; the other modes rename it
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final AtomicBoolean isEntryTypeUnknown = new AtomicBoolean();
        final AtomicBoolean isNullFlagReadFailing = new AtomicBoolean();
        final AtomicBoolean isLeftoverRemovalFailing = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int findType(long findPtr) {
                return isEntryTypeUnknown.get() ? Files.DT_UNKNOWN : super.findType(findPtr);
            }

            @Override
            public long openRO(LPSZ name) {
                // The ATTACH reads the symbol column of the linked partition for the column's null flag
                if (isNullFlagReadFailing.get() && isAttachDestination(name) && Utf8s.endsWithAscii(name, Files.SEPARATOR + "sym.d")) {
                    isNullFlagReadFailing.set(false);
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public boolean rmdir(Path name, boolean haltOnError) {
                if (isLeftoverRemovalFailing.get() && isAttachDestination(name)) {
                    return false;
                }
                return super.rmdir(name, haltOnError);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO tab VALUES
                        ('2024-01-01T00:00:00Z', 1, 'A'),
                        ('2024-01-01T01:00:00Z', 2, 'B'),
                        ('2024-01-02T00:00:00Z', 3, 'C')
                    """);
            drainWalQueue();
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            renameDetachedToAttachable("tab", "2024-01-01");
            final TableToken tableToken = engine.verifyTableName("tab");

            isEntryTypeUnknown.set(true);
            try {
                isLeftoverRemovalFailing.set(isLeftoverStuck);
                isNullFlagReadFailing.set(true);
                execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
                drainWalQueue();
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
                final String leftover = findAttachDestination(tableToken);

                if (isLeftoverStuck) {
                    // The retry stops on the leftover instead of skipping the ATTACH, and names the manual step
                    execute("ALTER TABLE tab RESUME WAL");
                    drainWalQueue();
                    Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
                    Assert.assertNotNull(leftover);
                    final String error = engine.getTableSequencerAPI().getTxnTracker(tableToken).getErrorMessage();
                    TestUtils.assertContains(error, ATTACH_ERR_DIR_EXISTS.name());
                    TestUtils.assertContains(error, tableToken.getDirName() + Files.SEPARATOR + leftover);
                    TestUtils.assertContains(error, "RESUME WAL");
                    isLeftoverRemovalFailing.set(false);
                    try (Path leftoverPath = new Path()) {
                        Assert.assertTrue(ff.rmdir(leftoverPath.of(configuration.getDbRoot()).concat(tableToken).concat(leftover)));
                    }
                }
                execute("ALTER TABLE tab RESUME WAL");
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));

                // A new partition, then a writer open: every row stays
                execute("INSERT INTO tab VALUES ('2024-01-03T00:00:00Z', 4, 'D')");
                drainWalQueue();
                engine.releaseAllWriters();
                execute("INSERT INTO tab VALUES ('2024-01-04T00:00:00Z', 5, 'E')");
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
                assertQuery("SELECT x, sym FROM tab")
                        .noLeakCheck().expectSize().inferRandomAccess().returns("x\tsym\n1\tA\n2\tB\n3\tC\n4\tD\n5\tE\n");
                if (!isLeftoverStuck) {
                    Assert.assertNull("the failed ATTACH left its directory", leftover);
                }
            } finally {
                isEntryTypeUnknown.set(false);
            }
        });
    }

    // The 2024-01-01 partition's directory that an ATTACH makes, named with the txn: not its source, not a detached copy
    private static @Nullable String findAttachDestination(TableToken tableToken) throws IOException {
        final java.nio.file.Path tablePath = java.nio.file.Path.of(configuration.getDbRoot().toString(), tableToken.getDirName());
        try (Stream<java.nio.file.Path> entries = java.nio.file.Files.list(tablePath)) {
            return entries.map(entry -> entry.getFileName().toString())
                    .filter(name -> name.startsWith("2024-01-01.") && !name.endsWith(configuration.getAttachPartitionSuffix()))
                    .findFirst()
                    .orElse(null);
        }
    }

    private static void assertDetachNamed(java.nio.file.Path tableDir, List<String> files) throws IOException {
        Assert.assertEquals(List.of("2024-01-01" + DETACHED_DIR_MARKER), listDetachNames(tableDir));
        Assert.assertEquals(files, listFileNames(tableDir.resolve("2024-01-01" + DETACHED_DIR_MARKER)));
    }

    // The single temporary DETACH name of 2024-01-01, which must hold the partition's files
    private static String assertTemporaryName(java.nio.file.Path tableDir) throws IOException {
        final List<String> temporaryNames = listDetachNames(tableDir).stream()
                .filter(name -> name.endsWith(DETACHING_DIR_MARKER))
                .collect(Collectors.toList());
        Assert.assertEquals(temporaryNames.toString(), 1, temporaryNames.size());
        Assert.assertFalse(listFileNames(tableDir.resolve(temporaryNames.getFirst())).isEmpty());
        return temporaryNames.getFirst();
    }

    private static List<String> listDetachNames(java.nio.file.Path tableDir) throws IOException {
        return listFileNames(tableDir).stream()
                .filter(name -> name.startsWith("2024-01-01") && (name.endsWith(DETACHED_DIR_MARKER) || name.endsWith(DETACHING_DIR_MARKER)))
                .collect(Collectors.toList());
    }

    private static List<String> listFileNames(java.nio.file.Path dir) throws IOException {
        try (Stream<java.nio.file.Path> entries = java.nio.file.Files.list(dir)) {
            return entries.map(entry -> entry.getFileName().toString()).sorted().collect(Collectors.toList());
        }
    }

    private static boolean isAttachDestination(Utf8Sequence path) {
        return Utf8s.containsAscii(path, "2024-01-01.")
                && !Utf8s.containsAscii(path, configuration.getAttachPartitionSuffix())
                && !Utf8s.containsAscii(path, DETACHED_DIR_MARKER);
    }

    private static void assertOnlyAttachableDir(String tableName, String partitionName) throws IOException {
        final java.nio.file.Path tablePath = java.nio.file.Path.of(configuration.getDbRoot().toString(), engine.verifyTableName(tableName).getDirName());
        try (Stream<java.nio.file.Path> entries = java.nio.file.Files.list(tablePath)) {
            final List<String> partitionDirs = entries
                    .map(entry -> entry.getFileName().toString())
                    .filter(name -> name.startsWith(partitionName))
                    .collect(Collectors.toList());
            Assert.assertEquals(List.of(partitionName + configuration.getAttachPartitionSuffix()), partitionDirs);
        }
    }

    private void assertAttachParquetNullRecoveryKeepsNonNullSiblingFalse(boolean hasStatistics) throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_STATISTICS_ENABLED, Boolean.toString(hasStatistics));
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", dropped INT, x INT, s SYMBOL, t SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO tab VALUES
                        ('2024-01-01T00:00:00Z', 0, 1, 'A', 'B'),
                        ('2024-01-01T01:00:00Z', 0, 2, 'A', 'B'),
                        ('2024-01-01T02:00:00Z', 0, 3, 'A', 'B'),
                        ('2024-01-01T03:00:00Z', 0, 4, 'A', 'B'),
                        ('2024-01-01T04:00:00Z', 0, 5, 'A', NULL),
                        ('2024-01-01T05:00:00Z', 0, 6, 'A', NULL),
                        ('2024-01-02T00:00:00Z', 0, 7, 'A', 'B')
                    """);
            execute("ALTER TABLE tab DROP COLUMN dropped");
            execute("ALTER TABLE tab CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            execute("ALTER TABLE tab RENAME COLUMN s TO sym");
            execute("ALTER TABLE tab RENAME COLUMN t TO tail");
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            assertDetachedSymbolStatistics(hasStatistics, new int[]{4, 2}, 3, 4);
            execute("TRUNCATE TABLE tab");
            execute("INSERT INTO tab VALUES ('2024-01-02T00:00:00Z', 8, 'A', 'B')");
            Assert.assertFalse(containsSymbolNullValue("tab", "sym"));
            Assert.assertFalse(containsSymbolNullValue("tab", "tail"));
            renameDetachedToAttachable("tab", "2024-01-01");
            execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
            engine.clear();
            assertQuery("SELECT x, sym, tail FROM tab")
                    .noLeakCheck().expectSize().inferRandomAccess().returns("x\tsym\ttail\n1\tA\tB\n2\tA\tB\n3\tA\tB\n4\tA\tB\n5\tA\t\n6\tA\t\n8\tA\tB\n");
            Assert.assertFalse(containsSymbolNullValue("tab", "sym"));
            assertQuery("SELECT x, sym FROM tab LATEST ON ts PARTITION BY sym")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\tsym\n8\tA\n");
            assertQuery("SELECT x, tail FROM tab LATEST ON ts PARTITION BY tail")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ttail\n6\t\n8\tB\n");
            Assert.assertTrue(containsSymbolNullValue("tab", "tail"));
        });
    }

    private void assertAttachParquetWithoutNulls(boolean hasStatistics) throws Exception {
        final ParquetDataCountingFilesFacade ff = new ParquetDataCountingFilesFacade();
        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_STATISTICS_ENABLED, Boolean.toString(hasStatistics));
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO tab VALUES
                        ('2024-01-01T00:00:00Z', 1, 'A'),
                        ('2024-01-01T01:00:00Z', 2, 'B'),
                        ('2024-01-01T02:00:00Z', 3, 'A'),
                        ('2024-01-01T03:00:00Z', 4, 'B'),
                        ('2024-01-02T00:00:00Z', 5, 'C')
                    """);
            execute("ALTER TABLE tab CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            assertDetachedSymbolStatistics(hasStatistics, new int[]{4}, 2);
            execute("TRUNCATE TABLE tab KEEP SYMBOL MAPS");
            renameDetachedToAttachable("tab", "2024-01-01");
            ff.isCounting = true;
            try {
                execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
            } finally {
                ff.isCounting = false;
            }
            if (hasStatistics) {
                Assert.assertEquals(0, ff.dataOpenCount);
                Assert.assertEquals(0, ff.dataMapCount);
            }
            engine.clear();
            Assert.assertFalse(containsSymbolNullValue("tab", "s"));
            assertQuery("SELECT x, s FROM tab")
                    .noLeakCheck().expectSize().inferRandomAccess().returns("x\ts\n1\tA\n2\tB\n3\tA\n4\tB\n");
            assertQuery("SELECT x, s FROM tab LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n3\tA\n4\tB\n");
        });
    }

    private void assertDetachedSymbolStatistics(boolean hasStatistics, int[] rowGroupSizes, int... symbolIds) {
        final FilesFacade ff = configuration.getFilesFacade();
        final ParquetMetaFileReader reader = new ParquetMetaFileReader();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(engine.verifyTableName("tab"))
                    .concat("2024-01-01").put(DETACHED_DIR_MARKER);
            final int partitionPathLen = path.size();
            final long dataSize = ff.length(path.concat(PARQUET_PARTITION_NAME).$());
            path.trimTo(partitionPathLen).concat(PARQUET_METADATA_FILE_NAME);
            final long addr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), reader);
            Assert.assertNotEquals(0, addr);
            final long size = reader.getFileSize();
            try {
                Assert.assertTrue(reader.resolveFooter(dataSize));
                Assert.assertEquals(rowGroupSizes.length, reader.getRowGroupCount());
                for (int symbolId : symbolIds) {
                    final int columnIndex = reader.getColumnIndexById(symbolId);
                    Assert.assertTrue(columnIndex >= 0);
                    for (int rg = 0; rg < rowGroupSizes.length; rg++) {
                        Assert.assertEquals(rowGroupSizes[rg], reader.getRowGroupSize(rg));
                        Assert.assertEquals(hasStatistics, (reader.getChunkStatFlags(rg, columnIndex) & 0x80) != 0);
                    }
                }
            } finally {
                reader.clear();
                ff.munmap(addr, size, MemoryTag.MMAP_PARQUET_METADATA_READER);
            }
        }
    }

    private void assertAttachPartitionAfterTruncateRestoresSymbolNullFlag(boolean isParquet, boolean hasParquetMetadata, boolean hasStatistics) throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_STATISTICS_ENABLED, Boolean.toString(hasStatistics));
            execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO tab VALUES
                        ('2024-01-01T00:00:00Z', 1, NULL),
                        ('2024-01-01T01:00:00Z', 2, NULL),
                        ('2024-01-02T00:00:00Z', 3, 'A')
                    """);
            if (isParquet) {
                execute("ALTER TABLE tab CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            }
            execute("ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            execute("TRUNCATE TABLE tab");
            Assert.assertFalse(containsSymbolNullValue("tab", "s"));
            if (!hasParquetMetadata) {
                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(engine.verifyTableName("tab"))
                            .concat("2024-01-01").put(DETACHED_DIR_MARKER).concat(PARQUET_METADATA_FILE_NAME);
                    Assert.assertTrue(TestUtils.remove(path.$()));
                }
            }
            renameDetachedToAttachable("tab", "2024-01-01");
            execute("ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");
            engine.clear();
            try (TableReader reader = getReader("tab")) {
                Assert.assertEquals(isParquet, reader.getTxFile().isPartitionParquet(0));
            }
            assertQuery("SELECT x, s FROM tab")
                    .noLeakCheck().expectSize().inferRandomAccess().returns("x\ts\n1\t\n2\t\n");
            assertQuery("SELECT x, s FROM tab LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary()
                    .withPlanContaining("LatestByDeferredListValuesFiltered")
                    .returns("x\ts\n2\t\n");
            Assert.assertTrue(containsSymbolNullValue("tab", "s"));
        });
    }

    // the column name txn a file name ends with, -1 for none
    private static long nameTxnOf(String fileName) {
        try {
            return Long.parseLong(fileName.substring(fileName.lastIndexOf('.') + 1));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * UPDATEs supersede the versions of v while a query keeps them in use, so the partition directory holds all of them
     * until the column purge job runs, and DETACH links them all. The directory lists them newest first, so that the
     * version listed last is the oldest. The attach takes the version from the _cv the partition was detached with,
     * even when a newer file is there. Without that file, and in an attach of a partition whose size the caller
     * knows, as the parallel CSV import stages, it takes the newest version the file names show.
     */
    private void assertAttachPartitionReadsCurrentVersionOfUpdatedColumn(boolean isWal, int attachMode) throws Exception {
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public void iterateDir(LPSZ path, FindVisitor func) {
                if (!Utf8s.containsAscii(path, "2024-01-03")) {
                    super.iterateDir(path, func);
                    return;
                }
                final List<String> names = new ArrayList<>();
                final Map<String, Integer> types = new HashMap<>();
                final DirectUtf8StringZ listed = new DirectUtf8StringZ();
                super.iterateDir(path, (pUtf8NameZ, type) -> {
                    final String name = Utf8s.toString(listed.of(pUtf8NameZ));
                    names.add(name);
                    types.put(name, type);
                });
                // the highest name txn first, so that the oldest version comes last
                names.sort(Comparator.comparingLong(AlterTableDetachPartitionTest::nameTxnOf).reversed());
                try (Path name = new Path()) {
                    for (int i = 0, n = names.size(); i < n; i++) {
                        func.onFind(name.of(names.get(i)).$().ptr(), types.get(names.get(i)));
                    }
                }
            }
        };
        assertMemoryLeak(ff, () -> {
            ddl(isWal, "CREATE TABLE tab (v LONG, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : " BYPASS WAL"));
            ddl(isWal, "INSERT INTO tab VALUES (1, '2024-01-03T01:00:00.000000Z'), (4, '2024-01-04T01:00:00.000000Z')");
            try (
                    RecordCursorFactory factory = select("SELECT * FROM tab");
                    RecordCursor ignored = factory.getCursor(sqlExecutionContext)
            ) {
                for (int i = 0; i < 6; i++) {
                    ddl(isWal, "UPDATE tab SET v = v + 1 WHERE ts < '2024-01-04'");
                }
            }
            final String expected = replaceTimestampSuffix("""
                    v\tts
                    7\t2024-01-03T01:00:00.000000Z
                    4\t2024-01-04T01:00:00.000000Z
                    """);
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(expected);

            ddl(isWal, "ALTER TABLE tab DETACH PARTITION LIST '2024-01-03'");
            final java.nio.file.Path detached = java.nio.file.Path.of(configuration.getDbRoot(), engine.verifyTableName("tab").getDirName())
                    .resolve("2024-01-03" + DETACHED_DIR_MARKER);
            // the superseded versions went with the partition
            try (Stream<java.nio.file.Path> files = java.nio.file.Files.list(detached)) {
                Assert.assertEquals(7, files.filter(file -> file.getFileName().toString().startsWith("v.d")).count());
            }
            switch (attachMode) {
                case ATTACH_WITH_COLUMN_VERSION_FILE ->
                    // a version newer than the one _cv names, as a write that failed can leave behind: _cv decides
                        java.nio.file.Files.copy(detached.resolve("v.d"), detached.resolve("v.d.99"));
                case ATTACH_WITHOUT_COLUMN_VERSION_FILE -> {
                    // as a partition detached by a version that wrote no _meta or _cv
                    java.nio.file.Files.delete(detached.resolve(TableUtils.META_FILE_NAME));
                    java.nio.file.Files.delete(detached.resolve(TableUtils.COLUMN_VERSION_FILE_NAME));
                }
                default -> Assert.assertEquals(ATTACH_WITH_KNOWN_SIZE, attachMode);
            }
            renameDetachedToAttachable("tab", "2024-01-03");
            if (attachMode == ATTACH_WITH_KNOWN_SIZE) {
                try (TableWriter writer = getWriter("tab")) {
                    Assert.assertEquals(AttachDetachStatus.OK, writer.attachPartition(timestampType.getDriver().parseFloorLiteral("2024-01-03"), 1));
                }
            } else {
                ddl(isWal, "ALTER TABLE tab ATTACH PARTITION LIST '2024-01-03'");
            }
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(expected);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(expected);

            // the next UPDATE builds on the current version
            ddl(isWal, "UPDATE tab SET v = v + 1 WHERE ts < '2024-01-04'");
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(replaceTimestampSuffix("""
                    v\tts
                    8\t2024-01-03T01:00:00.000000Z
                    4\t2024-01-04T01:00:00.000000Z
                    """));
        });
    }

    /**
     * ALTER COLUMN TYPE, and DROP COLUMN followed by ADD COLUMN of the same name, leave the old column deleted under its
     * name and give the name to a live column at another index. A partition detached after that matches the table,
     * and must attach back.
     */
    private void assertAttachPartitionDetachedAfterColumnReplaced(boolean isWal, boolean isParquet, boolean isTypeChange) throws Exception {
        assertMemoryLeak(() -> {
            ddl(isWal, "CREATE TABLE tab (ts " + timestampType.getTypeName() + ", x INT, y INT) TIMESTAMP(ts) PARTITION BY DAY"
                    + (isWal ? " WAL" : " BYPASS WAL"));
            ddl(isWal, "INSERT INTO tab VALUES ('2024-01-01T00:00:00.000000Z', 1, 10), ('2024-01-02T00:00:00.000000Z', 3, 30)");
            if (isParquet) {
                ddl(isWal, "ALTER TABLE tab CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            }
            if (isTypeChange) {
                ddl(isWal, "ALTER TABLE tab ALTER COLUMN x TYPE LONG");
            } else {
                ddl(isWal, "ALTER TABLE tab DROP COLUMN x");
                ddl(isWal, "ALTER TABLE tab ADD COLUMN x LONG");
            }
            ddl(isWal, "ALTER TABLE tab DETACH PARTITION LIST '2024-01-01'");
            renameDetachedToAttachable("tab", "2024-01-01");
            ddl(isWal, "ALTER TABLE tab ATTACH PARTITION LIST '2024-01-01'");

            final String query = "SELECT ts, x, y FROM tab";
            final String attached = replaceTimestampSuffix(isTypeChange ? """
                    ts\tx\ty
                    2024-01-01T00:00:00.000000Z\t1\t10
                    2024-01-02T00:00:00.000000Z\t3\t30
                    """ : """
                    ts\tx\ty
                    2024-01-01T00:00:00.000000Z\tnull\t10
                    2024-01-02T00:00:00.000000Z\tnull\t30
                    """);
            assertQuery(query).noLeakCheck().timestamp("ts").expectSize().returns(attached);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery(query).noLeakCheck().timestamp("ts").expectSize().returns(attached);

            // an out-of-order write merges into the attached partition
            ddl(isWal, "INSERT INTO tab (ts, x, y) VALUES ('2024-01-01T01:00:00.000000Z', 2, 20)");
            final String written = replaceTimestampSuffix(isTypeChange ? """
                    ts\tx\ty
                    2024-01-01T00:00:00.000000Z\t1\t10
                    2024-01-01T01:00:00.000000Z\t2\t20
                    2024-01-02T00:00:00.000000Z\t3\t30
                    """ : """
                    ts\tx\ty
                    2024-01-01T00:00:00.000000Z\tnull\t10
                    2024-01-01T01:00:00.000000Z\t2\t20
                    2024-01-02T00:00:00.000000Z\tnull\t30
                    """);
            assertQuery(query).noLeakCheck().timestamp("ts").expectSize().returns(written);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery(query).noLeakCheck().timestamp("ts").expectSize().returns(written);
        });
    }

    private void assertAttachPartitionDetachedBeforeTruncateReadsAddedColumnsAsNull(boolean isWal, boolean isParquet) throws Exception {
        assertAttachPartitionReadsAddedColumnsAsNull(isWal, isParquet, "TRUNCATE TABLE tab");
    }

    /**
     * @param ddlAfterDetach null, or the statement that runs between the DETACH and the ATTACH: TRUNCATE TABLE or a
     *                       DROP of the newest partition, 2024-01-04
     */
    private void assertAttachPartitionReadsAddedColumnsAsNull(boolean isWal, boolean isParquet, String ddlAfterDetach) throws Exception {
        // _cv tells that a column added after a partition was written is absent from it by the partition the column
        // was added at. TRUNCATE and a DROP of the newest partition move that "added at" partition back, so the
        // attach must take the column's absence from the _cv the partition was detached with.
        assertMemoryLeak(() -> {
            ddl(isWal, "CREATE TABLE tab (price DOUBLE, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY"
                    + (isWal ? " WAL" : " BYPASS WAL"));
            ddl(isWal, """
                    INSERT INTO tab VALUES
                        (1.0, '2024-01-01T01:00:00.000000Z'),
                        (2.0, '2024-01-02T01:00:00.000000Z'),
                        (3.0, '2024-01-03T01:00:00.000000Z'),
                        (3.5, '2024-01-03T02:00:00.000000Z'),
                        (4.0, '2024-01-04T01:00:00.000000Z')
                    """);
            // one at a time: a WAL table does not take several columns in one ALTER
            ddl(isWal, "ALTER TABLE tab ADD COLUMN qty LONG");
            ddl(isWal, "ALTER TABLE tab ADD COLUMN sym SYMBOL");
            ddl(isWal, "ALTER TABLE tab ADD COLUMN note VARCHAR");
            ddl(isWal, "INSERT INTO tab VALUES (5.0, '2024-01-04T02:00:00.000000Z', 50, 'e', 'n5')");
            if (isParquet) {
                ddl(isWal, "ALTER TABLE tab CONVERT PARTITION TO PARQUET LIST '2024-01-03'");
            }
            ddl(isWal, "ALTER TABLE tab DETACH PARTITION LIST '2024-01-03'");
            final boolean isTruncated = ddlAfterDetach != null && ddlAfterDetach.startsWith("TRUNCATE");
            final boolean isNewestDropped = ddlAfterDetach != null && !isTruncated;
            if (ddlAfterDetach != null) {
                ddl(isWal, ddlAfterDetach);
            }
            renameDetachedToAttachable("tab", "2024-01-03");
            ddl(isWal, "ALTER TABLE tab ATTACH PARTITION LIST '2024-01-03'");

            final String header = "price\tts\tqty\tsym\tnote\n";
            final String before = isTruncated ? "" : """
                    1.0\t2024-01-01T01:00:00.000000Z\tnull\t\t
                    2.0\t2024-01-02T01:00:00.000000Z\tnull\t\t
                    """;
            final String after = isTruncated || isNewestDropped ? "" : """
                    4.0\t2024-01-04T01:00:00.000000Z\tnull\t\t
                    5.0\t2024-01-04T02:00:00.000000Z\t50\te\tn5
                    """;
            final String query = "SELECT price, ts, qty, sym, note FROM tab";
            final String attached = replaceTimestampSuffix(header + before + """
                    3.0\t2024-01-03T01:00:00.000000Z\tnull\t\t
                    3.5\t2024-01-03T02:00:00.000000Z\tnull\t\t
                    """ + after);
            assertQuery(query).noLeakCheck().timestamp("ts").expectSize().returns(attached);
            // the symbol column holds only NULLs in the attached partition
            Assert.assertTrue(containsSymbolNullValue("tab", "sym"));
            assertQuery("SELECT price FROM tab WHERE sym IS NULL").noLeakCheck()
                    .returns(isTruncated ? "price\n3.0\n3.5\n" : isNewestDropped ? "price\n1.0\n2.0\n3.0\n3.5\n" : "price\n1.0\n2.0\n3.0\n3.5\n4.0\n");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery(query).noLeakCheck().timestamp("ts").expectSize().returns(attached);

            // The writer keeps the columns absent in the rows the partition already has. The newer partition goes
            // first, so that the write into the attached one is an out-of-order merge for Parquet too.
            ddl(isWal, "INSERT INTO tab VALUES (6.0, '2024-01-05T01:00:00.000000Z', 60, 'f', 'n6')");
            ddl(isWal, "INSERT INTO tab VALUES (3.7, '2024-01-03T03:00:00.000000Z', 37, 'c', 'n3')");
            final String written = replaceTimestampSuffix(header + before + """
                    3.0\t2024-01-03T01:00:00.000000Z\tnull\t\t
                    3.5\t2024-01-03T02:00:00.000000Z\tnull\t\t
                    3.7\t2024-01-03T03:00:00.000000Z\t37\tc\tn3
                    """ + after + """
                    6.0\t2024-01-05T01:00:00.000000Z\t60\tf\tn6
                    """);
            assertQuery(query).noLeakCheck().timestamp("ts").expectSize().returns(written);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery(query).noLeakCheck().timestamp("ts").expectSize().returns(written);
            if (isParquet) {
                ddl(isWal, "ALTER TABLE tab CONVERT PARTITION TO NATIVE LIST '2024-01-03'");
                assertQuery(query).noLeakCheck().timestamp("ts").expectSize().returns(written);
            }
        });
    }

    /**
     * RENAME COLUMN moves the file version of a column on and links the files of the attached partitions only, so a
     * partition detached meanwhile keeps the files of the version its detached _cv names. A column renamed away and
     * back has its name again, and the attach must read the version the partition holds.
     *
     * @param ddlAfterDetach null, or the statement that runs between the DETACH and the renames: TRUNCATE TABLE or a
     *                       DROP of the newest partition, 2024-01-03, either of which makes 2024-01-02 the newest
     */
    private void assertAttachPartitionAfterColumnsRenamedAwayAndBack(boolean isWal, boolean isParquet, String ddlAfterDetach) throws Exception {
        assertMemoryLeak(() -> {
            createTableWithColumnsToRename("tab", isWal);
            if (isParquet) {
                ddl(isWal, "ALTER TABLE tab CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
            }
            ddl(isWal, "ALTER TABLE tab DETACH PARTITION LIST '2024-01-02'");
            final boolean isTruncated = ddlAfterDetach != null && ddlAfterDetach.startsWith("TRUNCATE");
            final boolean isNewestDropped = ddlAfterDetach != null && !isTruncated;
            if (ddlAfterDetach != null) {
                ddl(isWal, ddlAfterDetach);
            }
            renameColumnsAwayAndBackAndDropIndex("tab", isWal);

            final String header = "ts\tkeep\tc\tz\ti\tu\tst\tvc\tarr\ts\tsb\tsp\tsd\tlate\ttopped\tabsent\n";
            final String older = isTruncated ? "" : "2024-01-01T00:00:00.000000Z\t1\t10\tA\tnull\t\t\t\tnull\t\t\t\t\tnull\tnull\tnull\n";
            final String newer = isTruncated || isNewestDropped ? "" : """
                    2024-01-03T00:00:00.000000Z\t5\t50\tC\t50\t11111111-1111-1111-1111-111111111111\tstring 50\tvarchar value 50\t[5.0,50.0]\tC\tC\tC\tC\tnull\tnull\tnull
                    2024-01-03T01:00:00.000000Z\t7\tnull\t\tnull\t\t\t\tnull\t\t\t\t\t70\tnull\tnull
                    """;
            final String attached = replaceTimestampSuffix(header + older + rowsOfRenamedPartition(0, false) + newer);
            try (RecordCursorFactory factory = select("SELECT * FROM tab")) {
                renameDetachedToAttachable("tab", "2024-01-02");
                ddl(isWal, "ALTER TABLE tab ATTACH PARTITION LIST '2024-01-02'");
                // a query compiled before the attach reads the attached partition
                new QueryAssertion(engine, factory).withContext(sqlExecutionContext).timestamp("ts").expectSize().returns(attached);
            }
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(attached);
            assertQuery("SELECT keep FROM tab WHERE s IS NULL").noLeakCheck()
                    .returns(isTruncated ? "keep\n4\n" : isNewestDropped ? "keep\n1\n4\n" : "keep\n1\n4\n7\n");
            assertQuery("SELECT keep FROM tab WHERE z IS NULL").noLeakCheck()
                    .returns(isTruncated || isNewestDropped ? "keep\n4\n" : "keep\n4\n7\n");
            assertQuery("SELECT keep FROM tab WHERE i = 20").noLeakCheck().returns("keep\n2\n");
            assertIndexedSymbolsRead("B", "keep\n3\n");
            assertQuery("SELECT keep, sb FROM tab LATEST ON ts PARTITION BY sb")
                    .noLeakCheck().inferRandomAccess().sizeMayVary()
                    .withPlanContaining("LatestByAllIndexed")
                    .returns(isTruncated || isNewestDropped ? "keep\tsb\n3\tB\n4\t\n6\tA\n" : "keep\tsb\n3\tB\n6\tA\n5\tC\n7\t\n");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(attached);

            insertIntoAttachedPartition("tab", isWal);
            final String written = replaceTimestampSuffix(header + older + rowsOfRenamedPartition(0, true) + newer);
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(written);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(written);
            assertIndexedSymbolsRead("D", "keep\n8\n");

            final String updated;
            if (isParquet) {
                ddl(isWal, "ALTER TABLE tab CONVERT PARTITION TO NATIVE LIST '2024-01-02'");
                updated = written;
            } else {
                ddl(isWal, "UPDATE tab SET i = i + 1 WHERE ts IN '2024-01-02'");
                updated = replaceTimestampSuffix(header + older + rowsOfRenamedPartition(1, true) + newer);
            }
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(updated);
            engine.releaseAllReaders();
            runPartitionPurgeJobs();
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(updated);

            // The partition leaves again with the versions the attach recorded for it, and takes them back. The row on
            // 2024-01-05 keeps 2024-01-02 from being the newest partition, which DETACH refuses.
            ddl(isWal, "INSERT INTO tab (ts, keep) VALUES ('2024-01-05T00:00:00.000000Z', 9)");
            ddl(isWal, "ALTER TABLE tab DETACH PARTITION LIST '2024-01-02'");
            renameColumnsAwayAndBack("tab", isWal, "i", "sb");
            renameDetachedToAttachable("tab", "2024-01-02");
            ddl(isWal, "ALTER TABLE tab ATTACH PARTITION LIST '2024-01-02'");
            final String reattached = replaceTimestampSuffix(header + older + rowsOfRenamedPartition(isParquet ? 0 : 1, true) + newer
                    + "2024-01-05T00:00:00.000000Z\t9\tnull\t\tnull\t\t\t\tnull\t\t\t\t\tnull\tnull\tnull\n");
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(reattached);
            assertIndexedSymbolsRead("B", "keep\n3\n");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT * FROM tab").noLeakCheck().timestamp("ts").expectSize().returns(reattached);
        });
    }

    // the indexes of z, sb and sp find the rows of expected for the symbol value
    private void assertIndexedSymbolsRead(String value, String expected) throws Exception {
        for (String symbol : new String[]{"z", "sb", "sp"}) {
            assertQuery("SELECT keep FROM tab WHERE " + symbol + " = '" + value + "'")
                    .noLeakCheck().inferRandomAccess()
                    .withPlanContaining("Index forward scan on: " + symbol)
                    .returns(expected);
        }
    }

    /**
     * Creates a table whose 2024-01-02 holds a column of every kind the attach resolves the file version of: z, the
     * first column added to the empty table, with column name txn 0; i to sd, added after the first row, with name
     * txns above 0; late and topped with an explicit column top; absent not at all, and with no record of it.
     */
    private void createTableWithColumnsToRename(String tableName, boolean isWal) throws Exception {
        ddl(isWal, "CREATE TABLE " + tableName + " (ts " + timestampType.getTypeName() + ", keep INT, c INT) TIMESTAMP(ts) PARTITION BY DAY"
                + (isWal ? " WAL" : " BYPASS WAL"));
        ddl(isWal, "ALTER TABLE " + tableName + " ADD COLUMN z SYMBOL INDEX TYPE BITMAP");
        Assert.assertEquals(0, defaultColumnNameTxn(tableName, "z"));
        ddl(isWal, "INSERT INTO " + tableName + " (ts, keep, c, z) VALUES ('2024-01-01T00:00:00.000000Z', 1, 10, 'A')");
        // one at a time: a WAL table does not take several columns in one ALTER
        for (String column : new String[]{
                "i INT", "u UUID", "st STRING", "vc VARCHAR", "arr DOUBLE[]", "s SYMBOL",
                "sb SYMBOL INDEX TYPE BITMAP", "sp SYMBOL INDEX TYPE POSTING", "sd SYMBOL INDEX TYPE BITMAP"
        }) {
            ddl(isWal, "ALTER TABLE " + tableName + " ADD COLUMN " + column);
        }
        // u is NULL in every row of 2024-01-02
        ddl(isWal, """
                INSERT INTO %s VALUES
                    ('2024-01-02T00:00:00.000000Z', 2, 20, 'A', 20, NULL, 'string 20', 'varchar value 20', ARRAY[2.0, 20.0], 'A', 'A', 'A', 'A'),
                    ('2024-01-02T01:00:00.000000Z', 3, 30, 'B', 30, NULL, 'string 30', 'varchar value 30', ARRAY[3.0, 30.0], 'B', 'B', 'B', 'B'),
                    ('2024-01-02T02:00:00.000000Z', 4, 40, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                    ('2024-01-03T00:00:00.000000Z', 5, 50, 'C', 50, '11111111-1111-1111-1111-111111111111', 'string 50', 'varchar value 50', ARRAY[5.0, 50.0], 'C', 'C', 'C', 'C')
                """.formatted(tableName));
        // added after 2024-01-02 was written
        ddl(isWal, "ALTER TABLE " + tableName + " ADD COLUMN late INT");
        ddl(isWal, "INSERT INTO " + tableName + " (ts, keep, late) VALUES ('2024-01-03T01:00:00.000000Z', 7, 70)");
        ddl(isWal, "ALTER TABLE " + tableName + " ADD COLUMN topped INT");
        // an out-of-order row, which gives 2024-01-02 explicit column tops of late and topped
        ddl(isWal, "INSERT INTO " + tableName + " VALUES ('2024-01-02T03:00:00.000000Z', 6, 60, 'A', 60, NULL, 'string 60', 'varchar value 60',"
                + " ARRAY[6.0, 60.0], 'A', 'A', 'A', 'A', 61, 62)");
        // added after that row
        ddl(isWal, "ALTER TABLE " + tableName + " ADD COLUMN absent INT");
    }

    // the column name txn of the column's default _cv record
    private static long defaultColumnNameTxn(String tableName, String columnName) {
        try (TableReader reader = getReader(tableName)) {
            final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(columnName));
            return reader.getColumnVersionReader().getDefaultColumnNameTxn(writerIndex);
        }
    }

    // A row into 2024-01-02: out of order while a newer partition exists, an append when 2024-01-02 is the newest
    private void insertIntoAttachedPartition(String tableName, boolean isWal) throws Exception {
        ddl(isWal, "INSERT INTO " + tableName + " VALUES ('2024-01-02T04:00:00.000000Z', 8, 80, 'D', 80, '22222222-2222-2222-2222-222222222222',"
                + " 'string 80', 'varchar value 80', ARRAY[8.0, 80.0], 'D', 'D', 'D', 'D', 81, 82, 83)");
    }

    private void renameColumnsAwayAndBack(String tableName, boolean isWal, String... columns) throws Exception {
        for (String column : columns) {
            ddl(isWal, "ALTER TABLE " + tableName + " RENAME COLUMN " + column + " TO " + column + "_away");
            ddl(isWal, "ALTER TABLE " + tableName + " RENAME COLUMN " + column + "_away TO " + column);
        }
    }

    // The statements that run while 2024-01-02 of a table from createTableWithColumnsToRename() is detached
    private void renameColumnsAwayAndBackAndDropIndex(String tableName, boolean isWal) throws Exception {
        // every column the table gained by ADD COLUMN; keep and c, from CREATE TABLE, hold files without a column
        // name txn, and keep their names
        renameColumnsAwayAndBack(tableName, isWal, "z", "i", "u", "st", "vc", "arr", "s", "sb", "sp", "sd", "late", "topped", "absent");
        // the attach removes the index files of sd from the partition; no column gains an index here
        ddl(isWal, "ALTER TABLE " + tableName + " ALTER COLUMN sd DROP INDEX");
    }

    // The rows of 2024-01-02 that createTableWithColumnsToRename() writes, and insertIntoAttachedPartition() when
    // isWritten, with i raised by iRaise
    private static String rowsOfRenamedPartition(int iRaise, boolean isWritten) {
        return "2024-01-02T00:00:00.000000Z\t2\t20\tA\t" + (20 + iRaise) + "\t\tstring 20\tvarchar value 20\t[2.0,20.0]\tA\tA\tA\tA\tnull\tnull\tnull\n"
                + "2024-01-02T01:00:00.000000Z\t3\t30\tB\t" + (30 + iRaise) + "\t\tstring 30\tvarchar value 30\t[3.0,30.0]\tB\tB\tB\tB\tnull\tnull\tnull\n"
                + "2024-01-02T02:00:00.000000Z\t4\t40\t\tnull\t\t\t\tnull\t\t\t\t\tnull\tnull\tnull\n"
                + "2024-01-02T03:00:00.000000Z\t6\t60\tA\t" + (60 + iRaise) + "\t\tstring 60\tvarchar value 60\t[6.0,60.0]\tA\tA\tA\tA\t61\t62\tnull\n"
                + (isWritten
                ? "2024-01-02T04:00:00.000000Z\t8\t80\tD\t" + (80 + iRaise) + "\t22222222-2222-2222-2222-222222222222\tstring 80\tvarchar value 80\t[8.0,80.0]\tD\tD\tD\tD\t81\t82\t83\n"
                : "");
    }

    // Fails the step that names a committed DETACH .detached, whether it renames the temporary name or links it
    private static class DetachedNameFailingFacade extends TestFilesFacadeImpl {
        // What errno() reports after the fault, as the operating system would
        private final int failErrno;
        private volatile boolean hasFailed;
        private volatile boolean isFailing;
        // A user's copy takes the .detached name while the DETACH runs, after it found the name free
        private volatile boolean isUserCopyArmed;

        private DetachedNameFailingFacade(int failErrno) {
            this.failErrno = failErrno;
        }

        @Override
        public int copy(LPSZ from, LPSZ to) {
            // The last copy into the temporary name
            if (isUserCopyArmed && Utf8s.containsAscii(to, DETACHING_DIR_MARKER) && Utf8s.endsWithAscii(to, TXN_FILE_NAME)) {
                isUserCopyArmed = false;
                final java.nio.file.Path userCopy = java.nio.file.Path.of(to.asAsciiCharSequence().toString()).getParent()
                        .resolveSibling("2024-01-01" + DETACHED_DIR_MARKER);
                try {
                    java.nio.file.Files.createDirectories(userCopy);
                    java.nio.file.Files.writeString(userCopy.resolve("user-file"), "kept");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return super.copy(from, to);
        }

        @Override
        public int errno() {
            return hasFailed ? failErrno : super.errno();
        }

        @Override
        public int hardLink(LPSZ src, LPSZ hardLink) {
            if (isFailing && Utf8s.containsAscii(hardLink, DETACHED_DIR_MARKER + Files.SEPARATOR)) {
                hasFailed = true;
                return -1;
            }
            return super.hardLink(src, hardLink);
        }

        @Override
        public int mkdir(LPSZ path, int mode) {
            if (isFailing && Utf8s.endsWithAscii(path, DETACHED_DIR_MARKER)) {
                hasFailed = true;
                return -1;
            }
            return super.mkdir(path, mode);
        }

        @Override
        public int rename(LPSZ from, LPSZ to) {
            if (isFailing && Utf8s.containsAscii(from, DETACHING_DIR_MARKER) && Utf8s.endsWithAscii(to, DETACHED_DIR_MARKER)) {
                hasFailed = true;
                return Files.FILES_RENAME_ERR_OTHER;
            }
            return super.rename(from, to);
        }

        private void disarm() {
            isFailing = false;
            hasFailed = false;
            isUserCopyArmed = false;
        }
    }

    private static class SymbolDataMapFailingFilesFacade extends TestFilesFacadeImpl {
        private long dataFd = -1;
        // the data file of sym whose mapping fails
        private String dataFileName = "sym.d";
        private boolean isFailing;

        @Override
        public boolean close(long fd) {
            if (fd == dataFd) {
                dataFd = -1;
            }
            return super.close(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            if (fd == dataFd) {
                return MAP_FAILED;
            }
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long openRO(LPSZ name) {
            final long fd = super.openRO(name);
            if (isFailing && Utf8s.endsWithAscii(name, dataFileName)) {
                dataFd = fd;
            }
            return fd;
        }
    }

    private static class SymbolIndexOpenFailingFilesFacade extends TestFilesFacadeImpl {
        private boolean isFailing;

        @Override
        public long openRO(LPSZ name) {
            if (isFailing && (Utf8s.endsWithAscii(name, "sym.k") || Utf8s.endsWithAscii(name, "sym.pk"))) {
                return -1;
            }
            return super.openRO(name);
        }
    }

    private static class SymbolDataOpenCountingFilesFacade extends TestFilesFacadeImpl {
        private int dataOpenCount;
        private boolean isCounting;

        @Override
        public long openRO(LPSZ name) {
            if (isCounting && Utf8s.endsWithAscii(name, "sym.d")) {
                dataOpenCount++;
            }
            return super.openRO(name);
        }
    }

    private static class ParquetDataCountingFilesFacade extends TestFilesFacadeImpl {
        private long dataFd = -1;
        private int dataMapCount;
        private int dataOpenCount;
        private boolean isCounting;

        @Override
        public boolean close(long fd) {
            if (fd == dataFd) {
                dataFd = -1;
            }
            return super.close(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            if (isCounting && fd == dataFd) {
                dataMapCount++;
            }
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long openRO(LPSZ name) {
            final long fd = super.openRO(name);
            if (Utf8s.endsWithAscii(name, PARQUET_PARTITION_NAME)) {
                dataFd = fd;
                if (isCounting) {
                    dataOpenCount++;
                }
            }
            return fd;
        }
    }

    private void assertFailure(String tableName, String operation, String errorMsg) throws Exception {
        assertFailure(null, tableName, operation, errorMsg);
    }

    private void assertFailure(
            @Nullable FilesFacade ff,
            String tableName,
            String operation,
            String errorMsg
    ) throws Exception {
        assertFailure(
                ff,
                tableName,
                operation,
                errorMsg,
                null
        );
    }

    private void assertFailure(
            @Nullable FilesFacade ff,
            String tableName,
            String operation,
            String errorMsg,
            @Nullable Runnable mutator
    ) throws Exception {
        assertMemoryLeak(ff, () -> {
            TableModel tab = new TableModel(configuration, tableName, PartitionBy.DAY);
            createPopulateTable(
                    tab
                            .timestamp("ts", timestampType.getTimestampType())
                            .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                            .col("i", ColumnType.INT)
                            .col("l", ColumnType.LONG)
                            .col("s2", ColumnType.SYMBOL),
                    100,
                    "2022-06-01",
                    5
            );
            if (mutator != null) {
                mutator.run();
            }
            assertFailure(operation, errorMsg);
        });
    }

    private void assertFailure(String operation, String errorMsg) {
        try {
            execute(operation, sqlExecutionContext);
            Assert.fail();
        } catch (SqlException | CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), errorMsg);
        }
    }

    private void assertParquetPmResolvesAgainstTxn(String tableName) {
        final FilesFacade ff = configuration.getFilesFacade();
        final TableToken token = engine.verifyTableName(tableName);

        // Snapshot every parquet partition's identity + the txn-encoded
        // parquet file size up front, then drop the reader so the _pm files
        // are not held by the engine when we open them directly below.
        long[] timestamps;
        long[] nameTxns;
        long[] parquetFileSizes;
        try (TableReader reader = engine.getReader(token)) {
            int parquetCount = 0;
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getTxFile().isPartitionParquet(i)) {
                    parquetCount++;
                }
            }
            Assert.assertTrue("table has at least one parquet partition", parquetCount > 0);
            timestamps = new long[parquetCount];
            nameTxns = new long[parquetCount];
            parquetFileSizes = new long[parquetCount];
            int idx = 0;
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getTxFile().isPartitionParquet(i)) {
                    timestamps[idx] = reader.getTxFile().getPartitionTimestampByIndex(i);
                    nameTxns[idx] = reader.getTxFile().getPartitionNameTxn(i);
                    parquetFileSizes[idx] = reader.getTxFile().getPartitionParquetFileSize(i);
                    idx++;
                }
            }
        }
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();

        for (int i = 0; i < timestamps.length; i++) {
            try (Path p = new Path()) {
                p.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(p, timestampType.getTimestampType(), PartitionBy.DAY, timestamps[i], nameTxns[i]);
                ParquetMetaFileReader pmReader = new ParquetMetaFileReader();
                long addr = ParquetMetaFileReader.openAndMapRO(ff, p.$(), pmReader);
                Assert.assertNotEquals("openAndMapRO on " + p, 0L, addr);
                long size = pmReader.getFileSize();
                try {
                    Assert.assertTrue(
                            "resolveFooter against txn parquetFileSize=" + parquetFileSizes[i] + " for " + p,
                            pmReader.resolveFooter(parquetFileSizes[i])
                    );
                } finally {
                    pmReader.clear();
                    ff.munmap(addr, size, MemoryTag.MMAP_PARQUET_METADATA_READER);
                }
            }
        }
    }

    // Table tab with one row on each of 2024-01-01..03; returns the table directory
    private java.nio.file.Path createDetachNamingTable(boolean isWal) throws Exception {
        execute("CREATE TABLE tab (ts " + timestampType.getTypeName() + ", v LONG) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00Z', 1), ('2024-01-02T00:00:00Z', 2), ('2024-01-03T00:00:00Z', 3)");
        if (isWal) {
            drainWalQueue();
        }
        return java.nio.file.Path.of(configuration.getDbRoot(), engine.verifyTableName("tab").getDirName());
    }

    private void ddl(boolean isWal, String sql) throws Exception {
        execute(sql);
        if (isWal) {
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("tab")));
        }
    }

    private void dropCurrentVersionOfPartition(String tableName, String partitionName) throws SqlException {
        engine.clear();
        // hide the detached partition
        TableToken tableToken = engine.verifyTableName(tableName);
        path.of(configuration.getDbRoot()).concat(tableToken).concat(partitionName + ".detached").$();
        other.of(configuration.getDbRoot()).concat(tableToken).concat(partitionName + ".detached.hide").$();

        Assert.assertEquals(Files.FILES_RENAME_OK, Files.rename(path.$(), other.$()));
        // drop the latest version of the partition
        execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
        // resurface the hidden detached partition
        Assert.assertEquals(Files.FILES_RENAME_OK, Files.rename(other.$(), path.$()));
    }

    private void renameDetachedToAttachable(String tableName, int timestampType, long... partitions) {
        TableToken tableToken = engine.verifyTableName(tableName);
        for (long partition : partitions) {
            TableUtils.setSinkForNativePartition(
                    path.of(configuration.getDbRoot()).concat(tableToken),
                    timestampType,
                    PartitionBy.DAY,
                    partition,
                    -1
            );
            path.put(DETACHED_DIR_MARKER).$();
            TableUtils.setSinkForNativePartition(
                    other.of(configuration.getDbRoot()).concat(tableToken),
                    timestampType,
                    PartitionBy.DAY,
                    partition,
                    -1
            );
            other.put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
        }
    }

    private void renameDetachedToAttachable(String tableName, String... partitions) {
        TableToken tableToken = engine.verifyTableName(tableName);
        for (String partition : partitions) {
            path.of(configuration.getDbRoot()).concat(tableToken).concat(partition).put(DETACHED_DIR_MARKER).$();
            other.of(configuration.getDbRoot()).concat(tableToken).concat(partition).put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
        }
    }

    private String replaceTimestampSuffix(String expected) {
        return ColumnType.isTimestampNano(timestampType.getTimestampType()) ? expected.replace("Z\t", "000Z\t").replace("Z\n", "000Z\n") : expected;
    }

    private void runPartitionPurgeJobs() {
        // when reader is returned to pool it remains in open state
        // holding files such that purge fails with access violation
        if (Os.isWindows()) {
            engine.releaseInactive();
        }
        purgeJob.drain(0);
    }
}
