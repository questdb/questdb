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

package io.questdb.test.cairo.idx;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.idx.PostingIndexChainHeader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.cairo.idx.PostingIndexUtils.*;

public class PostingIndexResetTest extends AbstractCairoTest {
    private static final int INDEX_KEY_COUNT = 4;
    private static final String INDEX_NAME = "s";
    private static final long NEW_CHAIN_TXN_AT_SEAL = 7;

    @Test
    public void testInitOfNewIndexStartsAtSealTxnZero() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path, INDEX_NAME, COLUMN_NAME_TXN_NONE, true);
                    write(writer, 0, 8);
                }
                Assert.assertEquals(0, headSealTxn(path, plen));
                assertRows(path, plen, 0, 8);
            }
        });
    }

    @Test
    public void testInitOverEmptyIndexStartsAtSealTxnZero() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path, INDEX_NAME, COLUMN_NAME_TXN_NONE, true);
                }
                reinitAndWrite(path, plen);
                Assert.assertEquals(0, headSealTxn(path, plen));
                assertRows(path, plen, 100, 104);
            }
        });
    }

    @Test
    public void testInitOverUnreadableKeyFileStartsPastSealedFiles() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final long replacedSealTxn = createSealedIndex(path, plen);
                final FilesFacade ff = configuration.getFilesFacade();
                final long fd = ff.openRW(PostingIndexUtils.keyFileName(path.trimTo(plen), INDEX_NAME, COLUMN_NAME_TXN_NONE), configuration.getWriterFileOpenOpts());
                try {
                    Assert.assertTrue(ff.truncate(fd, 64));
                } finally {
                    ff.close(fd);
                }

                reinitAndWrite(path, plen);
                Assert.assertEquals(replacedSealTxn + 1, headSealTxn(path, plen));
                assertRows(path, plen, 100, 104);
            }
        });
    }

    @Test
    public void testInitQueuesOnePurgePerReplacedSealTxn() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final long replacedSealTxn = createSealedIndex(path, plen);
                final long[] replacedValueFileSealTxns = valueFileSealTxns();
                Assert.assertTrue(configuration.getFilesFacade().touch(
                        PostingIndexUtils.coverDataFileName(path.trimTo(plen), INDEX_NAME, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, replacedSealTxn)
                ));

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), INDEX_NAME, COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(NEW_CHAIN_TXN_AT_SEAL);
                    write(writer, 100, 104);
                    Assert.assertEquals(replacedValueFileSealTxns.length, writer.getPendingPurgesSizeForTesting());
                    for (int i = 0; i < replacedValueFileSealTxns.length; i++) {
                        Assert.assertEquals(replacedValueFileSealTxns[i], writer.getPendingPurgeSealTxnForTesting(i));
                    }
                }
                Assert.assertEquals(replacedSealTxn + 1, headSealTxn(path, plen));
            }
        });
    }

    @Test
    public void testInitQueuesPurgeOfReplacedFilesAtFirstPublish() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                createSealedIndex(path, plen);
                final long[] replacedSealTxns = valueFileSealTxns();
                Assert.assertTrue("fixture: the replaced index must leave its head and an earlier file", replacedSealTxns.length > 1);

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), INDEX_NAME, COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(NEW_CHAIN_TXN_AT_SEAL);
                    Assert.assertEquals(0, writer.getPendingPurgesSizeForTesting());

                    write(writer, 100, 104);
                    Assert.assertEquals(replacedSealTxns.length, writer.getPendingPurgesSizeForTesting());
                    for (int i = 0; i < replacedSealTxns.length; i++) {
                        Assert.assertEquals(replacedSealTxns[i], writer.getPendingPurgeSealTxnForTesting(i));
                        Assert.assertEquals(COLUMN_NAME_TXN_NONE, writer.getPendingPurgeFromTxnForTesting(i));
                        Assert.assertEquals(NEW_CHAIN_TXN_AT_SEAL, writer.getPendingPurgeToTxnForTesting(i));
                    }

                    Assert.assertEquals(-1, writer.getPendingPurgeSealTxnForTesting(-1));
                    Assert.assertEquals(-1, writer.getPendingPurgeSealTxnForTesting(replacedSealTxns.length));

                    write(writer, 104, 108);
                    Assert.assertEquals(replacedSealTxns.length, writer.getPendingPurgesSizeForTesting());
                }
                assertRows(path, plen, 100, 108);
            }
        });
    }

    @Test
    public void testInitRetriedAfterFailedKeyFileCreationStartsPastSealedFiles() throws Exception {
        final AtomicBoolean isFailing = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                return Utf8s.endsWithAscii(name, Files.SEPARATOR + INDEX_NAME + ".pk") && isFailing.compareAndSet(true, false)
                        ? -1
                        : super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final long replacedSealTxn = createSealedIndex(path, plen);

                isFailing.set(true);
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), INDEX_NAME, COLUMN_NAME_TXN_NONE, true);
                    Assert.fail("the key file creation must fail");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-write");
                }
                Assert.assertFalse("the attempt must have reached the key file creation", isFailing.get());
                Assert.assertFalse("the failed attempt must leave no key file", ff.exists(PostingIndexUtils.keyFileName(path.trimTo(plen), INDEX_NAME, COLUMN_NAME_TXN_NONE)));

                reinitAndWrite(path, plen);
                Assert.assertEquals(replacedSealTxn + 1, headSealTxn(path, plen));
                assertRows(path, plen, 100, 104);
            }
        });
    }

    @Test
    public void testInitStartsPastSealTxnsOfReplacedIndex() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final long replacedSealTxn = createSealedIndex(path, plen);

                reinitAndWrite(path, plen);
                Assert.assertEquals(replacedSealTxn + 1, headSealTxn(path, plen));
                assertRows(path, plen, 100, 104);

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), INDEX_NAME, COLUMN_NAME_TXN_NONE, false);
                    write(writer, 104, 108);
                    writer.seal();
                }
                Assert.assertEquals(replacedSealTxn + 2, headSealTxn(path, plen));
                assertRows(path, plen, 100, 108);
            }
        });
    }

    @Test
    public void testInitStartsPastUnpublishedCoverFile() throws Exception {
        assertInitStartsPastUnpublishedFile(true);
    }

    @Test
    public void testInitStartsPastUnpublishedValueFile() throws Exception {
        assertInitStartsPastUnpublishedFile(false);
    }

    @Test
    public void testKeyFileOpenFailure() throws Exception {
        // Without the header the reset still starts past the sealed files on disk.
        final AtomicBoolean isFailing = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // One shot: the reset reads the header once.
                return Utf8s.endsWithAscii(name, ".pk") && isFailing.compareAndSet(true, false) ? -1 : super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            final long lastFileSealTxn = createIndex();
            isFailing.set(true);
            assertReset(ff, lastFileSealTxn);
            Assert.assertFalse("the reset must have read the header", isFailing.get());
        });
    }

    @Test
    public void testKeyFileRemovalFailure() throws Exception {
        final FailingRemoveFacade ff = new FailingRemoveFacade(".pk");
        assertMemoryLeak(ff, () -> {
            final long lastSealTxn = createIndex();
            ff.isFailing = true;
            try (Path path = partitionPath(); MemoryCMARW keyMem = Vm.getCMARWInstance()) {
                PostingIndexUtils.resetIndex(ff, path, path.size(), "s", -1, keyMem);
                Assert.fail("the reset must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not remove posting index key file");
            } finally {
                ff.isFailing = false;
            }
            // The sealed files went before the key file failed. The key file still counts the old chain, so a later
            // reset completes and starts past it.
            assertFiles("[s.pk]");
            assertReset(ff, lastSealTxn);
        });
    }

    @Test
    public void testNoKeyFile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG)");
            execute("INSERT INTO x VALUES ('a', 1)");
            engine.releaseAllWriters();
            try (Path path = partitionPath(); MemoryCMARW keyMem = Vm.getCMARWInstance()) {
                final int plen = path.size();
                Assert.assertFalse(PostingIndexUtils.resetIndex(configuration.getFilesFacade(), path, plen, "s", -1, keyMem));
                Assert.assertEquals(plen, path.size());
            }
            assertFiles("[]");
        });
    }

    @Test
    public void testRemovalsReportedAsFailedOfRemovedFiles() throws Exception {
        // A removal that reports a failure but leaves no file behind removed the file: the key file, the cover info
        // file and the sealed value and cover files alike.
        final AtomicBoolean isReportingFailures = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                final boolean isRemoved = super.removeQuiet(name);
                return isRemoved && !(isReportingFailures.get() && Utf8s.containsAscii(name, Files.SEPARATOR + "s.p"));
            }
        };
        assertMemoryLeak(ff, () -> {
            final long lastSealTxn = createIndex();
            isReportingFailures.set(true);
            assertReset(ff, lastSealTxn);
        });
    }

    @Test
    public void testResetKeepsOtherColumnInstances() throws Exception {
        assertMemoryLeak(() -> {
            final long lastSealTxn = createIndex();
            // Files of another instance of the column, as a column version change leaves until it is purged.
            try (Path path = partitionPath()) {
                final int plen = path.size();
                final FilesFacade ff = configuration.getFilesFacade();
                Assert.assertTrue(ff.touch(valueFileName(path, "s", 7, lastSealTxn + 10)));
                Assert.assertTrue(ff.touch(coverDataFileName(path.trimTo(plen), "s", 0, 7, -1, lastSealTxn + 10)));
                Assert.assertTrue(ff.touch(coverInfoFileName(path.trimTo(plen), "s", 7)));
            }
            assertReset(configuration.getFilesFacade(), lastSealTxn);
            final long otherSealTxn = lastSealTxn + 10;
            assertFiles("[s.pc0.7." + otherSealTxn + ", s.pci.7, s.pk, s.pv.7." + otherSealTxn + "]");
        });
    }

    @Test
    public void testResetListsPartitionOnce() throws Exception {
        // The pass that removes the instance's sealed files also finds the highest sealTxn they carry.
        final AtomicInteger listingCount = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long findFirst(LPSZ path) {
                listingCount.incrementAndGet();
                return super.findFirst(path);
            }
        };
        assertMemoryLeak(ff, () -> {
            final long lastSealTxn = createIndex();
            try (Path path = partitionPath(); MemoryCMARW keyMem = Vm.getCMARWInstance()) {
                listingCount.set(0);
                Assert.assertTrue(PostingIndexUtils.resetIndex(ff, path, path.size(), "s", -1, keyMem));
                Assert.assertEquals(1, listingCount.get());
            }
            assertReset(ff, lastSealTxn);
        });
    }

    @Test
    public void testSealedFileRemovalFailure() throws Exception {
        final FailingRemoveFacade ff = new FailingRemoveFacade(".pci");
        assertMemoryLeak(ff, () -> {
            final long lastSealTxn = createIndex();
            ff.isFailing = true;
            try (Path path = partitionPath(); MemoryCMARW keyMem = Vm.getCMARWInstance()) {
                PostingIndexUtils.resetIndex(ff, path, path.size(), "s", -1, keyMem);
                Assert.fail("the reset must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not remove posting index files");
            } finally {
                ff.isFailing = false;
            }
            // The key file stays, so a later reset still finds the instance and starts past its chain.
            assertReset(ff, lastSealTxn);
        });
    }

    @Test
    public void testTornHeaderPages() throws Exception {
        // Neither page of the key file is stable, so the reset starts past the sealed files on disk. Page B of a
        // fresh chain was never published; page A takes an odd sequence in one case, unequal halves in the other.
        final long[][] pageA = {{3, 3}, {4, 2}};
        for (long[] sequence : pageA) {
            assertMemoryLeak(() -> {
                final long lastFileSealTxn = createIndex();
                try (Path path = partitionPath(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                    final FilesFacade ff = configuration.getFilesFacade();
                    mem.smallFile(ff, keyFileName(path, "s", -1), MemoryTag.MMAP_DEFAULT);
                    mem.jumpTo(mem.size());
                    // Let the stable page count far past the files: a torn page must not raise the start.
                    mem.putLong(PAGE_A_OFFSET + V2_HEADER_OFFSET_GEN_COUNTER, lastFileSealTxn + 100);
                    mem.putLong(PAGE_A_OFFSET + V2_HEADER_OFFSET_SEQUENCE_START, sequence[0]);
                    mem.putLong(PAGE_A_OFFSET + V2_HEADER_OFFSET_SEQUENCE_END, sequence[1]);
                    mem.putLong(PAGE_B_OFFSET + V2_HEADER_OFFSET_SEQUENCE_START, 0);
                    mem.putLong(PAGE_B_OFFSET + V2_HEADER_OFFSET_SEQUENCE_END, 0);
                }
                assertReset(configuration.getFilesFacade(), lastFileSealTxn);
                execute("DROP TABLE x");
            });
        }
    }

    @Test
    public void testWriterRecreatesUnreadableKeyFilePastSealedFiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL INDEX TYPE POSTING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES ('a', '2024-01-01T00:00:00'), ('b', '2024-01-01T00:00:01')");
            engine.releaseAllWriters();
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path().of(configuration.getDbRoot()).concat(engine.verifyTableName("x").getDirName()).concat("2024-01-01")) {
                final int plen = path.size();
                final long fd = ff.openRW(PostingIndexUtils.keyFileName(path, INDEX_NAME, COLUMN_NAME_TXN_NONE), configuration.getWriterFileOpenOpts());
                try {
                    Assert.assertTrue(ff.truncate(fd, 0));
                } finally {
                    ff.close(fd);
                }
                Assert.assertTrue(ff.touch(PostingIndexUtils.valueFileName(path.trimTo(plen), INDEX_NAME, COLUMN_NAME_TXN_NONE, 5)));

                execute("INSERT INTO x VALUES ('c', '2024-01-01T00:00:02')");
                engine.releaseAllWriters();
                Assert.assertEquals(6, headSealTxn(path, plen));
            }
            assertQuery("SELECT count() c FROM x WHERE s = 'c'").noRandomAccess().expectSize().returns("c\n1\n");
        });
    }

    private static void assertFiles(String expected) {
        final String[] files = partitionDir().list((_, name) -> name.startsWith("s.p"));
        Assert.assertNotNull(files);
        Arrays.sort(files);
        Assert.assertEquals(expected, Arrays.toString(files));
    }

    private static void assertInitStartsPastUnpublishedFile(boolean isCoverFile) throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final long replacedSealTxn = createSealedIndex(path, plen);
                final LPSZ unpublishedFile = isCoverFile
                        ? PostingIndexUtils.coverDataFileName(path.trimTo(plen), INDEX_NAME, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, replacedSealTxn + 1)
                        : PostingIndexUtils.valueFileName(path.trimTo(plen), INDEX_NAME, COLUMN_NAME_TXN_NONE, replacedSealTxn + 1);
                Assert.assertTrue(configuration.getFilesFacade().touch(unpublishedFile));

                reinitAndWrite(path, plen);
                Assert.assertEquals(replacedSealTxn + 2, headSealTxn(path, plen));
                assertRows(path, plen, 100, 104);
            }
        });
    }

    // The partition holds the key file of an empty chain and none of the instance's sealed files, and the chain
    // starts past lastSealTxn.
    private static void assertReset(FilesFacade ff, long lastSealTxn) {
        try (Path path = partitionPath(); MemoryCMARW keyMem = Vm.getCMARWInstance(); MemoryCMR mem = Vm.getCMRInstance()) {
            final int plen = path.size();
            Assert.assertTrue(PostingIndexUtils.resetIndex(ff, path, plen, "s", -1, keyMem));
            Assert.assertEquals(plen, path.size());
            final LPSZ keyFile = keyFileName(path, "s", -1);
            Assert.assertTrue(configuration.getFilesFacade().length(keyFile) >= KEY_FILE_RESERVED);
            mem.of(configuration.getFilesFacade(), keyFile, configuration.getFilesFacade().getMapPageSize(), KEY_FILE_RESERVED, MemoryTag.MMAP_DEFAULT);
            final PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
            Assert.assertTrue(PostingIndexChainHeader.readUnderSeqlock(mem, header));
            Assert.assertEquals(V2_NO_HEAD, header.headEntryOffset);
            Assert.assertEquals(0, header.entryCount);
            // The first entry of the new chain takes lastSealTxn + 1.
            Assert.assertEquals(lastSealTxn, header.generationCounter);
        }
        // No file of the instance is left; the scan reports the files of the instance only.
        try (Path path = partitionPath()) {
            final int plen = path.size();
            Assert.assertFalse(configuration.getFilesFacade().exists(coverInfoFileName(path, "s", -1)));
            final LongList sealedFiles = new LongList();
            scanSealedFiles(configuration.getFilesFacade(), path, plen, "s", -1, new SealedFileVisitor() {
                @Override
                public void onCoverDataFile(int includeIdx, long coveredColumnNameTxn, long sealTxn) {
                    sealedFiles.add(sealTxn);
                }

                @Override
                public void onValueFile(long sealTxn) {
                    sealedFiles.add(sealTxn);
                }
            });
            Assert.assertEquals(0, sealedFiles.size());
        }
    }

    private static void assertRows(Path path, int plen, int rowLo, int rowHi) {
        try (PostingIndexFwdReader reader = new PostingIndexFwdReader(configuration, path.trimTo(plen), INDEX_NAME, COLUMN_NAME_TXN_NONE, -1, 0)) {
            int rowCount = 0;
            for (int key = 0; key < INDEX_KEY_COUNT; key++) {
                final RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE);
                try {
                    long expectedRow = rowLo + Math.floorMod(key - rowLo, INDEX_KEY_COUNT);
                    while (cursor.hasNext()) {
                        Assert.assertEquals("key " + key, expectedRow, cursor.next());
                        expectedRow += INDEX_KEY_COUNT;
                        rowCount++;
                    }
                    Assert.assertTrue("key " + key + " ends at " + expectedRow, expectedRow >= rowHi);
                } finally {
                    Misc.free(cursor);
                }
            }
            Assert.assertEquals(rowHi - rowLo, rowCount);
        }
    }

    // Builds a covering posting index over the non-partitioned table x, so its partition holds a key file, value
    // files and cover files, and returns the highest sealTxn those files carry.
    private static long createIndex() throws Exception {
        execute("CREATE TABLE x (s SYMBOL, v LONG)");
        execute("INSERT INTO x VALUES ('a', 1), (NULL, 2), ('b', 3), ('a', 4)");
        execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        final String[] values = partitionDir().list((_, name) -> name.startsWith("s.pv."));
        Assert.assertNotNull(values);
        long lastSealTxn = -1;
        for (String name : values) {
            lastSealTxn = Math.max(lastSealTxn, Long.parseLong(name.substring("s.pv.".length())));
        }
        Assert.assertTrue(lastSealTxn > 0);
        return lastSealTxn;
    }

    private static long createSealedIndex(Path path, int plen) {
        try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path.trimTo(plen), INDEX_NAME, COLUMN_NAME_TXN_NONE)) {
            write(writer, 0, 8);
            writer.seal();
            write(writer, 8, 16);
            writer.seal();
        }
        final long sealTxn = headSealTxn(path, plen);
        Assert.assertTrue("fixture: the replaced index must have sealed", sealTxn > 0);
        return sealTxn;
    }

    private static long headSealTxn(Path path, int plen) {
        return PostingIndexUtils.readSealTxnFromKeyFile(
                configuration.getFilesFacade(),
                PostingIndexUtils.keyFileName(path.trimTo(plen), INDEX_NAME, COLUMN_NAME_TXN_NONE)
        );
    }

    private static File partitionDir() {
        return new File(new File(configuration.getDbRoot(), engine.verifyTableName("x").getDirName()), "default");
    }

    private static Path partitionPath() {
        return new Path().of(configuration.getDbRoot()).concat(engine.verifyTableName("x")).concat("default");
    }

    private static void reinitAndWrite(Path path, int plen) {
        try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
            writer.of(path.trimTo(plen), INDEX_NAME, COLUMN_NAME_TXN_NONE, true);
            write(writer, 100, 104);
        }
    }

    private static long[] valueFileSealTxns() {
        final String prefix = INDEX_NAME + ".pv.";
        final String[] names = new File(configuration.getDbRoot().toString()).list((dir, name) -> name.startsWith(prefix));
        Assert.assertNotNull(names);
        final long[] sealTxns = new long[names.length];
        for (int i = 0; i < names.length; i++) {
            sealTxns[i] = Long.parseLong(names[i].substring(prefix.length()));
        }
        Arrays.sort(sealTxns);
        return sealTxns;
    }

    private static void write(PostingIndexWriter writer, int rowLo, int rowHi) {
        for (int row = rowLo; row < rowHi; row++) {
            writer.add(row % INDEX_KEY_COUNT, row);
        }
        writer.setMaxValue(rowHi - 1);
        writer.commit();
    }

    private static class FailingRemoveFacade extends TestFilesFacadeImpl {
        private final String suffix;
        private volatile boolean isFailing;

        private FailingRemoveFacade(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public boolean removeQuiet(LPSZ name) {
            if (isFailing && Utf8s.endsWithAscii(name, suffix)) {
                return false;
            }
            return super.removeQuiet(name);
        }
    }
}
