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
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
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

import static io.questdb.cairo.idx.PostingIndexUtils.*;

public class PostingIndexResetTest extends AbstractCairoTest {

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

    private static void assertFiles(String expected) {
        final String[] files = partitionDir().list((_, name) -> name.startsWith("s.p"));
        Assert.assertNotNull(files);
        Arrays.sort(files);
        Assert.assertEquals(expected, Arrays.toString(files));
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

    private static File partitionDir() {
        return new File(new File(configuration.getDbRoot(), engine.verifyTableName("x").getDirName()), "default");
    }

    private static Path partitionPath() {
        return new Path().of(configuration.getDbRoot()).concat(engine.verifyTableName("x")).concat("default");
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
