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

import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeSet;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * The sealed posting file names of one partition directory, by posting column name txn. The names
 * follow the format of {@link PostingIndexUtils#valueFileName} and
 * {@link PostingIndexUtils#coverDataFileName}: {@code s.pv[.<txn>].<seal>} leaves the txn out for
 * NONE (-1), and {@code s.pc<idx>.<txn>[.<coveredTxn>].<seal>} always writes the txn, as 0 for NONE,
 * so NONE and txn 0 share their {@code .pc} files.
 */
public class PostingIndexUtilsTest extends AbstractCairoTest {
    // file name -> the posting column name txns that own it
    private static final String[] OWNED_FILES = {
            "s.pv.0", "-1",
            "s.pv.12", "-1",
            "s.pv.0.0", "0",
            "s.pv.0.1", "0",
            "s.pv.1.1", "1",
            "s.pv.10.12", "10",
            "s.pc0.0.1", "-1,0",
            "s.pc1.0.7.12", "-1,0",
            "s.pc2.0.0.0", "-1,0",
            "s.pc0.1.1", "1",
            "s.pc0.1.7.1", "1",
            "s.pc0.10.0.12", "10",
            "s.pc3.10.12", "10",
    };
    // owned by no posting column instance of s
    private static final String[] FOREIGN_FILES = {
            "s.pc", "s.pcX.0.1", "s.pc0.", "s.pc0.0.", "s.pc0.a.1", "s.pc.0.1", "s.pc0", "s.pc0.0.1.2.3", "s.pc0.-1.1",
            "s.pv.", "s.pv.x", "s.pv.0.", "s.pv.a.1", "s.pv.1.2.3", "s.px.1", "s.p", "s.pvv.1",
            "s2.pc0.0.1", "s2.pv.1", "sx.pv.1", "t.pc0.0.1", "s_pc0.0.1",
            "s.pk", "s.pk.0", "s.d", "s.d.0", "s.k.0", "s.v.0",
    };
    // not sealed files: the scan ignores them, removeAllSealedFiles removes the one of its instance
    private static final String[] PCI_FILES = {"s.pci", "s.pci.0", "s.pci.1", "s.pci.10", "s.pci.5"};
    private int scanDirCount;

    @Test
    public void testCoverAndValueFileNames() {
        try (Path path = new Path()) {
            Assert.assertEquals("s.pc0.0.1", Utf8s.stringFromUtf8Bytes(PostingIndexUtils.coverDataFileName(path.of(""), "s", 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, 1)));
            Assert.assertEquals("s.pc0.0.1.1", Utf8s.stringFromUtf8Bytes(PostingIndexUtils.coverDataFileName(path.of(""), "s", 0, 0, 1, 1)));
            Assert.assertEquals("s.pc2.0.1", Utf8s.stringFromUtf8Bytes(PostingIndexUtils.coverDataFileName(path.of(""), "s", 2, 0, COLUMN_NAME_TXN_NONE, 1)));
            Assert.assertEquals("s.pc1.0.0.12", Utf8s.stringFromUtf8Bytes(PostingIndexUtils.coverDataFileName(path.of(""), "s", 1, COLUMN_NAME_TXN_NONE, 0, 12)));
            Assert.assertEquals("s.pc3.10.12", Utf8s.stringFromUtf8Bytes(PostingIndexUtils.coverDataFileName(path.of(""), "s", 3, 10, COLUMN_NAME_TXN_NONE, 12)));
            Assert.assertEquals("s.pv.1", Utf8s.stringFromUtf8Bytes(PostingIndexUtils.valueFileName(path.of(""), "s", COLUMN_NAME_TXN_NONE, 1)));
            Assert.assertEquals("s.pv.0.1", Utf8s.stringFromUtf8Bytes(PostingIndexUtils.valueFileName(path.of(""), "s", 0, 1)));
            Assert.assertEquals("s.pv.10.12", Utf8s.stringFromUtf8Bytes(PostingIndexUtils.valueFileName(path.of(""), "s", 10, 12)));
        }
    }

    @Test
    public void testRemoveAllSealedFilesRemovesOnlyTheFilesOfPostingColumnNameTxn() throws Exception {
        assertMemoryLeak(() -> {
            for (long postingColumnNameTxn : new long[]{COLUMN_NAME_TXN_NONE, 0, 1, 10, 5}) {
                final File dir = createScanDir();
                try (Path path = new Path()) {
                    path.of(dir.getAbsolutePath()).slash();
                    final int pathLen = path.size();
                    Assert.assertFalse(PostingIndexUtils.removeAllSealedFiles(configuration.getFilesFacade(), path, pathLen, "s", postingColumnNameTxn));
                }
                final TreeSet<String> expected = new TreeSet<>(Arrays.asList(FOREIGN_FILES));
                expected.addAll(Arrays.asList(PCI_FILES));
                for (int i = 0; i < OWNED_FILES.length; i += 2) {
                    if (!isOwnedBy(OWNED_FILES[i + 1], postingColumnNameTxn)) {
                        expected.add(OWNED_FILES[i]);
                    }
                }
                // the .pci of the instance goes with its sealed files
                expected.remove(postingColumnNameTxn == COLUMN_NAME_TXN_NONE ? "s.pci" : "s.pci." + postingColumnNameTxn);
                expected.add("s.pv.5");
                final String[] remaining = dir.list();
                Assert.assertNotNull(remaining);
                Assert.assertEquals("posting column name txn " + postingColumnNameTxn, expected.toString(), new TreeSet<>(Arrays.asList(remaining)).toString());
            }
        });
    }

    @Test
    public void testScanSealedFilesMatchesPostingColumnNameTxn() throws Exception {
        assertMemoryLeak(() -> {
            final File dir = createScanDir();
            final FilesFacade ff = configuration.getFilesFacade();
            final String[][] expectedCallbacks = {
                    // NONE
                    {"pc0 covered=-1 seal=1", "pc1 covered=7 seal=12", "pc2 covered=0 seal=0", "pv seal=0", "pv seal=12"},
                    // 0
                    {"pc0 covered=-1 seal=1", "pc1 covered=7 seal=12", "pc2 covered=0 seal=0", "pv seal=0", "pv seal=1"},
                    // 1
                    {"pc0 covered=-1 seal=1", "pc0 covered=7 seal=1", "pv seal=1"},
                    // 10
                    {"pc0 covered=0 seal=12", "pc3 covered=-1 seal=12", "pv seal=12"},
                    // 5: only a directory named like one of its .pv files
                    {}
            };
            final long[] postingColumnNameTxns = {COLUMN_NAME_TXN_NONE, 0, 1, 10, 5};
            try (Path path = new Path()) {
                path.of(dir.getAbsolutePath()).slash();
                final int pathLen = path.size();
                for (int i = 0; i < postingColumnNameTxns.length; i++) {
                    final ArrayList<String> callbacks = new ArrayList<>();
                    PostingIndexUtils.scanSealedFiles(ff, path, pathLen, "s", postingColumnNameTxns[i], new PostingIndexUtils.SealedFileVisitor() {
                        @Override
                        public void onCoverDataFile(int includeIdx, long coveredColumnNameTxn, long sealTxn) {
                            callbacks.add("pc" + includeIdx + " covered=" + coveredColumnNameTxn + " seal=" + sealTxn);
                        }

                        @Override
                        public void onValueFile(long sealTxn) {
                            callbacks.add("pv seal=" + sealTxn);
                        }
                    });
                    Assert.assertEquals(pathLen, path.size());
                    Collections.sort(callbacks);
                    Assert.assertEquals("posting column name txn " + postingColumnNameTxns[i], Arrays.toString(expectedCallbacks[i]), callbacks.toString());
                }
            }
        });
    }

    private static boolean isOwnedBy(String owners, long postingColumnNameTxn) {
        for (String owner : owners.split(",")) {
            if (Long.parseLong(owner) == postingColumnNameTxn) {
                return true;
            }
        }
        return false;
    }

    private File createScanDir() {
        final File dir = new File(root, "posting_scan_dir_" + scanDirCount++);
        Assert.assertTrue(dir.mkdirs());
        try {
            for (int i = 0; i < OWNED_FILES.length; i += 2) {
                Assert.assertTrue(new File(dir, OWNED_FILES[i]).createNewFile());
            }
            for (String name : FOREIGN_FILES) {
                Assert.assertTrue(name, new File(dir, name).createNewFile());
            }
            for (String name : PCI_FILES) {
                Assert.assertTrue(new File(dir, name).createNewFile());
            }
            // a directory is not a sealed file even when its name matches one
            Assert.assertTrue(new File(dir, "s.pv.5").mkdir());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return dir;
    }
}
