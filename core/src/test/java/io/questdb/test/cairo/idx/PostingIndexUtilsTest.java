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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
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
    // owned by no posting column instance of s; s.d.0 stays out, because it marks instance 0 of s and
    // changes what the removal of NONE takes (testRemoveAllSealedFilesKeepsCoverFilesOfInstanceZero)
    private static final String[] FOREIGN_FILES = creatableFileNames(
            "s.pc", "s.pcX.0.1", "s.pc0.", "s.pc0.0.", "s.pc0.a.1", "s.pc.0.1", "s.pc0", "s.pc0.0.1.2.3", "s.pc0.-1.1",
            "s.pv.", "s.pv.x", "s.pv.0.", "s.pv.a.1", "s.pv.1.2.3", "s.px.1", "s.p", "s.pvv.1",
            "s2.pc0.0.1", "s2.pv.1", "sx.pv.1", "t.pc0.0.1", "s_pc0.0.1",
            "s.pk", "s.pk.0", "s.d", "s.d.1", "s2.d.0", "s.k.0", "s.v.0"
    );
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
    public void testCoverFileHostTxn() {
        Assert.assertEquals(0, PostingIndexUtils.coverFileHostTxn(COLUMN_NAME_TXN_NONE));
        Assert.assertEquals(0, PostingIndexUtils.coverFileHostTxn(0));
        Assert.assertEquals(7, PostingIndexUtils.coverFileHostTxn(7));
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
    public void testRemoveAllSealedFilesKeepsCoverFilesOfInstanceZero() throws Exception {
        // DROP INDEX or ALTER COLUMN TYPE at table txn 0 writes <col>.d.0, instance 0 of the column, next
        // to the files of instance NONE in the default partition of a table that is not partitioned.
        // Every .pc file of host token 0 there belongs to instance 0, so the removal of NONE keeps them.
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            for (String columnName : new String[]{"s", "сим"}) {
                try (Path path = new Path()) {
                    final String[] noneFiles = {
                            fileName(PostingIndexUtils.valueFileName(path.of(""), columnName, COLUMN_NAME_TXN_NONE, 0)),
                            fileName(PostingIndexUtils.valueFileName(path.of(""), columnName, COLUMN_NAME_TXN_NONE, 3)),
                            fileName(PostingIndexUtils.coverInfoFileName(path.of(""), columnName, COLUMN_NAME_TXN_NONE)),
                    };
                    final String[] instanceZeroFiles = {
                            fileName(PostingIndexUtils.valueFileName(path.of(""), columnName, 0, 0)),
                            fileName(PostingIndexUtils.valueFileName(path.of(""), columnName, 0, 1)),
                            fileName(PostingIndexUtils.coverInfoFileName(path.of(""), columnName, 0)),
                    };
                    final String[] tokenZeroCoverFiles = {
                            fileName(PostingIndexUtils.coverDataFileName(path.of(""), columnName, 0, 0, COLUMN_NAME_TXN_NONE, 1)),
                            fileName(PostingIndexUtils.coverDataFileName(path.of(""), columnName, 1, 0, 7, 1)),
                            fileName(PostingIndexUtils.coverDataFileName(path.of(""), columnName, 2, 0, 0, 0)),
                    };
                    final String dataFileOfNone = fileName(TableUtils.dFile(path.of(""), columnName, COLUMN_NAME_TXN_NONE));
                    final String dataFileOfZero = fileName(TableUtils.dFile(path.of(""), columnName, 0));
                    final String dataFileOfOne = fileName(TableUtils.dFile(path.of(""), columnName, 1));
                    final String context = columnName + ", ";

                    // with the data file of instance 0, NONE leaves the token-0 .pc files
                    File dir = createRemovalDir(dataFileOfNone, dataFileOfZero, noneFiles, instanceZeroFiles, tokenZeroCoverFiles);
                    Assert.assertFalse(removeAllSealedFiles(ff, path, dir, columnName, COLUMN_NAME_TXN_NONE));
                    Assert.assertEquals(context + "NONE next to instance 0",
                            sortedNames(new String[]{dataFileOfNone, dataFileOfZero}, instanceZeroFiles, tokenZeroCoverFiles),
                            sortedNames(dir.list()));
                    // the removal of instance 0 takes every token-0 .pc file
                    Assert.assertFalse(removeAllSealedFiles(ff, path, dir, columnName, 0));
                    Assert.assertEquals(context + "0 after NONE", sortedNames(new String[]{dataFileOfNone, dataFileOfZero}), sortedNames(dir.list()));

                    dir = createRemovalDir(dataFileOfNone, dataFileOfZero, noneFiles, instanceZeroFiles, tokenZeroCoverFiles);
                    Assert.assertFalse(removeAllSealedFiles(ff, path, dir, columnName, 0));
                    Assert.assertEquals(context + "0 next to NONE",
                            sortedNames(new String[]{dataFileOfNone, dataFileOfZero}, noneFiles),
                            sortedNames(dir.list()));

                    // without the data file of instance 0, NONE owns the token-0 .pc files
                    for (String dataFile : new String[]{dataFileOfNone, dataFileOfOne}) {
                        dir = createRemovalDir(dataFile, null, noneFiles, instanceZeroFiles, tokenZeroCoverFiles);
                        Assert.assertFalse(removeAllSealedFiles(ff, path, dir, columnName, COLUMN_NAME_TXN_NONE));
                        Assert.assertEquals(context + "NONE next to " + dataFile,
                                sortedNames(new String[]{dataFile}, instanceZeroFiles),
                                sortedNames(dir.list()));
                    }
                }
            }
        });
    }

    @Test
    public void testSealedFileColumnNameTxn() {
        final long[] postingColumnNameTxns = {COLUMN_NAME_TXN_NONE, 0, 1, 10, 1_000_000_007L, Long.MAX_VALUE - 1};
        final long[] coveredColumnNameTxns = {COLUMN_NAME_TXN_NONE, 0, 5};
        final long[] sealTxns = {0, 1, 1_000_000};
        final int[] includeIdxs = {0, 1, 12};
        try (Path path = new Path()) {
            for (long postingColumnNameTxn : postingColumnNameTxns) {
                // a .pc name writes NONE as the token 0
                final long hostToken = postingColumnNameTxn == COLUMN_NAME_TXN_NONE ? 0 : postingColumnNameTxn;
                for (long sealTxn : sealTxns) {
                    final String valueFile = Utf8s.stringFromUtf8Bytes(PostingIndexUtils.valueFileName(path.of(""), "s", postingColumnNameTxn, sealTxn));
                    Assert.assertEquals(valueFile, postingColumnNameTxn, PostingIndexUtils.sealedFileColumnNameTxn(valueFile, 2));
                    for (long coveredColumnNameTxn : coveredColumnNameTxns) {
                        for (int includeIdx : includeIdxs) {
                            final String coverFile = Utf8s.stringFromUtf8Bytes(PostingIndexUtils.coverDataFileName(
                                    path.of(""), "s", includeIdx, postingColumnNameTxn, coveredColumnNameTxn, sealTxn));
                            Assert.assertEquals(coverFile, hostToken, PostingIndexUtils.sealedFileColumnNameTxn(coverFile, 2));
                        }
                    }
                }
            }
        }
        // the offset is the position after the column name and its dot
        Assert.assertEquals(7, PostingIndexUtils.sealedFileColumnNameTxn("price.pv.7.3", 6));
        Assert.assertEquals(7, PostingIndexUtils.sealedFileColumnNameTxn("price.pc2.7.1.3", 6));
        Assert.assertEquals(COLUMN_NAME_TXN_NONE, PostingIndexUtils.sealedFileColumnNameTxn("price.pv.3", 6));
        for (String fileName : new String[]{
                "s.pk", "s.pk.3", "s.pci", "s.pci.3", "s.d", "s.d.3", "s.c", "s.c.0", "s.k.0", "s.v.0", "s.",
                "s.p", "s.px.1", "s.pvv.1", "s.pv", "s.pv.", "s.pv.a", "s.pv.1.", "s.pv.1.a", "s.pv.a.1", "s.pv.1.2.3",
                "s.pc", "s.pc.0.1", "s.pcx.0.1", "s.pc0", "s.pc0.", "s.pc0.1", "s.pc0.1.", "s.pc0.a.1", "s.pc0.1.a",
                "s.pc0.1.a.1", "s.pc0.1.2.a", "s.pc0.1.2.3.4", "s.pc0x.1.2"
        }) {
            Assert.assertEquals(fileName, PostingIndexUtils.PARSE_FAIL, PostingIndexUtils.sealedFileColumnNameTxn(fileName, 2));
        }
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

    // Win32 drops trailing dots and spaces from a file name: "s.pc0." would create s.pc0, which collides
    // with the "s.pc0" entry. QuestDB never sees such a name on Windows, so the fixture leaves these names
    // out there; every other platform creates the full list.
    private static String[] creatableFileNames(String... names) {
        if (Os.type != Os.WINDOWS) {
            return names;
        }
        final ArrayList<String> creatable = new ArrayList<>(names.length);
        for (String name : names) {
            if (!name.endsWith(".") && !name.endsWith(" ")) {
                creatable.add(name);
            }
        }
        return creatable.toArray(new String[0]);
    }

    private static String fileName(LPSZ name) {
        return Utf8s.stringFromUtf8Bytes(name);
    }

    private static boolean removeAllSealedFiles(FilesFacade ff, Path path, File dir, String columnName, long postingColumnNameTxn) {
        path.of(dir.getAbsolutePath()).slash();
        final int pathLen = path.size();
        final boolean hasFailed = PostingIndexUtils.removeAllSealedFiles(ff, path, pathLen, columnName, postingColumnNameTxn);
        Assert.assertEquals(pathLen, path.size());
        return hasFailed;
    }

    private static String sortedNames(String[]... nameLists) {
        final TreeSet<String> names = new TreeSet<>();
        for (String[] nameList : nameLists) {
            Assert.assertNotNull(nameList);
            names.addAll(Arrays.asList(nameList));
        }
        return names.toString();
    }

    private static boolean isOwnedBy(String owners, long postingColumnNameTxn) {
        for (String owner : owners.split(",")) {
            if (Long.parseLong(owner) == postingColumnNameTxn) {
                return true;
            }
        }
        return false;
    }

    private File createRemovalDir(String dataFile, String dataFileOfZero, String[]... nameLists) {
        final File dir = new File(root, "posting_removal_dir_" + scanDirCount++);
        Assert.assertTrue(dir.mkdirs());
        try {
            Assert.assertTrue(new File(dir, dataFile).createNewFile());
            if (dataFileOfZero != null) {
                Assert.assertTrue(new File(dir, dataFileOfZero).createNewFile());
            }
            for (String[] names : nameLists) {
                for (String name : names) {
                    Assert.assertTrue(name, new File(dir, name).createNewFile());
                }
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return dir;
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
