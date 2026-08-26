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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CompositeCellManifest;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * The {@code _cell_manifest.d} format: round-trip, and the corruption cases that decide whether a new
 * on-disk format is safe to add.
 * <p>
 * A manifest is read from a directory a user can write to (an artifact is moved by hand -- that is the
 * documented operator step for ATTACH), so its length words are untrusted input in exactly the way a
 * corrupted file's are. Every read is bounds-checked against the file size BEFORE it happens; these
 * tests are what make that claim testable rather than asserted.
 */
public class CompositeCellManifestTest extends AbstractCairoTest {

    @Test
    public void testRoundTripsValuesAndNulls() throws Exception {
        assertMemoryLeak(() -> {
            final IntList keys = new IntList();
            keys.add(0);
            keys.add(7);
            keys.add(4);
            final ObjList<String> values = new ObjList<>();
            // dimCount = 2, row-major: (cell0.d0, cell0.d1), (cell7.d0, cell7.d1), ...
            values.add("BTC");
            values.add("spot");
            values.add(null);            // a NULL dimension value must survive as NULL
            values.add("futures");
            values.add("ETH-€/£");       // non-ASCII: the format stores UTF-8 bytes, not chars
            values.add(null);

            try (Path p = new Path()) {
                p.of(root);
                CompositeCellManifest.write(configuration.getFilesFacade(), p, configuration.getWriterFileOpenOpts(), 2, keys, values);

                final IntList keysBack = new IntList();
                final ObjList<String> valuesBack = new ObjList<>();
                p.of(root);
                final int dims = CompositeCellManifest.read(configuration.getFilesFacade(), p, keysBack, valuesBack);

                Assert.assertEquals(2, dims);
                Assert.assertEquals(3, keysBack.size());
                Assert.assertEquals(0, keysBack.getQuick(0));
                Assert.assertEquals(7, keysBack.getQuick(1));
                Assert.assertEquals(4, keysBack.getQuick(2));
                Assert.assertEquals(6, valuesBack.size());
                Assert.assertEquals("BTC", valuesBack.getQuick(0));
                Assert.assertEquals("spot", valuesBack.getQuick(1));
                Assert.assertNull(valuesBack.getQuick(2));
                Assert.assertEquals("futures", valuesBack.getQuick(3));
                Assert.assertEquals("ETH-€/£", valuesBack.getQuick(4));
                Assert.assertNull(valuesBack.getQuick(5));

                // EXACT byte layout, locked. The file must be tight, not page-padded: a reader
                // bounds-checks against the FILE SIZE, so trailing zeros would both weaken that check
                // and make a zero cellCount look like a valid empty manifest.
                //   header            12  = version + dimCount + cellCount
                //   cell 0            19  = key(4) + (4+3 "BTC") + (4+4 "spot")
                //   cell 7            19  = key(4) + (4+0 NULL)  + (4+7 "futures")
                //   cell 4            22  = key(4) + (4+10 "ETH-€/£" as UTF-8) + (4+0 NULL)
                Assert.assertEquals("manifest must be written tight, not page-padded",
                        72, Files.size(Paths.get(root, CompositeCellManifest.FILE_NAME)));
            }
        });
    }

    @Test
    public void testEmptyManifestRoundTrips() throws Exception {
        assertMemoryLeak(() -> {
            try (Path p = new Path()) {
                p.of(root);
                CompositeCellManifest.write(configuration.getFilesFacade(), p, configuration.getWriterFileOpenOpts(),
                        1, new IntList(), new ObjList<>());
                final IntList keysBack = new IntList();
                final ObjList<String> valuesBack = new ObjList<>();
                p.of(root);
                Assert.assertEquals(1, CompositeCellManifest.read(configuration.getFilesFacade(), p, keysBack, valuesBack));
                Assert.assertEquals(0, keysBack.size());
                Assert.assertEquals(0, valuesBack.size());
            }
        });
    }

    @Test
    public void testMissingFileIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            try (Path p = new Path()) {
                p.of(root).concat("no_such_dir");
                try {
                    CompositeCellManifest.read(configuration.getFilesFacade(), p, new IntList(), new ObjList<>());
                    Assert.fail("a missing manifest must be refused");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "missing");
                }
            }
        });
    }

    @Test
    public void testTruncatedFileIsRefusedNotReadPastTheEnd() throws Exception {
        assertMemoryLeak(() -> {
            final IntList keys = new IntList();
            keys.add(3);
            final ObjList<String> values = new ObjList<>();
            values.add("a-fairly-long-dimension-value-to-truncate-inside");

            try (Path p = new Path()) {
                p.of(root);
                CompositeCellManifest.write(configuration.getFilesFacade(), p, configuration.getWriterFileOpenOpts(), 1, keys, values);
            }
            // Cut the file mid-value: the length word promises more bytes than remain.
            final java.nio.file.Path f = Paths.get(root, CompositeCellManifest.FILE_NAME);
            final long full = Files.size(f);
            try (RandomAccessFile raf = new RandomAccessFile(f.toFile(), "rw")) {
                raf.setLength(full - 8);
            }

            try (Path p = new Path()) {
                p.of(root);
                try {
                    CompositeCellManifest.read(configuration.getFilesFacade(), p, new IntList(), new ObjList<>());
                    Assert.fail("a truncated manifest must be refused, not read past the mapping");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "truncated");
                }
            }
        });
    }

    @Test
    public void testWrongVersionIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            final IntList keys = new IntList();
            keys.add(1);
            final ObjList<String> values = new ObjList<>();
            values.add("x");
            try (Path p = new Path()) {
                p.of(root);
                CompositeCellManifest.write(configuration.getFilesFacade(), p, configuration.getWriterFileOpenOpts(), 1, keys, values);
            }
            // Stamp a future version into the header.
            final java.nio.file.Path f = Paths.get(root, CompositeCellManifest.FILE_NAME);
            try (RandomAccessFile raf = new RandomAccessFile(f.toFile(), "rw")) {
                raf.seek(0);
                raf.write(new byte[]{99, 0, 0, 0}); // little-endian 99
            }

            try (Path p = new Path()) {
                p.of(root);
                try {
                    CompositeCellManifest.read(configuration.getFilesFacade(), p, new IntList(), new ObjList<>());
                    Assert.fail("an unknown manifest version must be refused");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "unsupported composite cell manifest version");
                }
            }
        });
    }

    @Test
    public void testInsaneHeaderIsRefusedBeforeAllocating() throws Exception {
        assertMemoryLeak(() -> {
            final IntList keys = new IntList();
            keys.add(1);
            final ObjList<String> values = new ObjList<>();
            values.add("x");
            try (Path p = new Path()) {
                p.of(root);
                CompositeCellManifest.write(configuration.getFilesFacade(), p, configuration.getWriterFileOpenOpts(), 1, keys, values);
            }
            // A cellCount of ~2 billion in a 20-byte file: the guard must reject on the HEADER rather
            // than loop two billion times discovering truncation.
            final java.nio.file.Path f = Paths.get(root, CompositeCellManifest.FILE_NAME);
            try (RandomAccessFile raf = new RandomAccessFile(f.toFile(), "rw")) {
                raf.seek(8);
                raf.write(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F});
            }

            try (Path p = new Path()) {
                p.of(root);
                try {
                    CompositeCellManifest.read(configuration.getFilesFacade(), p, new IntList(), new ObjList<>());
                    Assert.fail("an insane header must be refused");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "not sane");
                }
            }
        });
    }

    @Test
    public void testWriteRejectsMismatchedValueCount() throws Exception {
        assertMemoryLeak(() -> {
            final IntList keys = new IntList();
            keys.add(1);
            keys.add(2);
            final ObjList<String> values = new ObjList<>();
            values.add("only-one"); // needs 2 cells * 2 dims = 4
            try (Path p = new Path()) {
                p.of(root);
                try {
                    CompositeCellManifest.write(configuration.getFilesFacade(), p, configuration.getWriterFileOpenOpts(), 2, keys, values);
                    Assert.fail("a mismatched value count must be refused at write time");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "value count mismatch");
                }
            }
        });
    }
}
