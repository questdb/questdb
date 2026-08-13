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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The live {@code _meta} carried no body checksum -- only the epoch manifest's snapshot COPY was
 * protected -- so a flipped byte in the live file was served silently.
 * <p>
 * The length and checksum are version-gated fields INSIDE the record, not a trailer at the end of the
 * file, because {@code _meta}'s on-disk length is page-rounded and not authoritative: the create path
 * reuses its memory for the symbol-map files instead of closing it, so the file is left extended and
 * an end-of-file trailer is never found.
 */
public class MetaChecksumTest extends AbstractCairoTest {

    @Test
    public void testConvertedTableKeepsAValidChecksum() throws Exception {
        // TableConverter mutates _meta IN PLACE via resetMetadataVersion. If the checksum were not
        // recomputed there, conversion would leave every converted table permanently unreadable.
        assertMemoryLeak(() -> {
            execute("create table meta_conv (ts timestamp, v long) timestamp(ts) partition by day wal");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("meta_conv");
            Assert.assertTrue(hasChecksum(token));

            execute("alter table meta_conv set type bypass wal");
            engine.releaseInactive();

            // The conversion rewrites _meta in place on restart; the checksum must still verify.
            forceMetadataReload(engine.verifyTableName("meta_conv"));
        });
    }

    @Test
    public void testRebasedTableKeepsAValidChecksum() throws Exception {
        // REBASE WAL clones the table and mutates the clone's _meta IN PLACE -- it rewrites
        // META_OFFSET_TABLE_ID and then resets the metadata version. Both land inside the checksummed
        // range, so without a recompute every rebased table would come back permanently unreadable.
        setProperty(io.questdb.PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            execute("create table meta_rebase (ts timestamp, x int) timestamp(ts) partition by day wal");
            execute("insert into meta_rebase values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            Assert.assertTrue(hasChecksum(engine.verifyTableName("meta_rebase")));

            execute("alter table meta_rebase suspend wal");
            execute("alter table meta_rebase rebase wal");
            drainWalQueue();

            final TableToken rebased = engine.verifyTableName("meta_rebase");
            Assert.assertTrue("the rebased clone must carry a checksum", hasChecksum(rebased));
            // The decisive check: reading the clone's metadata must verify, not throw.
            forceMetadataReload(rebased);
            assertQuery("select count() from meta_rebase").noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    @Test
    public void testCorruptedMetaIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table meta_rot (ts timestamp, v long) timestamp(ts) partition by day wal");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("meta_rot");
            // Precondition: without this the test would "pass" against a file that was never
            // checksummed, which is exactly how the first attempt at this feature failed.
            Assert.assertTrue("the _meta body checksum must be present for this test to mean anything",
                    hasChecksum(token));

            flipByteInMetaBody(token);
            try {
                forceMetadataReload(token);
                Assert.fail("expected a flipped _meta byte to be rejected");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_meta checksum mismatch");
            }
        });
    }

    @Test
    public void testLegacyMetaWithoutChecksumLoads() throws Exception {
        // The false-positive control: a _meta written before the field existed fails the version gate
        // and must load unverified rather than throw.
        assertMemoryLeak(() -> {
            execute("create table meta_legacy (ts timestamp, v long) timestamp(ts) partition by day wal");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("meta_legacy");
            downgradeMinorVersion(token);
            Assert.assertFalse(hasChecksum(token));

            forceMetadataReload(token); // must not throw
            assertQuery("select count() from meta_legacy").noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testStructuralChangeRewritesTheChecksum() throws Exception {
        // ALTER goes through TableWriter.rewriteMetadata, a different writer from create.
        assertMemoryLeak(() -> {
            execute("create table meta_alter (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("alter table meta_alter add column w double");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("meta_alter");
            Assert.assertTrue("ALTER must leave a valid checksum behind", hasChecksum(token));
            forceMetadataReload(token);
            assertQuery("select count() from meta_alter").noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    private void downgradeMinorVersion(TableToken token) {
        // Rewrite the minor-version high short to 4, i.e. the format before the checksum field, keeping
        // the low-short checksum intact so every OTHER gated field still reads.
        withMetaFd(token, true, (ff, fd) -> {
            final long buf = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                ff.read(fd, buf, Integer.BYTES, TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION);
                final int field = Unsafe.getUnsafe().getInt(buf);
                final short low = (short) (field & 0xFFFF);
                Unsafe.getUnsafe().putInt(buf, (low & 0xFFFF) | (4 << 16));
                ff.write(fd, buf, Integer.BYTES, TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION);
            } finally {
                Unsafe.free(buf, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private void flipByteInMetaBody(TableToken token) {
        withMetaFd(token, true, (ff, fd) -> {
            // Inside the covered range and away from the excluded [64,80) window.
            final long offset = TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS;
            final long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertEquals(1, ff.read(fd, buf, 1, offset));
                final byte b = Unsafe.getUnsafe().getByte(buf);
                Unsafe.getUnsafe().putByte(buf, (byte) (b ^ 0x01));
                Assert.assertEquals(1, ff.write(fd, buf, 1, offset));
            } finally {
                Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private void forceMetadataReload(TableToken token) {
        // Evict pooled metadata, or the assertion runs against a cached object and passes vacuously.
        engine.releaseInactive();
        engine.getTableMetadata(token).close();
    }

    private boolean hasChecksum(TableToken token) {
        final boolean[] present = {false};
        withMetaFd(token, false, (ff, fd) -> {
            final long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                ff.read(fd, buf, Long.BYTES, TableUtils.META_OFFSET_BODY_CHECKSUM_64);
                final long checksum = Unsafe.getUnsafe().getLong(buf);
                ff.read(fd, buf, Long.BYTES, TableUtils.META_OFFSET_BODY_LEN_64);
                final long bodyLen = Unsafe.getUnsafe().getLong(buf);
                // Presence is what the READER sees, which is the version gate -- not merely whether the
                // bytes happen to be non-zero.
                ff.read(fd, buf, Integer.BYTES, TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION);
                final short minor = (short) (Unsafe.getUnsafe().getInt(buf) >> 16);
                present[0] = checksum != 0 && bodyLen > 0
                        && minor >= TableUtils.META_FORMAT_MINOR_VERSION_BODY_CHECKSUM;
            } finally {
                Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
        return present[0];
    }

    private void withMetaFd(TableToken token, boolean rw, MetaFdAction action) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
            final long fd = rw ? ff.openRW(path.$(), CairoConfiguration.O_NONE) : ff.openRO(path.$());
            Assert.assertTrue("could not open " + path, fd > -1);
            try {
                action.run(ff, fd);
            } finally {
                ff.close(fd);
            }
        }
    }

    @FunctionalInterface
    private interface MetaFdAction {
        void run(FilesFacade ff, long fd);
    }
}
