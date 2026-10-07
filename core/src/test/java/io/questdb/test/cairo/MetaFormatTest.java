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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@code _meta} record this build writes, and the ones it reads, in every commit mode.
 * <p>
 * Every producer of the record writes the base layout plus the adaptive enrolment record: the minor format
 * version of the enrolment record, a minor-version checksum that keeps the version-gated fields readable, and
 * zeros from the end of the enrolment record to the column types. Earlier development
 * builds stamped minor versions 4 to 6 and stored a body length and checksum at [64, 80); this build never
 * reads those bytes and still reads every gated field of such a file.
 * <p>
 * The enrolment record is the one field written in place under live readers, as a single 4-byte store.
 */
@RunWith(Parameterized.class)
public class MetaFormatTest extends AbstractCairoTest {
    private static final int EARLIER_BUILD_MINOR_VERSION = 6;
    private static final int HEADER_SIZE = (int) TableUtils.META_OFFSET_COLUMN_TYPES;
    private static final long PADDING_LO = TableUtils.META_OFFSET_ENROLLED_COMMIT_MODE + Integer.BYTES;
    private final String commitMode;

    public MetaFormatTest(String commitMode) {
        this.commitMode = commitMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{"nosync"}, {"async"}, {"sync"}, {"adaptive"}});
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
    }

    @Test
    public void testEarlierBuildMetaLoads() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY TTL 2 DAYS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("t");
            final int enrolledCommitMode;
            try (TableReader reader = getReader(token)) {
                Assert.assertEquals(48, reader.getMetadata().getTtlHoursOrMonths());
                enrolledCommitMode = reader.getMetadata().getEnrolledCommitMode();
            }

            // The earlier build's values at [64, 80) are not reproduced: this build must not read them at all.
            stampEarlierBuildLayout(token);
            engine.releaseInactive();
            try (TableReader reader = getReader(token)) {
                Assert.assertEquals(48, reader.getMetadata().getTtlHoursOrMonths());
                Assert.assertEquals(enrolledCommitMode, reader.getMetadata().getEnrolledCommitMode());
            }
            assertQuery("SELECT count() FROM t").noRandomAccess().expectSize().returns("count\n1\n");

            // The next schema change writes the file in this build's layout.
            execute("ALTER TABLE t ADD COLUMN w DOUBLE");
            drainWalQueue();
            assertBaseLayout(metaPath(token, TableUtils.META_FILE_NAME));
            assertQuery("SELECT v, w FROM t").expectSize().returns("v\tw\n1\tnull\n");
        });
    }

    @Test
    public void testEnrolmentRecordWriteRacesReaderOpen() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE meta_enrol_race (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("meta_enrol_race");

            final int writes = 300;
            final AtomicReference<Throwable> writerError = new AtomicReference<>();
            final AtomicBoolean done = new AtomicBoolean();
            final CyclicBarrier start = new CyclicBarrier(2);
            final Thread writer = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < writes; i++) {
                        DurableEpochManifest.recordEnrolledCommitMode(
                                engine.getConfiguration(), token, (i & 1) == 0 ? CommitMode.ADAPTIVE : CommitMode.UNSET
                        );
                    }
                } catch (Throwable t) {
                    writerError.compareAndSet(null, t);
                } finally {
                    done.set(true);
                }
            });
            writer.start();
            start.await();
            int reloads = 0;
            try {
                while (!done.get()) {
                    forceMetadataReload(token);
                    reloads++;
                }
            } finally {
                writer.join();
            }
            if (writerError.get() != null) {
                throw new AssertionError("enrolment writer failed", writerError.get());
            }
            Assert.assertTrue("the reader must have raced at least one write", reloads > 0);
            forceMetadataReload(token);
        });
    }

    @Test
    public void testEnrolmentRecordWriteIsASingleStore() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE meta_enrol_store (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("meta_enrol_store");
            final String metaPath = metaPath(token, TableUtils.META_FILE_NAME);
            for (int mode : new int[]{CommitMode.ADAPTIVE, CommitMode.UNSET}) {
                final byte[] before = readHeader(metaPath);
                DurableEpochManifest.recordEnrolledCommitMode(engine.getConfiguration(), token, mode);
                final byte[] after = readHeader(metaPath);
                for (int offset = 0; offset < HEADER_SIZE; offset++) {
                    if (offset < TableUtils.META_OFFSET_ENROLLED_COMMIT_MODE || offset >= PADDING_LO) {
                        Assert.assertEquals("byte " + offset, before[offset], after[offset]);
                    }
                }
                Assert.assertEquals(mode, Unsafe.byteArrayGetInt(after, (int) TableUtils.META_OFFSET_ENROLLED_COMMIT_MODE));
                forceMetadataReload(token);
            }
        });
    }

    @Test
    public void testEveryProducerWritesTheBaseLayout() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            // CREATE writes the table _meta and, for a WAL table, the sequencer's initial copy.
            execute("CREATE TABLE plain (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO plain VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            execute("CREATE TABLE wal (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO wal VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            assertBaseLayout("plain", false);
            assertBaseLayout("wal", true);

            // A schema change rewrites _meta through the swap.
            execute("ALTER TABLE plain ADD COLUMN w DOUBLE");
            execute("ALTER TABLE wal ADD COLUMN w DOUBLE");
            drainWalQueue();
            assertBaseLayout("plain", false);
            assertBaseLayout("wal", true);

            // REBASE WAL clones the table and resets the clone's metadata version in place.
            execute("ALTER TABLE wal SUSPEND WAL");
            execute("ALTER TABLE wal REBASE WAL");
            drainWalQueue();
            assertBaseLayout("wal", true);

            // A type conversion resets the metadata version in place on the next start.
            execute("ALTER TABLE plain SET TYPE WAL");
            execute("ALTER TABLE wal SET TYPE BYPASS WAL");
            engine.releaseInactive();
            engine.load();
            assertBaseLayout("plain", true);
            assertBaseLayout("wal", false);
            assertQuery("SELECT count() FROM plain").noRandomAccess().expectSize().returns("count\n1\n");
            assertQuery("SELECT count() FROM wal").noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    private static void assertBaseLayout(String path) {
        final byte[] header = readHeader(path);
        final int minorVersionField = Unsafe.byteArrayGetInt(header, (int) TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION);
        // The enrolment record is the newest field of this layout.
        Assert.assertEquals(path, TableUtils.META_FORMAT_MINOR_VERSION_ENROLLED_COMMIT_MODE, Numbers.decodeHighShort(minorVersionField));
        // The version stays trusted: a mismatch here would switch off every version-gated field.
        Assert.assertEquals(
                path,
                TableUtils.checksumForMetaFormatMinorVersionField(
                        Unsafe.byteArrayGetLong(header, (int) TableUtils.META_OFFSET_METADATA_VERSION),
                        Unsafe.byteArrayGetInt(header, (int) TableUtils.META_OFFSET_COUNT)
                ),
                Numbers.decodeLowShort(minorVersionField)
        );
        for (int offset = (int) PADDING_LO; offset < HEADER_SIZE; offset++) {
            Assert.assertEquals(path + ", byte " + offset, 0, header[offset]);
        }
    }

    private static void forceMetadataReload(TableToken token) {
        // Evict pooled metadata, or the load is served from a cached object.
        engine.releaseInactive();
        engine.getTableMetadata(token).close();
    }

    private static String metaPath(TableToken token, CharSequence fileName) {
        try (Path path = new Path()) {
            return path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(fileName).toString();
        }
    }

    private static byte[] readHeader(String metaPath) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final byte[] header = new byte[HEADER_SIZE];
        final long buf = Unsafe.malloc(HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
        try (Path path = new Path()) {
            final long fd = ff.openRO(path.of(metaPath).$());
            Assert.assertTrue("could not open " + metaPath, fd > -1);
            try {
                Assert.assertEquals(HEADER_SIZE, ff.read(fd, buf, HEADER_SIZE, 0));
            } finally {
                ff.close(fd);
            }
            for (int i = 0; i < HEADER_SIZE; i++) {
                header[i] = Unsafe.getUnsafe().getByte(buf + i);
            }
            return header;
        } finally {
            Unsafe.free(buf, HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void stampEarlierBuildLayout(TableToken token) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
            final long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
            Assert.assertTrue("could not open " + path, fd > -1);
            try {
                Assert.assertEquals(Integer.BYTES, ff.read(fd, buf, Integer.BYTES, TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION));
                final int field = Unsafe.getUnsafe().getInt(buf);
                Unsafe.getUnsafe().putInt(buf, Numbers.encodeLowHighShorts(Numbers.decodeLowShort(field), (short) EARLIER_BUILD_MINOR_VERSION));
                Assert.assertEquals(Integer.BYTES, ff.write(fd, buf, Integer.BYTES, TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION));
                Unsafe.getUnsafe().putLong(buf, 4096);
                Assert.assertEquals(Long.BYTES, ff.write(fd, buf, Long.BYTES, 64));
                Unsafe.getUnsafe().putLong(buf, 0x5DEECE66DL);
                Assert.assertEquals(Long.BYTES, ff.write(fd, buf, Long.BYTES, 72));
            } finally {
                ff.close(fd);
            }
        } finally {
            Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertBaseLayout(String tableName, boolean isWal) {
        final TableToken token = engine.verifyTableName(tableName);
        Assert.assertEquals(tableName, isWal, token.isWal());
        assertBaseLayout(metaPath(token, TableUtils.META_FILE_NAME));
        if (isWal) {
            assertBaseLayout(metaPath(token, WalUtils.SEQ_DIR + "/" + WalUtils.INITIAL_META_FILE_NAME));
        }
    }
}
