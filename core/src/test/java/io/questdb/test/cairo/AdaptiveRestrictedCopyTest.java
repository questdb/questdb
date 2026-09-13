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
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The adaptive durability path must never publish a file by WHOLE-FILE COPY onto a destination that
 * already exists.
 * <p>
 * {@code FilesFacade.copy} is portable only when the destination is absent. On POSIX it is
 * {@code creat(O_TRUNC)} and silently replaces whatever is there; on Windows it is
 * {@code CopyFileW(.., bFailIfExists=TRUE)} and returns {@link Files#WINDOWS_ERROR_FILE_EXISTS}. Every
 * adaptive site that copies over a live file — the epoch copy of a REUSED A/B generation, and the
 * recovery roll-forward that restores {@code _meta}/{@code _txn}/{@code _cv} over the live ones — is
 * therefore green on Linux and dead on Windows:
 * <pre>
 * boot-essential component(s) failed [component=engine]: [80]
 * adaptive epoch roll-forward failed to restore metadata [table=sys.acl_links, src=..._meta.epoch.1, dst=..._meta]
 * </pre>
 * These tests run the same workloads on Linux behind a files facade that reproduces the Windows refusal,
 * so the divergence is caught where CI actually runs. They fail on the {@code ff.copy} implementation
 * they replaced, and fail with the refusal counter naming the file that was copied over.
 */
public class AdaptiveRestrictedCopyTest extends AbstractCairoTest {

    @Test
    public void testEpochCopyReplacesAReusedGenerationOnARestrictedCopyFileSystem() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        final RestrictedCopyFacade windowsFf = new RestrictedCopyFacade();
        final FilesFacade ffBefore = AbstractCairoTest.ff;
        try {
            AbstractCairoTest.ff = windowsFf;
            execute("create table reused (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("reused");

            // Four epochs over an A/B pair: generations 1, 0, 1, 0 — the second and later writes land on a
            // generation whose payloads are already on disk, which is the case ff.copy refuses on Windows.
            for (int i = 0; i < 4; i++) {
                execute("insert into reused values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ')');
                drainWalQueue();
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(1L);
                }
            }

            Assert.assertTrue("both A/B generations must have been written, else no generation was reused",
                    epochPayloadExists(tt, 0) && epochPayloadExists(tt, 1));
            assertNoRestrictedCopyRefusal(windowsFf);
            assertQuery("select count() from reused").noRandomAccess().expectSize().returns("count\n4\n");
        } finally {
            AbstractCairoTest.ff = ffBefore;
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    @Test
    public void testRecoveryRollForwardRestoresOverLiveFilesOnARestrictedCopyFileSystem() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        final RestrictedCopyFacade windowsFf = new RestrictedCopyFacade();
        final FilesFacade ffBefore = AbstractCairoTest.ff;
        try {
            AbstractCairoTest.ff = windowsFf;
            // A durable epoch at seqTxn=3, then four more rows applied lazily: recovery has a real gap to
            // roll forward, so it restores _meta/_txn/_cv over the live files rather than skipping.
            execute("create table rollfwd (ts timestamp, v long) timestamp(ts) partition by day wal");
            for (int i = 0; i < 3; i++) {
                execute("insert into rollfwd values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ')');
            }
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("rollfwd");
            try (TableWriter w = getWriter(tt)) {
                w.advanceDurableEpoch(1L);
            }
            for (int i = 3; i < 7; i++) {
                execute("insert into rollfwd values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ')');
            }
            drainWalQueue();
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertQuery("select count() from rollfwd").noRandomAccess().expectSize().returns("count\n7\n");

            new RecoveryCoordinator(engine).recover();

            assertNoRestrictedCopyRefusal(windowsFf);
            // The live _txn said seven rows going in and the epoch's copy says three, so the restored bytes
            // really did land on the live file. Replaying the lazy tail back on top of this cut is
            // AdaptiveRecoveryRollForwardCrashTest's subject, not this one's.
            assertQuery("select count() from rollfwd").noRandomAccess().expectSize().returns("count\n3\n");
        } finally {
            AbstractCairoTest.ff = ffBefore;
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    @Test
    public void testReplaceFileContentShrinksTheDestinationInPlace() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path src = new Path(); Path dst = new Path()) {
                src.of(root).concat("replace-src").$();
                dst.of(root).concat("replace-dst").$();
                writeBytes(ff, src, (byte) 0xA5, 7);
                writeBytes(ff, dst, (byte) 0x5A, 4096);
                final long dstFdBefore = ff.openRO(dst.$());
                ff.close(dstFdBefore);

                TableUtils.replaceFileContent(ff, src.$(), dst.$(), configuration.getWriterFileOpenOpts());

                // The stale 4089-byte tail must be gone, not merely overwritten at the head.
                Assert.assertEquals(7, ff.length(dst.$()));
                assertBytes(ff, dst, (byte) 0xA5, 7);

                // A missing destination is created, so the same helper serves first publication too.
                ff.remove(dst.$());
                TableUtils.replaceFileContent(ff, src.$(), dst.$(), configuration.getWriterFileOpenOpts());
                Assert.assertEquals(7, ff.length(dst.$()));
                assertBytes(ff, dst, (byte) 0xA5, 7);
            }
        });
    }

    private static void assertBytes(FilesFacade ff, Path path, byte expected, int length) {
        final long fd = ff.openRO(path.$());
        Assert.assertTrue(fd > -1);
        final long buf = Unsafe.malloc(length, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(length, ff.read(fd, buf, length, 0));
            for (int i = 0; i < length; i++) {
                Assert.assertEquals("byte " + i, expected, Unsafe.getUnsafe().getByte(buf + i));
            }
        } finally {
            Unsafe.free(buf, length, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    private static void assertNoRestrictedCopyRefusal(RestrictedCopyFacade windowsFf) {
        final String refused = windowsFf.lastRefused;
        Assert.assertEquals(
                "the adaptive durability path whole-file copied onto an existing destination, which"
                        + " Windows refuses with errno " + Files.WINDOWS_ERROR_FILE_EXISTS + " [dst=" + refused + ']',
                0,
                windowsFf.refusals.get()
        );
    }

    private static void writeBytes(FilesFacade ff, Path path, byte value, int length) {
        final long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
        Assert.assertTrue(fd > -1);
        final long buf = Unsafe.malloc(length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < length; i++) {
                Unsafe.getUnsafe().putByte(buf + i, value);
            }
            Assert.assertEquals(length, ff.write(fd, buf, length, 0));
            Assert.assertTrue(ff.truncate(fd, length));
        } finally {
            Unsafe.free(buf, length, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    private boolean epochPayloadExists(TableToken tt, int generation) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot())
                    .concat(tt)
                    .concat(TableUtils.TXN_FILE_NAME)
                    .put(TableUtils.EPOCH_COPY_SUFFIX)
                    .put('.')
                    .put(generation);
            return ff.exists(p.$());
        }
    }

    /**
     * Reproduces {@code CopyFileW(.., bFailIfExists=TRUE)}: a whole-file copy onto an existing destination
     * is refused with {@link Files#WINDOWS_ERROR_FILE_EXISTS}, exactly as it is on Windows.
     */
    private static final class RestrictedCopyFacade extends TestFilesFacadeImpl {
        final AtomicInteger refusals = new AtomicInteger();
        volatile String lastRefused = "<none>";
        private boolean justRefused;

        @Override
        public int copy(LPSZ from, LPSZ to) {
            if (exists(to)) {
                refusals.incrementAndGet();
                lastRefused = to.toString();
                justRefused = true;
                return -1;
            }
            return super.copy(from, to);
        }

        @Override
        public int errno() {
            if (justRefused) {
                justRefused = false;
                return Files.WINDOWS_ERROR_FILE_EXISTS;
            }
            return super.errno();
        }
    }
}
