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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Helpers shared by the group-commit and durable-ack tests: a held-writer commit, the background flusher
 * driven on the test clock, and a facade that records which files were fdatasync'd, in order.
 */
public final class GroupCommitTestUtils {

    private GroupCommitTestUtils() {
    }

    public static void assertDataSyncFailure(Runnable commit) {
        try {
            commit.run();
            Assert.fail("a failed durability barrier must be fatal");
        } catch (CairoException | CairoError expected) {
            Assert.assertTrue(String.valueOf(expected), CairoException.isDataSyncFailure(expected));
        }
    }

    /**
     * Appends one (ts, v) row to a held writer and commits it, without releasing the writer.
     */
    public static void commitRow(WalWriter w, long ts, long v) {
        TableWriter.Row row = w.newRow(ts);
        row.putLong(1, v);
        row.append();
        w.commit();
    }

    public static void markDistressed(WalWriter writer) throws ReflectiveOperationException {
        final Field distressed = WalWriter.class.getSuperclass().getDeclaredField("distressed");
        distressed.setAccessible(true);
        distressed.setBoolean(writer, true);
    }

    /**
     * Exposes {@link WalPurgeJob}'s {@code runSerially()}, whose sweep includes the group-commit flush of idle
     * writers, so a test can run it on the test microsecond clock.
     */
    public static class ExposedFlusher extends WalPurgeJob {
        public ExposedFlusher(CairoEngine engine) {
            super(engine);
        }

        public boolean flushNow() {
            return runSerially();
        }
    }

    /**
     * Fails the next {@code fdatasync} of a sequencer file once, after issuing it, then lets every later one
     * through: the way the kernel reports a writeback error on a file once and then reports success.
     */
    public static class FailingSeqFdatasyncFacade extends WalFdatasyncFacade {
        public boolean injected;
        private boolean armed;

        @Override
        public void fdatasync(long fd) {
            super.fdatasync(fd);
            final String p = fdToPath.get(fd);
            if (armed && p != null && isSequencerFile(p)) {
                armed = false;
                injected = true;
                throw CairoException.dataSyncFailure(this, 5, "fdatasync", "injected sequencer fdatasync failure");
            }
        }

        public void failNextSequencerFdatasync() {
            armed = true;
        }
    }

    /**
     * Fails the next {@code fdatasync} of any file, and counts every attempt from then on.
     */
    public static class FailingWalFdatasyncFacade extends WalFdatasyncFacade {
        public int failureAttempts;
        private boolean armed;
        private boolean failNext;

        @Override
        public void fdatasync(long fd) {
            if (armed) {
                failureAttempts++;
            }
            if (failNext) {
                failNext = false;
                throw CairoException.dataSyncFailure(this, 5, "fdatasync", "injected WAL fdatasync failure");
            }
            super.fdatasync(fd);
        }

        public void failNext() {
            armed = true;
            failNext = true;
        }
    }

    /**
     * Records the path of every {@code fdatasync}, in order. The writer-private WAL barriers issue
     * {@code barrierFsync}, which is folded in, so the counts are the same on Darwin, where it is not fdatasync.
     */
    public static class WalFdatasyncFacade extends TestFilesFacadeImpl {
        protected final Map<Long, String> fdToPath = new HashMap<>();
        private final List<String> fdatasyncPaths = new ArrayList<>();

        public static boolean isEventFile(String p) {
            return p.endsWith(WalUtils.EVENT_FILE_NAME) || p.endsWith(WalUtils.EVENT_FILE_NAME + ".");
        }

        public static boolean isSequencerFile(String p) {
            return p.contains(WalUtils.TXNLOG_PARTS_DIR)
                    || p.endsWith(WalUtils.TXNLOG_FILE_NAME)
                    || p.endsWith(WalUtils.TXNLOG_FILE_NAME + ".");
        }

        @Override
        public void barrierFsync(long fd) {
            fdatasync(fd);
        }

        @Override
        public boolean close(long fd) {
            fdToPath.remove(fd);
            return super.close(fd);
        }

        public int count(Predicate<String> path) {
            int c = 0;
            for (int i = 0, n = fdatasyncPaths.size(); i < n; i++) {
                if (path.test(fdatasyncPaths.get(i))) {
                    c++;
                }
            }
            return c;
        }

        public int eventFdatasyncs() {
            return count(WalFdatasyncFacade::isEventFile);
        }

        @Override
        public void fdatasync(long fd) {
            super.fdatasync(fd);
            final String p = fdToPath.get(fd);
            if (p != null) {
                fdatasyncPaths.add(p);
            }
        }

        public int firstIndex(Predicate<String> path) {
            for (int i = 0, n = fdatasyncPaths.size(); i < n; i++) {
                if (path.test(fdatasyncPaths.get(i))) {
                    return i;
                }
            }
            return -1;
        }

        @Override
        public void fsyncDurable(long fd) {
            fsync(fd);
        }

        @Override
        public long openAppend(LPSZ name) {
            return track(super.openAppend(name), name);
        }

        @Override
        public long openCleanRW(LPSZ name, long size) {
            return track(super.openCleanRW(name, size), name);
        }

        @Override
        public long openRO(LPSZ name) {
            return track(super.openRO(name), name);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            return track(super.openRW(name, opts), name);
        }

        @Override
        public long openRWExisting(LPSZ name) {
            return track(super.openRWExisting(name), name);
        }

        public int privateWalFdatasyncs() {
            return count(p -> (p.contains("/wal") || p.contains("\\wal")) && !isSequencerFile(p));
        }

        public void reset() {
            fdatasyncPaths.clear();
        }

        public int seqPartFdatasyncs(long partId) {
            return count(p -> p.endsWith(WalUtils.TXNLOG_PARTS_DIR + '/' + partId) || p.endsWith(WalUtils.TXNLOG_PARTS_DIR + '\\' + partId));
        }

        public int sequencerFdatasyncs() {
            return count(WalFdatasyncFacade::isSequencerFile);
        }

        private long track(long fd, LPSZ name) {
            if (fd > -1) {
                fdToPath.put(fd, Utf8String.newInstance(name).toString());
            }
            return fd;
        }
    }
}
