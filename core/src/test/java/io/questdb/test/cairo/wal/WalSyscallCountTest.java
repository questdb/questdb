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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The file system calls of a 1-row WAL commit and of the WAL apply pass that applies it, outside ADAPTIVE.
 * NOSYNC, ASYNC and SYNC must make exactly the calls the base version makes (OSS {@code d3b877cc}, measured
 * with this test): the same kinds, the same number of times, on the same files. A file that rides along
 * with every commit or every apply pass shows up here as extra opens, maps and msyncs. The apply pass is
 * measured on fixed-size columns and again with STRING, VARCHAR (not inlined) and ARRAY columns, whose data
 * file length the segment-length guard checks against the last aux entry: a read of that entry shows up here.
 * <p>
 * In every mode, ADAPTIVE included, a WAL table keeps only files the base version writes in its WAL segments
 * and in {@code txn_seq}. A file written through a mapping makes no system call on a NOSYNC commit, so only
 * the file set can see it there.
 */
@RunWith(Parameterized.class)
public class WalSyscallCountTest extends AbstractCairoTest {
    private static final int MEASURED = 20;
    // The files base writes in a WAL segment: the event file and its index, the segment _meta and the column
    // files (.d, plus .i for var-size columns).
    private static final Pattern SEGMENT_FILE = Pattern.compile("_event|_event\\.i|_meta|[a-z]+\\.[di]");
    // The files base writes in txn_seq: the txn log, its metadata index and data, the sequencer _meta and its
    // versions, the WAL index, and the numbered V2 part files.
    private static final Pattern SEQ_FILE = Pattern.compile("_txnlog|_txnlog\\.meta\\.[di]|_meta(\\.[0-9]+)?|_wal_index\\.d|[0-9]+");
    private static final int WARMUP = 5;
    private final String commitMode;
    private final boolean isV2;

    public WalSyscallCountTest(String commitMode, boolean isV2) {
        this.commitMode = commitMode;
        this.isV2 = isV2;
    }

    @Parameterized.Parameters(name = "mode={0}, v2={1}")
    public static Collection<Object[]> data() {
        final List<Object[]> params = new ArrayList<>();
        for (String mode : new String[]{"nosync", "async", "sync", "adaptive"}) {
            params.add(new Object[]{mode, false});
            params.add(new Object[]{mode, true});
        }
        return params;
    }

    @Test
    public void testApplyPass() throws Exception {
        assertCalls(false, false);
    }

    @Test
    public void testApplyPassVarSize() throws Exception {
        assertCalls(false, true);
    }

    @Test
    public void testCommit() throws Exception {
        assertCalls(true, false);
    }

    @Test
    public void testDdl() throws Exception {
        Assume.assumeFalse("ADAPTIVE makes its own calls; only its file set must match base", "adaptive".equals(commitMode));
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, isV2 ? 1000 : 0);
        final CountingFacade ff = new CountingFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (1000000, 1)");
            drainWalQueue();
            for (int i = 0; i < WARMUP + MEASURED; i++) {
                ff.start(i >= WARMUP);
                execute("ALTER TABLE x ADD COLUMN c" + i + " INT");
                ff.stop();
                drainWalQueue();
            }
            final List<String> calls = new ArrayList<>();
            for (String summary : ff.summaries) {
                if (!calls.contains(summary)) {
                    calls.add(summary);
                }
            }
            Assert.assertEquals("DDLs measured", MEASURED, ff.summaries.size());
            // Base msyncs the txn log's metadata index and data, the txn log header and the sequencer _meta
            // twice, in every mode. SYNC also syncs the header after it publishes the change, and a V2 log the
            // part file holding the record, ahead of the header both times.
            final int msyncs = "sync".equals(commitMode) ? (isV2 ? 8 : 6) : 5;
            Assert.assertEquals(
                    "DDL calls differ from base [mode=" + commitMode + ", v2=" + isV2 + ']',
                    "calls: exists=2 msyncSync=" + msyncs + "; opened: ",
                    String.join("\n", calls)
            );
        });
    }

    @Test
    public void testOnlyBaseFiles() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, isV2 ? 1000 : 0);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG, s VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (int i = 0; i < 3; i++) {
                execute("INSERT INTO x VALUES (" + (i + 1) * 1_000_000L + ", " + i + ", 'a" + i + "')");
            }
            try (WalWriter writer = getWalWriter("x")) {
                appendRow(writer, 3, null);
                // A concurrent ADD COLUMN ... SYMBOL makes the commit rewrite its pending event.
                execute("ALTER TABLE x ADD COLUMN c SYMBOL");
                writer.commit();
            }
            execute("UPDATE x SET v = v + 1 WHERE v > 1");
            drainWalQueue();
            engine.releaseInactive();
            engine.getTableSequencerAPI().releaseAll();
            // Reopen the sequencer and a WAL writer: a file written only on open must show up too.
            execute("INSERT INTO x (ts, v) VALUES (5000000, 4)");
            drainWalQueue();
            engine.releaseInactive();
            engine.getTableSequencerAPI().releaseAll();

            final Path tableDir = Paths.get(root, engine.verifyTableName("x").getDirName());
            final Set<String> foreign = new TreeSet<>();
            try (Stream<Path> dirs = Files.list(tableDir)) {
                for (Path walDir : dirs.filter(d -> Files.isDirectory(d) && d.getFileName().toString().startsWith(WalUtils.WAL_NAME_BASE)).toList()) {
                    addForeignFiles(tableDir, walDir, SEGMENT_FILE, foreign);
                }
            }
            addForeignFiles(tableDir, tableDir.resolve(WalUtils.SEQ_DIR), SEQ_FILE, foreign);
            Assert.assertEquals("files base does not write [mode=" + commitMode + ", v2=" + isV2 + ']', "[]", foreign.toString());
        });
    }

    private static void addForeignFiles(Path tableDir, Path dir, Pattern baseFile, Set<String> foreign) throws IOException {
        try (Stream<Path> files = Files.walk(dir)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                if (!baseFile.matcher(file.getFileName().toString()).matches()) {
                    foreign.add(tableDir.relativize(file).toString());
                }
            }
        }
    }

    private static void appendRow(WalWriter writer, long i, @Nullable DirectArray array) {
        final TableWriter.Row row = writer.newRow((i + 1) * 1_000_000L);
        row.putLong(1, i);
        if (array != null) {
            row.putStr(2, "string " + i);
            row.putVarchar(3, new Utf8String("varchar too long to inline " + i));
            row.putArray(4, array);
        }
        row.append();
    }

    private static String expected(String commitMode, boolean isV2, boolean isCommit, boolean isVarSize) {
        if (isCommit) {
            // A V2 commit in SYNC msyncs the open part file ahead of the header: one more than base, which
            // synced the header alone and left the record it publishes to writeback.
            return switch (commitMode) {
                case "async" -> "calls: msyncAsync=5; opened: ";
                case "sync" -> "calls: msyncSync=" + (isV2 ? 6 : 5) + "; opened: ";
                default -> "calls: ; opened: ";
            };
        }
        // The var-size columns add six files to open, close and sync, and three maps; base reads none of them
        // (measured on d3b877cc).
        final int files = isVarSize ? 6 : 0;
        final String msyncs = switch (commitMode) {
            case "async" -> " msyncAsync=" + (3 + files);
            case "sync" -> " msyncSync=" + (3 + files);
            default -> "";
        };
        // A V2 apply pass also opens the sequencer part file "0".
        return "calls: close=" + ((isV2 ? 11 : 10) + 2 * files) + " madvise=3 mmap=" + (3 + files / 2)
                + " mmapNoCache=1 mremap=1" + msyncs + " munmap=" + (4 + files / 2) + " openRO=" + ((isV2 ? 6 : 5) + files)
                + " readNonNegativeLong=3; opened: " + (isV2 ? "0=1 " : "") + "_event=1 _event.i=1 _txnlog=1 "
                + (isVarSize ? "a.d=1 a.i=1 s.d=1 s.i=1 ts.d=1 v.d=1 vc.d=1 vc.i=1" : "ts.d=1 v.d=1");
    }

    private void assertCalls(boolean isCommit, boolean isVarSize) throws Exception {
        Assume.assumeFalse("ADAPTIVE makes its own calls; only its file set must match base", "adaptive".equals(commitMode));
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, isV2 ? 1000 : 0);
        final CountingFacade ff = new CountingFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG" + (isVarSize ? ", s STRING, vc VARCHAR, a DOUBLE[]" : "")
                    + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("x");
            final List<String> calls = new ArrayList<>();
            try (
                    ApplyWal2TableJob job = new ApplyWal2TableJob(engine, 0);
                    WalWriter writer = getWalWriter("x");
                    DirectArray array = isVarSize ? new DirectArray() : null
            ) {
                if (array != null) {
                    array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
                    array.setDimLen(0, 2);
                    array.applyShape();
                    array.putDouble(0, 1.5);
                    array.putDouble(1, 2.5);
                }
                for (int i = 0; i < WARMUP + MEASURED; i++) {
                    appendRow(writer, i, array);
                    ff.start(isCommit && i >= WARMUP);
                    writer.commit();
                    ff.start(!isCommit && i >= WARMUP);
                    job.drain(0);
                    ff.stop();
                    Assert.assertEquals(
                            "each pass applies the txn just committed",
                            engine.getTableSequencerAPI().lastTxn(token),
                            engine.getTableSequencerAPI().getTxnTracker(token).getWriterTxn()
                    );
                }
            }
            for (String summary : ff.summaries) {
                if (!calls.contains(summary)) {
                    calls.add(summary);
                }
            }
            Assert.assertEquals("passes measured", MEASURED, ff.summaries.size());
            final String actual = String.join("\n", calls);
            Assert.assertEquals(
                    (isCommit ? "commit" : "apply pass") + " calls differ from base [mode=" + commitMode + ", v2=" + isV2
                            + ", varSize=" + isVarSize + ']',
                    expected(commitMode, isV2, isCommit, isVarSize),
                    actual
            );
        });
    }

    /**
     * Counts the file system calls made on the measuring thread while armed, and the names of the files
     * opened.
     */
    private static class CountingFacade extends TestFilesFacadeImpl {
        private final Map<String, int[]> calls = new TreeMap<>();
        private final Map<String, int[]> opened = new TreeMap<>();
        private final List<String> summaries = new ArrayList<>();
        private Thread measured;

        @Override
        public boolean allocate(long fd, long size) {
            count("allocate");
            return super.allocate(fd, size);
        }

        @Override
        public boolean allocateRange(long fd, long offset, long size) {
            count("allocateRange");
            return super.allocateRange(fd, offset, size);
        }

        @Override
        public void barrierFsync(long fd) {
            count("barrierFsync");
            super.barrierFsync(fd);
        }

        @Override
        public boolean close(long fd) {
            count("close");
            return super.close(fd);
        }

        @Override
        public boolean exists(LPSZ path) {
            count("exists");
            return super.exists(path);
        }

        @Override
        public void fdatasync(long fd) {
            count("fdatasync");
            super.fdatasync(fd);
        }

        @Override
        public void fsync(long fd) {
            count("fsync");
            super.fsync(fd);
        }

        @Override
        public void fsyncDurable(long fd) {
            count("fsyncDurable");
            super.fsyncDurable(fd);
        }

        @Override
        public long length(long fd) {
            count("length");
            return super.length(fd);
        }

        @Override
        public long length(LPSZ name) {
            count("lengthByName");
            return super.length(name);
        }

        @Override
        public void madvise(long address, long len, int advise) {
            count("madvise");
            super.madvise(address, len, advise);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            count("mmap");
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long mmapNoCache(long fd, long len, long offset, int flags, int memoryTag) {
            count("mmapNoCache");
            return super.mmapNoCache(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            count("mremap");
            return super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        }

        @Override
        public long mremapNoCache(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            count("mremapNoCache");
            return super.mremapNoCache(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            count(async ? "msyncAsync" : "msyncSync");
            super.msync(addr, len, async);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            count("munmap");
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openCleanRW(LPSZ name, long size) {
            countOpen("openCleanRW", name);
            return super.openCleanRW(name, size);
        }

        @Override
        public long openRO(LPSZ name) {
            countOpen("openRO", name);
            return super.openRO(name);
        }

        @Override
        public long openRONoCache(LPSZ path) {
            countOpen("openRONoCache", path);
            return super.openRONoCache(path);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            countOpen("openRW", name);
            return super.openRW(name, opts);
        }

        @Override
        public long openRWNoCache(LPSZ name, int opts) {
            countOpen("openRWNoCache", name);
            return super.openRWNoCache(name, opts);
        }

        @Override
        public long read(long fd, long buf, long size, long offset) {
            count("read");
            return super.read(fd, buf, size, offset);
        }

        @Override
        public long readIntAsUnsignedLong(long fd, long offset) {
            count("readIntAsUnsignedLong");
            return super.readIntAsUnsignedLong(fd, offset);
        }

        @Override
        public byte readNonNegativeByte(long fd, long offset) {
            count("readNonNegativeByte");
            return super.readNonNegativeByte(fd, offset);
        }

        @Override
        public int readNonNegativeInt(long fd, long offset) {
            count("readNonNegativeInt");
            return super.readNonNegativeInt(fd, offset);
        }

        @Override
        public long readNonNegativeLong(long fd, long offset) {
            count("readNonNegativeLong");
            return super.readNonNegativeLong(fd, offset);
        }

        @Override
        public int syncFileRange(long fd, long offset, long nbytes, int flags) {
            count("syncFileRange");
            return super.syncFileRange(fd, offset, nbytes, flags);
        }

        @Override
        public boolean truncate(long fd, long size) {
            count("truncate");
            return super.truncate(fd, size);
        }

        @Override
        public long write(long fd, long address, long len, long offset) {
            count("write");
            return super.write(fd, address, len, offset);
        }

        private static void increment(Map<String, int[]> counts, String key) {
            counts.computeIfAbsent(key, k -> new int[1])[0]++;
        }

        private static String join(Map<String, int[]> counts) {
            final StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, int[]> e : counts.entrySet()) {
                if (!sb.isEmpty()) {
                    sb.append(' ');
                }
                sb.append(e.getKey()).append('=').append(e.getValue()[0]);
            }
            return sb.toString();
        }

        private void count(String call) {
            if (Thread.currentThread() == measured) {
                increment(calls, call);
            }
        }

        private void countOpen(String call, LPSZ name) {
            if (Thread.currentThread() == measured) {
                increment(calls, call);
                final StringBuilder file = new StringBuilder();
                for (int i = 0, n = name.size(); i < n; i++) {
                    final char c = (char) (name.byteAt(i) & 0xFF);
                    if (c == '/' || c == '\\') {
                        file.setLength(0);
                    } else {
                        file.append(c);
                    }
                }
                increment(opened, file.toString());
            }
        }

        private void start(boolean isArmed) {
            stop();
            measured = isArmed ? Thread.currentThread() : null;
        }

        private void stop() {
            if (measured != null) {
                summaries.add("calls: " + join(calls) + "; opened: " + join(opened));
                calls.clear();
                opened.clear();
                measured = null;
            }
        }
    }
}
