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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;

/**
 * Fails one write a live view makes under its {@code _checkpoints} directory the way a full disk
 * fails it, and counts the writes it lets through.
 * <p>
 * Every checkpoint file goes through a mapped memory: {@code openRW}, {@code allocate} to reserve
 * the blocks, {@code mmap}, {@code mremap} each time the file grows, {@code msync} unless the
 * commit mode is nosync, and {@code truncate} back to the written length when the mapping closes.
 * Each of those is an op this facade can fail. {@code allocate} and {@code truncate} return false,
 * as {@code ENOSPC} makes them; {@code mmap} and {@code mremap} return {@code MAP_FAILED}; and
 * {@code msync} throws, as {@code FilesFacadeImpl.msync} does on a failed call. The errno read
 * right after a failed op reports {@code ENOSPC}.
 * <p>
 * {@link #record()} starts a log of the ops made on checkpoint files, so a test can see what one
 * seal writes and walk a failure across it. {@link #arm} fails the {@code nth} matching op from
 * then on, once. {@link #getSyncCount} reports how often each kind of checkpoint file was synced
 * with each flag, which tells a test that the commit mode reached the file. The facade issues
 * every checkpoint file's msync without waiting for the writeback, whatever flag the writer asked
 * for, and skips the syncs of every other file.
 */
final class LiveViewCheckpointDiskFullFault extends TestFilesFacadeImpl {
    static final int OP_ALLOCATE = 0;
    static final int OP_MMAP = 1;
    static final int OP_MREMAP = 2;
    static final int OP_MSYNC = 3;
    static final int OP_TRUNCATE = 4;
    private static final String CHECKPOINTS_DIR = Files.SEPARATOR + LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME + Files.SEPARATOR;
    // The errno ENOSPC has on Linux and macOS.
    private static final int ENOSPC = 28;
    private static final int OP_COUNT = 5;
    // Each live mapping of a checkpoint file, from its address to the file's name under _checkpoints.
    private final LongObjHashMap<String> mappedFiles = new LongObjHashMap<>();
    // Each checkpoint file open for writing, from its descriptor to its name under _checkpoints.
    private final LongObjHashMap<String> openFiles = new LongObjHashMap<>();
    // Per op, the name of the file each call since record() was made on, in call order.
    private final ObjList<ObjList<String>> recordedFiles = new ObjList<>();
    // Syncs per file kind and flag; see syncKey.
    private final CharSequenceIntHashMap syncCounts = new CharSequenceIntHashMap(8, 0.5, 0);
    // The kind of file (see kindOf) an op must be made on to match.
    private String armedKind;
    // The op to fail, or -1 when disarmed.
    private int armedOp = -1;
    // Matching ops still to let through before the one to fail.
    private int armedSkip;
    // The name of the file whose op failed, or null while none has.
    private String failedFile;
    private boolean isErrnoPending;
    private boolean isRecording;

    LiveViewCheckpointDiskFullFault() {
        for (int i = 0; i < OP_COUNT; i++) {
            recordedFiles.add(new ObjList<>());
        }
    }

    /**
     * The kind of a checkpoint file a sync or a failure names: the directory a segment lives
     * in ({@code data}, {@code meta} or {@code repair}), or a root-level file's own name
     * without its {@code .tmp} suffix.
     */
    static String kindOf(String file) {
        final int slash = file.indexOf(Files.SEPARATOR);
        if (slash > 0) {
            return file.substring(0, slash);
        }
        return file.endsWith(LiveViewCheckpointLayout.TMP_SUFFIX)
                ? file.substring(0, file.length() - LiveViewCheckpointLayout.TMP_SUFFIX.length())
                : file;
    }

    static String opName(int op) {
        return switch (op) {
            case OP_ALLOCATE -> "allocate";
            case OP_MMAP -> "mmap";
            case OP_MREMAP -> "mremap";
            case OP_MSYNC -> "msync";
            case OP_TRUNCATE -> "truncate";
            default -> throw new IllegalArgumentException("unknown op " + op);
        };
    }

    @Override
    public synchronized boolean allocate(long fd, long size) {
        final String file = openFiles.get(fd);
        if (file != null && isFailing(OP_ALLOCATE, file)) {
            return false;
        }
        return super.allocate(fd, size);
    }

    @Override
    public synchronized boolean close(long fd) {
        openFiles.remove(fd);
        return super.close(fd);
    }

    @Override
    public synchronized int errno() {
        if (isErrnoPending) {
            isErrnoPending = false;
            return ENOSPC;
        }
        return super.errno();
    }

    @Override
    public synchronized void fsync(long fd) {
        // As in msync, only a checkpoint file reaches the disk.
        if (openFiles.get(fd) != null) {
            super.fsync(fd);
        }
    }

    @Override
    public synchronized long mmap(long fd, long len, long offset, int flags, int memoryTag) {
        final String file = (flags & Files.MAP_RW) != 0 ? openFiles.get(fd) : null;
        if (file != null && isFailing(OP_MMAP, file)) {
            return FilesFacade.MAP_FAILED;
        }
        final long address = super.mmap(fd, len, offset, flags, memoryTag);
        if (file != null && address != FilesFacade.MAP_FAILED) {
            mappedFiles.put(address, file);
        }
        return address;
    }

    @Override
    public synchronized long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        final String file = openFiles.get(fd);
        if (file != null && isFailing(OP_MREMAP, file)) {
            return FilesFacade.MAP_FAILED;
        }
        final long address = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        if (file != null && address != FilesFacade.MAP_FAILED) {
            mappedFiles.remove(addr);
            mappedFiles.put(address, file);
        }
        return address;
    }

    @Override
    public synchronized void msync(long addr, long len, boolean async) {
        final String file = mappedFiles.get(addr);
        if (file == null) {
            // A syncing commit mode would otherwise flush every table file the test writes,
            // which costs seconds and tells nothing about the checkpoint writers. No test crashes
            // the machine, so a skipped sync is invisible to it.
            return;
        }
        if (isFailing(OP_MSYNC, file)) {
            isErrnoPending = false;
            throw CairoException.critical(ENOSPC).put("could not msync");
        }
        final String key = syncKey(kindOf(file), async);
        syncCounts.put(key, syncCounts.get(key) + 1);
        // The count keeps the flag the writer asked for. The call itself schedules the writeback
        // rather than waiting for it, which still has the kernel check the mapping's range, and
        // spares a seal-heavy test hundreds of disk flushes.
        super.msync(addr, len, true);
    }

    @Override
    public synchronized void munmap(long address, long size, int memoryTag) {
        mappedFiles.remove(address);
        super.munmap(address, size, memoryTag);
    }

    @Override
    public synchronized long openCleanRW(LPSZ name, long size) {
        final long fd = super.openCleanRW(name, size);
        track(name, fd);
        return fd;
    }

    @Override
    public synchronized long openRW(LPSZ name, int opts) {
        final long fd = super.openRW(name, opts);
        track(name, fd);
        return fd;
    }

    @Override
    public synchronized boolean truncate(long fd, long size) {
        final String file = openFiles.get(fd);
        if (file != null && isFailing(OP_TRUNCATE, file)) {
            return false;
        }
        return super.truncate(fd, size);
    }

    /**
     * Fails the {@code nth} {@code op} made from now on on a checkpoint file of {@code fileKind}
     * (see {@link #kindOf}), once.
     */
    synchronized void arm(int op, String fileKind, int nth) {
        assert nth > 0;
        armedOp = op;
        armedKind = fileKind;
        armedSkip = nth - 1;
        failedFile = null;
    }

    synchronized void disarm() {
        armedOp = -1;
    }

    /**
     * The name under {@code _checkpoints} of the file whose op failed, or null while the armed
     * op has not been reached.
     */
    synchronized String getFailedFile() {
        return failedFile;
    }

    /**
     * The files the calls of {@code op} since {@link #record()} were made on, in call order.
     */
    synchronized ObjList<String> getRecorded(int op) {
        final ObjList<String> copy = new ObjList<>();
        copy.addAll(recordedFiles.getQuick(op));
        return copy;
    }

    /**
     * How many times a checkpoint file of {@code fileKind} (see {@link #kindOf}) has been synced
     * with the {@code async} flag {@code isAsync}.
     */
    synchronized int getSyncCount(String fileKind, boolean isAsync) {
        return syncCounts.get(syncKey(fileKind, isAsync));
    }

    /**
     * Starts the log {@link #getRecorded} reads from scratch.
     */
    synchronized void record() {
        for (int i = 0; i < OP_COUNT; i++) {
            recordedFiles.getQuick(i).clear();
        }
        isRecording = true;
    }

    synchronized void stopRecording() {
        isRecording = false;
    }

    private static String syncKey(String fileKind, boolean isAsync) {
        return fileKind + (isAsync ? ":async" : ":sync");
    }

    private boolean isFailing(int op, String file) {
        if (isRecording) {
            recordedFiles.getQuick(op).add(file);
        }
        if (armedOp != op || !armedKind.equals(kindOf(file))) {
            return false;
        }
        if (armedSkip-- > 0) {
            return false;
        }
        armedOp = -1;
        failedFile = file;
        isErrnoPending = true;
        return true;
    }

    private void track(LPSZ name, long fd) {
        if (fd < 0) {
            return;
        }
        final String path = Utf8s.stringFromUtf8Bytes(name);
        final int dirIndex = path.lastIndexOf(CHECKPOINTS_DIR);
        if (dirIndex >= 0) {
            openFiles.put(fd, path.substring(dirIndex + CHECKPOINTS_DIR.length()));
        }
    }
}
