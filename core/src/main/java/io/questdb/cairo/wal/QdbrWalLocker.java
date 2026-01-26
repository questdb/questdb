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

package io.questdb.cairo.wal;

import io.questdb.cairo.TableToken;
import io.questdb.std.Os;
import io.questdb.std.str.DirectUtf8Sequence;
import org.jetbrains.annotations.NotNull;

public class QdbrWalLocker implements WalLocker {
    private long ptr;

    public QdbrWalLocker() {
        this.ptr = create();
    }

    @Override
    public void clear() {
        clear0(ptr);
    }

    @Override
    public void clearTable(@NotNull TableToken token) {
        final DirectUtf8Sequence tableDirName = token.getDirNameUtf8();
        clearTable0(ptr, tableDirName.ptr(), tableDirName.size());
    }

    @Override
    public void close() {
        destroy(ptr);
        ptr = 0;
    }

    @Override
    public boolean isSegmentLocked(@NotNull TableToken token, int walId, int segmentId) {
        final DirectUtf8Sequence tableDirName = token.getDirNameUtf8();
        return isSegmentLocked0(ptr, tableDirName.ptr(), tableDirName.size(), walId, segmentId);
    }

    @Override
    public boolean isWalLocked(@NotNull TableToken token, int walId) {
        final DirectUtf8Sequence tableDirName = token.getDirNameUtf8();
        return isWalLocked0(ptr, tableDirName.ptr(), tableDirName.size(), walId);
    }

    @Override
    public int lockPurge(@NotNull TableToken token, int walId) {
        // Race safety: minSegmentId is only mutated in 2 cases:
        //  - when the purge lock is released: as we assume that no 2 purge jobs will run concurrently
        //  on the same WAL, this read is safe.
        //  - when {@link #setWalSegmentMinId(CharSequence, int, int)} is called: this is safe as the caller
        //  must ensure that the new minSegmentId is greater than or equal to the current minSegmentId.
        //  A stale read returns a conservative (lower) max-purgeable ID, which is safe - we may under-purge
        //  but never over-purge.
        final DirectUtf8Sequence tableDirName = token.getDirNameUtf8();
        final int lockedMinSegmentId = lockPurge0(ptr, tableDirName.ptr(), tableDirName.size(), walId);
        if (lockedMinSegmentId == Integer.MAX_VALUE) {
            // We have exclusive lock, whole WAL can be purged
            return WalUtils.SEG_NONE_ID;
        } else {
            return lockedMinSegmentId - 1;
        }
    }

    @Override
    public void lockWriter(@NotNull TableToken token, int walId, int minSegmentId) {
        final DirectUtf8Sequence tableDirName = token.getDirNameUtf8();
        lockWriter0(ptr, tableDirName.ptr(), tableDirName.size(), walId, minSegmentId);
    }

    @Override
    public void setWalSegmentMinId(@NotNull TableToken token, int walId, int newMinSegmentId) {
        final DirectUtf8Sequence tableDirName = token.getDirNameUtf8();
        setWalSegmentMinId0(ptr, tableDirName.ptr(), tableDirName.size(), walId, newMinSegmentId);
    }

    @Override
    public void unlockPurge(@NotNull TableToken token, int walId) {
        final DirectUtf8Sequence tableDirName = token.getDirNameUtf8();
        unlockPurge0(ptr, tableDirName.ptr(), tableDirName.size(), walId);
    }

    @Override
    public void unlockWriter(@NotNull TableToken token, int walId) {
        final DirectUtf8Sequence tableDirName = token.getDirNameUtf8();
        unlockWriter0(ptr, tableDirName.ptr(), tableDirName.size(), walId);
    }

    // Java_io_questdb_std_cairo_wal_QdbrWalLocker_clear0
    private static native void clear0(long ptr);

    // Java_io_questdb_std_cairo_wal_QdbrWalLocker_clearTable0
    private static native void clearTable0(long ptr, long tableDirNamePtr, int tableDirNameSize);

    // Java_io_questdb_std_cairo_wal_QdbrWalLocker_create
    private static native long create();

    // Java_io_questdb_std_cairo_wal_QdbrWalLocker_destroy
    private static native void destroy(long ptr);

    // Java_io_questdb_std_cairo_wal_QdbrWalLocker_isSegmentLocked0
    private static native boolean isSegmentLocked0(long ptr, long tableDirNamePtr, int tableDirNameSize, int walId, int segmentId);

    // Java_io_questdb_std_cairo_wal_QdbrWalLocker_isWalLocked0
    private static native boolean isWalLocked0(long ptr, long tableDirNamePtr, int tableDirNameSize, int walId);

    // Java_io_questdb_std_cairo_wal_QdbrWalLocker_lockPurge0
    private static native int lockPurge0(long ptr, long tableDirNamePtr, int tableDirNameSize, int walId);

    // Java_io_questdb_std_cairo_wal_QdbrWalLocker_lockWriter0
    private static native void lockWriter0(long ptr, long tableDirNamePtr, int tableDirNameSize, int walId, int minSegmentId);

    // Java_io_questdb_std_cairo_wal_QdbrWalLocker_setWalSegmentMinId0
    private static native void setWalSegmentMinId0(long ptr, long tableDirNamePtr, int tableDirNameSize, int walId, int newMinSegmentId);

    // Java_io_questdb_std_cairo_wal_QdbrWalLocker_unlockPurge0
    private static native void unlockPurge0(long ptr, long tableDirNamePtr, int tableDirNameSize, int walId);

    // Java_io_questdb_std_cairo_wal_QdbrWalLocker_unlockWriter0
    private static native void unlockWriter0(long ptr, long tableDirNamePtr, int tableDirNameSize, int walId);

    static {
        Os.init();
    }
}
