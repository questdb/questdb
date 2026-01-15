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

import io.questdb.std.Os;

import java.io.Closeable;

public class WalLocker implements WalLockManager.WalLocker, Closeable {
    private long ptr;

    public WalLocker() {
        this.ptr = create();
    }

    @Override
    public void clear() {
        clear0(ptr);
    }

    @Override
    public void close() {
        destroy(ptr);
        ptr = 0;
    }

    @Override
    public boolean isSegmentLocked(int tableId, int walId, int segmentId) {
        return isSegmentLocked0(ptr, tableId, walId, segmentId);
    }

    @Override
    public boolean isWalLocked(int tableId, int walId) {
        return isWalLocked0(ptr, tableId, walId);
    }

    @Override
    public int lockPurge(int tableId, int walId) {
        return lockPurge0(ptr, tableId, walId);
    }

    @Override
    public void lockWriter(int tableId, int walId, int minSegmentId) {
        lockWriter0(ptr, tableId, walId, minSegmentId);
    }

    @Override
    public void setWalSegmentMinId(int tableId, int walId, int newMinSegmentId) {
        setWalSegmentMinId0(ptr, tableId, walId, newMinSegmentId);
    }

    @Override
    public void unlockPurge(int tableId, int walId) {
        unlockPurge0(ptr, tableId, walId);
    }

    @Override
    public void unlockWriter(int tableId, int walId) {
        unlockWriter0(ptr, tableId, walId);
    }

    // Java_io_questdb_std_cairo_wal_WalLocker_clear0
    private static native void clear0(long ptr);

    // Java_io_questdb_std_cairo_wal_WalLocker_create
    private static native long create();

    // Java_io_questdb_std_cairo_wal_WalLocker_destroy
    private static native void destroy(long ptr);

    // Java_io_questdb_std_cairo_wal_WalLocker_isSegmentLocked0
    private static native boolean isSegmentLocked0(long ptr, int tableId, int walId, int segmentId);

    // Java_io_questdb_std_cairo_wal_WalLocker_isWalLocked0
    private static native boolean isWalLocked0(long ptr, int tableId, int walId);

    // Java_io_questdb_std_cairo_wal_WalLocker_lockPurge0
    private static native int lockPurge0(long ptr, int tableId, int walId);

    // Java_io_questdb_std_cairo_wal_WalLocker_lockWriter0
    private static native void lockWriter0(long ptr, int tableId, int walId, int minSegmentId);

    // Java_io_questdb_std_cairo_wal_WalLocker_setWalSegmentMinId0
    private static native void setWalSegmentMinId0(long ptr, int tableId, int walId, int newMinSegmentId);

    // Java_io_questdb_std_cairo_wal_WalLocker_unlockPurge0
    private static native void unlockPurge0(long ptr, int tableId, int walId);

    // Java_io_questdb_std_cairo_wal_WalLocker_unlockWriter0
    private static native void unlockWriter0(long ptr, int tableId, int walId);

    static {
        Os.init();
    }
}
