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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Fails, once armed, a single read of a live view's own files: the next open of its table's
 * {@code _txn}, which a reader of the table the pool cannot serve makes, or the next open of its
 * checkpoint timeline's superblock, {@code _timeline}, which every read of the timeline makes.
 * Either fault is transient: the open after it goes ahead.
 */
class LiveViewTableReadFault extends TestFilesFacadeImpl {
    private final AtomicInteger failedTimelineOpens = new AtomicInteger();
    private final AtomicInteger failedTxnOpens = new AtomicInteger();
    private final AtomicReference<String> timelineDirName = new AtomicReference<>();
    private final AtomicReference<String> txnDirName = new AtomicReference<>();

    private static boolean isFileOf(LPSZ name, String dirName, String fileName) {
        return Utf8s.containsAscii(name, Files.SEPARATOR + dirName + Files.SEPARATOR)
                && Utf8s.endsWithAscii(name, Files.SEPARATOR + fileName);
    }

    @Override
    public long openRO(LPSZ name) {
        final String dirName = txnDirName.get();
        if (dirName != null && isFileOf(name, dirName, TableUtils.TXN_FILE_NAME) && txnDirName.compareAndSet(dirName, null)) {
            failedTxnOpens.incrementAndGet();
            return -1;
        }
        return super.openRO(name);
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        final String dirName = timelineDirName.get();
        if (dirName != null
                && isFileOf(name, dirName, LiveViewCheckpointLayout.TIMELINE_FILE_NAME)
                && timelineDirName.compareAndSet(dirName, null)) {
            failedTimelineOpens.incrementAndGet();
            return -1;
        }
        return super.openRW(name, opts);
    }

    // Fails the next open of the view's _timeline.
    void failTheNextTimelineRead(LiveViewInstance instance) {
        timelineDirName.set(instance.getLiveViewToken().getDirName());
    }

    // Fails the next read of the view's table: evicts the pooled readers, which would serve the
    // read without opening a file, and fails the next open of the table's _txn. The caller must
    // hold no reader of the view's table.
    void failTheNextViewTableRead(CairoEngine engine, LiveViewInstance instance) {
        Assert.assertTrue("no reader of the view's table may be in use", engine.lockReadersByTableToken(instance.getLiveViewToken()));
        engine.unlockReaders(instance.getLiveViewToken());
        txnDirName.set(instance.getLiveViewToken().getDirName());
    }

    int getFailedTimelineOpens() {
        return failedTimelineOpens.get();
    }

    int getFailedTxnOpens() {
        return failedTxnOpens.get();
    }
}
