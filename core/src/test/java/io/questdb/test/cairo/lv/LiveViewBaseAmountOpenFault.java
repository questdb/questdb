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

import io.questdb.cairo.CairoException;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Acts on the opens of the base's {@code amount} column file outside its WAL, which a scan of the
 * applied base makes and a raw-WAL drain never does: fails every such open in one partition until
 * lifted, reporting a missing file, and runs an action once, at the first such open in another, which
 * then goes ahead.
 */
class LiveViewBaseAmountOpenFault extends TestFilesFacadeImpl {
    private final AtomicReference<String> failedPartition = new AtomicReference<>();
    private final AtomicBoolean isMissingFileErrnoPending = new AtomicBoolean();
    private final AtomicReference<Runnable> openAction = new AtomicReference<>();
    private volatile String actionPartition;
    private volatile String baseDir;

    @Override
    public int errno() {
        return isMissingFileErrnoPending.compareAndSet(true, false) ? CairoException.ERRNO_FILE_DOES_NOT_EXIST : super.errno();
    }

    @Override
    public long openRO(LPSZ name) {
        final String dir = baseDir;
        if (dir != null
                && Utf8s.containsAscii(name, dir)
                && !Utf8s.containsAscii(name, "wal")
                && Utf8s.endsWithAscii(name, Files.SEPARATOR + "amount.d")) {
            final String failed = failedPartition.get();
            if (failed != null && Utf8s.containsAscii(name, failed)) {
                isMissingFileErrnoPending.set(true);
                return -1;
            }
            final String partition = actionPartition;
            if (partition != null && Utf8s.containsAscii(name, partition)) {
                final Runnable action = openAction.getAndSet(null);
                if (action != null) {
                    action.run();
                }
            }
        }
        return super.openRO(name);
    }

    // Fails every open in the partition, or none with null.
    void failOpensIn(String partition) {
        failedPartition.set(partition);
    }

    void of(String baseDir) {
        this.baseDir = baseDir;
        failedPartition.set(null);
        openAction.set(null);
    }

    void runAtFirstOpen(String partition, Runnable action) {
        actionPartition = partition;
        openAction.set(action);
    }
}
