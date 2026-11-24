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

import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Once armed, refuses every unlink of a view's published repair marker,
 * {@code _checkpoints/_repairing}, and leaves the file in place: what a clear that fails
 * between a repair's last publication and the end of its turn leaves behind.
 */
final class LiveViewMarkerClearFault extends TestFilesFacadeImpl {
    private final AtomicBoolean isArmed = new AtomicBoolean();
    private final AtomicInteger refusedClears = new AtomicInteger();

    @Override
    public boolean removeQuiet(LPSZ name) {
        if (isArmed.get() && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)) {
            refusedClears.incrementAndGet();
            return false;
        }
        return super.removeQuiet(name);
    }

    void arm() {
        refusedClears.set(0);
        isArmed.set(true);
    }

    void disarm() {
        isArmed.set(false);
    }

    int getRefusedClears() {
        return refusedClears.get();
    }
}
