/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo.vm;

import io.questdb.cairo.vm.api.*;
import io.questdb.std.Misc;

public class ReplMemoryLogCAImpl implements MemoryMATL {
    private MemoryA log;
    private MemoryMAT main;

    @Override
    public void close() {
        log = Misc.free(log);
        main = Misc.free(main);
    }

    @Override
    public long getAddress() {
        return main.getAddress();
    }

    @Override
    public long getAppendOffset() {
        return main.getAppendOffset();
    }

    @Override
    public void jumpTo(long offset) {
        main.jumpTo(offset);
    }

    @Override
    public void of(MemoryA log, MemoryMAT main) {
        this.log = log;
        this.main = main;
    }

    @Override
    public void putLong128(long timestamp, long row) {
        // O3 timestamp memory stores timestamp and row
        // transaction log does not need row numbers
        log.putLong(timestamp);
        main.putLong128(timestamp, row);
    }
}
