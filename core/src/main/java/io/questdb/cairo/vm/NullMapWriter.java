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

package io.questdb.cairo.vm;

import io.questdb.cairo.MapWriter;
import io.questdb.cairo.SymbolValueCountCollector;
import io.questdb.cairo.vm.api.MemoryR;

public class NullMapWriter implements MapWriter {
    public static final MapWriter INSTANCE = new NullMapWriter();

    @Override
    public int getColumnIndex() {
        return -1;
    }

    @Override
    public boolean getNullFlag() {
        return false;
    }

    @Override
    public int getSymbolCapacity() {
        return -1;
    }

    @Override
    public int getSymbolCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MemoryR getSymbolOffsetsMemory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MemoryR getSymbolValuesMemory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCached() {
        return true;
    }

    @Override
    public int put(char c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int put(CharSequence symbol) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int put(CharSequence symbol, SymbolValueCountCollector valueCountCollector) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback(int symbolCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSymbolIndexInTxWriter(int symbolIndexInTxWriter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync(boolean async) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void truncate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCacheFlag(boolean flag) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNullFlag(boolean flag) {
        throw new UnsupportedOperationException();
    }
}
