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

import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.MemoryFR;

/**
 * Fixed page memory implementation. It augments a pointer of fixed size with accessor methods without
 * owning the pointer. Therefore, memory cannot be extended.
 */
public class MemoryFCRImpl extends AbstractMemoryCR implements MemoryFR, MemoryCR {

    @Override
    public long addressHi() {
        return lim;
    }

    @Override
    public void close() {
        // nothing to do, we do not own the memory
        this.pageAddress = 0;
    }

    @Override
    public void extend(long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getFd() {
        return -1;
    }

    @Override
    public void of(long pageAddress, long size) {
        this.pageAddress = pageAddress;
        this.size = size;
        this.lim = pageAddress + size;
    }
}
