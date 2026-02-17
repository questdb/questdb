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

package io.questdb.std;

import io.questdb.cairo.ColumnTypes;

public class RostiAllocFacadeImpl implements RostiAllocFacade {

    public static final RostiAllocFacade INSTANCE = new RostiAllocFacadeImpl();

    @Override
    public long alloc(ColumnTypes types, long capacity) {
        return Rosti.alloc(types, capacity);
    }

    @Override
    public void clear(long pRosti) {
        Rosti.clear(pRosti);
    }

    @Override
    public void free(long pRosti) {
        Rosti.free(pRosti);
    }

    @Override
    public long getSize(long pRosti) {
        return Rosti.getSize(pRosti);
    }

    @Override
    public boolean reset(long pRosti, int toSize) {
        return Rosti.reset(pRosti, toSize);
    }

    @Override
    public void updateMemoryUsage(long pRosti, long oldSize) {
        Rosti.updateMemoryUsage(pRosti, oldSize);
    }
}
