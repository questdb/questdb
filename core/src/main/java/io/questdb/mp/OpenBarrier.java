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

package io.questdb.mp;

public final class OpenBarrier implements Barrier {
    public static final OpenBarrier INSTANCE = new OpenBarrier();

    private OpenBarrier() {
    }

    @Override
    public long availableIndex(long lo) {
        return Long.MAX_VALUE - 1;
    }

    @Override
    public long current() {
        return -1;
    }

    @Override
    public Barrier getBarrier() {
        return null;
    }

    @Override
    public WaitStrategy getWaitStrategy() {
        return NullWaitStrategy.INSTANCE;
    }

    @Override
    public Barrier root() {
        return this;
    }

    @Override
    public void setBarrier(Barrier barrier) {
    }

    @Override
    public void setCurrent(long value) {
        // ignored
    }

    @Override
    public Barrier then(Barrier barrier) {
        return null;
    }
}
