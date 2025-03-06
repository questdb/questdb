/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.NullCounter;
import io.questdb.metrics.NullLongGauge;

public class DefaultConcurrentCacheConfiguration implements ConcurrentCacheConfiguration {
    public static final ConcurrentCacheConfiguration DEFAULT = new DefaultConcurrentCacheConfiguration();

    @Override
    public int getBlocks() {
        return 2;
    }

    @Override
    public LongGauge getCachedGauge() {
        return NullLongGauge.INSTANCE;
    }

    @Override
    public Counter getHiCounter() {
        return NullCounter.INSTANCE;
    }

    @Override
    public Counter getMissCounter() {
        return NullCounter.INSTANCE;
    }

    @Override
    public int getRows() {
        return 8;
    }
}
