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

public interface Barrier {
    long availableIndex(long lo);

    long current();

    Barrier getBarrier();

    WaitStrategy getWaitStrategy();

    Barrier root();

    void setBarrier(Barrier barrier);

    /**
     * When barrier is added mid-flight, it should assume the current
     * sequence of the publisher (otherwise known as barrier's barrier)
     * as its own. Such behaviour should prevent the newly joined
     * barrier from processing sequences since before the join time.
     * <p>
     * Most notably this is called by FanOut when new consumer joins
     * the cohort of existing consumers.
     *
     * @param value typically the sequence of the published
     */
    void setCurrent(long value);

    Barrier then(Barrier barrier);
}
