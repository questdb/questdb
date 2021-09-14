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

package io.questdb.network;

import io.questdb.std.datetime.millitime.MillisecondClock;

public interface IODispatcherConfiguration {
    int BIAS_READ = 1;
    int BIAS_WRITE = 2;

    int getActiveConnectionLimit();

    int getBindIPv4Address();

    int getBindPort();

    MillisecondClock getClock();

    default String getDispatcherLogName() {
        return "IODispatcher";
    }

    EpollFacade getEpollFacade();

    int getEventCapacity();

    int getIOQueueCapacity();

    long getIdleConnectionTimeout();

    int getInitialBias();

    int getInterestQueueCapacity();

    int getListenBacklog();

    NetworkFacade getNetworkFacade();

    default boolean getPeerNoLinger() {
        return false;
    }

    int getRcvBufSize();

    SelectFacade getSelectFacade();

    int getSndBufSize();

    long getQueuedConnectionTimeout();
}
