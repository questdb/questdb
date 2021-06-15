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
import io.questdb.std.datetime.millitime.MillisecondClockImpl;

public class DefaultIODispatcherConfiguration implements IODispatcherConfiguration {

    @Override
    public int getActiveConnectionLimit() {
        return 128;
    }

    @Override
    public int getBindIPv4Address() {
        return 0;
    }

    @Override
    public int getBindPort() {
        return 9001;
    }

    @Override
    public MillisecondClock getClock() {
        return MillisecondClockImpl.INSTANCE;
    }

    @Override
    public int getEventCapacity() {
        return 256;
    }

    @Override
    public int getIOQueueCapacity() {
        return 256;
    }

    @Override
    public long getIdleConnectionTimeout() {
        return 5 * 60 * 1000L;
    }

    @Override
    public int getInterestQueueCapacity() {
        return 1024;
    }

    @Override
    public int getListenBacklog() {
        return 128;
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    @Override
    public EpollFacade getEpollFacade() {
        return EpollFacadeImpl.INSTANCE;
    }

    @Override
    public SelectFacade getSelectFacade() {
        return SelectFacadeImpl.INSTANCE;
    }

    @Override
    public int getInitialBias() {
        return BIAS_READ;
    }

    @Override
    public int getSndBufSize() {
        return -1; // use system default
    }

    @Override
    public int getRcvBufSize() {
        return -1; // use system default
    }

    @Override
    public long getQueuedConnectionTimeout() {
        return 300_000;
    }

    @Override
    public boolean getPeerNoLinger() {
        return true;
    }
}
