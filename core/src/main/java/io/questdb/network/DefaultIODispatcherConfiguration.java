/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
    public EpollFacade getEpollFacade() {
        return EpollFacadeImpl.INSTANCE;
    }

    @Override
    public long getHeartbeatInterval() {
        // don't send heartbeat messages by default
        return -1L;
    }

    @Override
    public int getInitialBias() {
        return BIAS_READ;
    }

    @Override
    public KqueueFacade getKqueueFacade() {
        return KqueueFacadeImpl.INSTANCE;
    }

    @Override
    public int getLimit() {
        return 64;
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    @Override
    public long getQueueTimeout() {
        return 300_000;
    }

    @Override
    public int getRcvBufSize() {
        return -1; // use system default
    }

    @Override
    public SelectFacade getSelectFacade() {
        return SelectFacadeImpl.INSTANCE;
    }

    @Override
    public int getSndBufSize() {
        return -1; // use system default
    }

    @Override
    public int getTestConnectionBufferSize() {
        return 64;
    }

    @Override
    public long getTimeout() {
        return 5 * 60 * 1000L;
    }
}
