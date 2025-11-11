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

package io.questdb.network;

import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.MillisecondClock;

public interface IODispatcherConfiguration {
    int BIAS_READ = 1;
    int BIAS_WRITE = 2;

    // millis
    long getAcceptLoopTimeout();

    int getBindIPv4Address();

    int getBindPort();

    MillisecondClock getClock();

    LongGauge getConnectionCountGauge();

    default String getDispatcherLogName() {
        return "IODispatcher";
    }

    EpollFacade getEpollFacade();

    default int getEventCapacity() {
        return Numbers.ceilPow2(Math.max(getLimit(), 64));
    }

    long getHeartbeatInterval();

    default boolean getHint() {
        return false;
    }

    default int getIOQueueCapacity() {
        return Numbers.ceilPow2(Math.max(getLimit(), 64));
    }

    default int getInitialBias() {
        return BIAS_READ;
    }

    default int getInterestQueueCapacity() {
        return Numbers.ceilPow2(Math.max(getLimit(), 64));
    }

    KqueueFacade getKqueueFacade();

    int getLimit();

    default int getListenBacklog() {
        if (Os.isWindows() && getHint()) {
            // Windows OS might have a limit of 200 concurrent connections. To overcome
            // this limit we set backlog value to a hint as described here:
            /// https://docs.microsoft.com/en-us/windows/win32/api/winsock2/nf-winsock2-listen
            // the hint is -backlog
            return -getLimit();
        }
        return getLimit();
    }

    // OS socket buffer size
    int getNetRecvBufferSize();

    // OS socket buffer size
    int getNetSendBufferSize();

    NetworkFacade getNetworkFacade();

    default boolean getPeerNoLinger() {
        return false;
    }

    long getQueueTimeout();

    // user-land buffer size
    int getRecvBufferSize();

    SelectFacade getSelectFacade();

    // user-land buffer size
    int getSendBufferSize();

    int getTestConnectionBufferSize();

    long getTimeout();

    Counter listenerStateChangeCounter();
}
