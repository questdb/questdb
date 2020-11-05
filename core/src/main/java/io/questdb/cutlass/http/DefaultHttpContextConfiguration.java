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

package io.questdb.cutlass.http;

import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.time.MillisecondClock;
import io.questdb.std.time.MillisecondClockImpl;

public class DefaultHttpContextConfiguration implements HttpContextConfiguration {
    @Override
    public int getConnectionPoolInitialCapacity() {
        return 16;
    }

    @Override
    public int getConnectionStringPoolCapacity() {
        return 128;
    }

    @Override
    public int getMultipartHeaderBufferSize() {
        return 512;
    }

    @Override
    public long getMultipartIdleSpinCount() {
        return 10_000;
    }

    @Override
    public int getRecvBufferSize() {
        return 1024 * 1024;
    }

    @Override
    public int getRequestHeaderBufferSize() {
        return 1024;
    }

    @Override
    public int getResponseHeaderBufferSize() {
        return 1024;
    }

    @Override
    public int getSendBufferSize() {
        return 1024 * 1024;
    }

    @Override
    public boolean getDumpNetworkTraffic() {
        return false;
    }

    @Override
    public boolean allowDeflateBeforeSend() {
        return false;
    }

    @Override
    public boolean readOnlySecurityContext() {
        return false;
    }

    @Override
    public boolean isInterruptOnClosedConnection() {
        return true;
    }

    @Override
    public int getInterruptorNIterationsPerCheck() {
        return 5;
    }

    @Override
    public int getInterruptorBufferSize() {
        return 64;
    }

    @Override
    public boolean getServerKeepAlive() {
        return true;
    }

    @Override
    public String getHttpVersion() {
        // trailing space is important
        return "HTTP/1.1 ";
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    @Override
    public MillisecondClock getClock() {
        return MillisecondClockImpl.INSTANCE;
    }
}
