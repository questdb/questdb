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

package io.questdb.cutlass.http;

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.Metrics;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.nanotime.NanosecondClockImpl;

public class DefaultHttpContextConfiguration implements HttpContextConfiguration {

    @Override
    public boolean allowDeflateBeforeSend() {
        return false;
    }

    @Override
    public boolean areCookiesEnabled() {
        return true;
    }

    @Override
    public int getConnectionPoolInitialCapacity() {
        return 16;
    }

    @Override
    public int getConnectionStringPoolCapacity() {
        return 128;
    }

    @Override
    public boolean getDumpNetworkTraffic() {
        return false;
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        return DefaultFactoryProvider.INSTANCE;
    }

    @Override
    public int getForceRecvFragmentationChunkSize() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getForceSendFragmentationChunkSize() {
        return Integer.MAX_VALUE;
    }

    @Override
    public String getHttpVersion() {
        // trailing space is important
        return "HTTP/1.1 ";
    }

    @Override
    public int getIlpConnectionLimit() {
        return -1;
    }

    @Override
    public int getJsonQueryConnectionLimit() {
        return -1;
    }

    @Override
    public Metrics getMetrics() {
        return Metrics.ENABLED;
    }

    @Override
    public MillisecondClock getMillisecondClock() {
        return MillisecondClockImpl.INSTANCE;
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
    public Clock getNanosecondClock() {
        return NanosecondClockImpl.INSTANCE;
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    @Override
    public int getRequestHeaderBufferSize() {
        return 4096;
    }

    @Override
    public boolean getServerKeepAlive() {
        return true;
    }

    @Override
    public boolean readOnlySecurityContext() {
        return false;
    }
}
