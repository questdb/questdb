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

package io.questdb;

import io.questdb.cutlass.http.HttpContextConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.NanosecondClock;
import io.questdb.std.NanosecondClockImpl;
import io.questdb.std.StationaryMillisClock;
import io.questdb.std.StationaryNanosClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;

class PropHttpContextConfiguration implements HttpContextConfiguration {

    private final int connectionPoolInitialCapacity;
    private final int connectionStringPoolCapacity;
    private final boolean httpAllowDeflateBeforeSend;
    private final int httpForceRecvFragmentationChunkSize;
    private final int httpForceSendFragmentationChunkSize;
    private final boolean httpFrozenClock;
    private final boolean httpReadOnlySecurityContext;
    private final int httpRecvBufferSize;
    private final int httpSendBufferSize;
    private final boolean httpServerCookiesEnabled;
    private final boolean httpServerKeepAlive;
    private final String httpVersion;
    private final boolean isReadOnlyInstance;
    private final int multipartHeaderBufferSize;
    private final long multipartIdleSpinCount;
    private final int requestHeaderBufferSize;
    private final ServerConfiguration serverConfiguration;

    PropHttpContextConfiguration(
            int connectionPoolInitialCapacity,
            int connectionStringPoolCapacity,
            ServerConfiguration serverConfiguration,
            boolean httpAllowDeflateBeforeSend,
            int httpForceRecvFragmentationChunkSize,
            int httpForceSendFragmentationChunkSize,
            boolean httpFrozenClock,
            boolean httpReadOnlySecurityContext,
            int httpRecvBufferSize,
            int httpSendBufferSize,
            boolean httpServerCookiesEnabled,
            boolean httpServerKeepAlive,
            String httpVersion,
            boolean isReadOnlyInstance,
            int multipartHeaderBufferSize,
            long multipartIdleSpinCount,
            int requestHeaderBufferSize
    ) {
        this.connectionPoolInitialCapacity = connectionPoolInitialCapacity;
        this.connectionStringPoolCapacity = connectionStringPoolCapacity;
        this.serverConfiguration = serverConfiguration;
        this.httpAllowDeflateBeforeSend = httpAllowDeflateBeforeSend;
        this.httpForceRecvFragmentationChunkSize = httpForceRecvFragmentationChunkSize;
        this.httpForceSendFragmentationChunkSize = httpForceSendFragmentationChunkSize;
        this.httpFrozenClock = httpFrozenClock;
        this.httpReadOnlySecurityContext = httpReadOnlySecurityContext;
        this.httpRecvBufferSize = httpRecvBufferSize;
        this.httpSendBufferSize = httpSendBufferSize;
        this.httpServerCookiesEnabled = httpServerCookiesEnabled;
        this.httpServerKeepAlive = httpServerKeepAlive;
        this.httpVersion = httpVersion;
        this.isReadOnlyInstance = isReadOnlyInstance;
        this.multipartHeaderBufferSize = multipartHeaderBufferSize;
        this.multipartIdleSpinCount = multipartIdleSpinCount;
        this.requestHeaderBufferSize = requestHeaderBufferSize;
    }

    @Override
    public boolean allowDeflateBeforeSend() {
        return httpAllowDeflateBeforeSend;
    }

    @Override
    public boolean areCookiesEnabled() {
        return httpServerCookiesEnabled;
    }

    @Override
    public int getConnectionPoolInitialCapacity() {
        return connectionPoolInitialCapacity;
    }

    @Override
    public int getConnectionStringPoolCapacity() {
        return connectionStringPoolCapacity;
    }

    @Override
    public boolean getDumpNetworkTraffic() {
        return false;
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        return serverConfiguration.getFactoryProvider();
    }

    @Override
    public int getForceRecvFragmentationChunkSize() {
        return httpForceRecvFragmentationChunkSize;
    }

    @Override
    public int getForceSendFragmentationChunkSize() {
        return httpForceSendFragmentationChunkSize;
    }

    @Override
    public String getHttpVersion() {
        return httpVersion;
    }

    @Override
    public MillisecondClock getMillisecondClock() {
        return httpFrozenClock ? StationaryMillisClock.INSTANCE : MillisecondClockImpl.INSTANCE;
    }

    @Override
    public int getMultipartHeaderBufferSize() {
        return multipartHeaderBufferSize;
    }

    @Override
    public long getMultipartIdleSpinCount() {
        return multipartIdleSpinCount;
    }

    @Override
    public NanosecondClock getNanosecondClock() {
        return httpFrozenClock ? StationaryNanosClock.INSTANCE : NanosecondClockImpl.INSTANCE;
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    @Override
    public int getRecvBufferSize() {
        return httpRecvBufferSize;
    }

    @Override
    public int getRequestHeaderBufferSize() {
        return requestHeaderBufferSize;
    }

    @Override
    public int getSendBufferSize() {
        return httpSendBufferSize;
    }

    @Override
    public boolean getServerKeepAlive() {
        return httpServerKeepAlive;
    }

    @Override
    public boolean readOnlySecurityContext() {
        return httpReadOnlySecurityContext || isReadOnlyInstance;
    }
}