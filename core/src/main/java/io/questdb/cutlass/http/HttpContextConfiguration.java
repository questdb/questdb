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

import io.questdb.FactoryProvider;
import io.questdb.Metrics;
import io.questdb.network.NetworkFacade;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.Clock;

public interface HttpContextConfiguration {

    boolean allowDeflateBeforeSend();

    boolean areCookiesEnabled();

    int getConnectionPoolInitialCapacity();

    int getConnectionStringPoolCapacity();

    boolean getDumpNetworkTraffic();

    FactoryProvider getFactoryProvider();

    int getForceRecvFragmentationChunkSize();

    int getForceSendFragmentationChunkSize();

    String getHttpVersion();

    int getIlpConnectionLimit();

    int getJsonQueryConnectionLimit();

    Metrics getMetrics();

    MillisecondClock getMillisecondClock();

    int getMultipartHeaderBufferSize();

    long getMultipartIdleSpinCount();

    Clock getNanosecondClock();

    NetworkFacade getNetworkFacade();

    int getRequestHeaderBufferSize();

    boolean getServerKeepAlive();

    boolean readOnlySecurityContext();
}
