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

package io.questdb;

import io.questdb.cutlass.http.client.HttpClientCookieHandlerFactory;
import io.questdb.network.*;

public class DefaultHttpClientConfiguration implements HttpClientConfiguration {
    public final static HttpClientConfiguration INSTANCE = new DefaultHttpClientConfiguration();
    private static final HttpClientCookieHandlerFactory COOKIE_HANDLER_FACTORY = () -> null;

    @Override
    public int getBufferSize() {
        return 64 * 1024;
    }

    @Override
    public HttpClientCookieHandlerFactory getCookieHandlerFactory() {
        return COOKIE_HANDLER_FACTORY;
    }

    @Override
    public EpollFacade getEpollFacade() {
        return EpollFacadeImpl.INSTANCE;
    }

    @Override
    public KqueueFacade getKQueueFacade() {
        return KqueueFacadeImpl.INSTANCE;
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    @Override
    public SelectFacade getSelectFacade() {
        return SelectFacadeImpl.INSTANCE;
    }

    @Override
    public int getTimeout() {
        return 60_000;
    }

    @Override
    public int getWaitQueueCapacity() {
        return 4;
    }
}
