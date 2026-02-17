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

package io.questdb;

import io.questdb.cutlass.http.client.HttpClientCookieHandlerFactory;
import io.questdb.network.EpollFacade;
import io.questdb.network.EpollFacadeImpl;
import io.questdb.network.KqueueFacade;
import io.questdb.network.KqueueFacadeImpl;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.SelectFacade;
import io.questdb.network.SelectFacadeImpl;

public interface HttpClientConfiguration {
    default boolean fixBrokenConnection() {
        return true;
    }

    HttpClientCookieHandlerFactory getCookieHandlerFactory();

    default EpollFacade getEpollFacade() {
        return EpollFacadeImpl.INSTANCE;
    }

    default int getInitialRequestBufferSize() {
        return Math.min(64 * 1024, getMaximumRequestBufferSize());
    }

    default KqueueFacade getKQueueFacade() {
        return KqueueFacadeImpl.INSTANCE;
    }

    default int getMaximumRequestBufferSize() {
        return Integer.MAX_VALUE;
    }

    default NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    default int getResponseBufferSize() {
        return 64 * 1024;
    }

    default SelectFacade getSelectFacade() {
        return SelectFacadeImpl.INSTANCE;
    }

    default String getSettingsPath() {
        return "/settings";
    }

    default int getTimeout() {
        return 600_000;
    }

    default int getWaitQueueCapacity() {
        return 4;
    }
}
