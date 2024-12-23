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

import io.questdb.metrics.LongGauge;
import io.questdb.std.Os;

public class IODispatchers {

    private IODispatchers() {
    }

    public static <C extends IOContext<C>> IODispatcher<C> create(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory,
            LongGauge connectionCountGauge
    ) {
        switch (Os.type) {
            case Os.LINUX:
                return new IODispatcherLinux<>(configuration, ioContextFactory, connectionCountGauge);
            case Os.DARWIN:
            case Os.FREEBSD:
                return new IODispatcherOsx<>(configuration, ioContextFactory, connectionCountGauge);
            case Os.WINDOWS:
                return new IODispatcherWindows<>(configuration, ioContextFactory, connectionCountGauge);
            default:
                throw new RuntimeException();
        }
    }
}
