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

import io.questdb.std.Os;

public class IODispatchers {

    private IODispatchers() {
    }

    public static <C extends IOContext> IODispatcher<C> create(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        switch (Os.type) {
            case Os.LINUX_AMD64:
            case Os.LINUX_ARM64:
                return new IODispatcherLinux<>(configuration, ioContextFactory);
            case Os.OSX_AMD64:
            case Os.OSX_ARM64:
            case Os.FREEBSD:
                return new IODispatcherOsx<>(configuration, ioContextFactory);
            case Os.WINDOWS:
                return new IODispatcherWindows<>(configuration, ioContextFactory);
            default:
                throw new RuntimeException();
        }
    }
}
