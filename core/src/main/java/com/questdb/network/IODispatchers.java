/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.network;

import com.questdb.std.Os;

public class IODispatchers {

    private IODispatchers() {
    }

    public static <C extends IOContext> IODispatcher<C> create(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        switch (Os.type) {
            case Os.LINUX:
                return new IODispatcherLinux<>(configuration, ioContextFactory);
            case Os.OSX:
                return new IODispatcherOsx<>(configuration, ioContextFactory);
            case Os.WINDOWS:
                return new IODispatcherWindows<>(configuration, ioContextFactory);
            default:
                throw new RuntimeException();
        }
    }
}
