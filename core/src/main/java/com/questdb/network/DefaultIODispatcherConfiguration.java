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

import com.questdb.std.time.MillisecondClock;
import com.questdb.std.time.MillisecondClockImpl;

public class DefaultIODispatcherConfiguration implements IODispatcherConfiguration {

    @Override
    public int getActiveConnectionLimit() {
        return 10;
    }

    @Override
    public int getBindIPv4Address() {
        return 0;
    }

    @Override
    public int getBindPort() {
        return 9001;
    }

    @Override
    public MillisecondClock getClock() {
        return MillisecondClockImpl.INSTANCE;
    }

    @Override
    public int getEventCapacity() {
        return 1024;
    }

    @Override
    public int getIOQueueCapacity() {
        return 1024;
    }

    @Override
    public long getIdleConnectionTimeout() {
        return 5 * 60 * 1000L;
    }

    @Override
    public int getInterestQueueCapacity() {
        return 1024;
    }

    @Override
    public int getListenBacklog() {
        return 50000;
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    @Override
    public EpollFacade getEpollFacade() {
        return EpollFacadeImpl.INSTANCE;
    }

    @Override
    public SelectFacade getSelectFacade() {
        return SelectFacadeImpl.INSTANCE;
    }

    @Override
    public int getInitialBias() {
        return IODispatcherConfiguration.BIAS_READ;
    }

    @Override
    public int getSndBufSize() {
        return -1; // use system default
    }

    @Override
    public int getRcvBufSize() {
        return -1; // use system default
    }
}
