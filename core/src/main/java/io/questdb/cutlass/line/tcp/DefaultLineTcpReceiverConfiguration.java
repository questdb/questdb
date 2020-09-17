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

package io.questdb.cutlass.line.tcp;

import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.CairoSecurityContext;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.LineProtoNanoTimestampAdapter;
import io.questdb.cutlass.line.LineProtoTimestampAdapter;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.microtime.MicrosecondClock;
import io.questdb.std.microtime.MicrosecondClockImpl;

public class DefaultLineTcpReceiverConfiguration implements LineTcpReceiverConfiguration {
    private final IODispatcherConfiguration ioDispatcherConfiguration = new DefaultIODispatcherConfiguration();

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public CairoSecurityContext getCairoSecurityContext() {
        return AllowAllCairoSecurityContext.INSTANCE;
    }

    @Override
    public LineProtoTimestampAdapter getTimestampAdapter() {
        return LineProtoNanoTimestampAdapter.INSTANCE;
    }

    @Override
    public int getConnectionPoolInitialCapacity() {
        return 64;
    }

    @Override
    public IODispatcherConfiguration getNetDispatcherConfiguration() {
        return ioDispatcherConfiguration;
    }

    @Override
    public int getNetMsgBufferSize() {
        return 2048;
    }

    @Override
    public int getMaxMeasurementSize() {
        return 512;
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    @Override
    public int getWriterQueueSize() {
        return 64;
    }

    @Override
    public MicrosecondClock getMicrosecondClock() {
        return MicrosecondClockImpl.INSTANCE;
    }

    @Override
    public WorkerPoolAwareConfiguration getWorkerPoolConfiguration() {
        return WorkerPoolAwareConfiguration.USE_SHARED_CONFIGURATION;
    }

    @Override
    public int getnUpdatesPerLoadRebalance() {
        return 1000;
    }

    @Override
    public double getMaxLoadRatio() {
        return 1.1;
    }

    @Override
    public int getMaxUncommittedRows() {
        return 1000;
    }

    @Override
    public long getMaintenanceJobHysteresisInMs() {
        return 100;
    }
	
	@Override
	public String getAuthDbPath() {
	    return null;
	}
}
