/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.LineProtoNanoTimestampAdapter;
import io.questdb.cutlass.line.LineProtoTimestampAdapter;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;

public class DefaultLineTcpReceiverConfiguration implements LineTcpReceiverConfiguration {
    private final IODispatcherConfiguration ioDispatcherConfiguration = new DefaultIODispatcherConfiguration();

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public boolean getDisconnectOnError() {
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
    public IODispatcherConfiguration getDispatcherConfiguration() {
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
    public int getWriterQueueCapacity() {
        return 64;
    }

    @Override
    public MicrosecondClock getMicrosecondClock() {
        return MicrosecondClockImpl.INSTANCE;
    }

    @Override
    public MillisecondClock getMillisecondClock() {
        return MillisecondClockImpl.INSTANCE;
    }

    @Override
    public WorkerPoolAwareConfiguration getWriterWorkerPoolConfiguration() {
        return WorkerPoolAwareConfiguration.USE_SHARED_CONFIGURATION;
    }

    @Override
    public WorkerPoolAwareConfiguration getIOWorkerPoolConfiguration() {
        return WorkerPoolAwareConfiguration.USE_SHARED_CONFIGURATION;
    }

    @Override
    public long getMaintenanceInterval() {
        return 100;
    }

    @Override
    public double getCommitIntervalFraction() {
        return 0.5;
    }

    @Override
    public long getCommitIntervalDefault() {
        return 2000;
    }

    @Override
    public String getAuthDbPath() {
        return null;
    }

    @Override
    public int getDefaultPartitionBy() {
        return PartitionBy.DAY;
    }

    @Override
    public long getWriterIdleTimeout() {
        return 30_000;
    }

    @Override
    public long getSymbolCacheWaitUsBeforeReload() {
        return 500_000;
    }

    @Override
    public boolean isStringToCharCastAllowed() {
        return false;
    }

    @Override
    public boolean isSymbolAsFieldSupported() {
        return false;
    }

    @Override
    public boolean isStringAsTagSupported() {
        return false;
    }

    @Override
    public short getDefaultColumnTypeForFloat() {
        return ColumnType.DOUBLE;
    }

    @Override
    public short getDefaultColumnTypeForInteger() {
        return ColumnType.LONG;
    }
}
