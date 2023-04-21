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

package io.questdb.cutlass.line.tcp;

import io.questdb.FactoryProvider;
import io.questdb.cutlass.line.LineProtoTimestampAdapter;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;

public interface LineTcpReceiverConfiguration {
    boolean getAutoCreateNewColumns();

    boolean getAutoCreateNewTables();

    long getCommitInterval();

    long getCommitIntervalDefault();

    double getCommitIntervalFraction();

    int getConnectionPoolInitialCapacity();

    short getDefaultColumnTypeForFloat();

    short getDefaultColumnTypeForInteger();

    int getDefaultPartitionBy();

    boolean getDisconnectOnError();

    IODispatcherConfiguration getDispatcherConfiguration();

    FilesFacade getFilesFacade();

    WorkerPoolConfiguration getIOWorkerPoolConfiguration();

    /**
     * Interval in milliseconds to perform writer maintenance. Such maintenance can
     * incur cost of commit (in case of using "lag"), load rebalance and writer release.
     *
     * @return interval in milliseconds
     */
    long getMaintenanceInterval();

    int getMaxFileNameLength();

    int getMaxMeasurementSize();

    MicrosecondClock getMicrosecondClock();

    MillisecondClock getMillisecondClock();

    int getNetMsgBufferSize();

    NetworkFacade getNetworkFacade();

    long getSymbolCacheWaitUsBeforeReload();

    LineProtoTimestampAdapter getTimestampAdapter();

    long getWriterIdleTimeout();

    int getWriterQueueCapacity();

    WorkerPoolConfiguration getWriterWorkerPoolConfiguration();

    boolean isEnabled();

    boolean isStringAsTagSupported();

    boolean isStringToCharCastAllowed();

    boolean isSymbolAsFieldSupported();

    boolean readOnlySecurityContext();

    FactoryProvider getFactoryProvider();
}
