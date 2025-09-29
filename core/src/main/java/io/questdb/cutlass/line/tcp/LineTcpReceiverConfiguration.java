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

package io.questdb.cutlass.line.tcp;

import io.questdb.FactoryProvider;
import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.millitime.MillisecondClock;

public interface LineTcpReceiverConfiguration extends IODispatcherConfiguration {

    String getAuthDB();

    boolean getAutoCreateNewColumns();

    boolean getAutoCreateNewTables();

    CairoConfiguration getCairoConfiguration();

    long getCommitInterval();

    long getCommitIntervalDefault();

    double getCommitIntervalFraction();

    int getConnectionPoolInitialCapacity();

    short getDefaultColumnTypeForFloat();

    short getDefaultColumnTypeForInteger();

    int getDefaultColumnTypeForTimestamp();

    int getDefaultPartitionBy();

    boolean getDisconnectOnError();

    FactoryProvider getFactoryProvider();

    FilesFacade getFilesFacade();

    WorkerPoolConfiguration getNetworkWorkerPoolConfiguration();

    /**
     * Interval in milliseconds to perform writer maintenance. Such maintenance can
     * incur cost of commit (in case of using "lag"), load rebalance and writer release.
     *
     * @return interval in milliseconds
     */
    long getMaintenanceInterval();

    int getMaxFileNameLength();

    int getMaxMeasurementSize();

    long getMaxRecvBufferSize();

    Metrics getMetrics();

    Clock getMicrosecondClock();

    MillisecondClock getMillisecondClock();

    NetworkFacade getNetworkFacade();

    long getSymbolCacheWaitBeforeReload();

    byte getTimestampUnit();

    long getWriterIdleTimeout();

    int getWriterQueueCapacity();

    WorkerPoolConfiguration getWriterWorkerPoolConfiguration();

    boolean isEnabled();

    boolean isStringToCharCastAllowed();

    boolean isUseLegacyStringDefault();

    boolean logMessageOnError();
}
