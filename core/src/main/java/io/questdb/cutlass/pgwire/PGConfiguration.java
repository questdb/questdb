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

package io.questdb.cutlass.pgwire;

import io.questdb.FactoryProvider;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.std.ConcurrentCacheConfiguration;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.DateLocale;

public interface PGConfiguration extends IODispatcherConfiguration, WorkerPoolConfiguration {

    int getBinParamCountCapacity();

    int getCharacterStoreCapacity();

    int getCharacterStorePoolCapacity();

    SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration();

    ConcurrentCacheConfiguration getConcurrentCacheConfiguration();

    int getConnectionPoolInitialCapacity();

    DateLocale getDefaultDateLocale();

    String getDefaultPassword();

    String getDefaultUsername();

    default boolean getDumpNetworkTraffic() {
        return false;
    }

    FactoryProvider getFactoryProvider();

    int getForceRecvFragmentationChunkSize();

    int getPipelineCapacity();

    int getForceSendFragmentationChunkSize();

    int getInsertCacheBlockCount();

    int getInsertCacheRowCount();

    int getMaxBlobSizeOnQuery();

    int getNamedStatementCacheCapacity();

    int getNamedStatementLimit();

    int getNamesStatementPoolCapacity();

    int getPendingWritersCacheSize();

    // this is used in tests to fix pseudo-random generator
    default Rnd getRandom() {
        return null;
    }

    String getReadOnlyPassword();

    String getReadOnlyUsername();

    String getServerVersion();

    int getUpdateCacheBlockCount();

    int getUpdateCacheRowCount();

    boolean isInsertCacheEnabled();

    boolean isReadOnlyUserEnabled();

    boolean isSelectCacheEnabled();

    boolean isUpdateCacheEnabled();

    boolean readOnlySecurityContext();
}
