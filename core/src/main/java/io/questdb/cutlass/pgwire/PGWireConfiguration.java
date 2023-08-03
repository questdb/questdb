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

package io.questdb.cutlass.pgwire;

import io.questdb.FactoryProvider;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.DateLocale;

public interface PGWireConfiguration extends WorkerPoolConfiguration {

    int getBinParamCountCapacity();

    int getCharacterStoreCapacity();

    int getCharacterStorePoolCapacity();

    SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration();

    int getConnectionPoolInitialCapacity();

    DateLocale getDefaultDateLocale();

    String getDefaultPassword();

    String getDefaultUsername();

    IODispatcherConfiguration getDispatcherConfiguration();

    default boolean getDumpNetworkTraffic() {
        return false;
    }

    FactoryProvider getFactoryProvider();

    int getInsertCacheBlockCount();

    int getInsertCacheRowCount();

    int getInsertPoolCapacity();

    int getMaxBlobSizeOnQuery();

    int getNamedStatementCacheCapacity();

    int getNamesStatementPoolCapacity();

    NetworkFacade getNetworkFacade();

    int getPendingWritersCacheSize();

    // this is used in tests to fix pseudo-random generator
    default Rnd getRandom() {
        return null;
    }

    String getReadOnlyPassword();

    String getReadOnlyUsername();

    int getRecvBufferSize();

    int getSelectCacheBlockCount();

    int getSelectCacheRowCount();

    int getSendBufferSize();

    String getServerVersion();

    int getUpdateCacheBlockCount();

    int getUpdateCacheRowCount();

    boolean isInsertCacheEnabled();

    boolean isReadOnlyUserEnabled();

    boolean isSelectCacheEnabled();

    boolean isUpdateCacheEnabled();

    boolean readOnlySecurityContext();
}
