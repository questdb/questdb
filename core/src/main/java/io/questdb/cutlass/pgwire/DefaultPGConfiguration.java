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

package io.questdb.cutlass.pgwire;

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.std.ConcurrentCacheConfiguration;
import io.questdb.std.DefaultConcurrentCacheConfiguration;
import io.questdb.std.datetime.DateLocale;

import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;

public class DefaultPGConfiguration extends DefaultIODispatcherConfiguration implements PGConfiguration {
    private final SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration();

    @Override
    public int getBinParamCountCapacity() {
        return 4;
    }

    @Override
    public int getBindPort() {
        return 8812;
    }

    @Override
    public int getCharacterStoreCapacity() {
        return 4096;
    }

    @Override
    public int getCharacterStorePoolCapacity() {
        return 64;
    }

    @Override
    public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        return circuitBreakerConfiguration;
    }

    @Override
    public ConcurrentCacheConfiguration getConcurrentCacheConfiguration() {
        return DefaultConcurrentCacheConfiguration.DEFAULT;
    }

    @Override
    public int getConnectionPoolInitialCapacity() {
        return 4;
    }

    @Override
    public DateLocale getDefaultDateLocale() {
        return EN_LOCALE;
    }

    @Override
    public String getDefaultPassword() {
        return "quest";
    }

    @Override
    public String getDefaultUsername() {
        return "admin";
    }

    @Override
    public String getDispatcherLogName() {
        return "pg-server";
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        return DefaultFactoryProvider.INSTANCE;
    }

    @Override
    public int getForceRecvFragmentationChunkSize() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getForceSendFragmentationChunkSize() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getInsertCacheBlockCount() {
        return 4;
    }

    @Override
    public int getInsertCacheRowCount() {
        return 4;
    }

    @Override
    public int getMaxBlobSizeOnQuery() {
        // BLOBs must fit inside send buffer together with other column values
        return 512 * 1024;
    }

    @Override
    public int getNamedStatementCacheCapacity() {
        return 32;
    }

    @Override
    public int getNamedStatementLimit() {
        return 10_000;
    }

    @Override
    public int getNamesStatementPoolCapacity() {
        return 32;
    }

    @Override
    public int getPendingWritersCacheSize() {
        return 16;
    }

    @Override
    public int getPipelineCapacity() {
        return 64;
    }

    @Override
    public String getPoolName() {
        return "pgwire";
    }

    @Override
    public String getReadOnlyPassword() {
        return "quest";
    }

    @Override
    public String getReadOnlyUsername() {
        return "user";
    }

    @Override
    public int getRecvBufferSize() {
        return 1024 * 1024;
    }

    @Override
    public int getSendBufferSize() {
        return 1024 * 1024;
    }

    @Override
    public String getServerVersion() {
        return "11.3";
    }

    @Override
    public int getUpdateCacheBlockCount() {
        return 4;
    }

    @Override
    public int getUpdateCacheRowCount() {
        return 4;
    }

    @Override
    public int getWorkerCount() {
        return 1;
    }

    @Override
    public boolean isInsertCacheEnabled() {
        return true;
    }

    @Override
    public boolean isReadOnlyUserEnabled() {
        return false;
    }

    @Override
    public boolean isSelectCacheEnabled() {
        return true;
    }

    @Override
    public boolean isUpdateCacheEnabled() {
        return true;
    }

    @Override
    public boolean readOnlySecurityContext() {
        return false;
    }
}
