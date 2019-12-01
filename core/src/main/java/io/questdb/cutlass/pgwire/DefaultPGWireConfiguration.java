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

package io.questdb.cutlass.pgwire;

import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;

public class DefaultPGWireConfiguration implements PGWireConfiguration {

    private final IODispatcherConfiguration ioDispatcherConfiguration = new DefaultIODispatcherConfiguration() {
        @Override
        public int getBindPort() {
            return 8812;
        }

        @Override
        public String getDispatcherLogName() {
            return "pg-server";
        }
    };

    private final int[] workerAffinity = new int[]{-1};

    @Override
    public int getCharacterStoreCapacity() {
        return 4096;
    }

    @Override
    public int getCharacterStorePoolCapacity() {
        return 64;
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
    public String getServerVersion() {
        return "11.3";
    }

    @Override
    public int getFactoryCacheColumnCount() {
        return 16;
    }

    @Override
    public int getFactoryCacheRowCount() {
        return 16;
    }

    @Override
    public int getIdleRecvCountBeforeGivingUp() {
        return 10_000;
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
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
    public int getIdleSendCountBeforeGivingUp() {
        return 10_000;
    }

    @Override
    public int getMaxBlobSizeOnQuery() {
        // BLOBs must fit inside send buffer together with other column values
        return 512 * 1024;
    }

    @Override
    public int[] getWorkerAffinity() {
        return workerAffinity;
    }

    @Override
    public int getWorkerCount() {
        return 1;
    }

    @Override
    public boolean haltOnError() {
        return false;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public String getDefaultPassword() {
        return "quest";
    }

    @Override
    public String getDefaultUsername() {
        return "admin";
    }
}
