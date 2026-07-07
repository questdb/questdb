/*+*****************************************************************************
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

package io.questdb.cutlass.qwp.server;

import io.questdb.cairo.PartitionBy;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;

public class DefaultQwpUdpReceiverConfiguration implements QwpUdpReceiverConfiguration {

    @Override
    public int getBindIPv4Address() {
        return 0;
    }

    @Override
    public int getMaxUncommittedDatagrams() {
        return 1_048_576;
    }

    @Override
    public int getDefaultPartitionBy() {
        return PartitionBy.DAY;
    }

    @Override
    public int getGroupIPv4Address() {
        return Net.parseIPv4("224.1.1.1");
    }

    @Override
    public int getMaxRowsPerTable() {
        return QwpConstants.DEFAULT_MAX_ROWS_PER_TABLE;
    }

    @Override
    public int getMaxTablesPerConnection() {
        return QwpConstants.DEFAULT_MAX_TABLES_PER_CONNECTION;
    }

    @Override
    public int getMsgBufferSize() {
        return 65_536;
    }

    @Override
    public int getMsgCount() {
        return 10_000;
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    @Override
    public int getPort() {
        return 9007;
    }

    @Override
    public int getReceiveBufferSize() {
        return -1;
    }

    @Override
    public boolean isAutoCreateNewColumns() {
        return true;
    }

    @Override
    public boolean isAutoCreateNewTables() {
        return true;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public boolean isOwnThread() {
        return true;
    }

    @Override
    public boolean isUnicast() {
        return true;
    }

    @Override
    public int ownThreadAffinity() {
        return -1;
    }
}
