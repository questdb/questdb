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

package io.questdb.cutlass.line.udp;

import io.questdb.cairo.CairoSecurityContext;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.LineProtoNanoTimestampAdapter;
import io.questdb.cutlass.line.LineProtoTimestampAdapter;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;

public class DefaultLineUdpReceiverConfiguration implements LineUdpReceiverConfiguration {

    @Override
    public int getBindIPv4Address() {
        return 0;
    }

    @Override
    public int getCommitRate() {
        return 1024 * 1024;
    }

    @Override
    public int getGroupIPv4Address() {
        return Net.parseIPv4("224.1.1.1");
    }

    @Override
    public int getMsgBufferSize() {
        return 2048;
    }

    @Override
    public int getMsgCount() {
        return 10000;
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    @Override
    public int getPort() {
        return 4567;
    }

    @Override
    public LineProtoTimestampAdapter getTimestampAdapter() {
        return LineProtoNanoTimestampAdapter.INSTANCE;
    }

    @Override
    public int getDefaultPartitionBy() {
        return PartitionBy.DAY;
    }

    @Override
    public int getReceiveBufferSize() {
        return -1;
    }

    @Override
    public CairoSecurityContext getCairoSecurityContext() {
        return AllowAllCairoSecurityContext.INSTANCE;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public boolean isUnicast() {
        return false;
    }

    @Override
    public boolean ownThread() {
        return true;
    }

    @Override
    public int ownThreadAffinity() {
        return -1;
    }

    @Override
    public int getCommitMode() {
        return CommitMode.NOSYNC;
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
