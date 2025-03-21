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

package io.questdb.cutlass.line.udp;

import io.questdb.cutlass.line.LineTimestampAdapter;
import io.questdb.network.NetworkFacade;

public interface LineUdpReceiverConfiguration {

    boolean getAutoCreateNewColumns();

    boolean getAutoCreateNewTables();

    int getBindIPv4Address();

    int getCommitMode();

    int getCommitRate();

    short getDefaultColumnTypeForFloat();

    short getDefaultColumnTypeForInteger();

    int getDefaultPartitionBy();

    int getGroupIPv4Address();

    int getMaxFileNameLength();

    int getMsgBufferSize();

    int getMsgCount();

    NetworkFacade getNetworkFacade();

    int getPort();

    int getReceiveBufferSize();

    LineTimestampAdapter getTimestampAdapter();

    boolean isEnabled();

    boolean isUnicast();

    boolean isUseLegacyStringDefault();

    boolean ownThread();

    int ownThreadAffinity();
}
