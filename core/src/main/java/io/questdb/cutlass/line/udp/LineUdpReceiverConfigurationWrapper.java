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

public class LineUdpReceiverConfigurationWrapper implements LineUdpReceiverConfiguration {
    private final LineUdpReceiverConfiguration delegate;

    protected LineUdpReceiverConfigurationWrapper() {
        delegate = null;
    }

    @Override
    public boolean getAutoCreateNewColumns() {
        return getDelegate().getAutoCreateNewColumns();
    }

    @Override
    public boolean getAutoCreateNewTables() {
        return getDelegate().getAutoCreateNewTables();
    }

    @Override
    public int getBindIPv4Address() {
        return getDelegate().getBindIPv4Address();
    }

    @Override
    public int getCommitMode() {
        return getDelegate().getCommitMode();
    }

    @Override
    public int getCommitRate() {
        return getDelegate().getCommitRate();
    }

    @Override
    public short getDefaultColumnTypeForFloat() {
        return getDelegate().getDefaultColumnTypeForFloat();
    }

    @Override
    public short getDefaultColumnTypeForInteger() {
        return getDelegate().getDefaultColumnTypeForInteger();
    }

    @Override
    public int getDefaultPartitionBy() {
        return getDelegate().getDefaultPartitionBy();
    }

    @Override
    public int getGroupIPv4Address() {
        return getDelegate().getGroupIPv4Address();
    }

    @Override
    public int getMaxFileNameLength() {
        return getDelegate().getMaxFileNameLength();
    }

    @Override
    public int getMsgBufferSize() {
        return getDelegate().getMsgBufferSize();
    }

    @Override
    public int getMsgCount() {
        return getDelegate().getMsgCount();
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return getDelegate().getNetworkFacade();
    }

    @Override
    public int getPort() {
        return getDelegate().getPort();
    }

    @Override
    public int getReceiveBufferSize() {
        return getDelegate().getReceiveBufferSize();
    }

    @Override
    public LineTimestampAdapter getTimestampAdapter() {
        return getDelegate().getTimestampAdapter();
    }

    @Override
    public boolean isEnabled() {
        return getDelegate().isEnabled();
    }

    @Override
    public boolean isUnicast() {
        return getDelegate().isUnicast();
    }

    @Override
    public boolean isUseLegacyStringDefault() {
        return getDelegate().isUseLegacyStringDefault();
    }

    @Override
    public boolean ownThread() {
        return getDelegate().ownThread();
    }

    @Override
    public int ownThreadAffinity() {
        return getDelegate().ownThreadAffinity();
    }

    protected LineUdpReceiverConfiguration getDelegate() {
        return delegate;
    }
}
