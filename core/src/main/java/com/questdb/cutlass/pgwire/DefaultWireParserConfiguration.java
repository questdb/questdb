/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cutlass.pgwire;

import com.questdb.network.NetworkFacade;
import com.questdb.network.NetworkFacadeImpl;

public class DefaultWireParserConfiguration implements WireParserConfiguration {
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
    public int getCharacterStoreCapacity() {
        return 4096;
    }

    @Override
    public int getCharacterStorePoolCapacity() {
        return 64;
    }

    @Override
    public int getFactoryCacheColumnCount() {
        return 16;
    }

    @Override
    public int getFactoryCacheRowCount() {
        return 16;
    }
}
