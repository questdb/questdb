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

import com.questdb.network.IODispatcherConfiguration;
import com.questdb.network.NetworkFacade;

public interface PGWireConfiguration {
    int getCharacterStoreCapacity();

    int getCharacterStorePoolCapacity();

    int getConnectionPoolInitialCapacity();

    String getDefaultPassword();

    String getDefaultUsername();

    IODispatcherConfiguration getDispatcherConfiguration();

    default boolean getDumpNetworkTraffic() {
        return false;
    }

    int getFactoryCacheColumnCount();

    int getFactoryCacheRowCount();

    int getIdleRecvCountBeforeGivingUp();

    int getIdleSendCountBeforeGivingUp();

    int getMaxBlobSizeOnQuery();

    NetworkFacade getNetworkFacade();

    int getRecvBufferSize();

    int getSendBufferSize();

    String getServerVersion();

    int[] getWorkerAffinity();

    int getWorkerCount();

    boolean isEnabled();

    boolean workerHaltOnError();
}
