/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.store;

import java.util.Arrays;

public class Tx {

    public static final byte TX_NORMAL = 0;
    public static final byte TX_FORCE = 1;
    public long address;
    public long prevTxAddress;
    public byte command;
    public long timestamp;
    public long txn;
    public long txPin;
    public long journalMaxRowID;
    public long lastPartitionTimestamp;
    public long lagSize;
    public String lagName;
    public int symbolTableSizes[];
    public long symbolTableIndexPointers[];
    public long indexPointers[];
    public long lagIndexPointers[];

    @Override
    public String toString() {
        return "Tx{" +
                "address=" + address +
                ", prevTxAddress=" + prevTxAddress +
                ", command=" + command +
                ", timestamp=" + timestamp +
                ", txn=" + txn +
                ", txPin=" + txPin +
                ", journalMaxRowID=" + journalMaxRowID +
                ", lastPartitionTimestamp=" + lastPartitionTimestamp +
                ", lagSize=" + lagSize +
                ", lagName='" + lagName + '\'' +
                ", symbolTableSizes=" + Arrays.toString(symbolTableSizes) +
                ", symbolTableIndexPointers=" + Arrays.toString(symbolTableIndexPointers) +
                ", indexPointers=" + Arrays.toString(indexPointers) +
                ", lagIndexPointers=" + Arrays.toString(lagIndexPointers) +
                '}';
    }
}
