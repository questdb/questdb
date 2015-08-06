/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.storage;

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
