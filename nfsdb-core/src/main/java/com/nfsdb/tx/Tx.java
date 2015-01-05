/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.tx;

import java.util.Arrays;

public class Tx {

    public static final byte TX_NORMAL = 0;
    public static final byte TX_FORCE = 1;
    // transient
    public long address;
    // 8
    public long prevTxAddress;
    // 1
    public byte command;
    // 8
    public long timestamp;
    // 8
    public long journalMaxRowID;
    // 8
    public long lastPartitionTimestamp;
    // 8
    public long lagSize;
    // 1 + 1 + 64
    public String lagName;
    // 2 + 4 * symbolTableSizes.len
    public int symbolTableSizes[];
    // 2 + 8 * symbolTableIndexPointers.len
    public long symbolTableIndexPointers[];
    // 2 + 8 * indexPointers.len
    public long indexPointers[];
    // 2 + 8 * lagIndexPointers.len
    public long lagIndexPointers[];

    @Override
    public String toString() {
        return "Tx{" +
                "address=" + address +
                ", prevTxAddress=" + prevTxAddress +
                ", command=" + command +
                ", timestamp=" + timestamp +
                ", journalMaxRowID=" + journalMaxRowID +
                ", lastPartitionTimestamp=" + lastPartitionTimestamp +
                ", lagSize=" + lagSize +
                ", lagName='" + lagName + '\'' +
                ", symbolTableSizes=" + Arrays.toString(symbolTableSizes) +
                ", symbolTableIndexPointers=" + Arrays.toString(symbolTableIndexPointers) +
                ", indexPointers=" + Arrays.toString(indexPointers) +
                ", lagIndexPointers=" + Arrays.toString(lagIndexPointers) +
                "}";
    }
}
