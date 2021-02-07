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

package io.questdb.tasks;

public class OutOfOrderUpdPartitionSizeTask {
    private long oooTimestampHi;
    private long srcOooPartitionLo;
    private long srcOooPartitionHi;
    private long srcDataMax;
    private long dataTimestampHi;

    public long getDataTimestampHi() {
        return dataTimestampHi;
    }

    public long getOooTimestampHi() {
        return oooTimestampHi;
    }

    public long getSrcDataMax() {
        return srcDataMax;
    }

    public long getSrcOooPartitionHi() {
        return srcOooPartitionHi;
    }

    public long getSrcOooPartitionLo() {
        return srcOooPartitionLo;
    }

    public void of(
            long oooTimestampHi,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long srcDataMax,
            long dataTimestampHi
    ) {
        this.oooTimestampHi = oooTimestampHi;
        this.srcOooPartitionLo = srcOooPartitionLo;
        this.srcOooPartitionHi = srcOooPartitionHi;
        this.srcDataMax = srcDataMax;
        this.dataTimestampHi = dataTimestampHi;
    }
}
