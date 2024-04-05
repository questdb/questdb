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

package io.questdb.tasks;

import io.questdb.std.ObjectFactory;

public class O3PartitionUpdateTask {
    public static final ObjectFactory<O3PartitionUpdateTask> CONSTRUCTOR = O3PartitionUpdateTask::new;
    private boolean partitionMutates;
    private long partitionTimestamp;
    private long srcDataMax;
    private long srcOooPartitionHi;
    private long srcOooPartitionLo;

    public long getPartitionTimestamp() {
        return partitionTimestamp;
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

    public boolean isPartitionMutates() {
        return partitionMutates;
    }

    public void of(
            long oooTimestampHi,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long srcDataMax,
            boolean partitionMutates
    ) {
        this.partitionTimestamp = oooTimestampHi;
        this.srcOooPartitionLo = srcOooPartitionLo;
        this.srcOooPartitionHi = srcOooPartitionHi;
        this.srcDataMax = srcDataMax;
        this.partitionMutates = partitionMutates;
    }
}
