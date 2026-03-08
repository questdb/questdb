/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cairo.sql;

import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for retrieving information about a partition frame.
 * <p>
 * Each partition frame holds a number of {@link PageFrame}s.
 * Think, a partition or a slice of a partition.
 * <p>
 * In case of a Parquet partition, frame corresponds to a Parquet file
 * or a slice within it.
 * <p>
 * Partition frame is an internal API and shouldn't be used for data access.
 * Page frames are meant to be used for data access.
 */
public interface PartitionFrame {

    /**
     * @return parquet decoder initialized for the partition for parquet partitions; null for native partitions
     */
    @Nullable
    PartitionDecoder getParquetDecoder();

    /**
     * @return format of the frame's partition; set to {@link PartitionFormat#NATIVE} or {@link PartitionFormat#PARQUET}
     */
    byte getPartitionFormat();

    /**
     * @return numeric index of the frame's partition
     */
    int getPartitionIndex();

    /**
     * @return upper boundary for last row of a partition frame, i.e. last row + 1
     */
    long getRowHi();

    /**
     * @return first row of a partition frame
     */
    long getRowLo();
}
