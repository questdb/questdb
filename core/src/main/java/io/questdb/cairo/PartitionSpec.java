/*+*****************************************************************************
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

package io.questdb.cairo;

import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * Mutable carrier describing a composite partitioning scheme: a time unit
 * (e.g. {@link PartitionBy#DAY}) combined with zero or more non-time
 * {@link PartitionDimension}s and an optional data-clustering column order.
 * <p>
 * A spec with no dimensions and no cluster columns is equivalent to plain
 * time partitioning; {@link #isComposite()} distinguishes the two.
 */
public final class PartitionSpec implements Mutable {

    public static final byte MODE_HIVE = 0;  // ts=2023-01-01/exchange=NYSE/
    public static final byte MODE_PLAIN = 1; // 2023-01-01/NYSE/

    /**
     * Shared, always-empty (non-composite) spec returned by
     * {@link TableStructure#getPartitionSpec()} for every structure that does not carry a real one
     * (plain tables, CREATE AS SELECT, LIKE, ILP/CSV adapters, sequencer copies, ...).
     * <p>
     * MUST NEVER be mutated. Sharing a single mutable instance is safe only because every consumer
     * checks {@link #isComposite()} (always {@code false} here) before touching any dimension or
     * cluster-column state, and none of them write to it.
     */
    public static final PartitionSpec EMPTY = new PartitionSpec();

    private final IntList clusterColumns = new IntList();
    private final ObjList<PartitionDimension> dimensions = new ObjList<>();
    private byte namingMode = MODE_HIVE;
    private int timeUnit = PartitionBy.NONE;

    public void addClusterColumn(int columnIndex) {
        clusterColumns.add(columnIndex);
    }

    public void addDimension(PartitionDimension d) {
        dimensions.add(d);
    }

    @Override
    public void clear() {
        timeUnit = PartitionBy.NONE;
        namingMode = MODE_HIVE;
        dimensions.clear();
        clusterColumns.clear();
    }

    public int getClusterColumn(int i) {
        return clusterColumns.getQuick(i);
    }

    public int getClusterColumnCount() {
        return clusterColumns.size();
    }

    public PartitionDimension getDimension(int i) {
        return dimensions.getQuick(i);
    }

    public int getDimensionCount() {
        return dimensions.size();
    }

    public byte getNamingMode() {
        return namingMode;
    }

    public int getTimeUnit() {
        return timeUnit;
    }

    public boolean isComposite() {
        return dimensions.size() > 0 || clusterColumns.size() > 0;
    }

    public void setNamingMode(byte mode) {
        this.namingMode = mode;
    }

    public void setTimeUnit(int unit) {
        this.timeUnit = unit;
    }
}
