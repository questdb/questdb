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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.sql.Record;
import org.jetbrains.annotations.Nullable;

/**
 * Consults a {@link JoinKeyFilter} for each master row of a CROSS or nested loop LEFT join. Each lookup
 * costs about as much as joining one master row with one slave row, and a dropped master row saves
 * joining it with every slave row. The gate therefore samples the master rows, and stops consulting the
 * filter for the rest of the execution when the rows it drops in a sample, multiplied by the slave
 * rows that each of them saves, do not reach twice the lookups. The join then runs as it would without
 * the filter.
 */
final class JoinKeyFilterGate {
    static final int SAMPLE_SIZE = 1024;
    private int checks;
    private int drops;
    private @Nullable JoinKeyFilter filter;
    private boolean isActive;
    // the rows of the slave that the join reads for each master row, -1 while unknown
    private long slaveRowCount;

    boolean hasFilter() {
        return filter != null;
    }

    // Returns true when the join's cursor does not know yet how many rows its slave has.
    boolean isCountingSlaveRows() {
        return isActive && slaveRowCount < 0;
    }

    // Returns true when the hash join that reads the rows of the join drops every row of this master row.
    boolean isRowDropped(Record record) {
        if (!isActive) {
            return false;
        }
        assert filter != null;
        final boolean isDropped = !filter.hasMatch(record);
        if (isDropped) {
            drops++;
        }
        if (++checks == SAMPLE_SIZE) {
            // A sample that dropped every master row ran no slave scan, and keeps the filter whatever the
            // slave holds. Otherwise the cursor has counted the slave rows during a scan.
            isActive = slaveRowCount < 0 ? drops == SAMPLE_SIZE : drops * slaveRowCount >= 2L * SAMPLE_SIZE;
            checks = 0;
            drops = 0;
        }
        return isDropped;
    }

    // Starts an execution: the sample of the previous one says nothing about the rows of this one.
    void of(long slaveRowCount) {
        isActive = filter != null;
        checks = 0;
        drops = 0;
        this.slaveRowCount = slaveRowCount;
    }

    void setFilter(@Nullable JoinKeyFilter filter) {
        this.filter = filter;
        of(-1);
    }

    void setSlaveRowCount(long slaveRowCount) {
        this.slaveRowCount = slaveRowCount;
    }
}
