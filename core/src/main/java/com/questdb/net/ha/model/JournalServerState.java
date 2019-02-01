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

package com.questdb.net.ha.model;

import com.questdb.std.ObjList;

public class JournalServerState {

    private final ObjList<PartitionMetadata> partitionMetadata = new ObjList<>();
    private final PartitionMetadata lagPartitionMetadata = new PartitionMetadata();
    private boolean symbolTables = false;
    private int nonLagPartitionCount = 0;
    private int addIndex = 0;
    private String lagPartitionName;
    private boolean detachLag = false;
    private long txn;
    private long txPin;

    public void addPartitionMetadata(int partitionIndex, long intervalStart, long intervalEnd, byte empty) {
        PartitionMetadata partitionMetadata = getMeta(addIndex++);
        partitionMetadata.partitionIndex = partitionIndex;
        partitionMetadata.intervalStart = intervalStart;
        partitionMetadata.intervalEnd = intervalEnd;
        partitionMetadata.empty = empty;
    }

    public PartitionMetadata getLagPartitionMetadata() {
        return lagPartitionMetadata;
    }

    public String getLagPartitionName() {
        return lagPartitionName;
    }

    public void setLagPartitionName(String lagPartitionName) {
        this.lagPartitionName = lagPartitionName;
    }

    public PartitionMetadata getMeta(int index) {
        if (index >= nonLagPartitionCount) {
            throw new ArrayIndexOutOfBoundsException(index);
        }

        PartitionMetadata result = partitionMetadata.getQuiet(index);
        if (result == null) {
            result = new PartitionMetadata();
            partitionMetadata.extendAndSet(index, result);
        }
        return result;
    }

    public int getNonLagPartitionCount() {
        return nonLagPartitionCount;
    }

    public void setNonLagPartitionCount(int nonLagPartitionCount) {
        this.nonLagPartitionCount = nonLagPartitionCount;
    }

    public long getTxPin() {
        return txPin;
    }

    public void setTxPin(long txPin) {
        this.txPin = txPin;
    }

    public long getTxn() {
        return txn;
    }

    public void setTxn(long txn) {
        this.txn = txn;
    }

    public boolean isSymbolTables() {
        return symbolTables;
    }

    public void setSymbolTables(boolean symbolTables) {
        this.symbolTables = symbolTables;
    }

    public boolean notEmpty() {
        return nonLagPartitionCount != 0 || symbolTables || lagPartitionName != null || detachLag || txn == -1;
    }

    public void reset() {
        nonLagPartitionCount = 0;
        addIndex = 0;
        symbolTables = false;
        lagPartitionMetadata.partitionIndex = -1;
        lagPartitionMetadata.intervalStart = 0;
        lagPartitionMetadata.intervalEnd = 0;
        detachLag = false;
        txn = 0;
        txPin = 0;
        lagPartitionName = null;
    }

    public void setDetachLag(boolean detachLag) {
        this.detachLag = detachLag;
    }

    public void setLagPartitionMetadata(int partitionIndex, long intervalStart, long intervalEnd, byte empty) {
        lagPartitionMetadata.partitionIndex = partitionIndex;
        lagPartitionMetadata.intervalStart = intervalStart;
        lagPartitionMetadata.intervalEnd = intervalEnd;
        lagPartitionMetadata.empty = empty;
    }

    public static final class PartitionMetadata {
        private int partitionIndex;
        private long intervalStart;
        private long intervalEnd;
        private byte empty = 0;

        public byte getEmpty() {
            return empty;
        }

        public long getIntervalEnd() {
            return intervalEnd;
        }

        public long getIntervalStart() {
            return intervalStart;
        }

        public int getPartitionIndex() {
            return partitionIndex;
        }
    }
}
