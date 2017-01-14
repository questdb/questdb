/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.net.ha.producer;

import com.questdb.misc.ByteBuffers;
import com.questdb.net.ha.AbstractObjectProducer;
import com.questdb.net.ha.model.JournalServerState;

import java.nio.ByteBuffer;

public class JournalServerStateProducer extends AbstractObjectProducer<JournalServerState> {

    private static final int SUMMARY_RECORD_SIZE = (4 /* partitionIndex */ + 8 /* interval start */ + 8 /* interval end*/ + 1 /* empty */);

    @Override
    protected int getBufferSize(JournalServerState value) {
        return 8 + 8 + 4 + 1 + value.getNonLagPartitionCount() * SUMMARY_RECORD_SIZE
                + 2 + (value.getLagPartitionName() != null ? 2 * value.getLagPartitionName().length() : 0)
                + SUMMARY_RECORD_SIZE;
    }

    @Override
    protected void write(JournalServerState value, ByteBuffer buffer) {
        buffer.putLong(value.getTxn());
        buffer.putLong(value.getTxPin());
        buffer.put((byte) (value.isSymbolTables() ? 1 : 0));
        buffer.putInt(value.getNonLagPartitionCount());
        for (int i = 0; i < value.getNonLagPartitionCount(); i++) {
            JournalServerState.PartitionMetadata partitionMetadata = value.getMeta(i);
            buffer.putInt(partitionMetadata.getPartitionIndex());
            buffer.putLong(partitionMetadata.getIntervalStart());
            buffer.putLong(partitionMetadata.getIntervalEnd());
            buffer.put(partitionMetadata.getEmpty());
        }
        ByteBuffers.putStringW(buffer, value.getLagPartitionName());
        buffer.putInt(value.getLagPartitionMetadata().getPartitionIndex());
        buffer.putLong(value.getLagPartitionMetadata().getIntervalStart());
        buffer.putLong(value.getLagPartitionMetadata().getIntervalEnd());
        buffer.put(value.getLagPartitionMetadata().getEmpty());
    }
}
