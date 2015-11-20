/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb.ha.producer;

import com.nfsdb.ha.AbstractObjectProducer;
import com.nfsdb.ha.model.JournalServerState;
import com.nfsdb.misc.ByteBuffers;

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
