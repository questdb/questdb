/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.net.ha.producer;

import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.net.ha.AbstractObjectProducer;
import com.nfsdb.net.ha.model.JournalServerState;

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
