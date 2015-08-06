/*
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
 */

package com.nfsdb.ha.comsumer;

import com.nfsdb.ha.AbstractMutableObjectConsumer;
import com.nfsdb.ha.model.JournalServerState;

import java.nio.ByteBuffer;

public class JournalServerStateConsumer extends AbstractMutableObjectConsumer<JournalServerState> {

    private char lagPartitionNameChars[];

    @Override
    protected JournalServerState newInstance() {
        return new JournalServerState();
    }

    @Override
    protected void read(ByteBuffer buffer, JournalServerState obj) {
        obj.reset();
        obj.setTxn(buffer.getLong());
        obj.setTxPin(buffer.getLong());
        obj.setSymbolTables(buffer.get() == 1);
        int partitionCount = buffer.getInt();

        obj.setNonLagPartitionCount(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            obj.addPartitionMetadata(buffer.getInt(), buffer.getLong(), buffer.getLong(), buffer.get());
        }

        int len = buffer.getChar();
        if (len > 0) {
            if (lagPartitionNameChars == null || lagPartitionNameChars.length < len) {
                lagPartitionNameChars = new char[len];
            }
            for (int i = 0; i < len; i++) {
                lagPartitionNameChars[i] = buffer.getChar();
            }
            obj.setLagPartitionName(new String(lagPartitionNameChars, 0, len));
        } else {
            obj.setLagPartitionName(null);
        }

        obj.setLagPartitionMetadata(buffer.getInt(), buffer.getLong(), buffer.getLong(), buffer.get());
    }
}
