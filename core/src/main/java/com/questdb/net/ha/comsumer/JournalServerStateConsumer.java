/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.net.ha.comsumer;

import com.questdb.net.ha.AbstractMutableObjectConsumer;
import com.questdb.net.ha.model.JournalServerState;

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
