/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.net.comsumer;

import com.nfsdb.journal.net.AbstractMutableObjectConsumer;
import com.nfsdb.journal.net.model.JournalClientState;

import java.nio.ByteBuffer;

public class JournalClientStateConsumer extends AbstractMutableObjectConsumer<JournalClientState> {

    private char lagPartitionNameChars[];

    @Override
    protected JournalClientState newInstance() {
        return new JournalClientState();
    }

    @Override
    protected void read(ByteBuffer buffer, JournalClientState status) {
        status.setJournalIndex(buffer.getInt());
        status.setMaxRowID(buffer.getLong());
        status.setLagSize(buffer.getLong());
        int len = buffer.getChar();
        if (len > 0) {
            if (lagPartitionNameChars == null || lagPartitionNameChars.length < len) {
                lagPartitionNameChars = new char[len];
            }
            for (int i = 0; i < len; i++) {
                lagPartitionNameChars[i] = buffer.getChar();
            }
            status.setLagPartitionName(new String(lagPartitionNameChars, 0, len));
        } else {
            status.setLagPartitionName(null);
        }
        int tabCount = buffer.getChar();
        status.reset();
        while (tabCount-- > 0) {
            status.setSymbolTableKey(buffer.getChar(), buffer.getInt());
        }
    }
}
