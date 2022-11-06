/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

public class TableRegistry implements Closeable {
    private final ConcurrentHashMap<Sequencer> tableRegistry = new ConcurrentHashMap<>();

    private final CairoEngine engine;

    TableRegistry(CairoEngine engine) {
        this.engine = engine;
    }

    // expected that caller holds the lock on the table
    void registerTable(TableStructure struct) {
        final String tableName = Chars.toString(struct.getTableName());
        final Sequencer other = tableRegistry.remove(tableName);
        if (other != null) {
            // could happen if there was a table with the same name before
            other.close();
        }

        final SequencerImpl sequencer = new SequencerImpl(engine, tableName);
        sequencer.of(struct);
        sequencer.close();
    }

    Sequencer getSequencer(CharSequence tableName) {
        Sequencer sequencer = tableRegistry.get(tableName);
        if (sequencer == null) {
            sequencer = new SequencerImpl(engine, Chars.toString(tableName));
            sequencer.open();
            final Sequencer other = tableRegistry.putIfAbsent(Chars.toString(tableName), sequencer);
            if (other != null) {
                sequencer.close();
                return other;
            }
        }
        return sequencer;
    }

    void clear() {
        // create proper clear() and close() methods
        final Set<Map.Entry<CharSequence, Sequencer>> entries =  tableRegistry.entrySet();
        for (Map.Entry<CharSequence, Sequencer> entry: entries) {
            entry.getValue().close();
        }
        tableRegistry.clear();
    }

    @Override
    public void close() {
        clear();
    }
}
