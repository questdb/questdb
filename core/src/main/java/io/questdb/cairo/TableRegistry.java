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
import io.questdb.std.Misc;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

import static io.questdb.cairo.Sequencer.SEQ_DIR;

public class TableRegistry implements Closeable {
    private final ConcurrentHashMap<SequencerImpl> tableRegistry = new ConcurrentHashMap<>();

    private final CairoEngine engine;

    TableRegistry(CairoEngine engine) {
        this.engine = engine;
    }

    public boolean hasSequencer(CharSequence tableName) {
        Sequencer sequencer = tableRegistry.get(tableName);
        if (sequencer != null) {
            return true;
        }

        // Check if sequencer files exist, e.g. is it WAL table sequencer must exist
        Path tempPath = Path.PATH2.get();
        CairoConfiguration configuration = engine.getConfiguration();
        tempPath.of(configuration.getRoot()).concat(tableName).concat(SEQ_DIR);
        return configuration.getFilesFacade().exists(tempPath.$());
    }

    // expected that caller holds the lock on the table
    void createTable(int tableId, TableStructure struct) {
        final String tableName = Chars.toString(struct.getTableName());
        final Sequencer other = tableRegistry.remove(tableName);
        if (other != null) {
            // could happen if there was a table with the same name before
            other.close();
        }

        final SequencerImpl sequencer = new SequencerImpl(engine, tableName);
        sequencer.create(tableId, struct);
        sequencer.close();
    }

    Sequencer getSequencer(String tableName) {
        SequencerImpl sequencer = tableRegistry.get(tableName);
        if (sequencer == null) {
            sequencer = new SequencerImpl(engine, tableName);
            final SequencerImpl other = tableRegistry.putIfAbsent(tableName, sequencer);

            if (other != null) {
                Misc.free(sequencer);
                // Race is lost
                sequencer = other;
            }
        }
        return sequencer.waitOpen();
    }

    void clear() {
        // create proper clear() and close() methods
        final Set<Map.Entry<CharSequence, SequencerImpl>> entries =  tableRegistry.entrySet();
        for (Map.Entry<CharSequence, SequencerImpl> entry: entries) {
            entry.getValue().close();
        }
        tableRegistry.clear();
    }

    @Override
    public void close() {
        clear();
    }
}
