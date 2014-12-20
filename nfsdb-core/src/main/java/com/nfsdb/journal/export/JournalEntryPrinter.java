/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.journal.export;

import com.nfsdb.journal.factory.configuration.ColumnMetadata;
import com.nfsdb.journal.lang.cst.EntrySource;
import com.nfsdb.journal.lang.cst.JournalEntry;
import com.nfsdb.journal.utils.Dates2;
import com.nfsdb.journal.utils.Numbers;

public class JournalEntryPrinter {
    private final boolean enabled;
    private final CharSink sink;

    public JournalEntryPrinter(CharSink sink, boolean enabled) {
        this.sink = sink;
        this.enabled = enabled;
    }

    public void print(JournalEntry e) {
        for (int i = 0, sz = e.partition.getJournal().getMetadata().getColumnCount(); i < sz; i++) {
            ColumnMetadata m = e.partition.getJournal().getMetadata().getColumnMetadata(i);
            switch (m.type) {
                case DATE:
                    Dates2.appendDateTime(sink, e.getLong(i));
                    break;
                case DOUBLE:
                    Numbers.append(sink, e.getDouble(i), 12);
                    break;
                case INT:
                    Numbers.append(sink, e.getInt(i));
                    break;
                case STRING:
                    sink.put(e.getStr(i));
                    break;
                case SYMBOL:
                    sink.put(e.getSym(i));
                    break;
                case SHORT:
                    Numbers.append(sink, e.getShort(i));
                    break;
                case LONG:
                    Numbers.append(sink, e.getLong(i));
                    break;
                case BYTE:
                    Numbers.append(sink, e.get(i));
                    break;
                case BOOLEAN:
                    sink.put(e.getBool(i) ? "true" : "false");
                    break;
            }
            sink.put('\t');
        }
        if (e.slave != null) {
            print(e.slave);
        } else {
            sink.put('\n');
            sink.flush();
        }
    }

    public void print(EntrySource src) {
        if (!enabled) {
            return;
        }

        for (JournalEntry e : src) {
            print(e);
        }
    }
}
