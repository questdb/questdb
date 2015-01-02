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

package com.nfsdb.journal.export;

import com.nfsdb.journal.lang.cst.DataRow;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Numbers;

import java.util.Iterator;

public class JournalEntryPrinter {
    private final boolean enabled;
    private final CharSink sink;

    public JournalEntryPrinter(CharSink sink, boolean enabled) {
        this.sink = sink;
        this.enabled = enabled;
    }

    public void print(DataRow e) {
        if (e == null) {
            sink.put("\n");
            sink.flush();
            return;
        }

        for (int i = 0, sz = e.getColumnCount(); i < sz; i++) {
            switch (e.getColumnType(i)) {
                case DATE:
                    Dates.appendDateTime(sink, e.getLong(i));
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
//                default:
//                    throw new JournalRuntimeException("Unsupported type: " + e.getColumnType(i));
            }
            sink.put('\t');
        }
        print(e.getSlave());
    }

    public <X extends DataRow> void print(Iterator<X> src) {
        if (!enabled) {
            return;
        }

        while (src.hasNext()) {
            print(src.next());
        }
    }
}
