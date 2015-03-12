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

package com.nfsdb.io;

import com.nfsdb.io.sink.CharSink;
import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordMetadata;
import com.nfsdb.lang.cst.RecordSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"SF_SWITCH_NO_DEFAULT"})
public class RecordSourcePrinter {
    private final CharSink sink;
    private final char delimiter;

    public RecordSourcePrinter(CharSink sink) {
        this.sink = sink;
        this.delimiter = '\t';
    }

    public RecordSourcePrinter(CharSink sink, char delimiter) {
        this.sink = sink;
        this.delimiter = delimiter;
    }

    public void print(Record r, RecordMetadata m) {

        for (int i = 0, sz = m.getColumnCount(); i < sz; i++) {
            if (i > 0) {
                sink.put(delimiter);
            }
            switch (m.getColumnType(i)) {
                case DATE:
                    sink.putISODate(r.getLong(i));
                    break;
                case DOUBLE:
                    sink.put(r.getDouble(i), 12);
                    break;
                case FLOAT:
                    sink.put(r.getFloat(i), 6);
                    break;
                case INT:
                    sink.put(r.getInt(i));
                    break;
                case STRING:
                    r.getStr(i, sink);
                    break;
                case SYMBOL:
                    sink.put(r.getSym(i));
                    break;
                case SHORT:
                    sink.put(r.getShort(i));
                    break;
                case LONG:
                    sink.put(r.getLong(i));
                    break;
                case BYTE:
                    sink.put(r.get(i));
                    break;
                case BOOLEAN:
                    sink.put(r.getBool(i));
                    break;
//                default:
//                    throw new JournalRuntimeException("Unsupported type: " + r.getColumnType(i));
            }
        }
        sink.put("\n");
        sink.flush();
    }

    public void print(RecordSource<? extends Record> src) {
        while (src.hasNext()) {
            print(src.next(), src.getMetadata());
        }
    }
}
