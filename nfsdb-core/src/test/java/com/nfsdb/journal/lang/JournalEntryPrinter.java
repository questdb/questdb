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

package com.nfsdb.journal.lang;

import com.nfsdb.journal.factory.configuration.ColumnMetadata;
import com.nfsdb.journal.lang.cst.EntrySource;
import com.nfsdb.journal.lang.cst.JournalEntry;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.io.OutputStream;

public class JournalEntryPrinter {
    private final OutputStream out;
    private final boolean enabled;

    public JournalEntryPrinter(OutputStream out, boolean enabled) {
        this.out = out;
        this.enabled = enabled;
    }

    public void print(JournalEntry e) throws IOException {
        for (int i = 0; i < e.partition.getJournal().getMetadata().getColumnCount(); i++) {
            ColumnMetadata m = e.partition.getJournal().getMetadata().getColumnMetadata(i);
            switch (m.type) {
                case DATE:
                    out.write(ISODateTimeFormat.dateTime().print(e.getLong(i)).getBytes());
                    break;
                case DOUBLE:
                    out.write(Double.toString(e.getDouble(i)).getBytes());
                    break;
                case INT:
                    out.write(Integer.toString(e.getInt(i)).getBytes());
                    break;
                case STRING:
                    out.write(e.getStr(i).getBytes());
                    break;
                case SYMBOL:
                    out.write(e.getSym(i).getBytes());
            }
            out.write("\t".getBytes());
        }

        if (e.slave != null) {
            print(e.slave);
        } else {
            out.write("\n".getBytes());
        }
    }

    public void print(EntrySource src) throws IOException {
        if (!enabled) {
            return;
        }

        for (JournalEntry e : src) {
            print(e);
        }
    }
}
