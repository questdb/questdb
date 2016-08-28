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
 ******************************************************************************/

package com.questdb.ql.impl;

import com.questdb.JournalEntryWriter;
import com.questdb.JournalWriter;
import com.questdb.ex.JournalException;
import com.questdb.factory.JournalFactory;
import com.questdb.factory.configuration.ColumnMetadata;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.RecordSource;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;

public final class JournalUtils {

    private JournalUtils() {
    }

    public static void createJournal(JournalFactory factory, String name, RecordSource rs) throws JournalException {
        final RecordMetadata metadata = rs.getMetadata();
        final int n = metadata.getColumnCount();

        JournalWriter w = factory.bulkWriter(createStructure(name, metadata).build());
        RecordCursor cursor = rs.prepareCursor(factory);
        while (cursor.hasNext()) {
            JournalEntryWriter ew = w.entryWriter();
            Record r = cursor.next();

            for (int i = 0; i < n; i++) {
                switch (metadata.getColumnQuick(i).getType()) {
                    case ColumnType.DATE:
                        ew.putDate(i, r.getDate(i));
                        break;
                    case ColumnType.DOUBLE:
                        ew.putDouble(i, r.getDouble(i));
                        break;
                    case ColumnType.FLOAT:
                        ew.putFloat(i, r.getFloat(i));
                        break;
                    case ColumnType.INT:
                        ew.putInt(i, r.getInt(i));
                        break;
                    case ColumnType.STRING:
                        ew.putStr(i, r.getFlyweightStr(i));
                        break;
                    case ColumnType.SYMBOL:
                        ew.putSym(i, r.getSym(i));
                        break;
                    case ColumnType.SHORT:
                        ew.putShort(i, r.getShort(i));
                        break;
                    case ColumnType.LONG:
                        ew.putLong(i, r.getLong(i));
                        break;
                    case ColumnType.BYTE:
                        ew.put(i, r.get(i));
                        break;
                    case ColumnType.BOOLEAN:
                        ew.putBool(i, r.getBool(i));
                        break;
                    case ColumnType.BINARY:
                        ew.putBin(i, r.getBin(i));
                        break;
                    default:
                        break;
                }
            }
            ew.append();
        }
        w.commit();
    }

    private static JournalStructure createStructure(String location, RecordMetadata rm) {
        int n = rm.getColumnCount();
        ObjList<ColumnMetadata> m = new ObjList<>(n);
        for (int i = 0; i < n; i++) {
            ColumnMetadata cm = new ColumnMetadata();
            RecordColumnMetadata im = rm.getColumnQuick(i);
            cm.name = im.getName();
            cm.type = im.getType();

            switch (cm.type) {
                case ColumnType.STRING:
                    cm.size = cm.avgSize + 4;
                    break;
                default:
                    cm.size = ColumnType.sizeOf(cm.type);
                    break;
            }
            m.add(cm);
        }
        return new JournalStructure(location, m);
    }
}
