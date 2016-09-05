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

package com.questdb.ql.parser;

import com.questdb.JournalEntryWriter;
import com.questdb.JournalWriter;
import com.questdb.PartitionBy;
import com.questdb.ex.JournalException;
import com.questdb.ex.NumericException;
import com.questdb.ex.ParserException;
import com.questdb.factory.JournalFactory;
import com.questdb.factory.configuration.*;
import com.questdb.misc.Numbers;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.RecordSource;
import com.questdb.ql.model.ColumnIndexModel;
import com.questdb.ql.model.CreateJournalModel;
import com.questdb.ql.model.ExprNode;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;

public final class QueryCompilerUtils {

    private QueryCompilerUtils() {
    }

    public static JournalWriter createJournal(JournalFactory factory, CreateJournalModel model) throws JournalException, ParserException {

        final String name = model.getName().token;
        switch (factory.getConfiguration().exists(name)) {
            case JournalConfiguration.EXISTS:
                throw QueryError.$(model.getName().position, "Journal already exists");
            case JournalConfiguration.EXISTS_FOREIGN:
                throw QueryError.$(model.getName().position, "Name is reserved");
            default:
                break;
        }

        JournalStructure struct = model.getStruct();
        RecordSource rs = model.getRecordSource();

        if (struct == null) {
            assert rs != null;
            struct = createStructure(name, rs.getMetadata());
        }

        validateAndSetTimestamp(struct, model.getTimestamp());
        validateAndSetPartitionBy(struct, model.getPartitionBy());

        ExprNode recordHint = model.getRecordHint();
        if (recordHint != null) {
            try {
                struct.recordCountHint(Numbers.parseInt(recordHint.token));
            } catch (NumericException e) {
                throw QueryError.$(recordHint.position, "Bad int");
            }
        }

        ObjList<ColumnIndexModel> columnIndexModels = model.getColumnIndexModels();
        for (int i = 0, n = columnIndexModels.size(); i < n; i++) {
            ColumnIndexModel cim = columnIndexModels.getQuick(i);

            ExprNode nn = cim.getName();
            ColumnMetadata m = struct.getColumnMetadata(nn.token);

            if (m == null) {
                throw QueryError.invalidColumn(nn.position, nn.token);
            }

            switch (m.getType()) {
                case ColumnType.INT:
                case ColumnType.LONG:
                case ColumnType.SYMBOL:
                case ColumnType.STRING:
                    m.indexed = true;
                    m.distinctCountHint = cim.getBuckets();
                    break;
                default:
                    throw QueryError.$(nn.position, "Type index not supported");
            }
        }

        JournalWriter w = factory.bulkWriter(struct);
        if (rs != null) {
            try {
                copy(factory, rs, w);
            } catch (Throwable e) {
                w.close();
                throw e;
            }
        }
        return w;
    }

    public static void validateAndSetPartitionBy(JournalStructure struct, ExprNode partitionBy) throws ParserException {
        if (partitionBy == null) {
            return;
        }

        if (struct.hasTimestamp()) {
            int p = PartitionBy.fromString(partitionBy.token);
            if (p == -1) {
                throw QueryError.$(partitionBy.position, "Invalid partition type");
            }
            struct.partitionBy(p);
        } else {
            throw QueryError.$(partitionBy.position, "No timestamp");
        }
    }

    public static void validateAndSetTimestamp(JournalStructure struct, ExprNode timestamp) throws ParserException {

        if (timestamp == null) {
            return;
        }

        int index = struct.getColumnIndex(timestamp.token);
        if (index == -1) {
            throw QueryError.invalidColumn(timestamp.position, timestamp.token);
        }

        if (struct.getColumnMetadata(index).getType() != ColumnType.DATE) {
            throw QueryError.$(timestamp.position, "Not a DATE");
        }

        struct.$ts(index);
    }

    private static void copy(JournalFactory factory, RecordSource rs, JournalWriter w) throws JournalException {
        RecordMetadata metadata = rs.getMetadata();
        final int n = metadata.getColumnCount();
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
        return new JournalStructure(location, m).$ts(rm.getTimestampIndex());
    }

}
