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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.pgwire.PGOids;
import io.questdb.std.Numbers;

import static io.questdb.cutlass.pgwire.PGOids.PG_TYPE_OIDS;
import static io.questdb.cutlass.pgwire.PGOids.PG_TYPE_TO_NAME;

class TypeCatalogueCursor implements NoRandomAccessRecordCursor {
    static final RecordMetadata METADATA;
    private static final int rowCount = PG_TYPE_OIDS.size();
    public final int[] intValues = new int[METADATA.getColumnCount()];
    private final TypeCatalogueRecord record = new TypeCatalogueRecord();
    private int row = -1;

    public TypeCatalogueCursor() {
        this.intValues[4] = PGOids.PG_PUBLIC_OID;
    }

    @Override
    public void close() {
        row = -1;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public boolean hasNext() {
        if (++row < rowCount) {
            intValues[0] = PG_TYPE_OIDS.get(row);
            intValues[8] = Numbers.INT_NaN;
            intValues[9] = 0;
            intValues[10] = 0;
            intValues[11] = 0;
            intValues[12] = 0;
            return true;
        }
        return false;
    }

    @Override
    public void toTop() {
        row = -1;
    }

    @Override
    public long size() {
        return rowCount;
    }

    class TypeCatalogueRecord implements Record {

        @Override
        public boolean getBool(int col) {
            return false;
        }

        @Override
        public char getChar(int col) {
            return 'b';
        }

        @Override
        public int getInt(int col) {
            return intValues[col];
        }

        @Override
        public CharSequence getStr(int col) {
            return PG_TYPE_TO_NAME[row];
        }

        @Override
        public CharSequence getStrB(int col) {
            return getStr(col);
        }

        @Override
        public int getStrLen(int col) {
            return getStr(col).length();
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("oid", 1, ColumnType.INT));
        metadata.add(new TableColumnMetadata("typname", 2, ColumnType.STRING));
        metadata.add(new TableColumnMetadata("typbasetype", 3, ColumnType.INT));
        metadata.add(new TableColumnMetadata("typarray", 4, ColumnType.INT));
        metadata.add(new TableColumnMetadata("typnamespace", 5, ColumnType.INT));
        metadata.add(new TableColumnMetadata("typnotnull", 6, ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("typtypmod", 7, ColumnType.INT));
        metadata.add(new TableColumnMetadata("typtype", 8, ColumnType.CHAR));
        metadata.add(new TableColumnMetadata("typrelid", 9, ColumnType.INT));
        metadata.add(new TableColumnMetadata("typelem", 10, ColumnType.INT));
        metadata.add(new TableColumnMetadata("typreceive", 11, ColumnType.INT));
        metadata.add(new TableColumnMetadata("typdelim", 12, ColumnType.INT));
        metadata.add(new TableColumnMetadata("typinput", 13, ColumnType.INT));
        METADATA = metadata;
    }
}
