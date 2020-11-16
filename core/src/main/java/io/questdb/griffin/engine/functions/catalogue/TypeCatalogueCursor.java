/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import static io.questdb.cutlass.pgwire.PGOids.PG_TYPE_OIDS;
import static io.questdb.cutlass.pgwire.PGOids.PG_TYPE_TO_NAME;

class TypeCatalogueCursor implements NoRandomAccessRecordCursor {
    static final RecordMetadata METADATA;
    private static final int rowCount = PG_TYPE_OIDS.size();
    private final TypeCatalogueRecord record = new TypeCatalogueRecord();
    public int[] intValues = new int[8];
    private int row = -1;

    public TypeCatalogueCursor() {
        this.intValues[4] = PgOIDs.PG_PUBLIC_OID;
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

        @Override
        public char getChar(int col) {
            return 'b';
        }

        @Override
        public boolean getBool(int col) {
            return false;
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("oid", ColumnType.INT, null));
        metadata.add(new TableColumnMetadata("typname", ColumnType.STRING, null));
        metadata.add(new TableColumnMetadata("typbasetype", ColumnType.INT, null));
        metadata.add(new TableColumnMetadata("typarray", ColumnType.INT, null));
        metadata.add(new TableColumnMetadata("typnamespace", ColumnType.INT, null));
        metadata.add(new TableColumnMetadata("typnotnull", ColumnType.BOOLEAN, null));
        metadata.add(new TableColumnMetadata("typtypmod", ColumnType.INT, null));
        metadata.add(new TableColumnMetadata("typtype", ColumnType.CHAR, null));
        METADATA = metadata;
    }
}
