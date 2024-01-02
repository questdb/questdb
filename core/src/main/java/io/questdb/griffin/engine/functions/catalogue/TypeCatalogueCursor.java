/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.pgwire.PGOids;
import io.questdb.std.Numbers;

import static io.questdb.cutlass.pgwire.PGOids.*;

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
            intValues[9] = Numbers.INT_NaN;
            intValues[10] = 0;
            intValues[11] = 0;
            intValues[12] = 0;
            intValues[13] = 0;
            intValues[21] = 0;//typndims
            intValues[22] = 0;//typcollation
            return true;
        }
        return false;
    }

    @Override
    public long size() {
        return rowCount;
    }

    @Override
    public void toTop() {
        row = -1;
    }

    class TypeCatalogueRecord implements Record {

        @Override
        public boolean getBool(int col) {
            if (col == 18) {//typisdefined
                return true;
            }
            return false;
        }

        @Override
        public char getChar(int col) {
            if (col == 8) {
                return PG_TYPE_TO_CATEGORY[row];
            }
            if (col == 19) {//typalign
                return 'c';//char alignment
            }
            if (col == 20) {//typstorage
                return 'p';//plain
            }
            return 'b';
        }

        @Override
        public int getInt(int col) {
            return intValues[col];
        }

        @Override
        public short getShort(int col) {
            if (col == 15) {
                return PG_TYPE_TO_LENGTH[row];
            }

            return -1;
        }

        @Override
        public CharSequence getStr(int col) {
            if (col == 1) {
                return PG_TYPE_TO_NAME[row];
            }
            if (col == 23) {
                return PG_TYPE_TO_DEFAULT[row];
            }
            return null;
        }

        @Override
        public CharSequence getStrB(int col) {
            return getStr(col);
        }

        @Override
        public int getStrLen(int col) {
            CharSequence str = getStr(col);
            return str == null ? TableUtils.NULL_LEN : str.length();
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("oid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("typname", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("typbasetype", ColumnType.INT));
        metadata.add(new TableColumnMetadata("typarray", ColumnType.INT));
        metadata.add(new TableColumnMetadata("typnamespace", ColumnType.INT));
        metadata.add(new TableColumnMetadata("typnotnull", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("typtypmod", ColumnType.INT));
        metadata.add(new TableColumnMetadata("typtype", ColumnType.CHAR));
        metadata.add(new TableColumnMetadata("typcategory", ColumnType.CHAR));
        metadata.add(new TableColumnMetadata("typrelid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("typelem", ColumnType.INT));
        metadata.add(new TableColumnMetadata("typreceive", ColumnType.INT));
        metadata.add(new TableColumnMetadata("typdelim", ColumnType.INT));
        metadata.add(new TableColumnMetadata("typinput", ColumnType.INT));

        metadata.add(new TableColumnMetadata("typowner", ColumnType.INT));
        metadata.add(new TableColumnMetadata("typlen", ColumnType.SHORT));
        metadata.add(new TableColumnMetadata("typbyval", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("typispreferred", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("typisdefined", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("typalign", ColumnType.CHAR));
        metadata.add(new TableColumnMetadata("typstorage", ColumnType.CHAR));
        metadata.add(new TableColumnMetadata("typndims", ColumnType.INT));
        metadata.add(new TableColumnMetadata("typcollation", ColumnType.INT));
        metadata.add(new TableColumnMetadata("typdefault", ColumnType.STRING));

        METADATA = metadata;
    }
}
