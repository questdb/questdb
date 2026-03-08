/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

class PgTypeCatalogueCursor implements NoRandomAccessRecordCursor {
    static final RecordMetadata METADATA;
    private static final int rowCount = PG_TYPE_OIDS.size();
    public final int[] intValues = new int[METADATA.getColumnCount()];
    private final PgTypeCatalogueRecord record = new PgTypeCatalogueRecord();
    private int row = -1;

    public PgTypeCatalogueCursor() {
        this.intValues[4] = PG_CATALOG_OID;
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
            int pgOid = PG_TYPE_OIDS.get(row);
            int arrayOid = PGOids.pgToArrayOid(pgOid);
            int elemOid = PGOids.pgArrayToElementType(pgOid);

            intValues[0] = pgOid;
            intValues[3] = arrayOid;
            intValues[9] = Numbers.INT_NULL;
            intValues[10] = elemOid;
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
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public long size() {
        return rowCount;
    }

    @Override
    public void toTop() {
        row = -1;
    }

    class PgTypeCatalogueRecord implements Record {

        @Override
        public boolean getBool(int col) {
            //typisdefined
            return col == 18;
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
        public CharSequence getStrA(int col) {
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
            return getStrA(col);
        }

        @Override
        public int getStrLen(int col) {
            return TableUtils.lengthOf(getStrA(col));
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("oid", ColumnType.INT));  // 0
        metadata.add(new TableColumnMetadata("typname", ColumnType.STRING));  // 1
        metadata.add(new TableColumnMetadata("typbasetype", ColumnType.INT)); // 2
        metadata.add(new TableColumnMetadata("typarray", ColumnType.INT)); // 3
        metadata.add(new TableColumnMetadata("typnamespace", ColumnType.INT)); // 4
        metadata.add(new TableColumnMetadata("typnotnull", ColumnType.BOOLEAN)); // 5
        metadata.add(new TableColumnMetadata("typtypmod", ColumnType.INT)); // 6
        metadata.add(new TableColumnMetadata("typtype", ColumnType.CHAR)); // 7
        metadata.add(new TableColumnMetadata("typcategory", ColumnType.CHAR)); // 8
        metadata.add(new TableColumnMetadata("typrelid", ColumnType.INT)); // 9
        metadata.add(new TableColumnMetadata("typelem", ColumnType.INT)); // 10
        metadata.add(new TableColumnMetadata("typreceive", ColumnType.INT)); // 11
        metadata.add(new TableColumnMetadata("typdelim", ColumnType.INT)); // 12
        metadata.add(new TableColumnMetadata("typinput", ColumnType.INT)); // 13
        metadata.add(new TableColumnMetadata("typowner", ColumnType.INT)); // 14
        metadata.add(new TableColumnMetadata("typlen", ColumnType.SHORT)); // 15
        metadata.add(new TableColumnMetadata("typbyval", ColumnType.BOOLEAN)); // 16
        metadata.add(new TableColumnMetadata("typispreferred", ColumnType.BOOLEAN)); // 17
        metadata.add(new TableColumnMetadata("typisdefined", ColumnType.BOOLEAN)); // 18
        metadata.add(new TableColumnMetadata("typalign", ColumnType.CHAR)); // 19
        metadata.add(new TableColumnMetadata("typstorage", ColumnType.CHAR)); // 20
        metadata.add(new TableColumnMetadata("typndims", ColumnType.INT)); // 21
        metadata.add(new TableColumnMetadata("typcollation", ColumnType.INT)); // 22
        metadata.add(new TableColumnMetadata("typdefault", ColumnType.STRING)); // 23

        METADATA = metadata;
    }
}
