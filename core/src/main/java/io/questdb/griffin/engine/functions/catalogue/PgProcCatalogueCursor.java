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

import static io.questdb.cutlass.pgwire.PGOids.*;

class PgProcCatalogueCursor implements NoRandomAccessRecordCursor {
    static final RecordMetadata METADATA;
    private static final int rowCount = PG_TYPE_TO_PROC_NAME.length;
    private final PgProdCatalogueRecord record = new PgProdCatalogueRecord();
    private int row = -1;

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
        return ++row < rowCount;
    }

    @Override
    public long size() {
        return rowCount;
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void toTop() {
        row = -1;
    }

    class PgProdCatalogueRecord implements Record {

        @Override
        public boolean getBool(int col) {
            switch (col) {
                case 10: // prosecdef
                    return false;
                case 11: // proleakproof
                    return false;
                case 12: // proisstrict
                    return true;
                case 13: // proretset
                    return false;
            }
            throw new UnsupportedOperationException("not a bool col: " + col);
        }

        @Override
        public char getChar(int col) {
            switch (col) {
                case 9: // prokind
                    return 'f';
                case 14: // provolatile
                    return 'i';
                case 15: //proparallel
                    return 's';
            }
            throw new UnsupportedOperationException("not a char col: " + col);
        }

        @Override
        public float getFloat(int col) {
            return 0;
        }

        @Override
        public int getInt(int col) {
            switch (col) {
                case 0: // oid (of type proc)
                    return PG_TYPE_PROC_OIDS.get(row);
                case 2: // pronamespace
                    return PGOids.PG_PUBLIC_OID;
                case 3: // proowner
                    return 0;
                case 4: // prolang
                    return 0;
                case 7: // provariadic
                    return 0;
                case 8: // prosupport
                    return 0;
                case 18: // prorettype
                    // todo: this assume there is the same number of procedures
                    // as types. this breaks at the moment we introduce
                    // array types beyond float[]. it will need some rework.
                    return PG_TYPE_OIDS.get(row);
            }
            throw new UnsupportedOperationException("not a int col: " + col);
        }

        @Override
        public short getShort(int col) {
            switch (col) {
                case 16: // pronargs
                    return 1;
                case 17: // pronargdefaults
                    return 0;
            }
            throw new UnsupportedOperationException("not a short col: " + col);
        }

        @Override
        public CharSequence getStrA(int col) {
            switch (col) {
                case 1: // proname
                    return PG_TYPE_TO_PROC_NAME[row];
                case 19:
                    return PG_TYPE_TO_PROC_SRC[row];
                case 20:
                    return null;

            }
            throw new UnsupportedOperationException("not a string col: " + col);
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
