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

class DescriptionCatalogueCursor implements NoRandomAccessRecordCursor {
    static final RecordMetadata METADATA;
    private int row = -1;
    private static final int[] objOids = {0};
    private static final int[] classOids = {PgOIDs.PG_CLASS_OID};
    private static final int[] objSubIds = {0};
    private static final String[] descriptions = {"table"};
    private static final  int objOidsLen = objOids.length;
    private final DescriptionCatalogueRecord record = new DescriptionCatalogueRecord();
    private static final int[][] intColumns = {
            objOids,
            classOids,
            objSubIds,
            null
    };

    @Override
    public void close() {
        row = 0;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public boolean hasNext() {
        return ++row < objOidsLen;
    }

    @Override
    public void toTop() {
        row = -1;
    }

    @Override
    public long size() {
        return 1;
    }

    private class DescriptionCatalogueRecord implements Record {

        @Override
        public int getInt(int col) {
            return intColumns[col][row];
        }

        @Override
        public CharSequence getStr(int col) {
            return descriptions[row];
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("objoid", ColumnType.INT, null));
        metadata.add(new TableColumnMetadata("classoid", ColumnType.INT, null));
        metadata.add(new TableColumnMetadata("objsubid", ColumnType.INT, null));
        metadata.add(new TableColumnMetadata("description", ColumnType.STRING, null));
        METADATA = metadata;
    }
}
