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

class NamespaceCatalogueCursor implements NoRandomAccessRecordCursor {
    static final RecordMetadata METADATA;
    private static final String[] namespaces = {"pg_catalog", "public"};
    private static final int[] oids = {PgOIDs.PG_CATALOG_OID, PgOIDs.PG_PUBLIC_OID};
    private static final int rowCount = namespaces.length;
    private final NamespaceCatalogueRecord record = new NamespaceCatalogueRecord();
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
    public void toTop() {
        row = -1;
    }

    @Override
    public long size() {
        return rowCount;
    }

    private class NamespaceCatalogueRecord implements Record {
        @Override
        public int getInt(int col) {
            return oids[row];
        }

        @Override
        public CharSequence getStr(int col) {
            return namespaces[row];
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
        metadata.add(new TableColumnMetadata("nspname", ColumnType.STRING, null));
        metadata.add(new TableColumnMetadata("oid", ColumnType.INT, null));
        METADATA = metadata;
    }
}
