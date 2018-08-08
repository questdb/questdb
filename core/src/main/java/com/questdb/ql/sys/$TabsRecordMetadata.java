/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.sys;

import com.questdb.ql.CollectionRecordMetadata;
import com.questdb.ql.RecordColumnMetadataImpl;
import com.questdb.store.ColumnType;
import com.questdb.store.RecordColumnMetadata;

public class $TabsRecordMetadata extends CollectionRecordMetadata {

    public static final RecordColumnMetadata NAME = new RecordColumnMetadataImpl("name", ColumnType.STRING);
    public static final RecordColumnMetadata PARTITION_BY = new RecordColumnMetadataImpl("partition_by", ColumnType.SYMBOL, PartitionBySymbolTable.INSTANCE, 5, false);
    public static final RecordColumnMetadata PARTITION_COUNT = new RecordColumnMetadataImpl("partition_count", ColumnType.INT);
    public static final RecordColumnMetadata COLUMN_COUNT = new RecordColumnMetadataImpl("column_count", ColumnType.INT);
    public static final RecordColumnMetadata LAST_MODIFIED = new RecordColumnMetadataImpl("last_modified", ColumnType.DATE);
    public static final RecordColumnMetadata SIZE = new RecordColumnMetadataImpl("size", ColumnType.LONG);

    public $TabsRecordMetadata() {
        add(NAME);
        add(PARTITION_BY);
        add(PARTITION_COUNT);
        add(COLUMN_COUNT);
        add(LAST_MODIFIED);
        add(SIZE);
    }

}
