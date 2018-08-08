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

public class $ColsRecordMetadata extends CollectionRecordMetadata {

    public static final RecordColumnMetadata TABLE_NAME = new RecordColumnMetadataImpl("table_name", ColumnType.STRING);
    public static final RecordColumnMetadata COLUMN_NAME = new RecordColumnMetadataImpl("column_name", ColumnType.STRING);
    public static final RecordColumnMetadata COLUMN_TYPE = new RecordColumnMetadataImpl("column_type", ColumnType.SYMBOL, ColumnTypeSymbolTable.INSTANCE, 5, false);
    public static final RecordColumnMetadata TIMESTAMP = new RecordColumnMetadataImpl("timestamp", ColumnType.BOOLEAN);
    public static final RecordColumnMetadata PARTITION_BY = new RecordColumnMetadataImpl("partition_by", ColumnType.SYMBOL, PartitionBySymbolTable.INSTANCE, 5, false);
    public static final RecordColumnMetadata INDEXED = new RecordColumnMetadataImpl("indexed", ColumnType.BOOLEAN);
    public static final RecordColumnMetadata INDEX_BUCKETS = new RecordColumnMetadataImpl("index_buckets", ColumnType.INT);

    public $ColsRecordMetadata() {
        add(TABLE_NAME);
        add(COLUMN_NAME);
        add(COLUMN_TYPE);
        add(TIMESTAMP);
        add(PARTITION_BY);
        add(INDEXED);
        add(INDEX_BUCKETS);
    }

}
