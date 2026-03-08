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

package io.questdb.cairo;

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;

public class GenericRecordMetadata extends AbstractRecordMetadata {

    /**
     * This method will throw duplicate column exception when called on table writer metadata in case
     * if it has deleted and re-created columns.
     */
    public static void copyColumns(RecordMetadata from, GenericRecordMetadata to) {
        for (int i = 0, n = from.getColumnCount(); i < n; i++) {
            to.add(from.getColumnMetadata(i));
        }
    }

    public static void copyColumns(RecordMetadata from, GenericRecordMetadata to, int columnCount) {
        for (int i = 0, n = Math.min(from.getColumnCount(), columnCount); i < n; i++) {
            to.add(from.getColumnMetadata(i));
        }
    }

    public static GenericRecordMetadata copyDense(TableRecordMetadata tableMetadata) {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        int columnCount = tableMetadata.getColumnCount();
        int timestampIndex = tableMetadata.getTimestampIndex();
        for (int i = 0; i < columnCount; i++) {
            TableColumnMetadata column = tableMetadata.getColumnMetadata(i);
            if (!column.isDeleted()) {
                metadata.add(column);
                if (i == timestampIndex) {
                    metadata.setTimestampIndex(metadata.getColumnCount() - 1);
                }
            }
        }
        return metadata;
    }

    public static GenericRecordMetadata copyOf(RecordMetadata that) {
        if (that != null) {
            if (that instanceof GenericRecordMetadata) {
                return (GenericRecordMetadata) that;
            }
            GenericRecordMetadata metadata = copyOfSansTimestamp(that);
            metadata.setTimestampIndex(that.getTimestampIndex());
            return metadata;
        }
        return null;
    }

    public static GenericRecordMetadata copyOf(RecordMetadata that, int columnCount) {
        if (that != null) {
            if (that instanceof GenericRecordMetadata) {
                return (GenericRecordMetadata) that;
            }
            GenericRecordMetadata metadata = copyOfSansTimestamp(that, columnCount);
            int timestampIndex = that.getTimestampIndex();
            if (timestampIndex < columnCount) {
                metadata.setTimestampIndex(timestampIndex);
            }
            return metadata;
        }
        return null;
    }

    public static GenericRecordMetadata copyOfSansTimestamp(RecordMetadata that, int columnCount) {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        copyColumns(that, metadata, columnCount);
        return metadata;
    }

    public static GenericRecordMetadata copyOfSansTimestamp(RecordMetadata that) {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        copyColumns(that, metadata);
        return metadata;
    }

    /**
     * Creates a new GenericRecordMetadata instance, copying column references (shallow copy).
     * Unlike {@link #copyOf}, this method always creates a new instance even if the input
     * is already a GenericRecordMetadata.
     */
    public static GenericRecordMetadata copyOfNew(RecordMetadata that, int columnCount) {
        if (that != null) {
            GenericRecordMetadata metadata = copyOfSansTimestamp(that, columnCount);
            int timestampIndex = that.getTimestampIndex();
            if (timestampIndex < columnCount) {
                metadata.setTimestampIndex(timestampIndex);
            }
            return metadata;
        }
        return null;
    }

    /**
     * Creates a new GenericRecordMetadata instance, copying column references (shallow copy).
     * Unlike {@link #copyOf}, this method always creates a new instance even if the input
     * is already a GenericRecordMetadata.
     */
    public static GenericRecordMetadata copyOfNew(RecordMetadata that) {
        if (that != null) {
            GenericRecordMetadata metadata = copyOfSansTimestamp(that);
            metadata.setTimestampIndex(that.getTimestampIndex());
            return metadata;
        }
        return null;
    }

    public static RecordMetadata removeTimestamp(RecordMetadata that) {
        if (that.getTimestampIndex() != -1) {
            if (that instanceof GenericRecordMetadata) {
                ((GenericRecordMetadata) that).setTimestampIndex(-1);
                return that;
            }
            return GenericRecordMetadata.copyOfSansTimestamp(that);
        }
        return that;
    }

    public GenericRecordMetadata add(TableColumnMetadata meta) {
        return add(columnCount, meta);
    }

    public GenericRecordMetadata add(int i, TableColumnMetadata meta) {
        int index = columnNameIndexMap.keyIndex(meta.getColumnName());
        if (index > -1) {
            columnNameIndexMap.putAt(index, meta.getColumnName(), i);
            columnMetadata.extendAndSet(i, meta);
            columnCount++;
            return this;
        }
        throw CairoException.duplicateColumn(meta.getColumnName());
    }

    public void addIfNotExists(int i, TableColumnMetadata meta) {
        int index = columnNameIndexMap.keyIndex(meta.getColumnName());
        if (index > -1) {
            columnNameIndexMap.putAt(index, meta.getColumnName(), i);
            columnMetadata.extendAndSet(i, meta);
            columnCount++;
        }
    }

    public void setTimestampIndex(int index) {
        this.timestampIndex = index;
    }
}
