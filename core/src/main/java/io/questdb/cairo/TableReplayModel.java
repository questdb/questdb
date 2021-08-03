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

package io.questdb.cairo;

import io.questdb.std.*;
import io.questdb.std.str.CharSink;

public class TableReplayModel implements Mutable, Sinkable {

    public static final int TABLE_ACTION_KEEP = 0;
    public static final int TABLE_ACTION_TRUNCATE = 1;
    public static final int COLUMN_META_ACTION_REPLACE = 1;
    public static final int COLUMN_META_ACTION_MOVE = 2;
    public static final int COLUMN_META_ACTION_REMOVE = 3;
    public static final int COLUMN_META_ACTION_ADD = 4;
    static final String[] ACTION_NAMES = {
            "whole",
            "append"
    };
    private static final int SLOTS_PER_PARTITION = 8;
    private static final int SLOTS_PER_COLUMN_META_INDEX = 2;
    private static final int SLOTS_PER_COLUMN_DATA_ACTION = 4;

    private final LongList partitions = new LongList();
    private final LongList columnMetaIndex = new LongList();
    private final LongList columnDataActions = new LongList();
    private final ObjList<TableColumnMetadata> addedColumnMetadata = new ObjList<>();

    private int tableAction = 0;
    private long dataVersion;

    public void addColumnDataAction(int index, long offset, long size, long top) {
        columnDataActions.add(index);
        columnDataActions.add(offset);
        columnDataActions.add(size);
        columnDataActions.add(top);
    }

    public void addColumnMetaAction(int action, int from, int to) {
        columnMetaIndex.add(action);
        columnMetaIndex.add(Numbers.encodeLowHighInts(from, to));
    }

    public void addColumnMetadata(TableColumnMetadata columnMetadata) {
        this.addedColumnMetadata.add(columnMetadata);
    }

    public void addPartitionAction(
            long action,
            long timestamp,
            long startRow,
            long rowCount,
            long nameTxn,
            long dataTxn
    ) {
        partitions.add(action);
        partitions.add(timestamp);
        partitions.add(startRow);
        partitions.add(rowCount);
        partitions.add(nameTxn);
        partitions.add(dataTxn);
        partitions.add(0); // reserved
        partitions.add(0); // reserved
    }

    @Override
    public void clear() {
        partitions.clear();
        columnMetaIndex.clear();
        tableAction = 0;
    }

    public long getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(long dataVersion) {
        this.dataVersion = dataVersion;
    }

    public int getPartitionCount() {
        return partitions.size() / SLOTS_PER_PARTITION;
    }

    public int getTableAction() {
        return tableAction;
    }

    public void setTableAction(int tableAction) {
        this.tableAction = tableAction;
    }

    @Override
    public void toSink(CharSink sink) {

        sink.put('{');
        sink.putQuoted("table").put(':').put('{');

        sink.putQuoted("action").put(':');

        switch (tableAction) {
            case TABLE_ACTION_KEEP:
                sink.putQuoted("keep");
                break;
            case TABLE_ACTION_TRUNCATE:
                sink.putQuoted("truncate");
                break;
            case 2:
                sink.putQuoted("replace");
                break;
        }

        sink.put(',');

        sink.putQuoted("dataVersion").put(':').put(dataVersion);

        sink.put('}');

        sink.put(',');

        sink.putQuoted("partitions").put(':').put('[');

        for (int i = 0, n = partitions.size(); i < n; i += SLOTS_PER_PARTITION) {
            if (i > 0) {
                sink.put(',');
            }
            sink.put('{');
            sink.putQuoted("action").put(':').putQuoted(ACTION_NAMES[(int) partitions.getQuick(i)]).put(',');
            sink.putQuoted("ts").put(':').put('"').putISODate(partitions.getQuick(i + 1)).put('"').put(',');
            sink.putQuoted("startRow").put(':').put(partitions.getQuick(i + 2)).put(',');
            sink.putQuoted("rowCount").put(':').put(partitions.getQuick(i + 3)).put(',');
            sink.putQuoted("nameTxn").put(':').put(partitions.getQuick(i + 4)).put(',');
            sink.putQuoted("dataTxn").put(':').put(partitions.getQuick(i + 5)).put(',');
            sink.put('}');
        }

        sink.put(']');

        sink.put(',');

        sink.putQuoted("columnMetaData").put(':').put('[');

        for (int i = 0, n = addedColumnMetadata.size(); i < n; i++) {
            if (i > 0) {
                sink.put(',');
            }

            sink.put('{');

            final TableColumnMetadata metadata = addedColumnMetadata.getQuick(i);
            sink.putQuoted("name").put(':').putQuoted(metadata.getName()).put(',');
            sink.putQuoted("type").put(':').putQuoted(ColumnType.nameOf(metadata.getType())).put(',');
            sink.putQuoted("index").put(':').put(metadata.isIndexed()).put(',');
            sink.putQuoted("indexCapacity").put(':').put(metadata.getIndexValueBlockCapacity());

            sink.put('}');
        }

        sink.put(']');

        sink.put(',');

        sink.putQuoted("columnMetaIndex").put(':').put('[');

        for (int i = 0, n = columnMetaIndex.size(); i < n; i += SLOTS_PER_COLUMN_META_INDEX) {

            if (i > 0) {
                sink.put(',');
            }

            int action = (int) columnMetaIndex.getQuick(i);
            sink.put('{');
            sink.putQuoted("action").put(':');
            switch (action) {
                case COLUMN_META_ACTION_REPLACE:
                    sink.putQuoted("replace");
                    break;
                case COLUMN_META_ACTION_MOVE:
                    sink.putQuoted("move");
                    break;
                case COLUMN_META_ACTION_REMOVE:
                    sink.putQuoted("remove");
                    break;
                case COLUMN_META_ACTION_ADD:
                    sink.putQuoted("add");
                    break;
                default:
                    break;
            }
            sink.put(',');

            long mix = columnMetaIndex.getQuick(i + 1);
            sink.putQuoted("fromIndex").put(':').put(Numbers.decodeLowInt(mix)).put(',');
            sink.putQuoted("toIndex").put(':').put(Numbers.decodeHighInt(mix));

            sink.put('}');
        }
        sink.put(']');
        sink.put('}');
    }
}
