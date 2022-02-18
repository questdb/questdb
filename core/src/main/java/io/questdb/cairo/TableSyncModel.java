/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.std.str.StringSink;
import io.questdb.tasks.TableWriterTask;

public class TableSyncModel implements Mutable, Sinkable {

    public static final int TABLE_ACTION_KEEP = 0;
    public static final int TABLE_ACTION_TRUNCATE = 1;
    public static final int COLUMN_META_ACTION_REPLACE = 1;
    public static final int COLUMN_META_ACTION_MOVE = 2;
    public static final int COLUMN_META_ACTION_REMOVE = 3;
    public static final int COLUMN_META_ACTION_ADD = 4;
    public static final int PARTITION_ACTION_WHOLE = 0;
    public static final int PARTITION_ACTION_APPEND = 1;
    static final String[] ACTION_NAMES = {
            "whole",
            "append"
    };
    private static final int SLOTS_PER_PARTITION = 8;
    private static final int SLOTS_PER_COLUMN_META_INDEX = 2;
    private static final int SLOTS_PER_COLUMN_TOP = 4;
    private static final int SLOTS_PER_VAR_COLUMN_SIZE = 4;
    // see toSink() method for example of how to unpack this structure
    private final LongList partitions = new LongList();
    // Array of (long,long) pairs. First long contains value of COLUMN_META_ACTION_*, second value encodes
    // (int,int) column movement indexes (from,to)
    private final LongList columnMetaIndex = new LongList();
    // this metadata is only for columns that need to added on slave
    private final ObjList<TableColumnMetadata> addedColumnMetadata = new ObjList<>();

    // this encodes (long,long,long,long) per non-zero column top
    // the idea here is to store tops densely to avoid issues sending a bunch of zeroes
    // across network. Structure is as follows:
    // long0 = partition timestamp
    // long1 = column index (this is really an int)
    // long2 = column top value
    // long3 = unused
    private final LongList columnTops = new LongList();

    // This encodes (long,long,long,long) per non-zero variable length column
    // we are not encoding lengths of the fixed-width column because it is enough to send row count.
    // Structure is as follows:
    // long0 = partition timestamp
    // long1 = column index (this is really an int)
    // long2 = size of column on master
    // long3 = unused
    private final LongList varColumnSizes = new LongList();

    private int tableAction = 0;
    private long dataVersion;
    private long maxTimestamp;

    public void addColumnMetaAction(int action, int from, int to) {
        columnMetaIndex.add((long) action, Numbers.encodeLowHighInts(from, to));
    }

    public void addColumnMetadata(TableColumnMetadata columnMetadata) {
        this.addedColumnMetadata.add(columnMetadata);
    }

    public void addColumnTop(long timestamp, int columnIndex, long topValue) {
        columnTops.add(timestamp, columnIndex, topValue, 0);
    }

    public void addPartitionAction(
            long action,
            long timestamp,
            long startRow,
            long rowCount,
            long nameTxn,
            long dataTxn
    ) {
        partitions.add(action, timestamp, startRow, rowCount, nameTxn, dataTxn, 0, 0);
    }

    public void addVarColumnSize(long timestamp, int columnIndex, long size) {
        varColumnSizes.add(timestamp, columnIndex, size, 0);
    }

    @Override
    public void clear() {
        partitions.clear();
        columnMetaIndex.clear();
        tableAction = 0;
        columnTops.clear();
        varColumnSizes.clear();
        addedColumnMetadata.clear();
    }

    public void fromBinary(long mem) {
        long p = mem;
        tableAction = Unsafe.getUnsafe().getInt(p);
        p += 4;
        dataVersion = Unsafe.getUnsafe().getLong(p);
        p += 8;
        maxTimestamp = Unsafe.getUnsafe().getLong(p);
        p += 8;

        int n = Unsafe.getUnsafe().getInt(p);
        p += 4;
        for (int i = 0; i < n; i += SLOTS_PER_COLUMN_TOP) {
            columnTops.add(
                    Unsafe.getUnsafe().getLong(p),
                    Unsafe.getUnsafe().getInt(p + 8),
                    Unsafe.getUnsafe().getLong(p + 12),
                    0
            );

            p += 20;
        }

        n = Unsafe.getUnsafe().getInt(p);
        p += 4;
        for (int i = 0; i < n; i += SLOTS_PER_VAR_COLUMN_SIZE) {
            varColumnSizes.add(
                    Unsafe.getUnsafe().getLong(p),
                    Unsafe.getUnsafe().getInt(p + 8),
                    Unsafe.getUnsafe().getLong(p + 12),
                    0
            );

            p += 20;
        }

        n = Unsafe.getUnsafe().getInt(p);
        p += 4;
        for (int i = 0; i < n; i += SLOTS_PER_PARTITION) {
            partitions.add(
                    Unsafe.getUnsafe().getInt(p), // action
                    Unsafe.getUnsafe().getLong(p + 4), // partition timestamp
                    Unsafe.getUnsafe().getLong(p + 12), // start row
                    Unsafe.getUnsafe().getLong(p + 20), // row count
                    Unsafe.getUnsafe().getLong(p + 28), // name txn
                    Unsafe.getUnsafe().getLong(p + 36), // data txn
                    0,
                    0
            );
            p += 44;
        }

        n = Unsafe.getUnsafe().getInt(p);
        p += 4;

        final StringSink nameSink = Misc.getThreadLocalBuilder();
        for (int i = 0; i < n; i++) {
            int nameLen = Unsafe.getUnsafe().getInt(p);
            p += 4;

            for (long lim = p + nameLen * 2L; p < lim; p += 2) {
                nameSink.put(Unsafe.getUnsafe().getChar(p));
            }
            int type = Unsafe.getUnsafe().getInt(p);
            p += 4;
            long hash = Unsafe.getUnsafe().getLong(p);
            p += 8;
            boolean indexed = Unsafe.getUnsafe().getByte(p++) == 0;
            int valueBlockCapacity = Unsafe.getUnsafe().getInt(p);
            p += 4;
            addedColumnMetadata.add(
                    new TableColumnMetadata(
                            Chars.toString(nameSink),
                            hash,
                            type,
                            indexed,
                            valueBlockCapacity,
                            true,
                            null
                    )
            );
        }

        n = Unsafe.getUnsafe().getInt(p);
        p += 4;
        for (int i = 0; i < n; i += SLOTS_PER_COLUMN_META_INDEX) {
            columnMetaIndex.add(
                    (long) Unsafe.getUnsafe().getInt(p),
                    Unsafe.getUnsafe().getLong(p + 4)
            );
            p += 12;
        }
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

    public void setDataVersion(long dataVersion) {
        this.dataVersion = dataVersion;
    }

    public void setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public void toBinary(TableWriterTask sink) {
        sink.putInt(tableAction);
        sink.putLong(dataVersion);
        sink.putLong(maxTimestamp);

        int n = columnTops.size();
        sink.putInt(n); // column top count
        if (n > 0) {
            for (int i = 0; i < n; i += SLOTS_PER_COLUMN_TOP) {
                sink.putLong(columnTops.getQuick(i)); // partition timestamp
                sink.putInt((int) columnTops.getQuick(i + 1)); // column index
                sink.putLong(columnTops.getQuick(i + 2)); // column top
            }
        }

        n = varColumnSizes.size();
        sink.putInt(n);
        if (n > 0) {
            for (int i = 0; i < n; i += SLOTS_PER_VAR_COLUMN_SIZE) {
                sink.putLong(varColumnSizes.getQuick(i)); // partition timestamp
                sink.putInt((int) varColumnSizes.getQuick(i + 1)); // column index
                sink.putLong(varColumnSizes.getQuick(i + 2)); // column top
            }
        }

        n = partitions.size();
        sink.putInt(n); // partition count
        for (int i = 0; i < n; i += SLOTS_PER_PARTITION) {
            sink.putInt((int) partitions.getQuick(i)); // action
            sink.putLong(partitions.getQuick(i + 1)); // partition timestamp
            sink.putLong(partitions.getQuick(i + 2)); // start row
            sink.putLong(partitions.getQuick(i + 3)); // row count
            sink.putLong(partitions.getQuick(i + 4)); // name txn (suffix for partition name)
            sink.putLong(partitions.getQuick(i + 5)); // data txn
        }

        n = addedColumnMetadata.size();
        sink.putInt(n); // column metadata count - this is metadata only for column that have been added
        for (int i = 0; i < n; i++) {
            final TableColumnMetadata metadata = addedColumnMetadata.getQuick(i);
            sink.putStr(metadata.getName());
            sink.putInt(metadata.getType()); // column type
            sink.putLong(metadata.getHash());
            sink.putByte((byte) (metadata.isIndexed() ? 0 : 1)); // column indexed flag
            sink.putInt(metadata.getIndexValueBlockCapacity());
        }

        n = columnMetaIndex.size();
        sink.putInt(n); // column metadata shuffle index - drives rearrangement of existing columns on the slave
        for (int i = 0; i < n; i += SLOTS_PER_COLUMN_META_INDEX) {
            sink.putInt((int) columnMetaIndex.getQuick(i)); // action
            sink.putLong(columnMetaIndex.getQuick(i + 1)); // (int,int) pair on where (from,to) column needs to move
        }
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

        sink.put(',');

        sink.put("maxTimestamp").put(':').put('"').putISODate(maxTimestamp).put('"');

        sink.put('}');

        int n = columnTops.size();
        if (n > 0) {
            sink.put(',');

            sink.putQuoted("columnTops").put(':').put('[');

            for (int i = 0; i < n; i += SLOTS_PER_COLUMN_TOP) {
                if (i > 0) {
                    sink.put(',');
                }

                sink.put('{');
                sink.putQuoted("ts").put(':').put('"').putISODate(columnTops.getQuick(i)).put('"').put(',');
                sink.putQuoted("index").put(':').put(columnTops.getQuick(i + 1)).put(',');
                sink.putQuoted("top").put(':').put(columnTops.getQuick(i + 2));
                sink.put('}');
            }

            sink.put(']');
        }

        n = varColumnSizes.size();
        if (n > 0) {
            sink.put(',');

            sink.putQuoted("varColumns").put(':').put('[');

            for (int i = 0; i < n; i += SLOTS_PER_VAR_COLUMN_SIZE) {
                if (i > 0) {
                    sink.put(',');
                }
                sink.put('{');
                sink.putQuoted("ts").put(':').put('"').putISODate(varColumnSizes.getQuick(i)).put('"').put(',');
                sink.putQuoted("index").put(':').put(varColumnSizes.getQuick(i + 1)).put(',');
                sink.putQuoted("size").put(':').put(varColumnSizes.getQuick(i + 2));
                sink.put('}');
            }

            sink.put(']');
        }

        n = partitions.size();
        if (n > 0) {

            sink.put(',');

            sink.putQuoted("partitions").put(':').put('[');

            for (int i = 0; i < n; i += SLOTS_PER_PARTITION) {
                if (i > 0) {
                    sink.put(',');
                }
                sink.put('{');
                sink.putQuoted("action").put(':').putQuoted(ACTION_NAMES[(int) partitions.getQuick(i)]).put(',');
                sink.putQuoted("ts").put(':').put('"').putISODate(partitions.getQuick(i + 1)).put('"').put(',');
                sink.putQuoted("startRow").put(':').put(partitions.getQuick(i + 2)).put(',');
                sink.putQuoted("rowCount").put(':').put(partitions.getQuick(i + 3)).put(',');
                sink.putQuoted("nameTxn").put(':').put(partitions.getQuick(i + 4)).put(',');
                sink.putQuoted("dataTxn").put(':').put(partitions.getQuick(i + 5));
                sink.put('}');
            }

            sink.put(']');

        }

        n = addedColumnMetadata.size();
        if (n > 0) {

            sink.put(',');

            sink.putQuoted("columnMetaData").put(':').put('[');

            for (int i = 0; i < n; i++) {
                if (i > 0) {
                    sink.put(',');
                }

                sink.put('{');

                final TableColumnMetadata metadata = addedColumnMetadata.getQuick(i);
                sink.putQuoted("name").put(':').putQuoted(metadata.getName()).put(',');
                sink.putQuoted("type").put(':').putQuoted(ColumnType.nameOf(Math.abs(metadata.getType()))).put(',');
                if (metadata.getType() < 0) {
                    sink.putQuoted("deleted").put(':').put("true").put(',');
                }
                sink.putQuoted("hash").put(':').put(metadata.getHash()).put(',');
                sink.putQuoted("index").put(':').put(metadata.isIndexed()).put(',');
                sink.putQuoted("indexCapacity").put(':').put(metadata.getIndexValueBlockCapacity());

                sink.put('}');
            }

            sink.put(']');

        }

        n = columnMetaIndex.size();

        if (n > 0) {
            sink.put(',');

            sink.putQuoted("columnMetaIndex").put(':').put('[');

            for (int i = 0; i < n; i += SLOTS_PER_COLUMN_META_INDEX) {

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
        }
        sink.put('}');
    }
}
