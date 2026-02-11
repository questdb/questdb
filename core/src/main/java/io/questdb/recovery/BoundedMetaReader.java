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

package io.questdb.recovery;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Reads and parses a {@code _meta} file into {@link MetaState}. The {@code _meta}
 * file stores the column count, partition-by strategy, designated timestamp index,
 * column types/flags, and UTF-16 column names.
 *
 * <p>Column count is capped by {@code maxColumns} to bound memory usage.
 */
public class BoundedMetaReader extends AbstractBoundedReader {
    public static final int DEFAULT_MAX_COLUMNS = 10_000;
    private static final int META_FLAG_BIT_INDEXED = 1;
    private final int maxColumns;

    public BoundedMetaReader(FilesFacade ff) {
        this(ff, DEFAULT_MAX_COLUMNS);
    }

    public BoundedMetaReader(FilesFacade ff, int maxColumns) {
        super(ff);
        this.maxColumns = Math.max(1, maxColumns);
    }

    public MetaState read(LPSZ metaPath) {
        final MetaState metaState = new MetaState();
        final String metaPathStr = metaPath.asAsciiCharSequence().toString();
        metaState.setMetaPath(metaPathStr);

        final long fd = ff.openRO(metaPath);
        if (fd < 0) {
            addFileOpenFailure(ff, metaPath, metaPathStr, "_meta", metaState.getIssues());
            return metaState;
        }

        try {
            final long fileSize = ff.length(fd);

            if (fileSize < TableUtils.META_OFFSET_COLUMN_TYPES) {
                metaState.addIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.SHORT_FILE,
                        "_meta file is shorter than header [path=" + metaPathStr + ", size=" + fileSize + ']'
                );
                return metaState;
            }

            // allocate scratch big enough for a column name read (4-byte length + up to 255*2 chars)
            final int scratchSize = Integer.BYTES + 255 * Character.BYTES;
            long scratch = Unsafe.malloc(scratchSize, MemoryTag.NATIVE_DEFAULT);
            try {
                readMeta(fd, fileSize, scratch, metaState);
            } finally {
                Unsafe.free(scratch, scratchSize, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            ff.close(fd);
        }

        return metaState;
    }

    public MetaState readForTable(CharSequence dbRoot, DiscoveredTable table) {
        try (Path path = new Path()) {
            return read(path.of(dbRoot).concat(table.getDirName()).concat(TableUtils.META_FILE_NAME).$());
        }
    }

    private void readColumnNames(long fd, long fileSize, long scratch, MetaState metaState, int columnCount, int onDiskColumnCount) {
        long offset = TableUtils.getColumnNameOffset(onDiskColumnCount);

        for (int i = 0; i < columnCount; i++) {
            int issuesBefore = metaState.getIssues().size();

            // read the 4-byte length prefix
            if (!isRangeReadable(offset, Integer.BYTES, fileSize)) {
                addShortReadIssue(
                        metaState.getIssues(),
                        RecoveryIssueCode.PARTIAL_READ,
                        "column name length outside file [column=" + i + ']',
                        offset,
                        Integer.BYTES,
                        fileSize
                );
                return;
            }

            int nameLen = readIntValue(fd, fileSize, scratch, metaState.getIssues(), offset, "column[" + i + "].nameLength");
            if (metaState.getIssues().size() > issuesBefore) {
                return;
            }

            if (nameLen < 0 || nameLen > 255) {
                metaState.addIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.INVALID_COUNT,
                        "invalid column name length [column=" + i + ", length=" + nameLen + ']'
                );
                return;
            }

            long nameDataOffset = offset + Integer.BYTES;
            long nameDataSize = (long) nameLen * Character.BYTES;

            if (!isRangeReadable(nameDataOffset, nameDataSize, fileSize)) {
                addShortReadIssue(
                        metaState.getIssues(),
                        RecoveryIssueCode.PARTIAL_READ,
                        "column name data outside file [column=" + i + ']',
                        nameDataOffset,
                        (int) nameDataSize,
                        fileSize
                );
                return;
            }

            String name;
            if (nameLen == 0) {
                name = "";
            } else {
                long bytesRead = ff.read(fd, scratch, nameDataSize, nameDataOffset);
                if (bytesRead != nameDataSize) {
                    metaState.addIssue(
                            RecoveryIssueSeverity.ERROR,
                            bytesRead < 0 ? RecoveryIssueCode.IO_ERROR : RecoveryIssueCode.SHORT_FILE,
                            "cannot read column name [column=" + i + ", offset=" + nameDataOffset + ']'
                    );
                    return;
                }

                char[] chars = new char[nameLen];
                for (int c = 0; c < nameLen; c++) {
                    chars[c] = Unsafe.getUnsafe().getChar(scratch + (long) c * Character.BYTES);
                }
                name = new String(chars);
            }

            // look up the previously read column type/indexed info
            MetaColumnState existing = metaState.getColumns().getQuick(i);
            metaState.getColumns().setQuick(i, new MetaColumnState(name, existing.type(), existing.typeName(), existing.indexed(), existing.indexBlockCapacity()));

            offset = nameDataOffset + nameDataSize;
        }
    }

    private void readColumnTypes(long fd, long fileSize, long scratch, MetaState metaState, int columnCount) {
        for (int i = 0; i < columnCount; i++) {
            long typeOffset = TableUtils.META_OFFSET_COLUMN_TYPES + (long) i * TableUtils.META_COLUMN_DATA_SIZE;
            int issuesBefore = metaState.getIssues().size();

            int type = readIntValue(fd, fileSize, scratch, metaState.getIssues(), typeOffset, "column[" + i + "].type");
            if (metaState.getIssues().size() > issuesBefore) {
                return;
            }

            long flagsOffset = typeOffset + Integer.BYTES;
            long flags = readLongValue(fd, fileSize, scratch, metaState.getIssues(), flagsOffset, "column[" + i + "].flags");
            if (metaState.getIssues().size() > issuesBefore) {
                return;
            }

            long indexBlockCapacityOffset = flagsOffset + Long.BYTES;
            int indexBlockCapacity = readIntValue(fd, fileSize, scratch, metaState.getIssues(), indexBlockCapacityOffset, "column[" + i + "].indexBlockCapacity");
            if (metaState.getIssues().size() > issuesBefore) {
                return;
            }

            boolean indexed = (flags & META_FLAG_BIT_INDEXED) != 0;
            String typeName = ColumnType.nameOf(type);

            // store with placeholder name; will be replaced in readColumnNames
            metaState.getColumns().add(new MetaColumnState("", type, typeName, indexed, indexBlockCapacity));
        }
    }

    private void readMeta(long fd, long fileSize, long scratch, MetaState metaState) {
        int issuesBefore = metaState.getIssues().size();

        int columnCount = readIntValue(fd, fileSize, scratch, metaState.getIssues(), TableUtils.META_OFFSET_COUNT, "columnCount");
        if (metaState.getIssues().size() > issuesBefore) {
            return;
        }
        metaState.setColumnCount(columnCount);

        int partitionBy = readIntValue(fd, fileSize, scratch, metaState.getIssues(), TableUtils.META_OFFSET_PARTITION_BY, "partitionBy");
        if (metaState.getIssues().size() > issuesBefore) {
            return;
        }
        metaState.setPartitionBy(partitionBy);

        int timestampIndex = readIntValue(fd, fileSize, scratch, metaState.getIssues(), TableUtils.META_OFFSET_TIMESTAMP_INDEX, "timestampIndex");
        if (metaState.getIssues().size() > issuesBefore) {
            return;
        }
        metaState.setTimestampIndex(timestampIndex);

        if (columnCount < 0) {
            metaState.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.INVALID_COUNT,
                    "column count is negative [value=" + columnCount + ']'
            );
            return;
        }

        if (columnCount == 0) {
            return;
        }

        int effectiveColumnCount = columnCount;
        if (effectiveColumnCount > maxColumns) {
            metaState.addIssue(
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.TRUNCATED_OUTPUT,
                    "column list is capped [requested=" + effectiveColumnCount + ", cap=" + maxColumns + ']'
            );
            effectiveColumnCount = maxColumns;
        }

        readColumnTypes(fd, fileSize, scratch, metaState, effectiveColumnCount);
        if (metaState.getColumns().size() < effectiveColumnCount) {
            return;
        }

        readColumnNames(fd, fileSize, scratch, metaState, effectiveColumnCount, columnCount);
    }
}
