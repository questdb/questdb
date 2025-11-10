/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public class PgAttrDefFunctionFactory implements FunctionFactory {

    static final RecordMetadata METADATA;
    private static final Log LOG = LogFactory.getLog(PgAttrDefFunctionFactory.class);
    private static final String SIGNATURE = "pg_attrdef()";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new CursorFunction(
                new AttrDefCatalogueCursorFactory(configuration, METADATA)
        ) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    private static class AttrDefCatalogueCursor implements NoRandomAccessRecordCursor {
        private final AttrDefCatalogueCursor.DiskReadingRecord diskReadingRecord = new AttrDefCatalogueCursor.DiskReadingRecord();
        private final FilesFacade ff;
        private final Path path;
        private final int plimit;
        private final long tempMem;
        private int columnCount;
        private int columnIndex = 0;
        // pointer to a struct containing file info,
        // special values:
        //  0: cursor opened, but hasNext() has not been called yet
        // -1: cursor reached the end, hasNext() returns false, call toTop() to reset the cursor
        private long findFileStruct = 0;
        private boolean foundMetadataFile = false;
        private boolean hasNextFile = true;
        private boolean readNextFileFromDisk = true;
        private int tableId = -1;

        public AttrDefCatalogueCursor(CairoConfiguration configuration, Path path, long tempMem) {
            this.ff = configuration.getFilesFacade();
            this.path = path;
            this.path.of(configuration.getDbRoot()).$();
            this.plimit = path.size();
            this.tempMem = tempMem;
        }

        @Override
        public void close() {
            findFileStruct = findFileStruct > 0 ? ff.findClose(findFileStruct) : -1;
        }

        @Override
        public Record getRecord() {
            return diskReadingRecord;
        }

        @Override
        public boolean hasNext() {
            if (findFileStruct == 0) {
                findFileStruct = ff.findFirst(path.trimTo(plimit).$());
                if (findFileStruct > 0) {
                    return next0();
                }

                findFileStruct = -1;
                return false;
            } else if (findFileStruct == -1) {
                return false;
            }
            return next0();
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            if (findFileStruct > 0) {
                ff.findClose(findFileStruct);
            }
            findFileStruct = 0;
            columnIndex = 0;
            readNextFileFromDisk = true;
            hasNextFile = true;
            foundMetadataFile = false;
            tableId = -1;
        }

        private boolean next0() {
            do {
                if (readNextFileFromDisk) {
                    foundMetadataFile = false;
                    final long pUtf8NameZ = ff.findName(findFileStruct);
                    if (hasNextFile) {
                        if (ff.isDirOrSoftLinkDirNoDots(path, plimit, pUtf8NameZ, ff.findType(findFileStruct))) {
                            if (ff.exists(path.concat(TableUtils.META_FILE_NAME).$())) {
                                long fd = ff.openRO(path.$());
                                if (fd > -1) {
                                    if (ff.read(fd, tempMem, Integer.BYTES, TableUtils.META_OFFSET_TABLE_ID) == Integer.BYTES) {
                                        tableId = Unsafe.getUnsafe().getInt(tempMem);
                                        if (ff.read(fd, tempMem, Integer.BYTES, TableUtils.META_OFFSET_COUNT) == Integer.BYTES) {
                                            foundMetadataFile = true;
                                            columnCount = Unsafe.getUnsafe().getInt(tempMem);
                                        } else {
                                            LOG.error().$("Could not read column count [fd=").$(fd).$(", errno=").$(ff.errno()).I$();
                                        }
                                    } else {
                                        LOG.error().$("Could not read table id [fd=").$(fd).$(", errno=").$(ff.errno()).I$();
                                    }
                                    ff.close(fd);
                                } else {
                                    LOG.error().$("could not read metadata [file=").$(path).I$();
                                }
                            }
                        }
                        hasNextFile = ff.findNext(findFileStruct) > 0;
                    }
                }

                if (foundMetadataFile) {
                    for (int i = 0; i < columnCount; i++) {
                        if (columnIndex == i) {
                            diskReadingRecord.columnNumber = (short) (i + 1);
                            columnIndex++;
                            if (columnIndex == columnCount) {
                                readNextFileFromDisk = true;
                                columnIndex = 0;
                            } else {
                                readNextFileFromDisk = false;
                            }
                            return true;
                        }
                    }
                }
            } while (hasNextFile);

            ff.findClose(findFileStruct);
            hasNextFile = true;
            foundMetadataFile = false;
            tableId = -1;
            findFileStruct = -1;
            return false;
        }

        private class DiskReadingRecord implements Record {

            public short columnNumber = 0;

            @Override
            public int getInt(int col) {
                return tableId;
            }

            @Override
            public short getShort(int col) {
                return columnNumber;
            }

            @Override
            public CharSequence getStrA(int col) {
                return "";
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStrA(col);
            }

            @Override
            public int getStrLen(int col) {
                return getStrA(col).length();
            }
        }
    }

    private static class AttrDefCatalogueCursorFactory extends AbstractRecordCursorFactory {
        private final AttrDefCatalogueCursor cursor;
        private final Path path;
        private long tempMem;

        public AttrDefCatalogueCursorFactory(CairoConfiguration configuration, RecordMetadata metadata) {
            super(metadata);
            try {
                this.path = new Path();
                this.tempMem = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_FUNC_RSS);
                this.cursor = new AttrDefCatalogueCursor(configuration, path, tempMem);
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type(SIGNATURE);
        }

        @Override
        protected void _close() {
            Misc.free(path);
            tempMem = Unsafe.free(tempMem, Integer.BYTES, MemoryTag.NATIVE_FUNC_RSS);
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("adrelid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("adnum", ColumnType.SHORT));
        metadata.add(new TableColumnMetadata("adbin", ColumnType.STRING));
        METADATA = metadata;
    }
}
