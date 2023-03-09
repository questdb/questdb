/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cutlass.pgwire.PGOids;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import static io.questdb.cutlass.pgwire.PGOids.PG_TYPE_TO_SIZE_MAP;

public class PgAttributeFunctionFactory implements FunctionFactory {

    private static final RecordMetadata METADATA;
    private static final String SIGNATURE = "pg_attribute()";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new CursorFunction(
                new AttributeCatalogueCursorFactory(
                        configuration,
                        METADATA
                )
        ) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    private static class AttributeCatalogueCursorFactory extends AbstractRecordCursorFactory {

        private final AttributeClassCatalogueCursor cursor;
        private final MemoryMR metaMem = Vm.getMRInstance();
        private final Path path = new Path();

        public AttributeCatalogueCursorFactory(CairoConfiguration configuration, RecordMetadata metadata) {
            super(metadata);
            this.cursor = new AttributeClassCatalogueCursor(configuration, path, metaMem);
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
            Misc.free(metaMem);
        }
    }

    private static class AttributeClassCatalogueCursor implements NoRandomAccessRecordCursor {
        private final DiskReadingRecord diskReadingRecord = new DiskReadingRecord();
        private final FilesFacade ff;
        private final MemoryMR metaMem;
        private final Path path;
        private final int plimit;
        private int columnCount;
        private int columnIndex = 0;
        private long findFileStruct = 0;
        private boolean foundMetadataFile = false;
        private boolean hasNextFile = true;
        private boolean readNextFileFromDisk = true;
        private int tableId = 1000;

        public AttributeClassCatalogueCursor(CairoConfiguration configuration, Path path, MemoryMR metaMem) {
            this.ff = configuration.getFilesFacade();
            this.path = path;
            this.path.of(configuration.getRoot()).$();
            this.plimit = this.path.length();
            this.metaMem = metaMem;
        }

        @Override
        public void close() {
            findFileStruct = ff.findClose(findFileStruct);
            metaMem.close();
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

                findFileStruct = 0;
                return false;
            }

            return next0();
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            findFileStruct = ff.findClose(findFileStruct);
        }

        private boolean next0() {
            do {
                if (readNextFileFromDisk) {
                    foundMetadataFile = false;
                    final long pUtf8NameZ = ff.findName(findFileStruct);
                    if (hasNextFile) {
                        if (ff.isDirOrSoftLinkDirNoDots(path, plimit, pUtf8NameZ, ff.findType(findFileStruct))) {
                            if (ff.exists(path.concat(TableUtils.META_FILE_NAME).$())) {
                                foundMetadataFile = true;
                                metaMem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
                                columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
                                tableId = metaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
                            }
                        }
                        hasNextFile = ff.findNext(findFileStruct) > 0;
                    }
                }

                if (foundMetadataFile) {
                    long offset = TableUtils.getColumnNameOffset(columnCount);
                    for (int i = 0; i < columnCount; i++) {
                        CharSequence name = metaMem.getStr(offset);
                        if (columnIndex == i) {
                            int type = PGOids.getTypeOid(TableUtils.getColumnType(metaMem, i));
                            diskReadingRecord.intValues[3] = type;
                            diskReadingRecord.name = name;
                            diskReadingRecord.shortValues[2] = (short) (i + 1);
                            diskReadingRecord.shortValues[6] = (short) PG_TYPE_TO_SIZE_MAP.get(type);
                            diskReadingRecord.intValues[0] = tableId;
                            columnIndex++;
                            if (columnIndex == columnCount) {
                                readNextFileFromDisk = true;
                                columnIndex = 0;
                            } else {
                                readNextFileFromDisk = false;
                            }
                            return true;
                        }
                        offset += Vm.getStorageLength(name);
                    }
                }
            } while (hasNextFile);

            findFileStruct = ff.findClose(findFileStruct);
            hasNextFile = true;
            foundMetadataFile = false;
            return false;
        }

        static class DiskReadingRecord implements Record {
            public final int[] intValues = new int[9];
            public final short[] shortValues = new short[9];
            private final StringSink strBSink = new StringSink();
            public CharSequence name = null;

            @Override
            public boolean getBool(int col) {
                return col == 9;
            }

            @Override
            public char getChar(int col) {
                //from the PG docs:
                // attidentity ->	If a zero byte (''), then not an identity column. Otherwise, a = generated always, d = generated by default.
                return Character.MIN_VALUE;
            }

            @Override
            public int getInt(int col) {
                return intValues[col];
            }

            @Override
            public short getShort(int col) {
                return shortValues[col];
            }

            @Override
            public CharSequence getStr(int col) {
                return name;
            }

            @Override
            public CharSequence getStrB(int col) {
                if (name != null) {
                    strBSink.clear();
                    strBSink.put(name);
                    return strBSink;
                }
                return null;
            }

            @Override
            public int getStrLen(int col) {
                return getStr(col).length();
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("attrelid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("attname", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("attnum", ColumnType.SHORT));
        metadata.add(new TableColumnMetadata("atttypid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("attnotnull", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("atttypmod", ColumnType.INT));
        metadata.add(new TableColumnMetadata("attlen", ColumnType.SHORT));
        metadata.add(new TableColumnMetadata("attidentity", ColumnType.CHAR));
        metadata.add(new TableColumnMetadata("attisdropped", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("atthasdef", ColumnType.BOOLEAN));
        METADATA = metadata;
    }
}
