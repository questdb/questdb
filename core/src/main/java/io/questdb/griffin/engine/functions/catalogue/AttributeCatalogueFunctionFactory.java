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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.MappedReadOnlyMemory;
import io.questdb.cairo.vm.SinglePageMappedReadOnlyPageMemory;
import io.questdb.cairo.vm.VmUtils;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.*;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cutlass.pgwire.PGOids.PG_TYPE_TO_SIZE_MAP;
import static io.questdb.cutlass.pgwire.PGOids.TYPE_OIDS;

public class AttributeCatalogueFunctionFactory implements FunctionFactory {

    private static final RecordMetadata METADATA;

    @Override
    public String getSignature() {
        return "pg_catalog.pg_attribute()";
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
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

        private final Path path = new Path();
        private final MappedReadOnlyMemory metaMem = new SinglePageMappedReadOnlyPageMemory();
        private final AttributeClassCatalogueCursor cursor;

        public AttributeCatalogueCursorFactory(CairoConfiguration configuration, RecordMetadata metadata) {
            super(metadata);
            this.cursor = new AttributeClassCatalogueCursor(configuration, path, metaMem);
        }

        @Override
        public void close() {
            Misc.free(path);
            Misc.free(metaMem);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }
    }

    private static class AttributeClassCatalogueCursor implements NoRandomAccessRecordCursor {
        private final Path path;
        private final FilesFacade ff;
        private final DiskReadingRecord diskReadingRecord = new DiskReadingRecord();
        private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
        private final int plimit;
        private final MappedReadOnlyMemory metaMem;
        private long findFileStruct = 0;
        private int columnIndex = 0;
        private int tableId = 1000;
        private boolean readNextFileFromDisk = true;
        private int columnCount;
        private boolean hasNextFile = true;
        private boolean foundMetadataFile = false;

        public AttributeClassCatalogueCursor(CairoConfiguration configuration, Path path, MappedReadOnlyMemory metaMem) {
            this.ff = configuration.getFilesFacade();
            this.path = path;
            this.path.of(configuration.getRoot()).$();
            this.plimit = this.path.length();
            this.metaMem = metaMem;
        }

        @Override
        public void close() {
            if (findFileStruct != 0) {
                ff.findClose(findFileStruct);
                findFileStruct = 0;
            }
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
        public void toTop() {
            if (findFileStruct != 0) {
                ff.findClose(findFileStruct);
                findFileStruct = 0;
            }
        }

        @Override
        public long size() {
            return -1;
        }

        private boolean next0() {
            do {
                if (readNextFileFromDisk) {
                    foundMetadataFile = false;
                    final long pname = ff.findName(findFileStruct);
                    if (hasNextFile) {
                        nativeLPSZ.of(pname);
                        if (
                                ff.findType(findFileStruct) == Files.DT_DIR && Chars.notDots(nativeLPSZ)
                        ) {
                            path.trimTo(plimit);
                            path.concat(pname);
                            if (ff.exists(path.concat(TableUtils.META_FILE_NAME).$())) {
                                foundMetadataFile = true;
                                metaMem.of(ff, path, ff.getPageSize(), ff.length(path));
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
                            int type = TYPE_OIDS.get(TableUtils.getColumnType(metaMem, i));
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
                        offset += VmUtils.getStorageLength(name);
                    }
                }
            } while (hasNextFile);

            ff.findClose(findFileStruct);
            findFileStruct = 0;
            hasNextFile = true;
            foundMetadataFile = false;
            return false;
        }

        static class DiskReadingRecord implements Record {
            public CharSequence name = null;
            public final short[] shortValues = new short[9];
            public final int[] intValues = new int[9];

            @Override
            public short getShort(int col) {
                return shortValues[col];
            }

            @Override
            public int getInt(int col) {
                return intValues[col];
            }

            @Override
            public CharSequence getStr(int col) {
                return name;
            }

            @Override
            public CharSequence getStrB(int col) {
                return name;
            }

            @Override
            public boolean getBool(int col) {
                return false;
            }

            @Override
            public char getChar(int col) {
                //from the PG docs:
                // attidentity ->	If a zero byte (''), then not an identity column. Otherwise, a = generated always, d = generated by default.
                return Character.MIN_VALUE;
            }

            @Override
            public int getStrLen(int col) {
                return getStr(col).length();
            }

        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("attrelid", ColumnType.INT, null));
        metadata.add(new TableColumnMetadata("attname", ColumnType.STRING, null));
        metadata.add(new TableColumnMetadata("attnum", ColumnType.SHORT, null));
        metadata.add(new TableColumnMetadata("atttypid", ColumnType.INT, null));
        metadata.add(new TableColumnMetadata("attnotnull", ColumnType.BOOLEAN, null));
        metadata.add(new TableColumnMetadata("atttypmod", ColumnType.INT, null));
        metadata.add(new TableColumnMetadata("attlen", ColumnType.SHORT, null));
        metadata.add(new TableColumnMetadata("attidentity", ColumnType.CHAR, null));
        metadata.add(new TableColumnMetadata("attisdropped", ColumnType.BOOLEAN, null));
        METADATA = metadata;
    }
}
