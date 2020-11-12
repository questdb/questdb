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
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.*;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cutlass.pgwire.PGJobContext.typeOids;

public class AttributeCatalogueFunctionFactory implements FunctionFactory {

    private static final RecordMetadata METADATA;

    @Override
    public String getSignature() {
        return "pg_catalog.pg_attribute()";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        return new CursorFunction(
                position,
                new AttributeCatalogueCursorFactory(
                        configuration,
                        METADATA
                )
        );
    }

    private static class AttributeCatalogueCursorFactory extends AbstractRecordCursorFactory {

        private final Path path = new Path();
        private final ReadOnlyColumn metaMem = new OnePageMemory();
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
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
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
        private final ReadOnlyColumn metaMem;
        private long findFileStruct = 0;
        private int columnIndex = 0;
        private int tableId = 1000;
        private boolean readNextFileFromDisk = true;
        private int columnCount;
        private boolean hasNextFile = true;
        private boolean foundMetadataFile = false;

        public AttributeClassCatalogueCursor(CairoConfiguration configuration, Path path, ReadOnlyColumn metaMem) {
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
                            diskReadingRecord.type = typeOids.get(TableUtils.getColumnType(metaMem, i));
                            diskReadingRecord.name = name;
                            diskReadingRecord.columnNumber = (short) (i + 1);
                            diskReadingRecord.tableId = tableId;
                            columnIndex++;
                            if (columnIndex == columnCount) {
                                readNextFileFromDisk = true;
                                columnIndex = 0;
                            } else {
                                readNextFileFromDisk = false;
                            }
                            return true;
                        }
                        offset += ReadOnlyMemory.getStorageLength(name);
                    }
                }
            } while (hasNextFile);

            ff.findClose(findFileStruct);
            findFileStruct = 0;
            hasNextFile = true;
            foundMetadataFile = false;
            return false;
        }

        private static class DiskReadingRecord implements Record {
            public CharSequence name = null;
            public short columnNumber = 0;
            public int tableId = 0;
            public int type = -1;

            @Override
            public short getShort(int col) {
                return col == 2 ? columnNumber : 0;
            }

            @Override
            public int getInt(int col) {
                return col == 0 ? tableId : col == 3 ? type : 0;
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
