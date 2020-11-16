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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;

public class DescriptionCatalogueFunctionFactory implements FunctionFactory {
    private static final Log LOG = LogFactory.getLog(DescriptionCatalogueFunctionFactory.class);
    static final RecordMetadata METADATA;

    @Override
    public String getSignature() {
        return "pg_description()";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        return new CursorFunction(
                position,
                new DescriptionCatalogueCursorFactory(
                        configuration,
                        METADATA
                )
        );
    }

    @Override
    public boolean isCursor() {
        return true;
    }

    static class DescriptionCatalogueCursorFactory extends AbstractRecordCursorFactory {

        private final Path path = new Path();
        private final DescriptionCatalogueCursor cursor;
        private final long tempMem;

        public DescriptionCatalogueCursorFactory(CairoConfiguration configuration, RecordMetadata metadata) {
            super(metadata);
            this.tempMem = Unsafe.malloc(Integer.BYTES);
            this.cursor = new DescriptionCatalogueCursor(configuration, path, tempMem);
        }

        @Override
        public void close() {
            Misc.free(path);
            Unsafe.free(tempMem, Integer.BYTES);
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

    static class DescriptionCatalogueCursor implements NoRandomAccessRecordCursor {
        private final Path path;
        private final FilesFacade ff;
        private final DiskReadingRecord diskReadingRecord = new DiskReadingRecord();
        private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
        private final int plimit;
        private final int[] intValues = new int[4];
        private final long tempMem;
        private long findFileStruct = 0;
        private int columnIndex = 0;
        private boolean readNextFileFromDisk = true;
        private int columnCount;
        private boolean hasNextFile = true;
        private boolean foundMetadataFile = false;

        public DescriptionCatalogueCursor(CairoConfiguration configuration, Path path, long tempMem) {
            this.ff = configuration.getFilesFacade();
            this.path = path;
            this.path.of(configuration.getRoot()).$();
            this.plimit = this.path.length();
            this.intValues[1] = PgOIDs.PG_CLASS_OID; // classoid
            this.intValues[2] = 0; // objsubid
            this.tempMem = tempMem;
        }

        @Override
        public void close() {
            if (findFileStruct != 0) {
                ff.findClose(findFileStruct);
                findFileStruct = 0;
            }
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
                        if (ff.findType(findFileStruct) == Files.DT_DIR && Chars.notDots(nativeLPSZ)) {
                            path.trimTo(plimit);
                            if (ff.exists(path.concat(pname).concat(TableUtils.META_FILE_NAME).$())) {
                                long fd = ff.openRO(path);
                                if (fd > -1) {
                                    if (ff.read(fd, tempMem, Integer.BYTES, TableUtils.META_OFFSET_TABLE_ID) == Integer.BYTES) {
                                        intValues[0] = Unsafe.getUnsafe().getInt(tempMem);
                                        if (ff.read(fd, tempMem, Integer.BYTES, TableUtils.META_OFFSET_COUNT) == Integer.BYTES) {
                                            foundMetadataFile = true;
                                            columnCount = Unsafe.getUnsafe().getInt(tempMem);
                                            diskReadingRecord.columnNumber = 0;
                                            diskReadingRecord.description = "table";
                                        } else {
                                            LOG.error().$("Could not read column count [fd=").$(fd).$(", errno=").$(ff.errno()).$(']').$();
                                        }
                                    } else {
                                        LOG.error().$("Could not read table id [fd=").$(fd).$(", errno=").$(ff.errno()).$(']').$();
                                    }
                                    ff.close(fd);
                                } else {
                                    LOG.error().$("could not read metadata [file=").$(path).$(']').$();
                                }
                            }
                        }
                        hasNextFile = ff.findNext(findFileStruct) > 0;
                        if (foundMetadataFile) {
                            readNextFileFromDisk = false;
                            return true;
                        }
                    }
                }

                if (foundMetadataFile) {
                    for (int i = 0; i < columnCount + 1; i++) {
                        if (columnIndex == i) {
                            diskReadingRecord.columnNumber = (short) (i + 1);
                            diskReadingRecord.description = "column";
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
            findFileStruct = 0;
            hasNextFile = true;
            foundMetadataFile = false;
            intValues[0] = -1;
            return false;
        }

        private class DiskReadingRecord implements Record {

            public short columnNumber = 0;
            public String description;

            @Override
            public int getInt(int col) {
                return intValues[col];
            }

            @Override
            public short getShort(int col) {
                return columnNumber;
            }

            @Override
            public CharSequence getStr(int col) {
                return description;
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStr(col);
            }

            @Override
            public int getStrLen(int col) {
                return getStr(col).length();
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("objoid", ColumnType.INT, null));
        metadata.add(new TableColumnMetadata("classoid", ColumnType.INT, null));
        //TODO the below column was downgraded to short. We need to support type downgrading of compatible types when joining
        metadata.add(new TableColumnMetadata("objsubid", ColumnType.SHORT, null));
        metadata.add(new TableColumnMetadata("description", ColumnType.STRING, null));
        METADATA = metadata;
    }
}
