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

import static io.questdb.griffin.engine.functions.catalogue.PgOIDs.PG_CATALOG_OID;

public class AttributeCatalogueFunctionFactory implements FunctionFactory {

    private static final RecordMetadata METADATA;

    private static final String[] relNames = {"pg_attribute"};
    private static final int[] relNamespaces = {PG_CATALOG_OID};
    private static final int[] oids = {PgOIDs.PG_CLASS_OID};
    private static final char[] relkinds = {'r'};
    private static final int[] relOwners = {0};

    private static final int[][] intColumns = {
            null,
            relNamespaces,
            null,
            relOwners,
            oids
    };

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
        private final AttributeClassCatalogueCursor cursor;

        public AttributeCatalogueCursorFactory(CairoConfiguration configuration, RecordMetadata metadata) {
            super(metadata);
            this.cursor = new AttributeClassCatalogueCursor(configuration, path);
        }

        @Override
        public void close() {
            Misc.free(path);
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
        private long findFileStruct = 0;
        private int columnIndex = 0;
        private int dummyTableId = 1000;

        public AttributeClassCatalogueCursor(CairoConfiguration configuration, Path path) {
            this.ff = configuration.getFilesFacade();
            this.path = path;
            this.path.of(configuration.getRoot()).$();
            this.plimit = this.path.length();
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
                final long pname = ff.findName(findFileStruct);
                nativeLPSZ.of(pname);
                if (
                        ff.findType(findFileStruct) == Files.DT_DIR
                                && !Chars.equals(nativeLPSZ, '.')
                                && !Chars.equals(nativeLPSZ, "..")
                ) {
                    path.trimTo(plimit);
                    path.concat(pname);
                    if (ff.exists(path.concat(TableUtils.META_FILE_NAME).$())) {
                        ReadOnlyColumn metaMem = new OnePageMemory(ff, path, ff.length(path));
                        int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
                        if (columnIndex == columnCount) {
                            dummyTableId++;
                            columnIndex = 0;
                            continue;
                        }
                        long offset = TableUtils.getColumnNameOffset(columnCount);
                        for (int i = 0; i < columnCount; i++) {
                            CharSequence name = metaMem.getStr(offset);
                            if (columnIndex == i) {
                                diskReadingRecord.name = name;
                                diskReadingRecord.columnNumber = (short) (i + 1);
                                diskReadingRecord.tableId = dummyTableId;
                                columnIndex++;
                                return true;
                            }
                            offset += ReadOnlyMemory.getStorageLength(name);
                        }
                    }
                }
            } while (ff.findNext(findFileStruct) > 0);


            ff.findClose(findFileStruct);
            findFileStruct = 0;
            return false;
        }

        private static class DiskReadingRecord implements Record {
            public CharSequence name = null;
            public short columnNumber = 0;
            public int tableId = 0;

            @Override
            public short getShort(int col) {
                return columnNumber;
            }

            @Override
            public int getInt(int col) {
                return tableId;
            }

            @Override
            public CharSequence getStr(int col) {
                return name;
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("attrelid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("attname", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("attnum", ColumnType.SHORT));
        METADATA = metadata;
    }
}
