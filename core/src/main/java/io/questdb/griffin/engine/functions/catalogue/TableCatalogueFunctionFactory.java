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
import io.questdb.std.str.StringSink;

public class TableCatalogueFunctionFactory implements FunctionFactory {

    private static final RecordMetadata METADATA;

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("relname", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("mintimestamp", ColumnType.TIMESTAMP));
        metadata.add(new TableColumnMetadata("maxtimestamp", ColumnType.TIMESTAMP));
        metadata.add(new TableColumnMetadata("rowcount", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("columncount", ColumnType.INT));
        metadata.add(new TableColumnMetadata("partitionby", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("partitioncount", ColumnType.INT));
        metadata.add(new TableColumnMetadata("structversion", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("dataversion", ColumnType.LONG));
        METADATA = metadata;
    }

    @Override
    public String getSignature() {
        return "pg_catalog.information_schema()";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        return new CursorFunction(
                position,
                new ClassCatalogueCursorFactory(
                        configuration,
                        METADATA
                )
        );
    }

    private static class ClassCatalogueCursorFactory extends AbstractRecordCursorFactory {

        private final Path path = new Path();
        private final TableCatalogueCursor cursor;

        public ClassCatalogueCursorFactory(CairoConfiguration configuration, RecordMetadata metadata) {
            super(metadata);
            this.cursor = new TableCatalogueCursor(configuration, path);
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
        public boolean isRandomAccessCursor() {
            return false;
        }
    }

    private static class TableCatalogueCursor implements NoRandomAccessRecordCursor {
        private final Path path;
        private final FilesFacade ff;
        private final CairoConfiguration configuration;
        private final TableCatalogueRecord record = new TableCatalogueRecord();
        private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
        private final int plimit;
        private long findFileStruct = 0;
        private TableMetadata metadata;

        public TableCatalogueCursor(CairoConfiguration configuration, Path path) {
            this.ff = configuration.getFilesFacade();
            this.configuration = configuration;
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
            return record;
        }

        @Override
        public boolean hasNext() {
            if (findFileStruct == 0) {
                findFileStruct = ff.findFirst(path.trimTo(plimit).$());
                if (findFileStruct > 0) {
                    return next0();
                }

                findFileStruct = 0;
                metadata = null;
                return false;
            }

            if (ff.findNext(findFileStruct) > 0) {
                return next0();
            }
            metadata = null;
            return false;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            if (findFileStruct != 0) {
                ff.findClose(findFileStruct);
                findFileStruct = 0;
            }
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
                    if (ff.exists(path.concat(pname).concat(TableUtils.TXN_FILE_NAME).$())) {
                        StringSink utf8Sink = new StringSink();
                        utf8Sink.clear();
                        Chars.utf8DecodeZ(ff.findName(findFileStruct), utf8Sink);
                        metadata = new TableMetadata(configuration, utf8Sink, null, null);
                        return true;
                    }
                }
            } while (ff.findNext(findFileStruct) > 0);

            ff.findClose(findFileStruct);
            findFileStruct = 0;
            metadata = null;
            return false;
        }

        private class TableCatalogueRecord implements Record {
            @Override
            public int getInt(int col) {
                if (metadata != null) {
                    if (col == 4)
                        return metadata.getColumnCount();
                    else
                        return metadata.getPartitionCount();
                } else {
                    return -1;
                }
            }

            @Override
            public CharSequence getStr(int col) {
                if (metadata != null) {
                    if (col == 0)
                        return metadata.getTableName();
                    else
                        return PartitionBy.toString(metadata.getPartitionedBy());
                } else {
                    return null;
                }
            }

            @Override
            public CharSequence getStrB(int col) {
                if (metadata != null) {
                    if (col == 0)
                        return metadata.getTableName();
                    else
                        return PartitionBy.toString(metadata.getPartitionedBy());
                } else {
                    return null;
                }
            }

            @Override
            public long getLong(int col) {
                if (metadata != null) {
                    if (col == 1)
                        return metadata.getMinTimestamp();
                    else if (col == 2)
                        return metadata.getMaxTimestamp();
                    else if (col == 3)
                        return metadata.getRowCount();
                    else if (col == 7)
                        return metadata.getStructVersion();
                    else
                        return metadata.getDataVersion();
                } else {
                    return -1;
                }
            }
        }
    }
}
