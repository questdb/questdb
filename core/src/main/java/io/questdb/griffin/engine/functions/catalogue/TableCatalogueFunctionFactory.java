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
        metadata.add(new TableColumnMetadata("partitioncount", ColumnType.INT));
        metadata.add(new TableColumnMetadata("partitionby", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("mintimestamp", ColumnType.TIMESTAMP));
        metadata.add(new TableColumnMetadata("maxtimestamp", ColumnType.TIMESTAMP));
        metadata.add(new TableColumnMetadata("version", ColumnType.LONG));
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
                return false;
            }

            if (ff.findNext(findFileStruct) > 0) {
                return next0();
            }
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
                        return true;
                    }
                }
            } while (ff.findNext(findFileStruct) > 0);

            ff.findClose(findFileStruct);
            findFileStruct = 0;
            return false;
        }

        private class TableCatalogueRecord implements Record {
            private final StringSink utf8SinkA = new StringSink();
            private final StringSink utf8SinkB = new StringSink();

            @Override
            public int getInt(int col) {
                CharSequence table = getStr(0);
                if (table != null) {
                    TableReader reader = new TableReader(configuration, getStr(0));
                    if (col == 1)
                        return reader.getPartitionCount();
                    else
                        return reader.getPartitionedBy();
                } else {
                    return -1;
                }
            }

            @Override
            public CharSequence getStr(int col) {
                CharSequence table;
                utf8SinkA.clear();
                if (Chars.utf8DecodeZ(ff.findName(findFileStruct), utf8SinkA)) {
                    table = utf8SinkA;
                } else {
                    return null;
                }

                if (col == 0) {
                    return table;
                } else {
                    TableReader reader = new TableReader(configuration, getStr(0));
                    return PartitionBy.toString(reader.getPartitionedBy());
                }
            }

            @Override
            public CharSequence getStrB(int col) {
                CharSequence table;
                utf8SinkB.clear();
                if (Chars.utf8DecodeZ(ff.findName(findFileStruct), utf8SinkB)) {
                    table = utf8SinkB;
                } else {
                    return null;
                }

                if (col == 0) {
                    return table;
                } else {
                    TableReader reader = new TableReader(configuration, getStr(0));
                    return PartitionBy.toString(reader.getPartitionedBy());
                }
            }

            @Override
            public long getLong(int col) {
                CharSequence table = getStr(0);
                if (table != null) {
                    TableReader reader = new TableReader(configuration, getStr(0));
                    if (col == 3)
                        return reader.getMinTimestamp();
                    else if (col == 4)
                        return reader.getMaxTimestamp();
                    else if (col == 5)
                        return reader.getVersion();
                    else
                        return reader.getDataVersion();
                } else {
                    return -1;
                }
            }
        }
    }
}
