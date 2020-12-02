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
import io.questdb.std.str.StringSink;

import static io.questdb.griffin.engine.functions.catalogue.PgOIDs.PG_CATALOG_OID;
import static io.questdb.griffin.engine.functions.catalogue.PgOIDs.PG_PUBLIC_OID;

public class ClassCatalogueFunctionFactory implements FunctionFactory {
    private static final Log LOG = LogFactory.getLog(ClassCatalogueFunctionFactory.class);
    private static final RecordMetadata METADATA;
    private static final String[] relNames = {"pg_class"};
    private static final int[] relNamespaces = {PG_CATALOG_OID};
    private static final int[] oids = {PgOIDs.PG_CLASS_OID};
    private static final char[] relkinds = {'r'};
    private static final int[] relOwners = {0};

    private static final int fixedClassLen = relNames.length;

    private static final int[][] intColumns = {
            null,
            relNamespaces,
            null,
            relOwners,
            oids
    };

    @Override
    public String getSignature() {
        return "pg_catalog.pg_class()";
    }

    @Override
    public boolean isCursor() {
        return true;
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
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
        private final ClassCatalogueCursor cursor;
        private final long tempMem;

        public ClassCatalogueCursorFactory(CairoConfiguration configuration, RecordMetadata metadata) {
            super(metadata);
            this.tempMem = Unsafe.malloc(Integer.BYTES);
            this.cursor = new ClassCatalogueCursor(configuration, path, tempMem);
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

    private static class ClassCatalogueCursor implements NoRandomAccessRecordCursor {
        private final Path path;
        private final FilesFacade ff;
        private final DelegatingRecord record = new DelegatingRecord();
        private final DiskReadingRecord diskReadingRecord = new DiskReadingRecord();
        private final StaticReadingRecord staticReadingRecord = new StaticReadingRecord();
        private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
        private final int plimit;
        private final int[] intValues = new int[5];
        private final long tempMem;
        private long findFileStruct = 0;
        private int fixedRelPos = -1;

        public ClassCatalogueCursor(CairoConfiguration configuration, Path path, long tempMem) {
            this.ff = configuration.getFilesFacade();
            this.path = path;
            this.path.of(configuration.getRoot()).$();
            this.plimit = this.path.length();
            this.record.setDelegate(staticReadingRecord);
            this.intValues[1] = PG_PUBLIC_OID; // relnamespace
            this.intValues[3] = 0; // relowner
            this.intValues[4] = 0; // OID
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
            return record;
        }

        @Override
        public boolean hasNext() {
            if (++fixedRelPos < fixedClassLen) {
                return true;
            }

            record.setDelegate(diskReadingRecord);
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
        public void toTop() {
            if (findFileStruct != 0) {
                ff.findClose(findFileStruct);
                findFileStruct = 0;
            }
            fixedRelPos = -1;
            record.setDelegate(staticReadingRecord);
        }

        @Override
        public long size() {
            return -1;
        }

        private boolean next0() {
            do {
                final long pname = ff.findName(findFileStruct);
                nativeLPSZ.of(pname);
                if (ff.findType(findFileStruct) == Files.DT_DIR && Chars.notDots(nativeLPSZ)) {
                    path.trimTo(plimit);
                    if (ff.exists(path.concat(pname).concat(TableUtils.META_FILE_NAME).$())) {
                        // open metadata file and read id
                        long fd = ff.openRO(path);
                        if (fd > -1) {
                            if (ff.read(fd, tempMem, Integer.BYTES, TableUtils.META_OFFSET_TABLE_ID) == Integer.BYTES) {
                                intValues[4] = Unsafe.getUnsafe().getInt(tempMem);
                                ff.close(fd);
                                return true;
                            }
                            LOG.error().$("Could not read table id [fd=").$(fd).$(", errno=").$(ff.errno()).$(']').$();
                            ff.close(fd);
                        } else {
                            LOG.error().$("could not read metadata [file=").$(path).$(']').$();
                        }
                        intValues[4] = -1;
                        return true;
                    }
                }
            } while (ff.findNext(findFileStruct) > 0);

            ff.findClose(findFileStruct);
            findFileStruct = 0;
            return false;
        }

        private final static class DelegatingRecord implements Record {
            private Record delegate;

            @Override
            public char getChar(int col) {
                return delegate.getChar(col);
            }

            @Override
            public int getInt(int col) {
                return delegate.getInt(col);
            }

            @Override
            public CharSequence getStr(int col) {
                return delegate.getStr(col);
            }

            @Override
            public CharSequence getStrB(int col) {
                return delegate.getStr(col);
            }

            @Override
            public int getStrLen(int col) {
                return delegate.getStrLen(col);
            }

            public Record getDelegate() {
                return delegate;
            }

            public void setDelegate(Record delegate) {
                this.delegate = delegate;
            }
        }

        private class StaticReadingRecord implements Record {
            @Override
            public char getChar(int col) {
                return relkinds[fixedRelPos];
            }

            @Override
            public int getInt(int col) {
                return intColumns[col][fixedRelPos];
            }

            @Override
            public CharSequence getStr(int col) {
                return relNames[fixedRelPos];
            }

            @Override
            public CharSequence getStrB(int col) {
                return relNames[fixedRelPos];
            }

            @Override
            public int getStrLen(int col) {
                return getStr(col).length();
            }
        }

        private class DiskReadingRecord implements Record {
            private final StringSink utf8SinkA = new StringSink();
            private final StringSink utf8SinkB = new StringSink();

            @Override
            public char getChar(int col) {
                return 'r';
            }

            @Override
            public int getInt(int col) {
                return intValues[col];
            }

            @Override
            public CharSequence getStr(int col) {
                utf8SinkA.clear();
                if (Chars.utf8DecodeZ(ff.findName(findFileStruct), utf8SinkA)) {
                    return utf8SinkA;
                } else {
                    return null;
                }
            }

            @Override
            public CharSequence getStrB(int col) {
                utf8SinkB.clear();
                if (Chars.utf8DecodeZ(ff.findName(findFileStruct), utf8SinkB)) {
                    return utf8SinkB;
                } else {
                    return null;
                }
            }

            @Override
            public int getStrLen(int col) {
                return getStr(col).length();
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("relname", ColumnType.STRING, null));
        metadata.add(new TableColumnMetadata("relnamespace", ColumnType.INT, null));
        metadata.add(new TableColumnMetadata("relkind", ColumnType.CHAR, null));
        metadata.add(new TableColumnMetadata("relowner", ColumnType.INT, null));
        metadata.add(new TableColumnMetadata("oid", ColumnType.INT, null));
        METADATA = metadata;
    }
}
