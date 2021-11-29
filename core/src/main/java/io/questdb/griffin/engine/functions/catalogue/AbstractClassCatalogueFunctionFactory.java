/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cutlass.pgwire.PGOids;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cutlass.pgwire.PGOids.PG_CATALOG_OID;
import static io.questdb.cutlass.pgwire.PGOids.PG_PUBLIC_OID;

public abstract class AbstractClassCatalogueFunctionFactory implements FunctionFactory {
    private static final Log LOG = LogFactory.getLog(AbstractClassCatalogueFunctionFactory.class);
    private static final RecordMetadata METADATA;
    private static final String[] relNames = {"pg_class"};
    private static final int[] relNamespaces = {PG_CATALOG_OID};
    private static final int[] oids = {PGOids.PG_CLASS_OID};
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
    public boolean isCursor() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new CursorFunction(
                new ClassCatalogueCursorFactory(
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

    private static class ClassCatalogueCursorFactory extends AbstractRecordCursorFactory {

        private final Path path = new Path();
        private final ClassCatalogueCursor cursor;
        private final long tempMem;

        public ClassCatalogueCursorFactory(CairoConfiguration configuration, RecordMetadata metadata) {
            super(metadata);
            this.tempMem = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            this.cursor = new ClassCatalogueCursor(configuration, path, tempMem);
        }

        @Override
        public void close() {
            Misc.free(path);
            Unsafe.free(tempMem, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
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
        private final DelegatingRecordImpl record = new DelegatingRecordImpl();
        private final DiskReadingRecord diskReadingRecord = new DiskReadingRecord();
        private final StaticReadingRecord staticReadingRecord = new StaticReadingRecord();
        private final StringSink sink = new StringSink();
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
            this.record.of(staticReadingRecord);
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

            record.of(diskReadingRecord);
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
            record.of(staticReadingRecord);
        }

        @Override
        public long size() {
            return -1;
        }

        private boolean next0() {
            do {
                final long pUtf8NameZ = ff.findName(findFileStruct);
                final long type = ff.findType(findFileStruct);
                if (Files.isDir(pUtf8NameZ, type, sink)) {
                    path.trimTo(plimit);
                    if (ff.exists(path.concat(pUtf8NameZ).concat(TableUtils.META_FILE_NAME).$())) {
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
                if (col == 0) {
                    return relNames[fixedRelPos];
                }
                return null;
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStr(col);
            }

            @Override
            public int getStrLen(int col) {
                if (col == 0) {
                    return relNames[fixedRelPos].length();
                }
                return -1;
            }
        }

        private class DiskReadingRecord implements Record {
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
                if (col == 0) {
                    return sink;
                }
                return null;
            }

            @Override
            public CharSequence getStrB(int col) {
                if (col == 0) {
                    return getName(utf8SinkB);
                }
                return null;
            }

            @Override
            public int getStrLen(int col) {
                if (col == 0) {
                    return sink.length();
                }
                return -1;
            }

            @Nullable
            private CharSequence getName(StringSink sink) {
                sink.clear();
                if (Chars.utf8DecodeZ(ff.findName(findFileStruct), sink)) {
                    return sink;
                } else {
                    return null;
                }
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("relname", 1, ColumnType.STRING));
        metadata.add(new TableColumnMetadata("relnamespace", 2, ColumnType.INT));
        metadata.add(new TableColumnMetadata("relkind", 3, ColumnType.CHAR));
        metadata.add(new TableColumnMetadata("relowner", 4, ColumnType.INT));
        metadata.add(new TableColumnMetadata("oid", 5, ColumnType.INT));
        metadata.add(new TableColumnMetadata("relpartbound", 6, ColumnType.STRING));
        METADATA = metadata;
    }
}
