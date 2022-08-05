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
import io.questdb.cairo.sql.Record;
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

public abstract class AbstractPgClassFunctionFactory implements FunctionFactory {
    private static final Log LOG = LogFactory.getLog(AbstractPgClassFunctionFactory.class);
    private static final RecordMetadata METADATA;
    private static final String[] relNames = {"pg_class"};
    private static final int fixedClassLen = relNames.length;
    private static final int INDEX_OID = 0;
    private static final int INDEX_RELNAME = 1;

    private static final int[] staticOid = {PGOids.PG_CLASS_OID};
    private static final int[] staticRelNamespace = {PG_CATALOG_OID};
    private static final int[] staticRelType = {0};
    private static final int[] staticRelOfType = {0};
    private static final int[] staticRelOwner = {0};
    private static final int[] staticRelAm = {0};
    private static final int[] staticRelFileNode = {0};
    // todo: adjust tablespace
    private static final int[] staticRelTablespace = {0};
    private static final int[] staticRelAllVisible = {0};
    private static final int[] staticRelToastRelId = {0};
    private static final int[] staticRelRewrite = {0};
    private static final int[][] staticIntColumns = {
            staticOid,
            null,
            staticRelNamespace,
            staticRelType,
            staticRelOfType,
            staticRelOwner,
            staticRelAm,
            staticRelFileNode,
            staticRelTablespace,
            null,
            null,
            staticRelAllVisible,
            staticRelToastRelId,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            staticRelRewrite
    };

    @Override
    public boolean isCursor() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new CursorFunction(
                new PgClassCursorFactory(
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

    private static class PgClassCursorFactory extends AbstractRecordCursorFactory {

        private final Path path = new Path();
        private final PgClassRecordCursor cursor;
        private final long tempMem;

        public PgClassCursorFactory(CairoConfiguration configuration, RecordMetadata metadata) {
            super(metadata);
            this.tempMem = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            this.cursor = new PgClassRecordCursor(configuration, path, tempMem);
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
        protected void _close() {
            Misc.free(path);
            Unsafe.free(tempMem, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static class PgClassRecordCursor implements NoRandomAccessRecordCursor {
        private final Path path;
        private final FilesFacade ff;
        private final DelegatingRecord record = new DelegatingRecord();
        private final DiskReadingRecord diskReadingRecord = new DiskReadingRecord();
        private final StaticReadingRecord staticReadingRecord = new StaticReadingRecord();
        private final StringSink sink = new StringSink();
        private final int plimit;
        private final int[] intValues = new int[28];
        private final long tempMem;
        private long findFileStruct = 0;
        private int fixedRelPos = -1;

        public PgClassRecordCursor(CairoConfiguration configuration, Path path, long tempMem) {
            this.ff = configuration.getFilesFacade();
            this.path = path;
            this.path.of(configuration.getRoot()).$();
            this.plimit = this.path.length();
            this.record.of(staticReadingRecord);
            // oid
            this.intValues[0] = 0; // OID
            // relnamespace
            this.intValues[2] = PG_PUBLIC_OID;
            // reltype
            this.intValues[3] = 0;
            // reloftype
            this.intValues[4] = 0;
            // relowner
            this.intValues[5] = 0;
            // relam
            this.intValues[6] = 0;
            // relfilenode
            this.intValues[7] = 0;
            // reltablespace
            this.intValues[8] = 0;
            // relallvisible
            this.intValues[11] = 0;
            // reltoastrelid
            this.intValues[12] = 0;
            // relrewrite
            this.intValues[27] = 0;
            this.tempMem = tempMem;
        }

        @Override
        public void close() {
            findFileStruct = ff.findClose(findFileStruct);
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
            findFileStruct = ff.findClose(findFileStruct);
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
                                intValues[INDEX_OID] = Unsafe.getUnsafe().getInt(tempMem);
                                ff.close(fd);
                                return true;
                            }
                            LOG.error().$("Could not read table id [fd=").$(fd).$(", errno=").$(ff.errno()).$(']').$();
                            ff.close(fd);
                        } else {
                            LOG.error().$("could not read metadata [file=").$(path).$(']').$();
                        }
                        intValues[INDEX_OID] = -1;
                        return true;
                    }
                }
            } while (ff.findNext(findFileStruct) > 0);

            findFileStruct = ff.findClose(findFileStruct);
            return false;
        }

        private class StaticReadingRecord implements Record {
            @Override
            public boolean getBool(int col) {
                return false;
            }

            @Override
            public char getChar(int col) {
                switch (col) {
                    case 15:
                        // relpersistence
                        return 'u';
                    case 16:
                        // relkind
                        return 'r';
                    default:
                        // relreplident
                        return 'd';
                }
            }

            @Override
            public short getShort(int col) {
                // todo: do we need the number of columns for 'relnatts'?
                return 0;
            }

            @Override
            public float getFloat(int col) {
                return -1;
            }

            @Override
            public int getInt(int col) {
                return staticIntColumns[col][fixedRelPos];
            }

            @Override
            public long getLong(int col) {
                return 0;
            }

            @Override
            public CharSequence getStr(int col) {
                if (col == INDEX_RELNAME) {
                    // relname
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
                if (col == INDEX_RELNAME) {
                    // relname
                    return relNames[fixedRelPos].length();
                }
                return -1;
            }
        }

        private class DiskReadingRecord implements Record {
            private final StringSink utf8SinkB = new StringSink();

            @Override
            public boolean getBool(int col) {
                // most 'bool' fields are false, except 'relispopulated'
                return col == 24;
            }

            @Override
            public char getChar(int col) {
                switch (col) {
                    case 15:
                        // relpersistence
                        return 'p';
                    case 16:
                        // relkind
                        return 'r';
                    default:
                        // relreplident
                        return 'd';
                }
            }

            @Override
            public short getShort(int col) {
                // todo: do we need the number of columns for 'relnatts'?
                return 0;
            }

            @Override
            public float getFloat(int col) {
                return -1;
            }

            @Override
            public int getInt(int col) {
                return intValues[col];
            }

            @Override
            public long getLong(int col) {
                return 0;
            }

            @Override
            public CharSequence getStr(int col) {
                if (col == INDEX_RELNAME) {
                    // relname
                    return sink;
                }
                return null;
            }

            @Override
            public CharSequence getStrB(int col) {
                if (col == INDEX_RELNAME) {
                    // relname
                    return getName(utf8SinkB);
                }
                return null;
            }

            @Override
            public int getStrLen(int col) {
                if (col == INDEX_RELNAME) {
                    // relname
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
        metadata.add(new TableColumnMetadata("oid", 1, ColumnType.INT));
        metadata.add(new TableColumnMetadata("relname", 2, ColumnType.STRING));
        // The OID of the namespace that contains this relation
        // references pg_namespace.oid)
        metadata.add(new TableColumnMetadata("relnamespace", 3, ColumnType.INT));
        // The OID of the data type that corresponds to this table's row type, if any; zero for indexes, sequences, and toast tables, which have no pg_type entry
        // references pg_type.oid
        metadata.add(new TableColumnMetadata("reltype", 4, ColumnType.INT));
        // For typed tables, the OID of the underlying composite type; zero for all other relations
        // references pg_type.oid
        metadata.add(new TableColumnMetadata("reloftype", 5, ColumnType.INT));
        // Owner of the relation
        // references pg_authid.oid
        metadata.add(new TableColumnMetadata("relowner", 6, ColumnType.INT));
        // If this is a table or an index, the access method used (heap, B-tree, hash, etc.); otherwise zero (zero occurs for sequences, as well as relations without storage, such as views)
        // references pg_am.oid
        metadata.add(new TableColumnMetadata("relam", 7, ColumnType.INT));
        // Name of the on-disk file of this relation; zero means this is a “mapped” relation whose disk file name is determined by low-level state
        metadata.add(new TableColumnMetadata("relfilenode", 8, ColumnType.INT));
        // The tablespace in which this relation is stored. If zero, the database's default tablespace is implied. (Not meaningful if the relation has no on-disk file.)
        // references pg_tablespace.oid
        metadata.add(new TableColumnMetadata("reltablespace", 9, ColumnType.INT));
        // Size of the on-disk representation of this table in pages (of size BLCKSZ). This is only an estimate used by the planner. It is updated by VACUUM, ANALYZE, and a few DDL commands such as CREATE INDEX.
        metadata.add(new TableColumnMetadata("relpages", 10, ColumnType.BOOLEAN));
        // Number of live rows in the table. This is only an estimate used by the planner. It is updated by VACUUM, ANALYZE, and a few DDL commands such as CREATE INDEX. If the table has never yet been vacuumed or analyzed, reltuples contains -1 indicating that the row count is unknown.
        metadata.add(new TableColumnMetadata("reltuples", 11, ColumnType.FLOAT));
        // Number of pages that are marked all-visible in the table's visibility map. This is only an estimate used by the planner. It is updated by VACUUM, ANALYZE, and a few DDL commands such as CREATE INDEX.
        metadata.add(new TableColumnMetadata("relallvisible", 12, ColumnType.INT));
        // OID of the TOAST table associated with this table, zero if none. The TOAST table stores large attributes “out of line” in a secondary table.
        // references pg_class.oid
        metadata.add(new TableColumnMetadata("reltoastrelid", 13, ColumnType.INT));
        // True if this is a table and it has (or recently had) any indexes
        metadata.add(new TableColumnMetadata("relhasindex", 14, ColumnType.BOOLEAN));
        // True if this table is shared across all databases in the cluster. Only certain system catalogs (such as pg_database) are shared.
        metadata.add(new TableColumnMetadata("relisshared", 15, ColumnType.BOOLEAN));
        //p = permanent table, u = unlogged table, t = temporary table
        metadata.add(new TableColumnMetadata("relpersistence", 16, ColumnType.CHAR));
        // r = ordinary table,
        // i = index,
        // S = sequence,
        // t = TOAST table,
        // v = view,
        // m = materialized view,
        // c = composite type,
        // f = foreign table,
        // p = partitioned table,
        // I = partitioned index
        metadata.add(new TableColumnMetadata("relkind", 17, ColumnType.CHAR));
        // Number of user columns in the relation (system columns not counted). There must be this many corresponding entries in pg_attribute. See also pg_attribute.attnum.
        metadata.add(new TableColumnMetadata("relnatts", 18, ColumnType.SHORT));
        // Number of CHECK constraints on the table; see pg_constraint catalog
        metadata.add(new TableColumnMetadata("relchecks", 19, ColumnType.SHORT));
        // True if table has (or once had) rules; see pg_rewrite catalog
        metadata.add(new TableColumnMetadata("relhasrules", 20, ColumnType.BOOLEAN));
        // True if table has (or once had) triggers; see pg_trigger catalog
        metadata.add(new TableColumnMetadata("relhastriggers", 21, ColumnType.BOOLEAN));
        // True if table or index has (or once had) any inheritance children
        metadata.add(new TableColumnMetadata("relhassubclass", 22, ColumnType.BOOLEAN));
        // True if table has row-level security enabled; see pg_policy catalog
        metadata.add(new TableColumnMetadata("relrowsecurity", 23, ColumnType.BOOLEAN));
        // True if row-level security (when enabled) will also apply to table owner; see pg_policy catalog
        metadata.add(new TableColumnMetadata("relforcerowsecurity", 24, ColumnType.BOOLEAN));
        // True if relation is populated (this is true for all relations other than some materialized views)
        metadata.add(new TableColumnMetadata("relispopulated", 25, ColumnType.BOOLEAN));
        // Columns used to form “replica identity” for rows: d = default (primary key, if any), n = nothing, f = all columns, i = index with indisreplident set (same as nothing if the index used has been dropped)
        metadata.add(new TableColumnMetadata("relreplident", 26, ColumnType.CHAR));
        // True if table or index is a partition
        metadata.add(new TableColumnMetadata("relispartition", 27, ColumnType.BOOLEAN));
        // For new relations being written during a DDL operation that requires a table rewrite, this contains the OID of the original relation; otherwise zero. That state is only visible internally; this field should never contain anything other than zero for a user-visible relation.
        // references pg_class.oid
        metadata.add(new TableColumnMetadata("relrewrite", 28, ColumnType.INT));
        // All transaction IDs before this one have been replaced with a permanent (“frozen”) transaction ID in this table. This is used to track whether the table needs to be vacuumed in order to prevent transaction ID wraparound or to allow pg_xact to be shrunk. Zero (InvalidTransactionId) if the relation is not a table.
        metadata.add(new TableColumnMetadata("relfrozenxid", 29, ColumnType.LONG));
        // All multixact IDs before this one have been replaced by a transaction ID in this table. This is used to track whether the table needs to be vacuumed in order to prevent multixact ID wraparound or to allow pg_multixact to be shrunk. Zero (InvalidMultiXactId) if the relation is not a table.
        metadata.add(new TableColumnMetadata("relminmxid", 30, ColumnType.LONG));
        metadata.add(new TableColumnMetadata("relacl", 31, ColumnType.STRING));
        metadata.add(new TableColumnMetadata("reloptions", 32, ColumnType.STRING));
        metadata.add(new TableColumnMetadata("relpartbound", 33, ColumnType.STRING));
        metadata.add(new TableColumnMetadata("relhasoids", 34, ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("xmin", 35, ColumnType.LONG));
        METADATA = metadata;
    }
}
