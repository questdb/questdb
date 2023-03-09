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
import io.questdb.cutlass.pgwire.PGOids;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import static io.questdb.cutlass.pgwire.PGOids.PG_CATALOG_OID;
import static io.questdb.cutlass.pgwire.PGOids.PG_PUBLIC_OID;

public class PgClassFunctionFactory implements FunctionFactory {
    private static final int INDEX_OID = 0;
    private static final int INDEX_RELNAME = 1;
    private static final RecordMetadata METADATA;
    private static final String[] relNames = {"pg_class"};
    private static final int fixedClassLen = relNames.length;
    private static final int[] staticOid = {PGOids.PG_CLASS_OID};
    private static final int[] staticRelAllVisible = {0};
    private static final int[] staticRelAm = {0};
    private static final int[] staticRelFileNode = {0};
    private static final int[] staticRelNamespace = {PG_CATALOG_OID};
    private static final int[] staticRelOfType = {0};
    private static final int[] staticRelOwner = {0};
    private static final int[] staticRelRewrite = {0};
    // todo: adjust tablespace
    private static final int[] staticRelTablespace = {0};
    private static final int[] staticRelToastRelId = {0};
    private static final int[] staticRelType = {0};
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
    public String getSignature() {
        return "pg_class()";
    }

    @Override
    public boolean isCursor() {
        return true;
    }

    @Override
    public boolean isRuntimeConstant() {
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

        private final PgClassRecordCursor cursor;
        private final Path path = new Path();
        private final long tempMem;

        public PgClassCursorFactory(CairoConfiguration configuration, RecordMetadata metadata) {
            super(metadata);
            this.tempMem = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_FUNC_RSS);
            this.cursor = new PgClassRecordCursor(configuration);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.of(executionContext.getCairoEngine());
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("pg_class");
        }

        @Override
        protected void _close() {
            Misc.free(path);
            Unsafe.free(tempMem, Integer.BYTES, MemoryTag.NATIVE_FUNC_RSS);
        }
    }

    private static class PgClassRecordCursor implements NoRandomAccessRecordCursor {
        private final PgClassRecordCursor.DiskReadingRecord diskReadingRecord = new PgClassRecordCursor.DiskReadingRecord();
        private final int[] intValues = new int[28];
        private final DelegatingRecord record = new DelegatingRecord();
        private final PgClassRecordCursor.StaticReadingRecord staticReadingRecord = new PgClassRecordCursor.StaticReadingRecord();
        private final ObjList<TableToken> tableBucket = new ObjList<>();
        private CairoEngine engine;
        private int fixedRelPos = -1;
        private int tableIndex = -1;
        private String tableName;

        public PgClassRecordCursor(CairoConfiguration configuration) {
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
        }

        @Override
        public void close() {
        }

        @Override
        public io.questdb.cairo.sql.Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (++fixedRelPos < fixedClassLen) {
                return true;
            }

            record.of(diskReadingRecord);
            if (tableIndex < 0) {
                engine.getTableTokens(tableBucket, false);
                tableIndex = 0;
            }

            if (tableIndex == tableBucket.size()) {
                return false;
            }
            TableToken token = tableBucket.get(tableIndex++);
            tableName = token.getTableName();
            intValues[INDEX_OID] = token.getTableId();
            return true;
        }

        public void of(CairoEngine engine) {
            this.engine = engine;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            fixedRelPos = -1;
            record.of(staticReadingRecord);
            tableIndex = -1;
        }

        private class DiskReadingRecord implements io.questdb.cairo.sql.Record {
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
            public short getShort(int col) {
                // todo: do we need the number of columns for 'relnatts'?
                return 0;
            }

            @Override
            public CharSequence getStr(int col) {
                if (col == INDEX_RELNAME) {
                    // relname
                    return tableName;
                }
                return null;
            }

            @Override
            public CharSequence getStrB(int col) {
                if (col == INDEX_RELNAME) {
                    // relname
                    return tableName;
                }
                return null;
            }

            @Override
            public int getStrLen(int col) {
                if (col == INDEX_RELNAME) {
                    // relname
                    return tableName.length();
                }
                return -1;
            }
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
            public short getShort(int col) {
                // todo: do we need the number of columns for 'relnatts'?
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
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("oid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("relname", ColumnType.STRING));
        // The OID of the namespace that contains this relation
        // references pg_namespace.oid)
        metadata.add(new TableColumnMetadata("relnamespace", ColumnType.INT));
        // The OID of the data type that corresponds to this table's row type, if any; zero for indexes, sequences, and toast tables, which have no pg_type entry
        // references pg_type.oid
        metadata.add(new TableColumnMetadata("reltype", ColumnType.INT));
        // For typed tables, the OID of the underlying composite type; zero for all other relations
        // references pg_type.oid
        metadata.add(new TableColumnMetadata("reloftype", ColumnType.INT));
        // Owner of the relation
        // references pg_authid.oid
        metadata.add(new TableColumnMetadata("relowner", ColumnType.INT));
        // If this is a table or an index, the access method used (heap, B-tree, hash, etc.); otherwise zero (zero occurs for sequences, as well as relations without storage, such as views)
        // references pg_am.oid
        metadata.add(new TableColumnMetadata("relam", ColumnType.INT));
        // Name of the on-disk file of this relation; zero means this is a “mapped” relation whose disk file name is determined by low-level state
        metadata.add(new TableColumnMetadata("relfilenode", ColumnType.INT));
        // The tablespace in which this relation is stored. If zero, the database's default tablespace is implied. (Not meaningful if the relation has no on-disk file.)
        // references pg_tablespace.oid
        metadata.add(new TableColumnMetadata("reltablespace", ColumnType.INT));
        // Size of the on-disk representation of this table in pages (of size BLCKSZ). This is only an estimate used by the planner. It is updated by VACUUM, ANALYZE, and a few DDL commands such as CREATE INDEX.
        metadata.add(new TableColumnMetadata("relpages", ColumnType.BOOLEAN));
        // Number of live rows in the table. This is only an estimate used by the planner. It is updated by VACUUM, ANALYZE, and a few DDL commands such as CREATE INDEX. If the table has never yet been vacuumed or analyzed, reltuples contains -1 indicating that the row count is unknown.
        metadata.add(new TableColumnMetadata("reltuples", ColumnType.FLOAT));
        // Number of pages that are marked all-visible in the table's visibility map. This is only an estimate used by the planner. It is updated by VACUUM, ANALYZE, and a few DDL commands such as CREATE INDEX.
        metadata.add(new TableColumnMetadata("relallvisible", ColumnType.INT));
        // OID of the TOAST table associated with this table, zero if none. The TOAST table stores large attributes “out of line” in a secondary table.
        // references pg_class.oid
        metadata.add(new TableColumnMetadata("reltoastrelid", ColumnType.INT));
        // True if this is a table and it has (or recently had) any indexes
        metadata.add(new TableColumnMetadata("relhasindex", ColumnType.BOOLEAN));
        // True if this table is shared across all databases in the cluster. Only certain system catalogs (such as pg_database) are shared.
        metadata.add(new TableColumnMetadata("relisshared", ColumnType.BOOLEAN));
        //p = permanent table, u = unlogged table, t = temporary table
        metadata.add(new TableColumnMetadata("relpersistence", ColumnType.CHAR));
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
        metadata.add(new TableColumnMetadata("relkind", ColumnType.CHAR));
        // Number of user columns in the relation (system columns not counted). There must be this many corresponding entries in pg_attribute. See also pg_attribute.attnum.
        metadata.add(new TableColumnMetadata("relnatts", ColumnType.SHORT));
        // Number of CHECK constraints on the table; see pg_constraint catalog
        metadata.add(new TableColumnMetadata("relchecks", ColumnType.SHORT));
        // True if table has (or once had) rules; see pg_rewrite catalog
        metadata.add(new TableColumnMetadata("relhasrules", ColumnType.BOOLEAN));
        // True if table has (or once had) triggers; see pg_trigger catalog
        metadata.add(new TableColumnMetadata("relhastriggers", ColumnType.BOOLEAN));
        // True if table or index has (or once had) any inheritance children
        metadata.add(new TableColumnMetadata("relhassubclass", ColumnType.BOOLEAN));
        // True if table has row-level security enabled; see pg_policy catalog
        metadata.add(new TableColumnMetadata("relrowsecurity", ColumnType.BOOLEAN));
        // True if row-level security (when enabled) will also apply to table owner; see pg_policy catalog
        metadata.add(new TableColumnMetadata("relforcerowsecurity", ColumnType.BOOLEAN));
        // True if relation is populated (this is true for all relations other than some materialized views)
        metadata.add(new TableColumnMetadata("relispopulated", ColumnType.BOOLEAN));
        // Columns used to form “replica identity” for rows: d = default (primary key, if any), n = nothing, f = all columns, i = index with indisreplident set (same as nothing if the index used has been dropped)
        metadata.add(new TableColumnMetadata("relreplident", ColumnType.CHAR));
        // True if table or index is a partition
        metadata.add(new TableColumnMetadata("relispartition", ColumnType.BOOLEAN));
        // For new relations being written during a DDL operation that requires a table rewrite, this contains the OID of the original relation; otherwise zero. That state is only visible internally; this field should never contain anything other than zero for a user-visible relation.
        // references pg_class.oid
        metadata.add(new TableColumnMetadata("relrewrite", ColumnType.INT));
        // All transaction IDs before this one have been replaced with a permanent (“frozen”) transaction ID in this table. This is used to track whether the table needs to be vacuumed in order to prevent transaction ID wraparound or to allow pg_xact to be shrunk. Zero (InvalidTransactionId) if the relation is not a table.
        metadata.add(new TableColumnMetadata("relfrozenxid", ColumnType.LONG));
        // All multixact IDs before this one have been replaced by a transaction ID in this table. This is used to track whether the table needs to be vacuumed in order to prevent multixact ID wraparound or to allow pg_multixact to be shrunk. Zero (InvalidMultiXactId) if the relation is not a table.
        metadata.add(new TableColumnMetadata("relminmxid", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("relacl", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("reloptions", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("relpartbound", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("relhasoids", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("xmin", ColumnType.LONG));
        METADATA = metadata;
    }
}
