/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.TableReader;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Index scans through a pooled reader that keeps the index readers of the partitions it has closed, while columns
 * come and go, change their type, index type and symbol capacity, get renamed, and inserts rewrite partitions. Each
 * check compares the index scans of every indexed column, both directions, with a scan that reads no index, and every
 * column the pooled reader reads with what a fresh reader reads.
 */
public class TableReaderSchemaChangeIndexFuzzTest extends AbstractCairoTest {
    private static final String[] INDEX_TYPES = {"BITMAP", "POSTING", "POSTING DELTA", "POSTING EF"};
    private static final int STEPS = 150;
    private final ObjList<Column> columns = new ObjList<>();
    private final StringSink sql = new StringSink();
    private int nextColumnId;
    private long nextTimestampMicros;

    @Test
    public void testIndexScansAcrossSchemaChanges() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final boolean isWal = rnd.nextBoolean();
        final int maxOpenPartitions = 1 + rnd.nextInt(3);
        final boolean isMergeAppend = rnd.nextBoolean();
        LOG.info().$("schema change index fuzz [wal=").$(isWal)
                .$(", maxOpenPartitions=").$(maxOpenPartitions)
                .$(", mergeAppend=").$(isMergeAppend)
                .I$();
        // the pooled reader closes all but the latest partitions when a query returns it to the pool
        node1.setProperty(PropertyKey.CAIRO_INACTIVE_READER_MAX_OPEN_PARTITIONS, maxOpenPartitions);
        // without merge-append, an out-of-order insert rewrites the partition it lands in
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, isMergeAppend);

        assertMemoryLeak(() -> {
            columns.clear();
            nextColumnId = 0;
            // a microsecond apart, so that every row has its own timestamp and both scan directions agree on the order
            nextTimestampMicros = 1;
            sql.clear();
            sql.put("CREATE TABLE t (ts TIMESTAMP");
            for (int i = 0; i < 3; i++) {
                final Column column = newColumn(rnd);
                columns.add(column);
                sql.put(", ").put(column.name).put(' ').put(column.definition());
            }
            sql.put(") TIMESTAMP(ts) PARTITION BY DAY").put(isWal ? " WAL" : " BYPASS WAL");
            execute(sql);
            insert(rnd, isWal);
            checkIndexScans();

            for (int step = 0; step < STEPS; step++) {
                final int op = rnd.nextInt(100);
                if (op < 30) {
                    insert(rnd, isWal);
                } else if (op < 42) {
                    final Column column = newColumn(rnd);
                    columns.add(column);
                    ddl("ALTER TABLE t ADD COLUMN " + column.name + ' ' + column.definition(), isWal);
                } else if (op < 52) {
                    if (columns.size() > 0) {
                        final int index = rnd.nextInt(columns.size());
                        ddl("ALTER TABLE t DROP COLUMN " + columns.getQuick(index).name, isWal);
                        columns.remove(index);
                        // the columns after it take the slots of the columns before them
                        checkAllColumns();
                    }
                } else if (op < 62) {
                    final Column column = pickSymbol(rnd, false);
                    if (column != null) {
                        column.indexType = INDEX_TYPES[rnd.nextInt(INDEX_TYPES.length)];
                        ddl("ALTER TABLE t ALTER COLUMN " + column.name + " ADD INDEX TYPE " + column.indexType, isWal);
                    }
                } else if (op < 72) {
                    final Column column = pickSymbol(rnd, true);
                    if (column != null) {
                        column.indexType = null;
                        ddl("ALTER TABLE t ALTER COLUMN " + column.name + " DROP INDEX", isWal);
                    }
                } else if (op < 78) {
                    if (columns.size() > 0) {
                        final Column column = columns.getQuick(rnd.nextInt(columns.size()));
                        final String newName = "c" + nextColumnId++;
                        ddl("ALTER TABLE t RENAME COLUMN " + column.name + " TO " + newName, isWal);
                        column.name = newName;
                    }
                } else if (op < 84) {
                    // the converted column keeps its position as a new column instance; a symbol loses its index
                    final Column column = pickText(rnd);
                    if (column != null) {
                        column.type = column.type.equals("SYMBOL") ? "VARCHAR" : "SYMBOL";
                        column.indexType = null;
                        column.symbolCapacity = 128;
                        ddl("ALTER TABLE t ALTER COLUMN " + column.name + " TYPE " + column.type, isWal);
                    }
                } else if (op < 88) {
                    final Column column = pickSymbol(rnd, rnd.nextBoolean());
                    if (column != null) {
                        column.symbolCapacity *= 2;
                        ddl("ALTER TABLE t ALTER COLUMN " + column.name + " SYMBOL CAPACITY " + column.symbolCapacity, isWal);
                    }
                } else {
                    checkIndexScans();
                    checkAllColumns();
                }
            }
            checkIndexScans();
            checkAllColumns();
        });
    }

    private void assertPlanUsesIndex(String query, String expected) throws Exception {
        final StringSink plan = new StringSink();
        printSql("EXPLAIN " + query, plan);
        Assert.assertTrue("index scan expected, plan was:\n" + plan, plan.toString().contains(expected));
    }

    private void checkAllColumns() throws Exception {
        // the pooled reader reopens a closed partition in the objects its slots kept across the schema changes, a
        // fresh reader in objects of its own
        final StringSink actual = new StringSink();
        printSql("SELECT * FROM t", actual);
        final StringSink expected = new StringSink();
        try (
                TableReader reader = newOffPoolReader(configuration, "t");
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
            CursorPrinter.println(cursor, reader.getMetadata(), expected, true, false);
        }
        TestUtils.assertEquals(expected, actual);
    }

    private void checkIndexScans() throws Exception {
        for (int i = 0, n = columns.size(); i < n; i++) {
            final Column column = columns.getQuick(i);
            if (column.indexType != null) {
                for (String value : new String[]{"'x'", "'y'", "NULL"}) {
                    final String where = " FROM t WHERE " + column.name + (value.equals("NULL") ? " IS NULL" : " = " + value);
                    assertSqlCursors("SELECT /*+ no_index */ ts, " + column.name + where, "SELECT ts, " + column.name + where);
                    assertSqlCursors(
                            "SELECT /*+ no_index */ ts, " + column.name + where + " ORDER BY ts DESC",
                            "SELECT ts, " + column.name + where + " ORDER BY ts DESC"
                    );
                }
                // the plan must read the index, or the check above proves nothing
                assertPlanUsesIndex("SELECT ts, " + column.name + " FROM t WHERE " + column.name + " = 'x'", "Index forward scan on: " + column.name);
                assertPlanUsesIndex("SELECT ts, " + column.name + " FROM t WHERE " + column.name + " = 'x' ORDER BY ts DESC", "Index backward scan on: " + column.name);
            }
        }
    }

    private void ddl(String ddl, boolean isWal) throws Exception {
        execute(ddl);
        if (isWal) {
            drainWalQueue();
        }
    }

    private void insert(Rnd rnd, boolean isWal) throws Exception {
        sql.clear();
        sql.put("INSERT INTO t (ts");
        for (int i = 0, n = columns.size(); i < n; i++) {
            sql.put(", ").put(columns.getQuick(i).name);
        }
        sql.put(") VALUES ");
        final int rowCount = 1 + rnd.nextInt(4);
        for (int r = 0; r < rowCount; r++) {
            if (r > 0) {
                sql.put(", ");
            }
            // any of four days, so that a row can land before the rows a partition already has and rewrite it
            final long micros = rnd.nextInt(4) * 86_400_000_000L + rnd.nextInt(1000) * 60_000_000L + nextTimestampMicros++;
            sql.put('(').put(micros).put("::TIMESTAMP");
            for (int i = 0, n = columns.size(); i < n; i++) {
                final Column column = columns.getQuick(i);
                sql.put(", ");
                if (!column.type.equals("INT")) {
                    final int pick = rnd.nextInt(3);
                    sql.put(pick == 0 ? "'x'" : pick == 1 ? "'y'" : "NULL");
                } else {
                    sql.put(rnd.nextInt(100));
                }
            }
            sql.put(')');
        }
        execute(sql);
        if (isWal) {
            drainWalQueue();
        }
    }

    private Column newColumn(Rnd rnd) {
        final Column column = new Column();
        column.name = "c" + nextColumnId++;
        final int pick = rnd.nextInt(6);
        column.type = pick < 4 ? "SYMBOL" : pick == 4 ? "VARCHAR" : "INT";
        if (column.type.equals("SYMBOL") && rnd.nextBoolean()) {
            column.indexType = INDEX_TYPES[rnd.nextInt(INDEX_TYPES.length)];
        }
        return column;
    }

    private Column pickSymbol(Rnd rnd, boolean isIndexed) {
        final int n = columns.size();
        if (n == 0) {
            return null;
        }
        final int start = rnd.nextInt(n);
        for (int i = 0; i < n; i++) {
            final Column column = columns.getQuick((start + i) % n);
            if (column.type.equals("SYMBOL") && (column.indexType != null) == isIndexed) {
                return column;
            }
        }
        return null;
    }

    private Column pickText(Rnd rnd) {
        final int n = columns.size();
        if (n == 0) {
            return null;
        }
        final int start = rnd.nextInt(n);
        for (int i = 0; i < n; i++) {
            final Column column = columns.getQuick((start + i) % n);
            if (!column.type.equals("INT")) {
                return column;
            }
        }
        return null;
    }

    private static class Column {
        String indexType;
        String name;
        int symbolCapacity = 128;
        String type;

        String definition() {
            if (!type.equals("SYMBOL")) {
                return type;
            }
            return "SYMBOL CAPACITY " + symbolCapacity + (indexType == null ? "" : " INDEX TYPE " + indexType);
        }
    }
}
