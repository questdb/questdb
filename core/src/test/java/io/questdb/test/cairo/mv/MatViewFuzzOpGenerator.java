/*+*****************************************************************************
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

package io.questdb.test.cairo.mv;

import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;

/**
 * Generates the operations of {@link MatViewFuzzTest#testViewAndBaseOperations()}: ingestion into the base table,
 * base partition operations, TRUNCATE, UPDATE, TTL and replacement, view column DDL, view DROP PARTITION,
 * {@code ALTER MATERIALIZED VIEW ... AS}, the REFRESH statements, forced invalidations, restarts of the view states
 * and the job runs that interleave them.
 * <p>
 * The generator keeps the part of the test's state that it draws from: the view's columns in their order, the
 * columns the view's SQL produces, the base days it believes hold a partition and the detached partitions that can be
 * attached. It predicts the outcome of the statements it draws, and the test asserts that the engine agrees: an
 * {@code ALTER ... AS} that replaces a view column with a differently named one at its position is refused. It uses no
 * engine, so {@link MatViewFuzzOpGeneratorCoverageTest} can pin the shapes it draws.
 * <p>
 * Every column name of the SQL stands for one aggregate, so a view row computed under any SQL holds the value the
 * name stands for, see {@link #CATALOGUE_NAMES}. The view's own columns can take another type with an implicit cast,
 * and the view can have columns no SQL produces.
 */
public final class MatViewFuzzOpGenerator {
    // Each name always stands for the same aggregate, whatever SQL a view has.
    public static final String[] CATALOGUE_EXPRESSIONS = {"last(price)", "sum(qty)", "count()", "max(price)"};
    public static final String[] CATALOGUE_NAMES = {"price", "qty", "cnt", "hi"};
    // The view column types each name can take: the aggregate's type and the types it casts to implicitly.
    public static final String[][] CATALOGUE_VIEW_TYPES = {{"DOUBLE"}, {"LONG", "DOUBLE"}, {"LONG", "DOUBLE"}, {"DOUBLE"}};
    // View columns that no SQL produces; they hold NULL in every row.
    public static final String[] EXTRA_NAMES = {"note", "flag"};
    public static final String[] EXTRA_TYPES = {"VARCHAR", "INT"};
    public static final String SYM = "sym";
    public static final String TS = "ts";
    private final boolean[] attachableDays;
    private final boolean[] baseDays;
    private final int dayCount;
    private final ObjList<String> sqlColumns = new ObjList<>();
    private final ObjList<String> viewColumnTypes = new ObjList<>();
    private final ObjList<String> viewColumns = new ObjList<>();
    private int totalWeight;

    public MatViewFuzzOpGenerator(int dayCount) {
        this.dayCount = dayCount;
        this.attachableDays = new boolean[dayCount];
        this.baseDays = new boolean[dayCount];
        for (Kind kind : Kind.values()) {
            totalWeight += weight(kind);
        }
    }

    public static String catalogueExpression(String name) {
        return CATALOGUE_EXPRESSIONS[catalogueIndex(name)];
    }

    public static int catalogueIndex(String name) {
        for (int i = 0; i < CATALOGUE_NAMES.length; i++) {
            if (CATALOGUE_NAMES[i].equals(name)) {
                return i;
            }
        }
        return -1;
    }

    public static boolean isCatalogueName(String name) {
        return catalogueIndex(name) > -1;
    }

    // The same-position rule of ALTER ... AS: refused when a view column that the new SQL does not produce sits at
    // the position of a SQL column that the view lacks.
    public static boolean isRefusedBySamePositionRule(ObjList<String> viewColumns, ObjList<String> sqlColumns) {
        for (int i = 0, n = Math.min(viewColumns.size(), sqlColumns.size()); i < n; i++) {
            if (sqlColumns.indexOf(viewColumns.getQuick(i)) < 0 && viewColumns.indexOf(sqlColumns.getQuick(i)) < 0) {
                return true;
            }
        }
        return false;
    }

    // The SQL of a view or of its twin, its columns in their order.
    public static String viewSql(ObjList<String> sqlColumns) {
        final StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0, n = sqlColumns.size(); i < n; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            final String name = sqlColumns.getQuick(i);
            if (isCatalogueName(name)) {
                sb.append(catalogueExpression(name)).append(" AS ").append(name);
            } else {
                sb.append(name);
            }
        }
        return sb.append(" FROM base SAMPLE BY 1h").toString();
    }

    // How often each operation comes up: ingestion most, the operations that reset most of the oracle's checks least.
    public static int weight(Kind kind) {
        return switch (kind) {
            case INSERT -> 30;
            // an ATTACH needs a detached partition, and falls back to INSERT without one
            case BASE_ATTACH_PARTITION -> 8;
            case BASE_DROP_PARTITION_LIST, BASE_DETACH_PARTITION, VIEW_DROP_COLUMN, VIEW_DROP_PARTITION_LIST,
                 VIEW_RANGE, VIEW_RESUME -> 3;
            case BASE_DROP_PARTITION_WHERE, VIEW_DROP_PARTITION_WHERE, VIEW_FULL, BACKLOG_UPDATE, PURGE_WAL -> 2;
            case BASE_TRUNCATE, BASE_SET_TTL, BASE_REPLACE, BASE_REPLACE_FULL_RESTART, BASE_REPLACE_RESUME_RESTART -> 1;
            case BASE_UPDATE, VIEW_ADD_COLUMN, VIEW_INVALIDATE, RESTART, APPLY_WAL, RUN_REFRESH_JOB -> 4;
            case VIEW_ALTER_AS -> 5;
        };
    }

    public ObjList<String> getSqlColumns() {
        return sqlColumns;
    }

    public ObjList<String> getViewColumnTypes() {
        return viewColumnTypes;
    }

    public ObjList<String> getViewColumns() {
        return viewColumns;
    }

    // The view's first SQL: ts, sym and one to all catalogue columns. The view's columns are the SQL's.
    public ObjList<String> initView(Rnd rnd) {
        sqlColumns.clear();
        sqlColumns.add(TS);
        sqlColumns.add(SYM);
        addRandomCatalogueColumns(rnd, sqlColumns);
        viewColumns.clear();
        viewColumnTypes.clear();
        for (int i = 0, n = sqlColumns.size(); i < n; i++) {
            final String name = sqlColumns.getQuick(i);
            viewColumns.add(name);
            viewColumnTypes.add(name.equals(TS) ? "TIMESTAMP" : name.equals(SYM) ? "SYMBOL" : CATALOGUE_VIEW_TYPES[catalogueIndex(name)][0]);
        }
        return sqlColumns;
    }

    public boolean isAttachable(int day) {
        return attachableDays[day];
    }

    public Op next(Rnd rnd, Op op) {
        op.clear();
        int pick = rnd.nextInt(totalWeight);
        Kind kind = Kind.INSERT;
        for (Kind k : Kind.values()) {
            pick -= weight(k);
            if (pick < 0) {
                kind = k;
                break;
            }
        }
        op.kind = kind;
        switch (kind) {
            case INSERT -> {
                final int rowCount = rnd.nextInt(8) == 0 ? 20 + rnd.nextInt(40) : 1 + rnd.nextInt(5);
                generateRows(rnd, op, rowCount, -1);
                // a row into a detached day recreates the partition, which then no longer attaches, so most rows
                // keep away from detached days
                for (int i = 0, n = op.rowTs.size(); i < n; i++) {
                    final long ts = op.rowTs.getQuick(i);
                    final int day = (int) (ts / Micros.DAY_MICROS);
                    if (attachableDays[day] && rnd.nextInt(10) > 0) {
                        op.rowTs.setQuick(i, (ts + (1 + rnd.nextInt(dayCount - 1)) * Micros.DAY_MICROS) % (dayCount * Micros.DAY_MICROS));
                    }
                }
            }
            case BASE_DROP_PARTITION_LIST, BASE_UPDATE, VIEW_DROP_PARTITION_LIST -> op.day = rnd.nextInt(dayCount);
            case BASE_DROP_PARTITION_WHERE, VIEW_DROP_PARTITION_WHERE -> op.day = 1 + rnd.nextInt(dayCount);
            case BASE_DETACH_PARTITION -> {
                // the newest partition does not detach, so pick one with a newer one after it
                final int newest = newestBaseDay();
                op.day = -1;
                if (newest > 0) {
                    final int start = rnd.nextInt(newest);
                    for (int i = 0; i < newest; i++) {
                        final int day = (start + i) % newest;
                        if (baseDays[day]) {
                            op.day = day;
                            break;
                        }
                    }
                }
                if (op.day < 0) {
                    return insteadInsert(rnd, op);
                }
            }
            case BASE_ATTACH_PARTITION -> {
                final int start = rnd.nextInt(dayCount);
                op.day = -1;
                for (int i = 0; i < dayCount; i++) {
                    final int day = (start + i) % dayCount;
                    if (attachableDays[day]) {
                        op.day = day;
                        break;
                    }
                }
                if (op.day < 0) {
                    return insteadInsert(rnd, op);
                }
            }
            case BASE_SET_TTL -> op.day = 1 + rnd.nextInt(dayCount - 1);
            case BASE_REPLACE, BASE_REPLACE_FULL_RESTART, BASE_REPLACE_RESUME_RESTART ->
                // the new base has rows in a random set of days
                    generateRows(rnd, op, 1 + rnd.nextInt(3 * dayCount), rnd.nextInt(dayCount));
            case VIEW_ADD_COLUMN -> {
                final ObjList<String> candidates = new ObjList<>();
                for (String name : CATALOGUE_NAMES) {
                    if (viewColumns.indexOf(name) < 0) {
                        candidates.add(name);
                    }
                }
                for (String name : EXTRA_NAMES) {
                    if (viewColumns.indexOf(name) < 0) {
                        candidates.add(name);
                    }
                }
                if (candidates.size() == 0) {
                    return insteadInsert(rnd, op);
                }
                op.column = candidates.getQuick(rnd.nextInt(candidates.size()));
                final int catalogueIndex = catalogueIndex(op.column);
                if (catalogueIndex > -1) {
                    final String[] types = CATALOGUE_VIEW_TYPES[catalogueIndex];
                    op.columnType = types[rnd.nextInt(types.length)];
                } else {
                    op.columnType = EXTRA_TYPES[op.column.equals(EXTRA_NAMES[0]) ? 0 : 1];
                }
            }
            case VIEW_DROP_COLUMN -> {
                final ObjList<String> candidates = new ObjList<>();
                for (int i = 0, n = viewColumns.size(); i < n; i++) {
                    final String name = viewColumns.getQuick(i);
                    if (!name.equals(TS) && !name.equals(SYM)) {
                        candidates.add(name);
                    }
                }
                if (candidates.size() == 0) {
                    return insteadInsert(rnd, op);
                }
                op.column = candidates.getQuick(rnd.nextInt(candidates.size()));
            }
            case VIEW_ALTER_AS -> {
                op.sqlColumns.clear();
                addRandomCatalogueColumns(rnd, op.sqlColumns);
                // ts and sym go first, as in CREATE, or anywhere
                if (rnd.nextInt(3) == 0) {
                    op.sqlColumns.insert(rnd.nextInt(op.sqlColumns.size() + 1), 1, TS);
                    op.sqlColumns.insert(rnd.nextInt(op.sqlColumns.size() + 1), 1, SYM);
                } else {
                    op.sqlColumns.insert(0, 1, SYM);
                    op.sqlColumns.insert(0, 1, TS);
                }
                op.isRefused = isRefusedBySamePositionRule(viewColumns, op.sqlColumns);
            }
            case VIEW_RANGE -> {
                op.day = rnd.nextInt(dayCount);
                op.dayHi = op.day + 1 + rnd.nextInt(dayCount - op.day);
            }
            case VIEW_RESUME -> op.isSkipBacklog = rnd.nextInt(3) == 0;
            default -> {
                // no parameters
            }
        }
        apply(op);
        return op;
    }

    // The executor's answer to a DETACH: the partition detaches only when the base holds it and a newer one.
    public void onDetached(int day, boolean isDetached) {
        attachableDays[day] = isDetached;
        if (!isDetached) {
            baseDays[day] = true;
        }
    }

    private static void addRandomCatalogueColumns(Rnd rnd, ObjList<String> columns) {
        final int start = rnd.nextInt(CATALOGUE_NAMES.length);
        final int count = 1 + rnd.nextInt(CATALOGUE_NAMES.length);
        final ObjList<String> picked = new ObjList<>();
        for (int i = 0; i < count; i++) {
            picked.add(CATALOGUE_NAMES[(start + i) % CATALOGUE_NAMES.length]);
        }
        // shuffle
        for (int i = picked.size() - 1; i > 0; i--) {
            final int j = rnd.nextInt(i + 1);
            final String tmp = picked.getQuick(i);
            picked.setQuick(i, picked.getQuick(j));
            picked.setQuick(j, tmp);
        }
        for (int i = 0, n = picked.size(); i < n; i++) {
            columns.add(picked.getQuick(i));
        }
    }

    private void apply(Op op) {
        switch (op.kind) {
            case INSERT -> {
                for (int i = 0, n = op.rowTs.size(); i < n; i++) {
                    final int day = (int) (op.rowTs.getQuick(i) / Micros.DAY_MICROS);
                    baseDays[day] = true;
                    // the partition is back, so its detached copy no longer attaches
                    attachableDays[day] = false;
                }
            }
            case BASE_DROP_PARTITION_LIST -> baseDays[op.day] = false;
            case BASE_DROP_PARTITION_WHERE -> {
                for (int d = 0; d < op.day; d++) {
                    baseDays[d] = false;
                }
            }
            case BASE_DETACH_PARTITION -> {
                baseDays[op.day] = false;
                attachableDays[op.day] = true;
            }
            case BASE_ATTACH_PARTITION -> {
                baseDays[op.day] = true;
                attachableDays[op.day] = false;
            }
            case BASE_TRUNCATE -> {
                for (int d = 0; d < dayCount; d++) {
                    baseDays[d] = false;
                }
            }
            case BASE_REPLACE, BASE_REPLACE_FULL_RESTART, BASE_REPLACE_RESUME_RESTART -> {
                for (int d = 0; d < dayCount; d++) {
                    baseDays[d] = false;
                    attachableDays[d] = false;
                }
                for (int i = 0, n = op.rowTs.size(); i < n; i++) {
                    baseDays[(int) (op.rowTs.getQuick(i) / Micros.DAY_MICROS)] = true;
                }
            }
            case VIEW_ADD_COLUMN -> {
                viewColumns.add(op.column);
                viewColumnTypes.add(op.columnType);
            }
            case VIEW_DROP_COLUMN -> {
                final int index = viewColumns.indexOf(op.column);
                viewColumns.remove(index);
                viewColumnTypes.remove(index);
            }
            case VIEW_ALTER_AS -> {
                if (!op.isRefused) {
                    sqlColumns.clear();
                    sqlColumns.addAll(op.sqlColumns);
                }
            }
            default -> {
                // nothing the generator draws from changes
            }
        }
    }

    // Rows at random offsets from the first day, in all days or, when firstDay is not negative, in the days from
    // firstDay on, so a replacement base can leave out the days before it.
    private void generateRows(Rnd rnd, Op op, int rowCount, int firstDay) {
        final long lo = firstDay < 0 ? 0 : firstDay * Micros.DAY_MICROS;
        final long span = dayCount * Micros.DAY_MICROS - lo;
        for (int i = 0; i < rowCount; i++) {
            op.rowTs.add(lo + rnd.nextLong(span));
            op.rowSyms.add(rnd.nextInt(3));
            op.rowPrices.add(rnd.nextInt(1000));
            op.rowQtys.add(rnd.nextInt(100));
        }
    }

    private Op insteadInsert(Rnd rnd, Op op) {
        op.clear();
        op.kind = Kind.INSERT;
        generateRows(rnd, op, 1 + rnd.nextInt(5), -1);
        apply(op);
        return op;
    }

    private int newestBaseDay() {
        for (int d = dayCount - 1; d > -1; d--) {
            if (baseDays[d]) {
                return d;
            }
        }
        return -1;
    }

    public enum Kind {
        INSERT,
        BASE_DROP_PARTITION_LIST,
        BASE_DROP_PARTITION_WHERE,
        BASE_DETACH_PARTITION,
        BASE_ATTACH_PARTITION,
        BASE_TRUNCATE,
        BASE_UPDATE,
        BASE_SET_TTL,
        BASE_REPLACE,
        // a base replacement, FULL once the view is invalid, and a restart right after the FULL's refresh
        BASE_REPLACE_FULL_RESTART,
        // a base replacement, RESUME SKIP BACKLOG once the view is invalid, and a restart right after it
        BASE_REPLACE_RESUME_RESTART,
        VIEW_ADD_COLUMN,
        VIEW_DROP_COLUMN,
        VIEW_DROP_PARTITION_LIST,
        VIEW_DROP_PARTITION_WHERE,
        VIEW_ALTER_AS,
        VIEW_RANGE,
        VIEW_FULL,
        VIEW_RESUME,
        // a forced invalidation, which stands for a failed refresh
        VIEW_INVALIDATE,
        // the refresh intervals update that the timer job enqueues for an invalid view's backlog
        BACKLOG_UPDATE,
        RESTART,
        APPLY_WAL,
        RUN_REFRESH_JOB,
        PURGE_WAL
    }

    public static final class Op {
        public final LongList rowPrices = new LongList();
        public final LongList rowQtys = new LongList();
        public final IntList rowSyms = new IntList();
        // offsets from the first day
        public final LongList rowTs = new LongList();
        public final ObjList<String> sqlColumns = new ObjList<>();
        public String column;
        public String columnType;
        public int day;
        public int dayHi;
        public boolean isRefused;
        public boolean isSkipBacklog;
        public Kind kind;

        public void clear() {
            kind = null;
            day = -1;
            dayHi = -1;
            column = null;
            columnType = null;
            isRefused = false;
            isSkipBacklog = false;
            sqlColumns.clear();
            rowTs.clear();
            rowSyms.clear();
            rowPrices.clear();
            rowQtys.clear();
        }
    }
}
