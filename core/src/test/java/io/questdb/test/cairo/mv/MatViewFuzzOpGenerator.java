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
 * {@code ALTER MATERIALIZED VIEW ... AS}, the view's EXPIRE ROWS policy, the REFRESH statements, forced invalidations,
 * restarts of the view states and the job runs that interleave them. For a period view of one day, it also draws the
 * period timer's firings, which complete the days one at a time from the first.
 * <p>
 * It also draws a reload of the base: a TRUNCATE, the ATTACH of a detached partition and an UPDATE of the attached
 * day, which one WAL apply run applies, and whose range task the refresh job takes before the apply job moves the
 * base's SeqTxnTracker past the batch, as a refresh worker can. The UPDATE changes the day's values, so the view's
 * buckets of the day, which the view usually computed before the partition detached, must change too: the oracle
 * finds a lost range there. A period view's round reloads a day whose period ended, which the oracle compares.
 * <p>
 * It draws the round's initial rows, one to ten in each day. In a period view's round, half of the days after the
 * first are gap days, as a weekend is: the base takes no row there until the day's period ends, so the period timer's
 * firing finds the day without a base partition. Most UPDATEs of such a round correct a day whose period ended, so
 * the range they queue in the view's cache can meet the firing over a gap day.
 * <p>
 * The generator keeps the part of the test's state that it draws from: the view's columns in their order, the
 * columns the view's SQL produces and whether that SQL copies them by position, the base days it believes hold a
 * partition and the detached partitions that can be attached, and the columns the view's EXPIRE ROWS policy reads. It
 * predicts the outcome of the statements it draws, and the test asserts that the engine agrees: an
 * {@code ALTER ... AS} that replaces a view column with a differently named one at its position is refused, one whose
 * SQL copies by position is refused unless its columns are the view's in their order, a view ADD or DROP COLUMN is
 * refused while the view's SQL copies by position, a view DROP COLUMN of a column the policy reads is refused, and a
 * SET EXPIRE whose policy reads a column the view lacks is refused. It uses no engine, so
 * {@link MatViewFuzzOpGeneratorCoverageTest} can pin the shapes it draws.
 * <p>
 * Every policy it draws hides no row of a view: a WHEN predicate that compares a column with a value below every
 * value it holds, or with the column's own minimum over all rows; a KEEP policy partitioned by the view's ts and sym,
 * which hold one row each. So the oracle's checks stay as they are, and every read of the view goes through the
 * policy's rewrite.
 * <p>
 * Every column name of the SQL stands for one aggregate, so a view row computed under any SQL holds the value the
 * name stands for, see {@link #CATALOGUE_NAMES}. The view's own columns can take another type with an implicit cast,
 * and the view can have columns no SQL produces.
 * <p>
 * A bare-name round, see {@link #forBareNameRound(int)}, drives {@link MatViewFuzzTest#testBareNameSwitchResume()}
 * instead: a view as the versions before the explicit-name rule store it, whose SQL calls count without parentheses,
 * over a base that gains and drops a column named count, which switches the bare count between a call and a column.
 * It draws ingestion, the base column changes, ALTER ... AS writing {@code count() AS count} or {@code base.count AS
 * count}, forced invalidations, RESUME, FULL, restarts and job runs, and predicts the outcome of RESUME and ALTER ...
 * AS: RESUME refuses an invalid view whose SQL still calls count bare once a switch stands that no FULL rebuilt the
 * view after, and refuses on its compile while count is no column. It also keeps the reading the view's rows hold.
 */
public final class MatViewFuzzOpGenerator {
    // The SQL of the bare-name round's view, see bareNameViewSql()
    public static final int BARE_NAME_SQL_BARE = 0;
    public static final int BARE_NAME_SQL_CALL = 1;
    public static final int BARE_NAME_SQL_QUALIFIED = 2;
    // Each name always stands for the same aggregate, whatever SQL a view has.
    public static final String[] CATALOGUE_EXPRESSIONS = {"last(price)", "sum(qty)", "count()", "max(price)"};
    public static final String[] CATALOGUE_NAMES = {"price", "qty", "cnt", "hi"};
    // The view column types each name can take: the aggregate's type and the types it casts to implicitly.
    public static final String[][] CATALOGUE_VIEW_TYPES = {{"DOUBLE"}, {"LONG", "DOUBLE"}, {"LONG", "DOUBLE"}, {"DOUBLE"}};
    // View columns that no SQL produces; they hold NULL in every row.
    public static final String[] EXTRA_NAMES = {"note", "flag"};
    public static final String[] EXTRA_TYPES = {"VARCHAR", "INT"};
    // A column name longer than 64 characters: a SQL that references it without an alias relies on a generated column
    // name, so the view copies its columns by position.
    public static final String LONG_NAME = "long_" + "l".repeat(60);
    // The outcome of a RESUME of the bare-name round
    public static final int RESUME_ACCEPTED = 0;
    // the compile reads count as a column, which the base does not have
    public static final int RESUME_REFUSED_COMPILE = 1;
    // a switch of the bare count stands, which no FULL covers
    public static final int RESUME_REFUSED_SWITCH = 2;
    public static final String SYM = "sym";
    public static final String TS = "ts";
    private final boolean[] attachableDays;
    private final boolean[] baseDays;
    private final int dayCount;
    // the days of a period view's round that take no row until their period ends
    private final boolean[] gapDays;
    private final boolean isBareNameRound;
    private final boolean isPeriodView;
    // the columns the view's last accepted SET EXPIRE reads, none without a policy
    private final ObjList<String> policyColumns = new ObjList<>();
    private final ObjList<String> sqlColumns = new ObjList<>();
    private final ObjList<String> viewColumnTypes = new ObjList<>();
    private final ObjList<String> viewColumns = new ObjList<>();
    // the days, from the first, whose period ended: the clock of a period view is at the end of the last of them
    private int completeDays;
    // the view's EXPIRE ROWS policy as materialized_views().expire_clause shows it, null without one
    private String expireClause;
    // the bare-name round's view SQL, one of BARE_NAME_SQL_*
    private int bareNameSql = BARE_NAME_SQL_BARE;
    // the bare-name round's base has the column count, once the WAL applies the changes drawn so far
    private boolean hasBaseCountColumn;
    // the view dropped a column, whose slot the view's table keeps
    private boolean hasDroppedViewColumn;
    // the bare-name round's view is invalid, once the jobs have run
    private boolean isBareNameViewInvalid;
    // a base column change switched the bare count of the bare-name round's SQL, and no FULL rebuilt the view since
    private boolean isBareNameSwitchStanding;
    // the test ran a view ADD or DROP COLUMN and has not applied the view's WAL since, as far as it has told
    private boolean isColumnChangePending;
    // the test ran a SET or DROP EXPIRE and has not applied the view's WAL since, as far as it has told
    private boolean isPolicyChangePending;
    // an ALTER ... AS of the bare-name round changed the reading of the view's later rows since the last FULL, so the
    // view's rows may hold both readings
    private boolean isReadingMixedByAlter;
    // the bare-name round's view rows read count as the column, which holds NULL, instead of calling count()
    private boolean isRowsReadingColumn;
    // the view's SQL relies on a generated column name, so refresh copies its columns by position
    private boolean isSqlPositional;
    private int totalWeight;

    public MatViewFuzzOpGenerator(int dayCount, boolean isPeriodView) {
        this(dayCount, isPeriodView, false);
    }

    private MatViewFuzzOpGenerator(int dayCount, boolean isPeriodView, boolean isBareNameRound) {
        this.dayCount = dayCount;
        this.isPeriodView = isPeriodView;
        this.isBareNameRound = isBareNameRound;
        this.attachableDays = new boolean[dayCount];
        this.baseDays = new boolean[dayCount];
        this.gapDays = new boolean[dayCount];
        for (Kind kind : Kind.values()) {
            totalWeight += isBareNameRound ? bareNameRoundWeight(kind) : weight(kind, isPeriodView);
        }
    }

    // How often each operation of a bare-name round comes up; the other operations do not.
    public static int bareNameRoundWeight(Kind kind) {
        return switch (kind) {
            case INSERT -> 10;
            case VIEW_RESUME -> 6;
            case BASE_ADD_BARE_NAME_COLUMN, BASE_DROP_BARE_NAME_COLUMN, VIEW_INVALIDATE, APPLY_WAL, RUN_REFRESH_JOB -> 4;
            case VIEW_FULL -> 3;
            case VIEW_ALTER_AS, RESTART -> 2;
            default -> 0;
        };
    }

    // The SQL of the bare-name round's view: count bare, as the versions before the explicit-name rule store it,
    // count() or the qualified column.
    public static String bareNameViewSql(int bareNameSql) {
        final String count = switch (bareNameSql) {
            case BARE_NAME_SQL_BARE -> "count";
            case BARE_NAME_SQL_CALL -> "count() AS count";
            default -> "base.count AS count";
        };
        return "SELECT ts, sym, " + count + ", last(price) AS price FROM base SAMPLE BY 1h";
    }

    // A generator of a bare-name round, see the class comment.
    public static MatViewFuzzOpGenerator forBareNameRound(int dayCount) {
        return new MatViewFuzzOpGenerator(dayCount, false, true);
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

    // Whether the rows that the bare-name round's view computes under the SQL read count as the column.
    private static boolean isColumnReading(int bareNameSql, boolean hasBaseCountColumn) {
        return bareNameSql == BARE_NAME_SQL_QUALIFIED || (bareNameSql == BARE_NAME_SQL_BARE && hasBaseCountColumn);
    }

    // The rule of ALTER ... AS for a SQL that copies by position: refused unless the SQL's columns are the view's, in
    // their order. A SQL with another number of columns fails on the count, any other on the same-position check.
    public static boolean isRefusedByPositionalCopy(ObjList<String> viewColumns, ObjList<String> sqlColumns) {
        return !viewColumns.equals(sqlColumns);
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
        return viewSql(sqlColumns, false);
    }

    // The SQL of a view, its columns in their order. A SQL that copies by position reads the base through two nested
    // SELECTs: the inner one names the price LONG_NAME, the outer one references it without an alias. Its column
    // never reaches the output, so the SQL produces the same values as the one that reads the base.
    public static String viewSql(ObjList<String> sqlColumns, boolean isPositional) {
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
        if (isPositional) {
            sb.append(" FROM (SELECT ts, sym, price, qty, ").append(LONG_NAME)
                    .append(" FROM (SELECT ts, sym, price, qty, price AS ").append(LONG_NAME).append(" FROM base))");
        } else {
            sb.append(" FROM base");
        }
        return sb.append(" SAMPLE BY 1h").toString();
    }

    // How often each operation comes up: ingestion most, the operations that reset most of the oracle's checks least.
    // A period view's round draws the period timer's firings, and more UPDATEs, so that the range of an UPDATE often
    // waits in the view's cache when a firing comes.
    public static int weight(Kind kind, boolean isPeriodView) {
        return switch (kind) {
            // the period timer's firing, which only a period view has; once every day is complete, it falls back to
            // INSERT
            case VIEW_PERIOD_REFRESH -> isPeriodView ? 10 : 0;
            case BASE_UPDATE -> isPeriodView ? 6 : 4;
            case INSERT -> 30;
            // an ATTACH needs a detached partition, and falls back to INSERT without one
            case BASE_ATTACH_PARTITION -> 8;
            // so does a reload of the base, which needs one of a complete day in a period view's round
            case BASE_TRUNCATE_ATTACH_UPDATE -> 8;
            case BASE_DROP_PARTITION_LIST, BASE_DETACH_PARTITION, VIEW_DROP_COLUMN, VIEW_DROP_PARTITION_LIST,
                 VIEW_RANGE, VIEW_RESUME -> 3;
            case BASE_DROP_PARTITION_WHERE, VIEW_DROP_PARTITION_WHERE, VIEW_FULL, BACKLOG_UPDATE, PURGE_WAL -> 2;
            case BASE_TRUNCATE, BASE_SET_TTL, BASE_REPLACE, BASE_REPLACE_FULL_RESTART, BASE_REPLACE_RESUME_RESTART -> 1;
            case VIEW_ADD_COLUMN, VIEW_INVALIDATE, RESTART, APPLY_WAL, RUN_REFRESH_JOB, VIEW_SET_EXPIRE -> 4;
            case VIEW_DROP_EXPIRE -> 2;
            case VIEW_ALTER_AS -> 5;
            // only a bare-name round draws them, see bareNameRoundWeight()
            case BASE_ADD_BARE_NAME_COLUMN, BASE_DROP_BARE_NAME_COLUMN -> 0;
        };
    }

    // The recovery of the bare-name round's invalid view at a view check: RESUME when the generator predicts that it
    // accepts the view, FULL otherwise.
    public Op bareNameRecovery(Op op) {
        op.clear();
        op.kind = predictBareNameResume() == RESUME_ACCEPTED ? Kind.VIEW_RESUME : Kind.VIEW_FULL;
        op.resumeOutcome = RESUME_ACCEPTED;
        apply(op);
        return op;
    }

    public int getBareNameSql() {
        return bareNameSql;
    }

    // The days, from the first, whose period ended.
    public int getCompleteDays() {
        return completeDays;
    }

    public String getExpireClause() {
        return expireClause;
    }

    public ObjList<String> getPolicyColumns() {
        return policyColumns;
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

    // The round's initial rows of the day, which the test inserts before it creates the view: one to ten rows, and
    // none on a gap day of a period view's round.
    public Op initRows(Rnd rnd, int day, Op op) {
        op.clear();
        op.kind = Kind.INSERT;
        if (isPeriodView && day > 0 && rnd.nextBoolean()) {
            gapDays[day] = true;
            return op;
        }
        for (int i = 0, n = 1 + rnd.nextInt(10); i < n; i++) {
            op.rowTs.add(day * Micros.DAY_MICROS + rnd.nextLong(Micros.DAY_MICROS));
            op.rowSyms.add(rnd.nextInt(3));
            op.rowPrices.add(rnd.nextInt(1000));
            op.rowQtys.add(rnd.nextInt(100));
        }
        apply(op);
        return op;
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

    public boolean hasBaseCountColumn() {
        return hasBaseCountColumn;
    }

    public boolean isAttachable(int day) {
        return attachableDays[day];
    }

    public boolean isBareNameSwitchStanding() {
        return isBareNameSwitchStanding;
    }

    public boolean isBareNameViewInvalid() {
        return isBareNameViewInvalid;
    }

    // Whether the generator believes that the base holds a partition of the day.
    public boolean isBaseDay(int day) {
        return baseDays[day];
    }

    // Whether the view's WAL may hold a view ADD or DROP COLUMN, which the read probe of a KEEP or window SET EXPIRE
    // does not see: it compiles against the view's applied columns.
    public boolean isColumnChangePending() {
        return isColumnChangePending;
    }

    // Whether the day is a gap day of a period view's round, which takes no row until its period ends.
    public boolean isGapDay(int day) {
        return gapDays[day];
    }

    public boolean isPeriodView() {
        return isPeriodView;
    }

    // Whether the view's WAL may hold a SET or DROP EXPIRE, which a view DROP COLUMN does not see: it checks the policy
    // the view has applied. A DROP COLUMN behind a pending SET EXPIRE that reads the column is a known limitation
    // (questdb-enterprise#1479, #1480), so the test applies the view's WAL first.
    public boolean isPolicyChangePending() {
        return isPolicyChangePending;
    }

    public boolean isReadingMixedByAlter() {
        return isReadingMixedByAlter;
    }

    public boolean isRowsReadingColumn() {
        return isRowsReadingColumn;
    }

    // Whether the view's SQL relies on a generated column name, so refresh copies its columns by position.
    public boolean isSqlPositional() {
        return isSqlPositional;
    }

    public Op next(Rnd rnd, Op op) {
        op.clear();
        int pick = rnd.nextInt(totalWeight);
        Kind kind = Kind.INSERT;
        for (Kind k : Kind.values()) {
            pick -= isBareNameRound ? bareNameRoundWeight(k) : weight(k, isPeriodView);
            if (pick < 0) {
                kind = k;
                break;
            }
        }
        op.kind = kind;
        if (isBareNameRound) {
            return nextOfBareNameRound(rnd, op);
        }
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
                keepOffOpenGapDays(op);
            }
            case BASE_DROP_PARTITION_LIST, VIEW_DROP_PARTITION_LIST -> op.day = rnd.nextInt(dayCount);
            // in a period view's round, mostly a correction of a day whose period ended, which the view holds
            case BASE_UPDATE -> op.day = isPeriodView && completeDays > 0 && rnd.nextInt(4) > 0
                    ? rnd.nextInt(completeDays)
                    : rnd.nextInt(dayCount);
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
                op.day = pickAttachableDay(rnd, dayCount);
                if (op.day < 0) {
                    return insteadInsert(rnd, op);
                }
            }
            case BASE_TRUNCATE_ATTACH_UPDATE -> {
                // the oracle compares the complete days of a period view only
                op.day = pickAttachableDay(rnd, isPeriodView ? completeDays : dayCount);
                if (op.day < 0) {
                    return insteadInsert(rnd, op);
                }
            }
            case BASE_SET_TTL -> op.day = 1 + rnd.nextInt(dayCount - 1);
            case BASE_REPLACE, BASE_REPLACE_FULL_RESTART, BASE_REPLACE_RESUME_RESTART -> {
                // the new base has rows in a random set of days
                generateRows(rnd, op, 1 + rnd.nextInt(3 * dayCount), rnd.nextInt(dayCount));
                keepOffOpenGapDays(op);
            }
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
                // a column change would shift the columns of a SQL that copies by position
                op.isRefused = isSqlPositional;
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
                // the view's EXPIRE ROWS policy reads the column, which the statement checks first, or the view's SQL
                // copies by position
                op.isRefused = policyColumns.indexOf(op.column) > -1 || isSqlPositional;
            }
            case VIEW_SET_EXPIRE -> generatePolicy(rnd, op);
            case VIEW_ALTER_AS -> {
                op.sqlColumns.clear();
                // Steers clear of a known bug until it is fixed: ALTER ... AS takes a SQL that copies by position on a
                // view that dropped a column, whose slot the view's table keeps, and every refresh then fails on the
                // slot. So a SQL that copies by position comes only while the view never dropped a column. Remove the
                // steering with the fix.
                op.isPositional = !hasDroppedViewColumn && rnd.nextInt(3) == 0;
                if (op.isPositional && isEveryViewColumnProducible()) {
                    // the view's columns in their order, which the statement takes, or with two of them swapped, or
                    // with one replaced by a column the view lacks; a random column list, as below, would rarely be
                    // the view's
                    op.sqlColumns.addAll(viewColumns);
                    switch (rnd.nextInt(4)) {
                        case 0 -> {
                            final int i = rnd.nextInt(op.sqlColumns.size());
                            final int j = (i + 1 + rnd.nextInt(op.sqlColumns.size() - 1)) % op.sqlColumns.size();
                            final String tmp = op.sqlColumns.getQuick(i);
                            op.sqlColumns.setQuick(i, op.sqlColumns.getQuick(j));
                            op.sqlColumns.setQuick(j, tmp);
                        }
                        case 1 -> {
                            final ObjList<String> absent = new ObjList<>();
                            for (String name : CATALOGUE_NAMES) {
                                if (viewColumns.indexOf(name) < 0) {
                                    absent.add(name);
                                }
                            }
                            if (absent.size() > 0) {
                                // the view has a catalogue column past ts and sym
                                final int i = 2 + rnd.nextInt(op.sqlColumns.size() - 2);
                                op.sqlColumns.setQuick(i, absent.getQuick(rnd.nextInt(absent.size())));
                            }
                        }
                        default -> {
                            // the view's columns as they are
                        }
                    }
                } else {
                    addRandomCatalogueColumns(rnd, op.sqlColumns);
                    // ts and sym go first, as in CREATE, or anywhere
                    if (rnd.nextInt(3) == 0) {
                        op.sqlColumns.insert(rnd.nextInt(op.sqlColumns.size() + 1), 1, TS);
                        op.sqlColumns.insert(rnd.nextInt(op.sqlColumns.size() + 1), 1, SYM);
                    } else {
                        op.sqlColumns.insert(0, 1, SYM);
                        op.sqlColumns.insert(0, 1, TS);
                    }
                }
                op.isRefused = op.isPositional
                        ? isRefusedByPositionalCopy(viewColumns, op.sqlColumns)
                        : isRefusedBySamePositionRule(viewColumns, op.sqlColumns);
            }
            case VIEW_RANGE -> {
                op.day = rnd.nextInt(dayCount);
                op.dayHi = op.day + 1 + rnd.nextInt(dayCount - op.day);
            }
            case VIEW_RESUME -> op.isSkipBacklog = rnd.nextInt(3) == 0;
            case VIEW_PERIOD_REFRESH -> {
                // the day whose period ends
                if (completeDays == dayCount) {
                    return insteadInsert(rnd, op);
                }
                op.day = completeDays;
            }
            default -> {
                // no parameters
            }
        }
        apply(op);
        return op;
    }

    // The test ran a view ADD or DROP COLUMN, which waits in the view's WAL.
    public void onColumnChangeIssued() {
        isColumnChangePending = true;
    }

    // The executor's answer to a DETACH: the partition detaches only when the base holds it and a newer one.
    public void onDetached(int day, boolean isDetached) {
        attachableDays[day] = isDetached;
        if (!isDetached) {
            baseDays[day] = true;
        }
    }

    // The test ran a SET or DROP EXPIRE, which waits in the view's WAL.
    public void onPolicyChangeIssued() {
        isPolicyChangePending = true;
    }

    // The test has applied the view's WAL: the view's applied columns and policy are the generator's.
    public void onViewWalApplied() {
        isColumnChangePending = false;
        isPolicyChangePending = false;
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
        if (isBareNameRound) {
            applyBareNameRound(op);
            return;
        }
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
            case BASE_TRUNCATE_ATTACH_UPDATE -> {
                for (int d = 0; d < dayCount; d++) {
                    baseDays[d] = false;
                }
                baseDays[op.day] = true;
                attachableDays[op.day] = false;
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
                if (!op.isRefused) {
                    viewColumns.add(op.column);
                    viewColumnTypes.add(op.columnType);
                }
            }
            case VIEW_DROP_COLUMN -> {
                if (!op.isRefused) {
                    final int index = viewColumns.indexOf(op.column);
                    viewColumns.remove(index);
                    viewColumnTypes.remove(index);
                    hasDroppedViewColumn = true;
                }
            }
            case VIEW_SET_EXPIRE -> {
                if (!op.isRefused) {
                    policyColumns.clear();
                    policyColumns.addAll(op.policyColumns);
                    expireClause = op.expireClause;
                }
            }
            case VIEW_DROP_EXPIRE -> {
                policyColumns.clear();
                expireClause = null;
            }
            case VIEW_ALTER_AS -> {
                if (!op.isRefused) {
                    sqlColumns.clear();
                    sqlColumns.addAll(op.sqlColumns);
                    isSqlPositional = op.isPositional;
                }
            }
            case VIEW_PERIOD_REFRESH -> completeDays++;
            default -> {
                // nothing the generator draws from changes
            }
        }
    }

    private void applyBareNameRound(Op op) {
        switch (op.kind) {
            case BASE_ADD_BARE_NAME_COLUMN, BASE_DROP_BARE_NAME_COLUMN -> {
                hasBaseCountColumn = op.kind == Kind.BASE_ADD_BARE_NAME_COLUMN;
                if (bareNameSql == BARE_NAME_SQL_BARE) {
                    // the bare-name guard records the switch and invalidates a valid view; an invalid view stays so
                    isBareNameSwitchStanding = true;
                    isBareNameViewInvalid = true;
                }
            }
            case VIEW_ALTER_AS -> {
                if (!op.isRefused) {
                    // rows computed from now on read the base as the new SQL does
                    isReadingMixedByAlter |= isColumnReading(op.bareNameSql, hasBaseCountColumn) != isRowsReadingColumn;
                    bareNameSql = op.bareNameSql;
                }
            }
            case VIEW_INVALIDATE -> isBareNameViewInvalid = true;
            case VIEW_RESUME -> {
                if (op.resumeOutcome == RESUME_ACCEPTED) {
                    isBareNameViewInvalid = false;
                }
            }
            case VIEW_FULL -> {
                // FULL rebuilds every row from a snapshot that includes every change drawn so far
                isBareNameViewInvalid = false;
                isBareNameSwitchStanding = false;
                isRowsReadingColumn = isColumnReading(bareNameSql, hasBaseCountColumn);
                isReadingMixedByAlter = false;
            }
            default -> {
                // nothing the model keeps changes
            }
        }
    }

    private Op nextOfBareNameRound(Rnd rnd, Op op) {
        switch (op.kind) {
            case INSERT -> generateRows(rnd, op, 1 + rnd.nextInt(5), -1);
            case BASE_ADD_BARE_NAME_COLUMN -> {
                if (hasBaseCountColumn) {
                    return insteadInsert(rnd, op);
                }
            }
            case BASE_DROP_BARE_NAME_COLUMN -> {
                // a SQL that reads the qualified column keeps it, so that no refresh fails
                if (!hasBaseCountColumn || bareNameSql == BARE_NAME_SQL_QUALIFIED) {
                    return insteadInsert(rnd, op);
                }
            }
            case VIEW_ALTER_AS -> {
                op.bareNameSql = rnd.nextBoolean() ? BARE_NAME_SQL_CALL : BARE_NAME_SQL_QUALIFIED;
                op.isRefused = op.bareNameSql == BARE_NAME_SQL_QUALIFIED && !hasBaseCountColumn;
            }
            case VIEW_RESUME -> {
                op.isSkipBacklog = rnd.nextInt(3) == 0;
                op.resumeOutcome = predictBareNameResume();
            }
            default -> {
                // no parameters
            }
        }
        apply(op);
        return op;
    }

    // The outcome of a RESUME of the bare-name round's view, plain or SKIP BACKLOG: nothing to do on a valid view; a
    // refusal on the compile while the SQL calls count bare and the base has no count, and on the switch while the base
    // has one and a switch stands; an accepted view otherwise.
    private int predictBareNameResume() {
        if (!isBareNameViewInvalid || bareNameSql != BARE_NAME_SQL_BARE) {
            return RESUME_ACCEPTED;
        }
        if (!hasBaseCountColumn) {
            return RESUME_REFUSED_COMPILE;
        }
        return isBareNameSwitchStanding ? RESUME_REFUSED_SWITCH : RESUME_ACCEPTED;
    }

    // A policy that hides no row: one of the shapes of the class comment, over view columns of a numeric type, or of
    // any type for a WHEN comparison. With a small chance it reads a column the view lacks, which SET EXPIRE refuses.
    private void generatePolicy(Rnd rnd, Op op) {
        final ObjList<String> numeric = new ObjList<>();
        final ObjList<String> any = new ObjList<>();
        for (int i = 0, n = viewColumns.size(); i < n; i++) {
            final String name = viewColumns.getQuick(i);
            if (name.equals(TS) || name.equals(SYM)) {
                continue;
            }
            any.add(name);
            if (!"VARCHAR".equals(viewColumnTypes.getQuick(i))) {
                numeric.add(name);
            }
        }
        final int shape = rnd.nextInt(7);
        // a WHEN comparison takes any column, the other shapes a numeric one
        final ObjList<String> candidates = shape < 2 ? any : numeric;
        if (rnd.nextInt(8) == 0 || candidates.size() == 0) {
            // a name the view lacks: a catalogue name or flag that the view does not have
            final ObjList<String> absent = new ObjList<>();
            for (String name : CATALOGUE_NAMES) {
                if (viewColumns.indexOf(name) < 0) {
                    absent.add(name);
                }
            }
            if (viewColumns.indexOf(EXTRA_NAMES[1]) < 0) {
                absent.add(EXTRA_NAMES[1]);
            }
            if (absent.size() == 0) {
                // the view has every name: a predicate over no column
                op.policy = "WHEN 1 = 2";
                op.expireClause = "1 = 2";
                return;
            }
            op.column = absent.getQuick(rnd.nextInt(absent.size()));
            op.isRefused = true;
        } else {
            op.column = candidates.getQuick(rnd.nextInt(candidates.size()));
        }
        op.policyColumns.add(op.column);
        final String c = op.column;
        switch (shape) {
            case 0 -> op.policy = "WHEN " + lowComparison(c);
            case 1 -> {
                // a second column, which can be the first
                final String other = any.size() > 0 ? any.getQuick(rnd.nextInt(any.size())) : c;
                if (op.policyColumns.indexOf(other) < 0) {
                    op.policyColumns.add(other);
                }
                op.policy = "WHEN " + lowComparison(c) + " AND " + lowComparison(other);
            }
            case 2 -> op.policy = "WHEN " + c + " < min(" + c + ") OVER ()";
            case 3 -> op.policy = "KEEP " + (rnd.nextBoolean() ? "HIGHEST" : "LOWEST") + " ON " + c + " PARTITION BY ts, sym";
            case 4 -> op.policy = "KEEP " + (1 + rnd.nextInt(3)) + (rnd.nextBoolean() ? " HIGHEST" : " LOWEST") + " ON " + c + " PARTITION BY ts, sym";
            default -> op.policy = "KEEP LATEST PARTITION BY sym, ts, " + c;
        }
        // the KEEP and window shapes compile a read probe against the view's applied columns
        op.isProbed = shape > 1;
        op.expireClause = op.policy.startsWith("WHEN ") ? op.policy.substring("WHEN ".length()) : op.policy;
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
        keepOffOpenGapDays(op);
        apply(op);
        return op;
    }

    // Whether a SQL can produce every view column: ts, sym and the catalogue columns, but none of EXTRA_NAMES.
    private boolean isEveryViewColumnProducible() {
        for (int i = 0, n = viewColumns.size(); i < n; i++) {
            final String name = viewColumns.getQuick(i);
            if (!name.equals(TS) && !name.equals(SYM) && !isCatalogueName(name)) {
                return false;
            }
        }
        return true;
    }

    // Moves each row on a gap day whose period has not ended to the same time of the closest earlier day that is not
    // one. The first day is never a gap day.
    private void keepOffOpenGapDays(Op op) {
        for (int i = 0, n = op.rowTs.size(); i < n; i++) {
            long ts = op.rowTs.getQuick(i);
            for (int day = (int) (ts / Micros.DAY_MICROS); gapDays[day] && day >= completeDays; day--) {
                ts -= Micros.DAY_MICROS;
            }
            op.rowTs.setQuick(i, ts);
        }
    }

    // A comparison of a view column with a value below every value the column holds; NULL compares false.
    private String lowComparison(String name) {
        final int index = viewColumns.indexOf(name);
        if (index > -1 && "VARCHAR".equals(viewColumnTypes.getQuick(index))) {
            return name + " = 'never'";
        }
        return name + " < -1";
    }

    private int newestBaseDay() {
        for (int d = dayCount - 1; d > -1; d--) {
            if (baseDays[d]) {
                return d;
            }
        }
        return -1;
    }

    // A random day before dayHi with a detached partition that ATTACH takes, or -1 when there is none.
    private int pickAttachableDay(Rnd rnd, int dayHi) {
        final int start = rnd.nextInt(dayCount);
        for (int i = 0; i < dayCount; i++) {
            final int day = (start + i) % dayCount;
            if (day < dayHi && attachableDays[day]) {
                return day;
            }
        }
        return -1;
    }

    public enum Kind {
        INSERT,
        // the bare-name round's base gains, or drops, its column count
        BASE_ADD_BARE_NAME_COLUMN,
        BASE_DROP_BARE_NAME_COLUMN,
        BASE_DROP_PARTITION_LIST,
        BASE_DROP_PARTITION_WHERE,
        BASE_DETACH_PARTITION,
        BASE_ATTACH_PARTITION,
        BASE_TRUNCATE,
        // a TRUNCATE, the ATTACH of a detached partition and an UPDATE of the attached day, which one WAL apply run
        // applies, and whose range task the refresh job takes before the apply job moves the base's SeqTxnTracker
        BASE_TRUNCATE_ATTACH_UPDATE,
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
        // a SET EXPIRE of a policy that hides no row, see the class comment
        VIEW_SET_EXPIRE,
        VIEW_DROP_EXPIRE,
        VIEW_RANGE,
        VIEW_FULL,
        VIEW_RESUME,
        // the period timer's firing at the end of the next day, which moves a period view's clock there and enqueues
        // the period's range refresh
        VIEW_PERIOD_REFRESH,
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
        // the columns the policy of a SET EXPIRE reads
        public final ObjList<String> policyColumns = new ObjList<>();
        public final LongList rowPrices = new LongList();
        public final LongList rowQtys = new LongList();
        public final IntList rowSyms = new IntList();
        // offsets from the first day
        public final LongList rowTs = new LongList();
        public final ObjList<String> sqlColumns = new ObjList<>();
        // the SQL of an ALTER ... AS of the bare-name round, one of BARE_NAME_SQL_*
        public int bareNameSql;
        public String column;
        public String columnType;
        public int day;
        public int dayHi;
        // the policy as materialized_views().expire_clause shows it
        public String expireClause;
        // the SQL of an ALTER ... AS copies by position, see viewSql(ObjList, boolean)
        public boolean isPositional;
        public boolean isProbed;
        public boolean isRefused;
        public boolean isSkipBacklog;
        public Kind kind;
        // the text after SET EXPIRE ROWS
        public String policy;
        // the outcome of a RESUME of the bare-name round, one of RESUME_*
        public int resumeOutcome;

        public void clear() {
            kind = null;
            bareNameSql = BARE_NAME_SQL_BARE;
            resumeOutcome = RESUME_ACCEPTED;
            day = -1;
            dayHi = -1;
            column = null;
            columnType = null;
            expireClause = null;
            isPositional = false;
            isProbed = false;
            isRefused = false;
            policy = null;
            policyColumns.clear();
            isSkipBacklog = false;
            sqlColumns.clear();
            rowTs.clear();
            rowSyms.clear();
            rowPrices.clear();
            rowQtys.clear();
        }
    }
}
