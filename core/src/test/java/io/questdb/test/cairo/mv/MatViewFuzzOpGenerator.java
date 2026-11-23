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
 * {@code ALTER MATERIALIZED VIEW ... AS}, the view's EXPIRE ROWS policy, the REFRESH statements, invalidations, some of
 * which publish their marker first, as an apply-time invalidation does, see {@link #INVALIDATE_MARKER_FIRST}, restarts
 * of the view states and the job runs that interleave them. For a period view of one day, it also draws the period
 * timer's firings, which complete the days one at a time from the first.
 * <p>
 * It also draws a reload of the base: a TRUNCATE, the ATTACH of a detached partition and an UPDATE of the attached
 * day, which one WAL apply run applies, and whose range task the refresh job takes before the apply job moves the
 * base's SeqTxnTracker past the batch, as a refresh worker can. The UPDATE changes the day's values, so the view's
 * buckets of the day, which the view usually computed before the partition detached, must change too: the oracle
 * finds a lost range there. A period view's round reloads a day whose period ended, which the oracle compares.
 * <p>
 * It draws the operations around a pending ALTER ... AS too: the apply of the base's WAL alone, which lets the refresh
 * job meet the definition event that waits in the view's WAL, a reset of the view's SeqTxnTracker, as a republish of
 * the WAL apply job does, and SUSPEND WAL and RESUME WAL of the view, the latter also as RESUME WAL FROM TXN past the
 * pending definition event alone, after which the view keeps the SQL it had, and, for a view without a period, a boot
 * load of the view, as a booting server runs it before any WAL apply, which records the definition event that the
 * view's WAL holds unapplied, so the operations after it wait for the event as before it. While the view's WAL is suspended, it draws only the operations
 * whose outcome it predicts there, see {@link #isAllowedWhileViewWalSuspended(Kind)}, and an ALTER ... AS is refused
 * while an earlier one's definition event waits.
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
 * A bare-name round, see {@link #forBareNameRound(int, boolean)}, drives
 * {@link MatViewFuzzTest#testBareNameSwitchResume()} instead: a view as the versions before the explicit-name rule
 * store it, whose SQL calls count without parentheses, over a base that gains and drops a column named count, which
 * switches the bare count between a call and a column.
 * It draws ingestion, the base column changes, ALTER ... AS writing {@code count() AS count} or {@code base.count AS
 * count}, forced invalidations, RESUME, FULL, restarts and job runs, and predicts the outcome of RESUME and ALTER ...
 * AS: RESUME refuses an invalid view whose SQL still calls count bare once a switch stands that no FULL rebuilt the
 * view after, and refuses on its compile while count is no column. It also keeps the reading the view's rows hold.
 * A base column change can race other operations: the test pauses its apply after the bare-name guard ran and before
 * the change is visible, and runs one to three of the RACER_* operations meanwhile, see
 * {@link Kind#BASE_RACED_BARE_NAME_COLUMN_CHANGE}. The view of a deferred round completes no refresh before the
 * round's first operation, a raced change whose racers build the view, see {@link #isBareNameViewBuilt()}. Two racers
 * give a SQL that reads count bare to a view after the guard checked the base's views: one creates a second view,
 * whose outcome the generator predicts, see {@link Op#racedViewOutcome}, the other alters the round's view to it, which
 * the guard's check after the change then switches like a SQL that the guard checked before it.
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
    // The shapes of an invalidation, see Op.invalidationShape: the INVALIDATE task alone, which publishes its marker when
    // the refresh job runs it; a forced marker that the test publishes on a valid view before it queues the task, as an
    // apply-time invalidation does; and the marker of a base table change at the base's first transaction that does not
    // force, as the bare-name guard publishes it, which the last FULL refresh covers. Before it publishes a marker, the
    // test applies the base's WAL, so that the range tasks of the UPDATEs and ATTACHes drawn since the last refresh job
    // run wait ahead of the task and meet the marker on the still valid view. An UPDATE or ATTACH followed by no
    // invalidation has none.
    public static final int INVALIDATE_GUARD_MARKER = 2;
    public static final int INVALIDATE_MARKER_FIRST = 1;
    public static final int INVALIDATE_NONE = -1;
    public static final int INVALIDATE_TASK = 0;
    // A column name longer than 64 characters: a SQL that references it without an alias relies on a generated column
    // name, so the view copies its columns by position.
    public static final String LONG_NAME = "long_" + "l".repeat(60);
    // The outcome of the view mv2 that a RACER_CREATE_VIEW creates while the raced change applies, see
    // Op.racedViewOutcome: no such racer; a CREATE that the explicit-name rule refuses, as the base has no column count
    // until the raced ADD is visible; a view that a later racer's run of the refresh job builds from the base without
    // the raced DROP, which the bare-name guard's check after the change invalidates; a view whose first build runs
    // once the DROP is visible and calls count(), which stays valid.
    public static final int RACED_VIEW_BUILT_AFTER_CHANGE = 3;
    public static final int RACED_VIEW_BUILT_BEFORE_CHANGE = 2;
    public static final int RACED_VIEW_NONE = 0;
    public static final int RACED_VIEW_REFUSED = 1;
    // The operations that run while a raced base column change applies, see Kind.BASE_RACED_BARE_NAME_COLUMN_CHANGE: a
    // FULL refresh, which runs then and there; a forced invalidation; REFRESH ... INCREMENTAL, which an invalid view
    // refuses; the apply of the view's WAL; a run of the refresh job; the CREATE of the REFRESH MANUAL view mv2 whose
    // SQL reads count bare, see RACED_VIEW_*, at most once per change; ALTER ... AS of the round's view to the SQL that
    // reads count bare, whose definition event the racer applies at once, and which the explicit-name rule refuses
    // while the base has no column count.
    public static final int RACER_ALTER_TO_BARE = 6;
    public static final int RACER_APPLY_WAL = 0;
    public static final int RACER_COUNT = 7;
    public static final int RACER_CREATE_VIEW = 5;
    public static final int RACER_FULL = 1;
    public static final int RACER_INCREMENTAL = 2;
    public static final int RACER_INVALIDATE = 3;
    public static final int RACER_RUN_REFRESH_JOB = 4;
    // The outcome of a RESUME of the bare-name round
    public static final int RESUME_ACCEPTED = 0;
    // the compile reads count as a column, which the base does not have
    public static final int RESUME_REFUSED_COMPILE = 1;
    // a switch of the bare count stands, which no FULL covers
    public static final int RESUME_REFUSED_SWITCH = 2;
    public static final String SYM = "sym";
    public static final String TS = "ts";
    // The operations that meet a definition event that waits in the view's WAL, which a third of the draws take while one
    // waits: base rows, the base's WAL applied alone, the refresh job, a reset of the view's tracker, the suspension of
    // the view's WAL and its resume, another ALTER ... AS, and, for a view without a period, a boot load of the view.
    private static final Kind[] DEFINITION_WINDOW_KINDS = {
            Kind.INSERT,
            Kind.APPLY_BASE_WAL,
            Kind.APPLY_BASE_WAL,
            Kind.RUN_REFRESH_JOB,
            Kind.RUN_REFRESH_JOB,
            Kind.RUN_REFRESH_JOB,
            Kind.RESET_VIEW_TRACKER,
            Kind.VIEW_SUSPEND_WAL,
            Kind.VIEW_SUSPEND_WAL,
            Kind.VIEW_RESUME_WAL,
            Kind.VIEW_RESUME_WAL,
            Kind.VIEW_ALTER_AS,
            Kind.VIEW_ALTER_AS,
            Kind.BOOT_LOAD,
            Kind.BOOT_LOAD
    };
    // The operations of DEFINITION_WINDOW_KINDS for a period view, which draws no boot load: its round's shapes around
    // a definition event, which MatViewFuzzOpGeneratorCoverageTest pins, need the draws as they are.
    private static final Kind[] PERIOD_DEFINITION_WINDOW_KINDS = {
            Kind.INSERT,
            Kind.APPLY_BASE_WAL,
            Kind.APPLY_BASE_WAL,
            Kind.RUN_REFRESH_JOB,
            Kind.RUN_REFRESH_JOB,
            Kind.RUN_REFRESH_JOB,
            Kind.RESET_VIEW_TRACKER,
            Kind.VIEW_SUSPEND_WAL,
            Kind.VIEW_SUSPEND_WAL,
            Kind.VIEW_RESUME_WAL,
            Kind.VIEW_RESUME_WAL,
            Kind.VIEW_ALTER_AS,
            Kind.VIEW_ALTER_AS
    };
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
    // the SQL's columns before the accepted ALTER ... AS whose definition event waits, which a RESUME WAL FROM TXN past
    // the event leaves the view with
    private final ObjList<String> sqlColumnsBeforeDefinition = new ObjList<>();
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
    // the bare-name round's view completed a refresh; a deferred round's view does so in the round's first operation
    private boolean isBareNameViewBuilt = true;
    // the bare-name round's view is invalid, once the jobs have run
    private boolean isBareNameViewInvalid;
    // a base column change switched the bare count of the bare-name round's SQL, and no FULL rebuilt the view since
    private boolean isBareNameSwitchStanding;
    // the test ran a view ADD or DROP COLUMN and has not applied the view's WAL since, as far as it has told
    private boolean isColumnChangePending;
    // the test ran an ALTER ... AS and has not applied the view's WAL since, as far as it has told
    private boolean isDefinitionPending;
    // the view's WAL is suspended: the test ran SUSPEND WAL and no RESUME WAL since
    private boolean isViewWalSuspended;
    // whether the SQL before that ALTER ... AS relied on a generated column name, see sqlColumnsBeforeDefinition
    private boolean isSqlPositionalBeforeDefinition;
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
            case BASE_ADD_BARE_NAME_COLUMN, BASE_DROP_BARE_NAME_COLUMN, VIEW_INVALIDATE, APPLY_WAL, RUN_REFRESH_JOB ->
                    4;
            case VIEW_FULL, BASE_RACED_BARE_NAME_COLUMN_CHANGE -> 3;
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

    // A generator of a bare-name round, see the class comment. The view of a deferred round is created REFRESH
    // IMMEDIATE DEFERRED, so it completes no refresh until the round's first operation builds it, see
    // isBareNameViewBuilt().
    public static MatViewFuzzOpGenerator forBareNameRound(int dayCount, boolean isViewDeferred) {
        final MatViewFuzzOpGenerator generator = new MatViewFuzzOpGenerator(dayCount, false, true);
        generator.isBareNameViewBuilt = !isViewDeferred;
        return generator;
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

    // The number of transactions per part of the transaction logs a round's tables keep: 0 keeps each log in one file,
    // 1 starts a part with every transaction, and 2 to 5 a few. It draws from a copy of the round's Rnd, so the round's
    // own draws, and the seeds that reproduce a round, stay as they were.
    public static int pickSeqPartTxnCount(Rnd rnd) {
        final Rnd copy = new Rnd(rnd.getSeed1(), rnd.getSeed0());
        return switch (copy.nextInt(3)) {
            case 0 -> 0;
            case 1 -> 1;
            default -> 2 + copy.nextInt(4);
        };
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

    // The operations drawn while the view's WAL is suspended: those that touch the base alone, run the jobs, reset the
    // view's tracker, load the view as a boot does, or that a pending definition decides, ALTER ... AS, and RESUME WAL.
    // The others fall back to INSERT there: their outcome on a suspended view WAL is not the one the generator predicts.
    public static boolean isAllowedWhileViewWalSuspended(Kind kind) {
        return switch (kind) {
            case INSERT, APPLY_WAL, APPLY_BASE_WAL, RUN_REFRESH_JOB, RESET_VIEW_TRACKER, VIEW_ALTER_AS, VIEW_RESUME_WAL,
                 BOOT_LOAD -> true;
            default -> false;
        };
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
            case APPLY_BASE_WAL -> 4;
            case RESET_VIEW_TRACKER, VIEW_SUSPEND_WAL -> 2;
            // a view without a period only, see PERIOD_DEFINITION_WINDOW_KINDS
            case BOOT_LOAD -> isPeriodView ? 0 : 2;
            // drawn only while the view's WAL is suspended, INSERT otherwise
            case VIEW_RESUME_WAL -> 6;
            // only a bare-name round draws them, see bareNameRoundWeight()
            case BASE_ADD_BARE_NAME_COLUMN, BASE_DROP_BARE_NAME_COLUMN, BASE_RACED_BARE_NAME_COLUMN_CHANGE -> 0;
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

    // Whether the bare-name round's view completed a refresh. Until it has, the next operation is the view's first
    // build racing a change of the bare count: the racers run the refresh job, whose INVALIDATE handler declines the
    // guard's invalidation of a view without a refresh position, then REFRESH ... INCREMENTAL and the refresh job
    // again, which builds the view from the base without the change. The build commits a refresh position below the
    // switch the guard recorded, so it publishes the guard's invalidation again: the view ends invalid, and the switch
    // stands. Up to two drawn racers follow.
    public boolean isBareNameViewBuilt() {
        return isBareNameViewBuilt;
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

    // Whether the view's WAL may hold the definition event of an ALTER ... AS, which a view ADD COLUMN, DROP COLUMN or
    // ALTER ... AS waits for, and which a restart of the view states forgets.
    public boolean isDefinitionPending() {
        return isDefinitionPending;
    }

    // Whether the view's WAL is suspended, see VIEW_SUSPEND_WAL.
    public boolean isViewWalSuspended() {
        return isViewWalSuspended;
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
        Kind kind = Kind.INSERT;
        if (!isBareNameRound && isDefinitionPending && rnd.nextInt(3) == 0) {
            final Kind[] windowKinds = isPeriodView ? PERIOD_DEFINITION_WINDOW_KINDS : DEFINITION_WINDOW_KINDS;
            kind = windowKinds[rnd.nextInt(windowKinds.length)];
        } else {
            int pick = rnd.nextInt(totalWeight);
            for (Kind k : Kind.values()) {
                pick -= isBareNameRound ? bareNameRoundWeight(k) : weight(k, isPeriodView);
                if (pick < 0) {
                    kind = k;
                    break;
                }
            }
        }
        op.kind = kind;
        if (isBareNameRound) {
            return nextOfBareNameRound(rnd, op);
        }
        if (isViewWalSuspended ? !isAllowedWhileViewWalSuspended(kind) || kind == Kind.VIEW_SUSPEND_WAL : kind == Kind.VIEW_RESUME_WAL) {
            return insteadInsert(rnd, op);
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
            case BASE_UPDATE -> {
                op.day = isPeriodView && completeDays > 0 && rnd.nextInt(4) > 0
                        ? rnd.nextInt(completeDays)
                        : rnd.nextInt(dayCount);
                op.invalidationShape = drawInvalidationAfterRangeOperation(rnd);
            }
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
                op.invalidationShape = drawInvalidationAfterRangeOperation(rnd);
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
                // the statement waits for the definition event of an earlier ALTER ... AS, and refuses while the view's
                // WAL is suspended short of it, before it checks the SQL
                op.isRefusedBySuspension = isViewWalSuspended && isDefinitionPending;
                op.isRefused |= op.isRefusedBySuspension;
            }
            // RESUME WAL FROM TXN past the pending definition event, half of the time
            case VIEW_RESUME_WAL -> op.isSkipDefinition = isDefinitionPending && rnd.nextBoolean();
            case VIEW_RANGE -> {
                op.day = rnd.nextInt(dayCount);
                op.dayHi = op.day + 1 + rnd.nextInt(dayCount - op.day);
            }
            case VIEW_RESUME -> op.isSkipBacklog = rnd.nextInt(3) == 0;
            case VIEW_INVALIDATE -> op.invalidationShape = rnd.nextInt(3);
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

    // The test ran an ALTER ... AS, whose definition event waits in the view's WAL.
    public void onDefinitionChangeIssued() {
        isDefinitionPending = true;
    }

    // The view's WAL applied the pending definition event, while a view column or policy change may still wait.
    public void onDefinitionApplied() {
        isDefinitionPending = false;
    }

    // RESUME WAL FROM TXN skipped the view's WAL past the pending definition event: the view keeps the SQL it had
    // before the statement.
    public void onDefinitionSkipped() {
        sqlColumns.clear();
        sqlColumns.addAll(sqlColumnsBeforeDefinition);
        isSqlPositional = isSqlPositionalBeforeDefinition;
        isDefinitionPending = false;
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

    // The RESUME WAL that the test runs before a view check, which takes a view with no suspended WAL: a VIEW_RESUME_WAL
    // as next() draws it.
    public Op resumeViewWal(Rnd rnd, Op op) {
        op.clear();
        op.kind = Kind.VIEW_RESUME_WAL;
        op.isSkipDefinition = isDefinitionPending && rnd.nextBoolean();
        apply(op);
        return op;
    }

    // The test has applied the view's WAL: the view's applied columns, SQL and policy are the generator's.
    public void onViewWalApplied() {
        isColumnChangePending = false;
        isDefinitionPending = false;
        isPolicyChangePending = false;
    }

    // Adds a random racer to the raced change: one CREATE of mv2 at most, a second draw of it runs the refresh job.
    private static void addRacer(Rnd rnd, Op op) {
        final int racer = rnd.nextInt(RACER_COUNT);
        op.racers.add(racer == RACER_CREATE_VIEW && op.racers.contains(RACER_CREATE_VIEW) ? RACER_RUN_REFRESH_JOB : racer);
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

    // The invalidation that follows an UPDATE or ATTACH, see Op.invalidationShape: for a quarter of them, one that
    // publishes its marker first once the operation applied, as when a base column change in a later WAL batch makes the
    // bare-name guard invalidate the view while the operation's range task waits in the refresh queue.
    private static int drawInvalidationAfterRangeOperation(Rnd rnd) {
        if (rnd.nextInt(4) > 0) {
            return INVALIDATE_NONE;
        }
        return rnd.nextBoolean() ? INVALIDATE_MARKER_FIRST : INVALIDATE_GUARD_MARKER;
    }

    // The outcome of the view mv2 that the raced change's RACER_CREATE_VIEW creates, see RACED_VIEW_*: a racer after
    // the CREATE that runs the refresh job, FULL's included, builds the view before the change is visible.
    private static int predictRacedViewOutcome(Op op) {
        final int createIndex = op.racers.indexOf(RACER_CREATE_VIEW, 0, op.racers.size());
        if (createIndex < 0) {
            return RACED_VIEW_NONE;
        }
        if (op.isColumnAdded) {
            return RACED_VIEW_REFUSED;
        }
        for (int i = createIndex + 1, n = op.racers.size(); i < n; i++) {
            final int racer = op.racers.getQuick(i);
            if (racer == RACER_RUN_REFRESH_JOB || racer == RACER_FULL) {
                return RACED_VIEW_BUILT_BEFORE_CHANGE;
            }
        }
        return RACED_VIEW_BUILT_AFTER_CHANGE;
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
                    sqlColumnsBeforeDefinition.clear();
                    sqlColumnsBeforeDefinition.addAll(sqlColumns);
                    isSqlPositionalBeforeDefinition = isSqlPositional;
                    sqlColumns.clear();
                    sqlColumns.addAll(op.sqlColumns);
                    isSqlPositional = op.isPositional;
                }
            }
            case VIEW_PERIOD_REFRESH -> completeDays++;
            case VIEW_SUSPEND_WAL -> isViewWalSuspended = true;
            case VIEW_RESUME_WAL -> isViewWalSuspended = false;
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
            case BASE_RACED_BARE_NAME_COLUMN_CHANGE -> {
                // the operations that race the change see the base without it
                for (int i = 0, n = op.racers.size(); i < n; i++) {
                    switch (op.racers.getQuick(i)) {
                        case RACER_INVALIDATE -> isBareNameViewInvalid = true;
                        case RACER_FULL -> {
                            // FULL rebuilds every row from a snapshot without the change, and covers the earlier ones
                            isBareNameViewInvalid = false;
                            isBareNameSwitchStanding = false;
                            isRowsReadingColumn = isColumnReading(bareNameSql, hasBaseCountColumn);
                            isReadingMixedByAlter = false;
                        }
                        case RACER_ALTER_TO_BARE -> {
                            // ALTER ... AS validates the bare count against the base without the change, which has the
                            // column count unless the raced change adds it
                            if (!op.isColumnAdded) {
                                isReadingMixedByAlter |= isColumnReading(BARE_NAME_SQL_BARE, hasBaseCountColumn) != isRowsReadingColumn;
                                bareNameSql = BARE_NAME_SQL_BARE;
                            }
                        }
                        default -> {
                            // nothing the model keeps changes
                        }
                    }
                }
                hasBaseCountColumn = op.isColumnAdded;
                if (bareNameSql == BARE_NAME_SQL_BARE) {
                    // the guard records the switch, before the change applies or, for a SQL that ALTER ... AS installed
                    // while it applied, after it, and invalidates the view, which has a refresh position; a refresh
                    // that commits below the switch, a FULL that raced the change included, publishes the guard's
                    // invalidation again
                    isBareNameSwitchStanding = true;
                    isBareNameViewInvalid = true;
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
        if (!isBareNameViewBuilt) {
            // the deferred view's first build races the first change of the bare count, see isBareNameViewBuilt()
            op.kind = Kind.BASE_RACED_BARE_NAME_COLUMN_CHANGE;
            op.isColumnAdded = !hasBaseCountColumn;
            op.racers.add(RACER_RUN_REFRESH_JOB);
            op.racers.add(RACER_INCREMENTAL);
            op.racers.add(RACER_RUN_REFRESH_JOB);
            for (int i = 0, n = rnd.nextInt(3); i < n; i++) {
                addRacer(rnd, op);
            }
            op.racedViewOutcome = predictRacedViewOutcome(op);
            isBareNameViewBuilt = true;
            apply(op);
            return op;
        }
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
            case BASE_RACED_BARE_NAME_COLUMN_CHANGE -> {
                // a SQL that reads the qualified column keeps it, so that no refresh fails
                if (bareNameSql == BARE_NAME_SQL_QUALIFIED) {
                    return insteadInsert(rnd, op);
                }
                op.isColumnAdded = !hasBaseCountColumn;
                for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                    addRacer(rnd, op);
                }
                op.racedViewOutcome = predictRacedViewOutcome(op);
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
            case 3 ->
                    op.policy = "KEEP " + (rnd.nextBoolean() ? "HIGHEST" : "LOWEST") + " ON " + c + " PARTITION BY ts, sym";
            case 4 ->
                    op.policy = "KEEP " + (1 + rnd.nextInt(3)) + (rnd.nextBoolean() ? " HIGHEST" : " LOWEST") + " ON " + c + " PARTITION BY ts, sym";
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
        // the bare-name round's base gains its column count, or drops it, while the RACER_* operations of Op.racers run
        // after the bare-name guard ran and before the change is visible
        BASE_RACED_BARE_NAME_COLUMN_CHANGE,
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
        // an invalidation, which stands for a failed refresh or, in the shapes that publish the marker first, see
        // Op.invalidationShape, for an apply-time invalidation such as the bare-name guard's
        VIEW_INVALIDATE,
        // the refresh intervals update that the timer job enqueues for an invalid view's backlog
        BACKLOG_UPDATE,
        RESTART,
        // a boot load of the view, as a booting server runs it before any WAL apply, with no WAL apply before it: the
        // load records the definition event that the view's WAL holds unapplied, and the generator's state stays as it
        // is, since the load applies nothing
        BOOT_LOAD,
        APPLY_WAL,
        RUN_REFRESH_JOB,
        PURGE_WAL,
        // the WAL apply of the base alone, as an apply run that serves the base before the view, so that the refresh job
        // meets the base rows before the view's WAL applies a pending definition event
        APPLY_BASE_WAL,
        // a reset of the view's SeqTxnTracker, as a republish of the WAL apply job does, see
        // CairoEngine.notifyWalTxnRepublisher()
        RESET_VIEW_TRACKER,
        VIEW_SUSPEND_WAL,
        // RESUME WAL of the view, or RESUME WAL FROM TXN past the pending definition event alone, see Op.isSkipDefinition
        VIEW_RESUME_WAL
    }

    public static final class Op {
        // the columns the policy of a SET EXPIRE reads
        public final ObjList<String> policyColumns = new ObjList<>();
        // the RACER_* operations of a BASE_RACED_BARE_NAME_COLUMN_CHANGE, in their order
        public final IntList racers = new IntList();
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
        // the shape of a VIEW_INVALIDATE, one of INVALIDATE_*, and of the invalidation that follows a BASE_UPDATE or
        // BASE_ATTACH_PARTITION, INVALIDATE_NONE for none
        public int invalidationShape;
        // a BASE_RACED_BARE_NAME_COLUMN_CHANGE adds the column count, rather than drops it
        public boolean isColumnAdded;
        // the SQL of an ALTER ... AS copies by position, see viewSql(ObjList, boolean)
        public boolean isPositional;
        public boolean isProbed;
        public boolean isRefused;
        // an ALTER ... AS refused because the view's WAL is suspended short of an earlier ALTER ... AS
        public boolean isRefusedBySuspension;
        public boolean isSkipBacklog;
        // a VIEW_RESUME_WAL that skips the pending definition event with RESUME WAL FROM TXN, when it is the view's only
        // transaction that the WAL apply has not applied
        public boolean isSkipDefinition;
        public Kind kind;
        // the text after SET EXPIRE ROWS
        public String policy;
        // the outcome of the view mv2 that a racer of a BASE_RACED_BARE_NAME_COLUMN_CHANGE creates, one of RACED_VIEW_*
        public int racedViewOutcome;
        // the outcome of a RESUME of the bare-name round, one of RESUME_*
        public int resumeOutcome;

        public void clear() {
            kind = null;
            bareNameSql = BARE_NAME_SQL_BARE;
            racedViewOutcome = RACED_VIEW_NONE;
            resumeOutcome = RESUME_ACCEPTED;
            day = -1;
            dayHi = -1;
            column = null;
            columnType = null;
            expireClause = null;
            invalidationShape = INVALIDATE_NONE;
            isColumnAdded = false;
            isPositional = false;
            isProbed = false;
            isRefused = false;
            isRefusedBySuspension = false;
            isSkipDefinition = false;
            policy = null;
            policyColumns.clear();
            racers.clear();
            isSkipBacklog = false;
            sqlColumns.clear();
            rowTs.clear();
            rowSyms.clear();
            rowPrices.clear();
            rowQtys.clear();
        }
    }
}
