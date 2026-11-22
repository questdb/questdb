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

import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the operations {@link MatViewFuzzOpGenerator} has to keep drawing for
 * {@link MatViewFuzzTest#testViewAndBaseOperations()}, as sequences of the length one fuzz round runs. A fuzzer finds
 * bugs only in what it generates, so each operation and each shape below must turn up in a corpus of rounds:
 * <ul>
 *     <li>every operation kind, among them the base partition operations, TRUNCATE, UPDATE and TTL, view ADD and DROP
 *     COLUMN, view DROP PARTITION in both forms, ALTER ... AS, and the restarts right after a base replacement followed
 *     by FULL or by RESUME;</li>
 *     <li>an ALTER ... AS that the same-position rule refuses and one it accepts, and one that moves ts and sym out of
 *     their place;</li>
 *     <li>an ALTER ... AS whose SQL copies by position: refused for a moved column, for a renamed one and for another
 *     column count, and accepted with the view's columns in their order, after which view ADD and DROP COLUMN are
 *     refused; and none on a view that dropped a column;</li>
 *     <li>a view ADD COLUMN, DROP COLUMN and ALTER ... AS, accepted and refused, while the definition event of an
 *     earlier ALTER ... AS waits in the view's WAL, which the statement waits for;</li>
 *     <li>a view ADD COLUMN of a column the SQL produces under another type than the SQL's, which name mapping casts,
 *     and of a column no SQL produces;</li>
 *     <li>an ATTACH of a partition the same round detached, and a row into a day whose base partition the round
 *     dropped, which recreates it;</li>
 *     <li>a reload of the base: TRUNCATE, the ATTACH of a partition the round detached and an UPDATE of the attached
 *     day, applied in one WAL apply run whose range task the refresh job takes before the apply job moves the base's
 *     SeqTxnTracker; in a period view's round, of a day whose period ended;</li>
 *     <li>RESUME with and without SKIP BACKLOG;</li>
 *     <li>an invalidation that publishes its marker first, after the WAL apply that queues the range task of an UPDATE
 *     or ATTACH and before the refresh job runs it, so that the task meets the marker on a valid view: the forced
 *     marker, and the bare-name guard's marker, after a FULL refresh that covers it and with no FULL since the last
 *     restart; drawn as a VIEW_INVALIDATE, and as the invalidation that follows an UPDATE or ATTACH;</li>
 *     <li>a base replacement that leaves out the first day, so the view keeps the old base's rows there;</li>
 *     <li>a SET EXPIRE of each policy shape, one that reads two columns, and one over a column the view lacks, which
 *     the statement refuses;</li>
 *     <li>a view DROP COLUMN of a column the EXPIRE ROWS policy reads, which the statement refuses, and one of a
 *     column the policy does not read while the view has a policy;</li>
 *     <li>for a period view, the period timer's firings, which complete the days in order; gap days, which the
 *     initial rows and every later row keep off until their period ends; and the sequence that finds a period refresh
 *     with nothing to recompute while the view's cache holds a range that no WAL scan finds again: an UPDATE of a
 *     complete day with a base partition, or an ATTACH of a complete day, then the firing over a day without a base
 *     partition, before an incremental refresh recomputes the range. An EVERY view refreshes incrementally at a refresh
 *     job run, which stands for its EVERY firing, and at the view check after every sixth operation, and a FULL, a
 *     TRUNCATE and a base replacement drop the range too. A view without a period draws no firing.</li>
 * </ul>
 * The rounds start with the initial rows the generator draws, as the fuzz test's do. The generator draws without an
 * engine; the seed is fixed, so a failure reproduces.
 */
public class MatViewFuzzOpGeneratorCoverageTest {
    // MatViewFuzzTest checks the view after every sixth operation, and the check catches a period view up with an
    // incremental refresh
    private static final int CHECK_STEPS = 6;
    private static final int DAY_COUNT = 6;
    // each outcome of a view column change and of an ALTER ... AS while a definition event waits in the view's WAL
    private static final int DEFINITION_PENDING_MIN = 10;
    // each shape of an invalidation that publishes its marker while a range task waits in the refresh queue
    private static final int MARKER_FIRST_SHAPE_MIN = 20;
    // each shape of an ALTER ... AS that copies by position, and the column changes it refuses
    private static final int POSITIONAL_ALTER_MIN = 20;
    // the restarts of testRestartShapesOverViewWal() above the purged txn-log parts, in RESTART_SHAPE_ROUNDS rounds
    private static final int PURGE_THEN_RESTART_MIN = 40;
    // each shape of testBareNameRoundShapes() that a racer creating mv2, or altering the view to the bare count, draws
    // while a raced base column change applies, in BARE_NAME_ROUNDS rounds
    private static final int RACED_VIEW_SHAPE_MIN = 50;
    // the rounds of testRestartShapesOverViewWal(), half of them of a period view: each of its restart shapes needs a few
    // given operations between two view checks, so the generator draws it only a few times in a thousand rounds
    private static final int RESTART_SHAPE_ROUNDS = 10_000;
    // the restarts of testRestartShapesOverViewWal() above a RESUME event and a view operation without view state, in
    // RESTART_SHAPE_ROUNDS rounds
    private static final int RESUME_THEN_RESTART_MIN = 8;
    // the restarts of testRestartShapesOverViewWal() above a view state event and a view operation without view state,
    // in RESTART_SHAPE_ROUNDS rounds
    private static final int STATE_OP_THEN_RESTART_MIN = 10;
    // the rounds of the bare-name round's shapes, which share their draws with the raced base column change; the
    // minimums of testBareNameRoundShapes() are for this many rounds
    private static final int BARE_NAME_ROUNDS = 1_000;
    private static final int ROUNDS = 500;
    private static final int STEPS_PER_ROUND = 60;

    @Test
    public void testEveryOperationAndShape() {
        final Rnd rnd = new Rnd(42, 42);
        final int[] kindCounts = new int[MatViewFuzzOpGenerator.Kind.values().length];
        int alterRefused = 0;
        int alterAccepted = 0;
        int alterReordered = 0;
        int addCast = 0;
        int addUnproduced = 0;
        int attachAfterDetach = 0;
        int reloadAfterDetach = 0;
        int insertIntoDropped = 0;
        int resumeSkip = 0;
        int resumePlain = 0;
        // an invalidation that publishes its marker first while the range task of an UPDATE or ATTACH waits in the
        // refresh queue: the forced marker, and the bare-name guard's marker after a VIEW_FULL that a view check ran and
        // with no VIEW_FULL, since the last restart or base replacement; of them, the invalidations that follow an
        // UPDATE or ATTACH
        int markerFirstBehindRangeTask = 0;
        int guardMarkerBehindRangeTaskCovered = 0;
        int guardMarkerBehindRangeTaskUncovered = 0;
        int markerFirstAfterRangeOperation = 0;
        int replaceLeavesFirstDay = 0;
        int bulkInsert = 0;
        int setExpireRefused = 0;
        int setExpireTwoColumns = 0;
        int dropColumnRefused = 0;
        int dropColumnBesidePolicy = 0;
        int positionalAlterMoved = 0;
        int positionalAlterRenamed = 0;
        int positionalAlterCountDiffers = 0;
        int positionalAlterAccepted = 0;
        int addColumnRefusedPositional = 0;
        int dropColumnRefusedPositional = 0;
        // the view ADD COLUMN, DROP COLUMN and ALTER ... AS drawn while the definition event of an earlier ALTER ... AS
        // waits in the view's WAL, which they wait for, accepted and refused
        final int[] addWhileDefinitionPending = new int[2];
        final int[] dropWhileDefinitionPending = new int[2];
        final int[] alterWhileDefinitionPending = new int[2];
        final int[] policyShapes = new int[5];
        final MatViewFuzzOpGenerator.Op op = new MatViewFuzzOpGenerator.Op();
        for (int round = 0; round < ROUNDS; round++) {
            final MatViewFuzzOpGenerator generator = new MatViewFuzzOpGenerator(DAY_COUNT, false);
            generator.initView(rnd);
            for (int day = 0; day < DAY_COUNT; day++) {
                Assert.assertTrue("initial rows in every day of a view without a period", generator.initRows(rnd, day, op).rowTs.size() > 0);
                Assert.assertTrue(generator.isBaseDay(day));
            }
            final boolean[] isDetached = new boolean[DAY_COUNT];
            final boolean[] isDropped = new boolean[DAY_COUNT];
            boolean hasDroppedViewColumn = false;
            boolean isViewColumnAdded = false;
            // the range task of an UPDATE or ATTACH waits in the refresh queue: no refresh job run, restart, TRUNCATE,
            // base replacement or view check came since
            boolean isRangeTaskQueued = false;
            // a VIEW_FULL was drawn, and a view check ran it, with no restart or base replacement since
            boolean isFullIssued = false;
            boolean isFullCovering = false;
            for (int step = 0; step < STEPS_PER_ROUND; step++) {
                final ObjList<String> sqlBefore = new ObjList<>();
                sqlBefore.addAll(generator.getSqlColumns());
                final boolean wasSqlPositional = generator.isSqlPositional();
                final boolean wasDefinitionPending = generator.isDefinitionPending();
                generator.next(rnd, op);
                kindCounts[op.kind.ordinal()]++;
                if (wasDefinitionPending) {
                    final int outcome = op.isRefused ? 1 : 0;
                    switch (op.kind) {
                        case VIEW_ADD_COLUMN -> addWhileDefinitionPending[outcome]++;
                        case VIEW_DROP_COLUMN -> dropWhileDefinitionPending[outcome]++;
                        case VIEW_ALTER_AS -> alterWhileDefinitionPending[outcome]++;
                        default -> {
                        }
                    }
                }
                switch (op.kind) {
                    case VIEW_ALTER_AS -> {
                        if (op.isRefused) {
                            Assert.assertEquals("a refused ALTER ... AS must keep the SQL", sqlBefore, generator.getSqlColumns());
                            Assert.assertEquals(wasSqlPositional, generator.isSqlPositional());
                        } else {
                            Assert.assertEquals(op.sqlColumns, generator.getSqlColumns());
                            Assert.assertEquals(op.isPositional, generator.isSqlPositional());
                        }
                        if (op.isPositional) {
                            Assert.assertFalse("a SQL that copies by position on a view that dropped a column", hasDroppedViewColumn);
                            final ObjList<String> viewColumns = generator.getViewColumns();
                            if (!op.isRefused) {
                                positionalAlterAccepted++;
                                Assert.assertEquals("an accepted SQL that copies by position has the view's columns", viewColumns, op.sqlColumns);
                            } else if (op.sqlColumns.size() != viewColumns.size()) {
                                positionalAlterCountDiffers++;
                            } else {
                                boolean isEveryColumnInView = true;
                                for (int i = 0, n = op.sqlColumns.size(); i < n; i++) {
                                    isEveryColumnInView &= viewColumns.indexOf(op.sqlColumns.getQuick(i)) > -1;
                                }
                                if (isEveryColumnInView) {
                                    positionalAlterMoved++;
                                } else {
                                    positionalAlterRenamed++;
                                }
                            }
                        } else {
                            if (op.isRefused) {
                                alterRefused++;
                            } else {
                                alterAccepted++;
                            }
                            if (!op.sqlColumns.getQuick(0).equals(MatViewFuzzOpGenerator.TS) || !op.sqlColumns.getQuick(1).equals(MatViewFuzzOpGenerator.SYM)) {
                                alterReordered++;
                            }
                        }
                    }
                    case VIEW_ADD_COLUMN -> {
                        Assert.assertEquals("view ADD COLUMN refused exactly while the SQL copies by position", generator.isSqlPositional(), op.isRefused);
                        if (op.isRefused) {
                            addColumnRefusedPositional++;
                            Assert.assertTrue("a refused ADD COLUMN keeps the view's columns", generator.getViewColumns().indexOf(op.column) < 0);
                        } else {
                            Assert.assertTrue(generator.getViewColumns().indexOf(op.column) > -1);
                        }
                        final int catalogueIndex = MatViewFuzzOpGenerator.catalogueIndex(op.column);
                        if (catalogueIndex < 0) {
                            addUnproduced++;
                        } else if (!MatViewFuzzOpGenerator.CATALOGUE_VIEW_TYPES[catalogueIndex][0].equals(op.columnType)) {
                            addCast++;
                        }
                    }
                    case BASE_DETACH_PARTITION -> isDetached[op.day] = true;
                    case BASE_ATTACH_PARTITION -> {
                        Assert.assertTrue("ATTACH of a partition that did not detach: " + op.day, isDetached[op.day]);
                        isDetached[op.day] = false;
                        attachAfterDetach++;
                    }
                    case BASE_TRUNCATE_ATTACH_UPDATE -> {
                        Assert.assertTrue("reload of a partition that did not detach: " + op.day, isDetached[op.day]);
                        isDetached[op.day] = false;
                        for (int d = 0; d < DAY_COUNT; d++) {
                            isDropped[d] = d != op.day;
                        }
                        Assert.assertTrue(generator.isBaseDay(op.day));
                        reloadAfterDetach++;
                    }
                    case BASE_DROP_PARTITION_LIST -> isDropped[op.day] = true;
                    case INSERT -> {
                        if (op.rowTs.size() >= 20) {
                            bulkInsert++;
                        }
                        for (int i = 0, n = op.rowTs.size(); i < n; i++) {
                            final int day = (int) (op.rowTs.getQuick(i) / Micros.DAY_MICROS);
                            if (isDropped[day]) {
                                insertIntoDropped++;
                                isDropped[day] = false;
                            }
                            // a row recreates the partition, so its detached copy no longer attaches
                            isDetached[day] = false;
                        }
                    }
                    case VIEW_SET_EXPIRE -> {
                        if (op.isRefused) {
                            setExpireRefused++;
                            Assert.assertTrue("a refused SET EXPIRE reads a column the view lacks: " + op.policy,
                                    generator.getViewColumns().indexOf(op.column) < 0);
                        } else {
                            for (int i = 0, n = op.policyColumns.size(); i < n; i++) {
                                Assert.assertTrue("a policy reads a column the view has: " + op.policy,
                                        generator.getViewColumns().indexOf(op.policyColumns.getQuick(i)) > -1);
                            }
                            Assert.assertEquals(op.policyColumns, generator.getPolicyColumns());
                            Assert.assertEquals(op.expireClause, generator.getExpireClause());
                        }
                        if (op.policyColumns.size() == 2) {
                            setExpireTwoColumns++;
                        }
                        if (op.policy.startsWith("WHEN ") && op.policy.contains(" OVER ")) {
                            policyShapes[0]++;
                        } else if (op.policy.startsWith("WHEN ")) {
                            policyShapes[1]++;
                        } else if (op.policy.startsWith("KEEP LATEST ")) {
                            policyShapes[2]++;
                        } else if (op.policy.startsWith("KEEP HIGHEST ") || op.policy.startsWith("KEEP LOWEST ")) {
                            policyShapes[3]++;
                        } else if (op.policy.matches("KEEP [0-9]+ (HIGHEST|LOWEST) .*")) {
                            policyShapes[4]++;
                        } else {
                            Assert.fail("unexpected policy: " + op.policy);
                        }
                        if (op.isProbed != (op.policy.startsWith("KEEP ") || op.policy.contains(" OVER "))) {
                            Assert.fail("a KEEP or window policy has a read probe: " + op.policy);
                        }
                    }
                    case VIEW_DROP_EXPIRE -> {
                        Assert.assertNull(generator.getExpireClause());
                        Assert.assertEquals(0, generator.getPolicyColumns().size());
                    }
                    case VIEW_DROP_COLUMN -> {
                        if (op.isRefused) {
                            if (generator.getPolicyColumns().indexOf(op.column) > -1) {
                                dropColumnRefused++;
                            } else {
                                Assert.assertTrue("view DROP COLUMN refused while the SQL maps by name", generator.isSqlPositional());
                                dropColumnRefusedPositional++;
                            }
                            Assert.assertTrue("a refused DROP COLUMN keeps the column", generator.getViewColumns().indexOf(op.column) > -1);
                        } else {
                            Assert.assertFalse("view DROP COLUMN accepted while the SQL copies by position", generator.isSqlPositional());
                            Assert.assertTrue(generator.getViewColumns().indexOf(op.column) < 0);
                            hasDroppedViewColumn = true;
                            if (generator.getExpireClause() != null) {
                                dropColumnBesidePolicy++;
                            }
                        }
                    }
                    case VIEW_RESUME -> {
                        if (op.isSkipBacklog) {
                            resumeSkip++;
                        } else {
                            resumePlain++;
                        }
                    }
                    case BASE_REPLACE, BASE_REPLACE_FULL_RESTART, BASE_REPLACE_RESUME_RESTART -> {
                        boolean hasFirstDay = false;
                        for (int i = 0, n = op.rowTs.size(); i < n; i++) {
                            hasFirstDay |= op.rowTs.getQuick(i) < Micros.DAY_MICROS;
                        }
                        if (!hasFirstDay) {
                            replaceLeavesFirstDay++;
                        }
                        for (int d = 0; d < DAY_COUNT; d++) {
                            isDetached[d] = false;
                            isDropped[d] = false;
                        }
                    }
                    case BASE_TRUNCATE -> {
                        for (int d = 0; d < DAY_COUNT; d++) {
                            isDropped[d] = true;
                        }
                    }
                    default -> {
                    }
                }
                switch (op.kind) {
                    // an UPDATE of a day the base holds no partition of queues no range
                    case BASE_UPDATE -> isRangeTaskQueued |= generator.isBaseDay(op.day);
                    case BASE_ATTACH_PARTITION -> isRangeTaskQueued = true;
                    case VIEW_INVALIDATE -> Assert.assertNotEquals(MatViewFuzzOpGenerator.INVALIDATE_NONE, op.invalidationShape);
                    case RUN_REFRESH_JOB, BASE_TRUNCATE, BASE_TRUNCATE_ATTACH_UPDATE -> isRangeTaskQueued = false;
                    case VIEW_FULL -> isFullIssued = true;
                    case RESTART, BASE_REPLACE, BASE_REPLACE_FULL_RESTART, BASE_REPLACE_RESUME_RESTART -> {
                        isRangeTaskQueued = false;
                        isFullIssued = false;
                        isFullCovering = false;
                    }
                    default -> {
                    }
                }
                switch (op.kind) {
                    case BASE_UPDATE, BASE_ATTACH_PARTITION, VIEW_INVALIDATE -> {
                        if (isRangeTaskQueued) {
                            switch (op.invalidationShape) {
                                case MatViewFuzzOpGenerator.INVALIDATE_MARKER_FIRST -> markerFirstBehindRangeTask++;
                                case MatViewFuzzOpGenerator.INVALIDATE_GUARD_MARKER -> {
                                    if (isFullCovering) {
                                        guardMarkerBehindRangeTaskCovered++;
                                    } else if (!isFullIssued) {
                                        guardMarkerBehindRangeTaskUncovered++;
                                    }
                                }
                                default -> {
                                }
                            }
                            if (op.kind != MatViewFuzzOpGenerator.Kind.VIEW_INVALIDATE && op.invalidationShape != MatViewFuzzOpGenerator.INVALIDATE_NONE) {
                                markerFirstAfterRangeOperation++;
                            }
                        }
                    }
                    default -> {
                    }
                }
                if (step % CHECK_STEPS == CHECK_STEPS - 1) {
                    // the view check drains the queues, which runs the range tasks and a FULL refresh
                    isRangeTaskQueued = false;
                    isFullCovering |= isFullIssued;
                    isFullIssued = false;
                }
                // the view's columns always start with ts and sym, which no statement drops
                Assert.assertEquals(MatViewFuzzOpGenerator.TS, generator.getViewColumns().getQuick(0));
                Assert.assertEquals(MatViewFuzzOpGenerator.SYM, generator.getViewColumns().getQuick(1));
                trackDefinitionPending(generator, op, wasDefinitionPending, isViewColumnAdded, step);
                isViewColumnAdded |= op.kind == MatViewFuzzOpGenerator.Kind.VIEW_ADD_COLUMN && !op.isRefused;
            }
        }

        assertKindShares(kindCounts, false);
        final int detachCount = kindCounts[MatViewFuzzOpGenerator.Kind.BASE_DETACH_PARTITION.ordinal()];
        assertShare("ATTACH of a partition the round detached, of " + detachCount + " DETACH", attachAfterDetach, detachCount / 4);
        assertShare("reload of a partition the round detached, of " + detachCount + " DETACH", reloadAfterDetach, detachCount / 8);
        assertShare("ALTER ... AS refused by the same-position rule", alterRefused, 100);
        assertShare("ALTER ... AS accepted", alterAccepted, 100);
        assertShare("ALTER ... AS with ts and sym moved", alterReordered, 100);
        assertShare("view ADD COLUMN under a cast type", addCast, 50);
        assertShare("view ADD COLUMN that no SQL produces", addUnproduced, 50);
        assertShare("row that recreates a dropped partition", insertIntoDropped, 100);
        assertShare("RESUME SKIP BACKLOG", resumeSkip, 50);
        assertShare("plain RESUME", resumePlain, 100);
        assertShare("forced invalidation marker published while a range task waits", markerFirstBehindRangeTask, MARKER_FIRST_SHAPE_MIN);
        assertShare(
                "bare-name guard's invalidation marker published while a range task waits, after a covering FULL",
                guardMarkerBehindRangeTaskCovered,
                MARKER_FIRST_SHAPE_MIN
        );
        assertShare(
                "bare-name guard's invalidation marker published while a range task waits, without a FULL",
                guardMarkerBehindRangeTaskUncovered,
                MARKER_FIRST_SHAPE_MIN
        );
        assertShare(
                "invalidation marker published right after an UPDATE or ATTACH applied",
                markerFirstAfterRangeOperation,
                MARKER_FIRST_SHAPE_MIN
        );
        assertShare("base replacement without the first day", replaceLeavesFirstDay, 50);
        assertShare("bulk INSERT", bulkInsert, 100);
        assertShare("SET EXPIRE refused over a column the view lacks", setExpireRefused, 100);
        assertShare("SET EXPIRE over two columns", setExpireTwoColumns, 50);
        assertShare("SET EXPIRE of a window WHEN policy", policyShapes[0], 100);
        assertShare("SET EXPIRE of a scalar WHEN policy", policyShapes[1], 100);
        assertShare("SET EXPIRE of a KEEP LATEST policy", policyShapes[2], 100);
        assertShare("SET EXPIRE of a KEEP HIGHEST or LOWEST policy", policyShapes[3], 100);
        assertShare("SET EXPIRE of a KEEP N HIGHEST or LOWEST policy", policyShapes[4], 100);
        assertShare("view DROP COLUMN refused by the EXPIRE ROWS policy", dropColumnRefused, 100);
        assertShare("view DROP COLUMN of a column the EXPIRE ROWS policy does not read", dropColumnBesidePolicy, 100);
        assertShare("ALTER ... AS copying by position refused for a moved column", positionalAlterMoved, POSITIONAL_ALTER_MIN);
        assertShare("ALTER ... AS copying by position refused for a renamed column", positionalAlterRenamed, POSITIONAL_ALTER_MIN);
        assertShare("ALTER ... AS copying by position refused on the column count", positionalAlterCountDiffers, POSITIONAL_ALTER_MIN);
        assertShare("ALTER ... AS copying by position accepted", positionalAlterAccepted, POSITIONAL_ALTER_MIN);
        assertShare("view ADD COLUMN refused while the SQL copies by position", addColumnRefusedPositional, POSITIONAL_ALTER_MIN);
        assertShare("view DROP COLUMN refused while the SQL copies by position", dropColumnRefusedPositional, POSITIONAL_ALTER_MIN);
        assertShare("view ADD COLUMN accepted while a definition event is pending", addWhileDefinitionPending[0], DEFINITION_PENDING_MIN);
        assertShare("view DROP COLUMN accepted while a definition event is pending", dropWhileDefinitionPending[0], DEFINITION_PENDING_MIN);
        assertShare(
                "view ADD or DROP COLUMN refused while a definition event is pending",
                addWhileDefinitionPending[1] + dropWhileDefinitionPending[1],
                DEFINITION_PENDING_MIN
        );
        assertShare("ALTER ... AS accepted while a definition event is pending", alterWhileDefinitionPending[0], DEFINITION_PENDING_MIN);
        assertShare("ALTER ... AS refused while a definition event is pending", alterWhileDefinitionPending[1], DEFINITION_PENDING_MIN);

        // rounds of a period view
        final int[] periodKindCounts = new int[MatViewFuzzOpGenerator.Kind.values().length];
        int gapDayCount = 0;
        int gapDayFirings = 0;
        int lateRowsIntoGapDay = 0;
        int updatesOfCompleteDay = 0;
        int reloadsOfCompleteDay = 0;
        int periodRefreshOverEmptyDayWithQueuedRange = 0;
        for (int round = 0; round < ROUNDS; round++) {
            final MatViewFuzzOpGenerator generator = new MatViewFuzzOpGenerator(DAY_COUNT, true);
            generator.initView(rnd);
            for (int day = 0; day < DAY_COUNT; day++) {
                final int rowCount = generator.initRows(rnd, day, op).rowTs.size();
                Assert.assertFalse("the first day is never a gap day", day == 0 && generator.isGapDay(day));
                Assert.assertEquals("initial rows in every day but the gap days", !generator.isGapDay(day), rowCount > 0);
                Assert.assertEquals(!generator.isGapDay(day), generator.isBaseDay(day));
                if (generator.isGapDay(day)) {
                    gapDayCount++;
                }
            }
            // the range of an UPDATE or ATTACH of a complete day waits in the view's cache: no incremental refresh,
            // FULL, TRUNCATE or base replacement came since
            boolean isRangeQueued = false;
            for (int step = 0; step < STEPS_PER_ROUND; step++) {
                final int completeDays = generator.getCompleteDays();
                generator.next(rnd, op);
                periodKindCounts[op.kind.ordinal()]++;
                for (int i = 0, n = op.rowTs.size(); i < n; i++) {
                    final int day = (int) (op.rowTs.getQuick(i) / Micros.DAY_MICROS);
                    Assert.assertFalse("a row on a gap day whose period has not ended", generator.isGapDay(day) && day >= completeDays);
                    if (generator.isGapDay(day) && op.kind == MatViewFuzzOpGenerator.Kind.INSERT) {
                        lateRowsIntoGapDay++;
                    }
                }
                switch (op.kind) {
                    case BASE_UPDATE -> {
                        if (op.day < completeDays) {
                            updatesOfCompleteDay++;
                            isRangeQueued |= generator.isBaseDay(op.day);
                        }
                    }
                    case BASE_ATTACH_PARTITION -> isRangeQueued |= op.day < completeDays;
                    case BASE_TRUNCATE_ATTACH_UPDATE -> {
                        // the TRUNCATE drops the ranges queued before it, and the reload queues the day's
                        Assert.assertTrue("reload of a day whose period has not ended: " + op.day, op.day < completeDays);
                        reloadsOfCompleteDay++;
                        isRangeQueued = true;
                    }
                    // the EVERY firing, a rebuild, the TRUNCATE cut and a new base
                    case RUN_REFRESH_JOB, VIEW_FULL, BASE_TRUNCATE, BASE_REPLACE, BASE_REPLACE_FULL_RESTART,
                         BASE_REPLACE_RESUME_RESTART -> isRangeQueued = false;
                    case VIEW_PERIOD_REFRESH -> {
                        Assert.assertEquals("a firing must complete the next day", completeDays, op.day);
                        Assert.assertEquals(completeDays + 1, generator.getCompleteDays());
                        if (generator.isGapDay(op.day)) {
                            Assert.assertFalse("a gap day holds no base partition when its period ends", generator.isBaseDay(op.day));
                            gapDayFirings++;
                        }
                        if (isRangeQueued && !generator.isBaseDay(op.day)) {
                            periodRefreshOverEmptyDayWithQueuedRange++;
                        }
                    }
                    default -> {
                    }
                }
                if (step % CHECK_STEPS == CHECK_STEPS - 1) {
                    // the view check
                    isRangeQueued = false;
                }
                Assert.assertTrue(generator.getCompleteDays() <= DAY_COUNT);
            }
        }
        assertKindShares(periodKindCounts, true);
        assertShare("gap day", gapDayCount, ROUNDS);
        assertShare("period timer firing over a gap day", gapDayFirings, ROUNDS);
        assertShare("late row into a gap day whose period ended", lateRowsIntoGapDay, 200);
        assertShare("UPDATE of a complete day", updatesOfCompleteDay, 500);
        assertShare("reload of a complete day", reloadsOfCompleteDay, 50);
        assertShare(
                "period timer firing over a day without a base partition while the range of an UPDATE or ATTACH of a complete day is queued",
                periodRefreshOverEmptyDayWithQueuedRange,
                50
        );
    }

    @Test
    public void testRestartShapesOverViewWal() {
        // The shapes that the load of a view's state at a restart needs, over the rounds of a view without a period and
        // of a period view: each class of the transaction logs' part size; a view state event that waits in the view's
        // WAL, then a view operation that writes no view state, then RESTART; the same with the event of a RESUME, the
        // shape that found the restart's load of a stale state file; and, in a round that keeps the transaction logs in
        // parts, an accepted view column change, a WAL purge and a view operation that writes no view state, then
        // RESTART, which puts the restart above the purged txn-log parts. A shape counts only when the fuzz runs it: no
        // view check, which applies the view's WAL, and no operation that applies it, see isViewWalApplied(), comes
        // between the view state event, or the purge shape's operation without view state, and the RESTART.
        // A view state event waits in the view's WAL after RUN_REFRESH_JOB ran the task that VIEW_INVALIDATE, VIEW_FULL
        // or VIEW_PERIOD_REFRESH queued, which a RESTART loses; after a RESUME of a view that VIEW_INVALIDATE and
        // RUN_REFRESH_JOB made invalid, since a RESUME of a valid view writes nothing; and after
        // BASE_REPLACE_RESUME_RESTART. The task of BACKLOG_UPDATE writes an event only when the base's WAL applied
        // commits since the view's refresh, which the generator does not follow, so it counts for none. A RUN_REFRESH_JOB
        // can write a refresh commit above the operation without view state, so the shape starts over at its view state
        // event, and a RESUME event no longer counts. The fuzz also drains the queues after an operation at random,
        // which these counts leave out, as the other counts of this class do.
        final Rnd rnd = new Rnd(42, 42);
        final int[] partSizeClasses = new int[3];
        int stateOpThenRestart = 0;
        int resumeThenRestart = 0;
        int purgeThenRestart = 0;
        final MatViewFuzzOpGenerator.Op op = new MatViewFuzzOpGenerator.Op();
        for (int round = 0; round < RESTART_SHAPE_ROUNDS; round++) {
            final int partSize = MatViewFuzzOpGenerator.pickSeqPartTxnCount(rnd);
            Assert.assertTrue("part size out of range: " + partSize, partSize > -1 && partSize < 6);
            partSizeClasses[Math.min(partSize, 2)]++;
            final MatViewFuzzOpGenerator generator = new MatViewFuzzOpGenerator(DAY_COUNT, round >= RESTART_SHAPE_ROUNDS / 2);
            generator.initView(rnd);
            for (int day = 0; day < DAY_COUNT; day++) {
                generator.initRows(rnd, day, op);
            }
            // the round added a view column, after which the fuzz applies the view's WAL before a view DROP PARTITION
            boolean isViewColumnAdded = false;
            // 1: a view state event waits in the view's WAL, 2: then a view operation without view state
            int stateOpStage = 0;
            // the same for the event of a RESUME
            int resumeStage = 0;
            // 1: an accepted view column change, 2: then a WAL purge, 3: then a view operation without view state
            int purgeStage = 0;
            // a queued task writes a view state event when RUN_REFRESH_JOB runs it
            boolean isStateTaskQueued = false;
            // VIEW_INVALIDATE queued the invalidation of a valid view
            boolean isInvalidationQueued = false;
            // RUN_REFRESH_JOB ran that invalidation, so a RESUME writes its event
            boolean isInvalidatedByForce = false;
            // a base replacement or a FULL may have left the view invalid or a FULL pending, so no invalidation, RESUME
            // or period firing counts until the view check
            boolean isValidityUnknown = false;
            for (int step = 0; step < STEPS_PER_ROUND; step++) {
                final boolean wasDefinitionPending = generator.isDefinitionPending();
                generator.next(rnd, op);
                if (isViewWalApplied(op, wasDefinitionPending, isViewColumnAdded)) {
                    stateOpStage = 0;
                    resumeStage = 0;
                    if (purgeStage == 3) {
                        purgeStage = 2;
                    }
                } else if (isViewOperationWithoutState(op)) {
                    if (stateOpStage == 1) {
                        stateOpStage = 2;
                    }
                    if (resumeStage == 1) {
                        resumeStage = 2;
                    }
                    if (purgeStage == 2) {
                        purgeStage = 3;
                    }
                }
                switch (op.kind) {
                    case VIEW_INVALIDATE, BASE_UPDATE, BASE_ATTACH_PARTITION -> {
                        // a forced invalidation, of a VIEW_INVALIDATE or after an UPDATE or ATTACH; the bare-name guard's
                        // marker leaves the view valid when the last FULL refresh covers it, which this does not follow,
                        // so it counts for none
                        final boolean isForced = op.kind == MatViewFuzzOpGenerator.Kind.VIEW_INVALIDATE
                                ? op.invalidationShape != MatViewFuzzOpGenerator.INVALIDATE_GUARD_MARKER
                                : op.invalidationShape == MatViewFuzzOpGenerator.INVALIDATE_MARKER_FIRST;
                        if (isForced && !isInvalidatedByForce && !isValidityUnknown) {
                            isInvalidationQueued = true;
                            isStateTaskQueued = true;
                        }
                    }
                    case VIEW_FULL -> {
                        isStateTaskQueued = true;
                        isInvalidationQueued = false;
                        isInvalidatedByForce = false;
                        isValidityUnknown = true;
                    }
                    case VIEW_PERIOD_REFRESH -> {
                        // the firing queues no refresh of a view that is invalid or about to go invalid
                        if (!isInvalidationQueued && !isInvalidatedByForce && !isValidityUnknown) {
                            isStateTaskQueued = true;
                        }
                    }
                    case RUN_REFRESH_JOB -> {
                        if (isStateTaskQueued || stateOpStage == 2) {
                            stateOpStage = 1;
                        }
                        resumeStage = 0;
                        isStateTaskQueued = false;
                        if (isInvalidationQueued) {
                            isInvalidationQueued = false;
                            isInvalidatedByForce = true;
                        }
                    }
                    case VIEW_RESUME -> {
                        if (isInvalidatedByForce) {
                            isInvalidatedByForce = false;
                            stateOpStage = 1;
                            resumeStage = 1;
                        }
                    }
                    case BASE_REPLACE_RESUME_RESTART -> {
                        // the operation drains the queues before its RESUME, and restarts after it
                        isStateTaskQueued = false;
                        isInvalidationQueued = false;
                        isInvalidatedByForce = false;
                        stateOpStage = 1;
                        resumeStage = 1;
                    }
                    case BASE_REPLACE, BASE_REPLACE_FULL_RESTART -> {
                        if (op.kind == MatViewFuzzOpGenerator.Kind.BASE_REPLACE_FULL_RESTART) {
                            // the operation drains the queues before its FULL
                            isStateTaskQueued = false;
                        }
                        isInvalidationQueued = false;
                        isInvalidatedByForce = false;
                        isValidityUnknown = true;
                    }
                    case VIEW_ADD_COLUMN, VIEW_DROP_COLUMN -> {
                        if (!op.isRefused) {
                            purgeStage = 1;
                        }
                    }
                    case PURGE_WAL -> {
                        // the purge deletes txn-log parts only of a log kept in parts
                        if (purgeStage == 1 && partSize > 0) {
                            purgeStage = 2;
                        }
                    }
                    case RESTART -> {
                        if (stateOpStage == 2) {
                            stateOpThenRestart++;
                            stateOpStage = 0;
                        }
                        if (resumeStage == 2) {
                            resumeThenRestart++;
                            resumeStage = 0;
                        }
                        if (purgeStage == 3) {
                            purgeThenRestart++;
                            purgeStage = 0;
                        }
                        // the restart loses the queued tasks
                        isStateTaskQueued = false;
                        isInvalidationQueued = false;
                    }
                    default -> {
                    }
                }
                trackDefinitionPending(generator, op, wasDefinitionPending, isViewColumnAdded, step);
                isViewColumnAdded |= op.kind == MatViewFuzzOpGenerator.Kind.VIEW_ADD_COLUMN && !op.isRefused;
                if (step % CHECK_STEPS == CHECK_STEPS - 1) {
                    // the view check drains the queues, which applies the view's WAL, and leaves the view valid
                    stateOpStage = 0;
                    resumeStage = 0;
                    if (purgeStage == 3) {
                        purgeStage = 2;
                    }
                    isStateTaskQueued = false;
                    isInvalidationQueued = false;
                    isInvalidatedByForce = false;
                    isValidityUnknown = false;
                }
            }
        }
        assertShare("transaction logs in one file", partSizeClasses[0], 1_000);
        assertShare("transaction logs in parts of one transaction", partSizeClasses[1], 1_000);
        assertShare("transaction logs in parts of two to five transactions", partSizeClasses[2], 1_000);
        assertShare("view state event, view operation without view state, RESTART", stateOpThenRestart, STATE_OP_THEN_RESTART_MIN);
        assertShare("RESUME event, view operation without view state, RESTART", resumeThenRestart, RESUME_THEN_RESTART_MIN);
        assertShare("view column change, WAL purge, view operation without view state, RESTART", purgeThenRestart, PURGE_THEN_RESTART_MIN);
    }

    @Test
    public void testBareNameRoundShapes() {
        // The shapes a bare-name round, which MatViewFuzzTest#testBareNameSwitchResume() runs, has to keep drawing:
        // every operation of the round and no other; a switch of the bare count while the view is invalid, which RESUME
        // refuses, plain and with SKIP BACKLOG, until ALTER ... AS writes count(), after which RESUME accepts the view;
        // a switch that a FULL then covers, after which RESUME accepts the invalid view, which still calls count bare
        // and reads the base's column; RESUME refused on the compile; ALTER ... AS to count() and to the qualified
        // column, and one refused for a qualified column the base lacks; and the view's rows mixed by such an ALTER. A
        // raced switch of the bare count, with every racer and with a FULL among its racers, and a raced change under
        // count(), which switches nothing. A deferred view's first build racing the round's first switch, which leaves
        // the view invalid with the switch standing. A second view created while a raced DROP applies, built before the
        // change by a later racer and built after it, and its CREATE refused while a raced ADD applies; ALTER ... AS
        // from count() to the bare count while a raced DROP applies, which switches the view's SQL, and refused while a
        // raced ADD applies.
        final Rnd rnd = new Rnd(42, 42);
        final int[] kindCounts = new int[MatViewFuzzOpGenerator.Kind.values().length];
        int resumeRefusedOnSwitchPlain = 0;
        int resumeRefusedOnSwitchSkip = 0;
        int resumeRefusedOnCompile = 0;
        int alterToCall = 0;
        int alterToQualified = 0;
        int alterRefused = 0;
        int alterMixingReadings = 0;
        int recoveredByAlter = 0;
        int resumedAfterCoveringFull = 0;
        final int[] racerCounts = new int[MatViewFuzzOpGenerator.RACER_COUNT];
        int racedSwitches = 0;
        int racedSwitchesWithFull = 0;
        int racedChangesUnderCall = 0;
        int racedFirstBuilds = 0;
        int racedViewsBuiltBeforeChange = 0;
        int racedViewsBuiltAfterChange = 0;
        int racedViewsRefused = 0;
        int racedAltersFromCallToBare = 0;
        int racedAltersRefused = 0;
        final MatViewFuzzOpGenerator.Op op = new MatViewFuzzOpGenerator.Op();
        for (int round = 0; round < BARE_NAME_ROUNDS; round++) {
            // MatViewFuzzTest#testBareNameSwitchResume() draws a deferred view in the same way
            final boolean isViewDeferred = rnd.nextBoolean();
            final MatViewFuzzOpGenerator generator = MatViewFuzzOpGenerator.forBareNameRound(DAY_COUNT, isViewDeferred);
            Assert.assertEquals(!isViewDeferred, generator.isBareNameViewBuilt());
            for (int day = 0; day < DAY_COUNT; day++) {
                Assert.assertTrue(generator.initRows(rnd, day, op).rowTs.size() > 0);
            }
            // the steps of the recovery with ALTER ... AS: a switch while the view is invalid, RESUME refused on it,
            // ALTER ... AS to count()
            int alterRecoveryStep = 0;
            // a FULL came after a switch, and no switch since
            boolean isSwitchCovered = false;
            for (int step = 0; step < STEPS_PER_ROUND; step++) {
                final boolean wasBuilt = generator.isBareNameViewBuilt();
                final boolean wasInvalid = generator.isBareNameViewInvalid();
                final int sqlBefore = generator.getBareNameSql();
                final boolean wasMixed = generator.isReadingMixedByAlter();
                generator.next(rnd, op);
                kindCounts[op.kind.ordinal()]++;
                Assert.assertTrue(generator.isBareNameViewBuilt());
                if (!wasBuilt) {
                    // The deferred view's first build: the racers deliver the guard's invalidation, which the handler
                    // declines, then build the view below the switch, so the build invalidates the view again.
                    Assert.assertTrue(isViewDeferred);
                    Assert.assertEquals(0, step);
                    Assert.assertEquals(MatViewFuzzOpGenerator.Kind.BASE_RACED_BARE_NAME_COLUMN_CHANGE, op.kind);
                    Assert.assertEquals(MatViewFuzzOpGenerator.BARE_NAME_SQL_BARE, sqlBefore);
                    Assert.assertTrue(op.isColumnAdded);
                    Assert.assertTrue(generator.hasBaseCountColumn());
                    Assert.assertTrue(op.racers.size() > 2 && op.racers.size() < 6);
                    Assert.assertEquals(MatViewFuzzOpGenerator.RACER_RUN_REFRESH_JOB, op.racers.getQuick(0));
                    Assert.assertEquals(MatViewFuzzOpGenerator.RACER_INCREMENTAL, op.racers.getQuick(1));
                    Assert.assertEquals(MatViewFuzzOpGenerator.RACER_RUN_REFRESH_JOB, op.racers.getQuick(2));
                    Assert.assertTrue(generator.isBareNameViewInvalid());
                    Assert.assertTrue(generator.isBareNameSwitchStanding());
                    // the build called count(), as the base had no column count
                    Assert.assertFalse(generator.isRowsReadingColumn());
                    racedFirstBuilds++;
                    continue;
                }
                switch (op.kind) {
                    case BASE_ADD_BARE_NAME_COLUMN, BASE_DROP_BARE_NAME_COLUMN -> {
                        Assert.assertEquals(op.kind == MatViewFuzzOpGenerator.Kind.BASE_ADD_BARE_NAME_COLUMN, generator.hasBaseCountColumn());
                        if (sqlBefore == MatViewFuzzOpGenerator.BARE_NAME_SQL_BARE) {
                            Assert.assertTrue(generator.isBareNameSwitchStanding());
                            Assert.assertTrue(generator.isBareNameViewInvalid());
                            alterRecoveryStep = wasInvalid ? 1 : 0;
                            isSwitchCovered = false;
                        }
                    }
                    case BASE_RACED_BARE_NAME_COLUMN_CHANGE -> {
                        Assert.assertEquals(op.isColumnAdded, generator.hasBaseCountColumn());
                        Assert.assertTrue(op.racers.size() > 0 && op.racers.size() < 4);
                        int createRacerCount = 0;
                        for (int i = 0, n = op.racers.size(); i < n; i++) {
                            racerCounts[op.racers.getQuick(i)]++;
                            if (op.racers.getQuick(i) == MatViewFuzzOpGenerator.RACER_CREATE_VIEW) {
                                createRacerCount++;
                            }
                        }
                        Assert.assertTrue("one CREATE of mv2 at most", createRacerCount < 2);
                        final boolean isAltered = op.racers.contains(MatViewFuzzOpGenerator.RACER_ALTER_TO_BARE);
                        if (isAltered && op.isColumnAdded) {
                            // the explicit-name rule refuses the bare count while the base has no column count
                            racedAltersRefused++;
                        }
                        if (sqlBefore == MatViewFuzzOpGenerator.BARE_NAME_SQL_BARE) {
                            // a FULL that raced the change rebuilt the view without it, so the switch still stands
                            Assert.assertTrue(generator.isBareNameSwitchStanding());
                            Assert.assertTrue(generator.isBareNameViewInvalid());
                            Assert.assertEquals(MatViewFuzzOpGenerator.BARE_NAME_SQL_BARE, generator.getBareNameSql());
                            racedSwitches++;
                            if (op.racers.contains(MatViewFuzzOpGenerator.RACER_FULL)) {
                                racedSwitchesWithFull++;
                            }
                            alterRecoveryStep = wasInvalid ? 1 : 0;
                            isSwitchCovered = false;
                        } else if (isAltered && !op.isColumnAdded) {
                            // the racer's ALTER ... AS installed the bare count before the DROP was visible, and the
                            // guard's check after the change switches it
                            Assert.assertEquals(MatViewFuzzOpGenerator.BARE_NAME_SQL_CALL, sqlBefore);
                            Assert.assertEquals(MatViewFuzzOpGenerator.BARE_NAME_SQL_BARE, generator.getBareNameSql());
                            Assert.assertTrue(generator.isBareNameSwitchStanding());
                            Assert.assertTrue(generator.isBareNameViewInvalid());
                            racedAltersFromCallToBare++;
                            alterRecoveryStep = wasInvalid ? 1 : 0;
                            isSwitchCovered = false;
                        } else {
                            // a SQL that reads the qualified column keeps it
                            Assert.assertEquals(MatViewFuzzOpGenerator.BARE_NAME_SQL_CALL, sqlBefore);
                            Assert.assertEquals(MatViewFuzzOpGenerator.BARE_NAME_SQL_CALL, generator.getBareNameSql());
                            racedChangesUnderCall++;
                        }
                        final int createIndex = op.racers.indexOf(MatViewFuzzOpGenerator.RACER_CREATE_VIEW, 0, op.racers.size());
                        switch (op.racedViewOutcome) {
                            case MatViewFuzzOpGenerator.RACED_VIEW_NONE -> Assert.assertEquals(-1, createIndex);
                            case MatViewFuzzOpGenerator.RACED_VIEW_REFUSED -> {
                                Assert.assertTrue(createIndex > -1 && op.isColumnAdded);
                                racedViewsRefused++;
                            }
                            case MatViewFuzzOpGenerator.RACED_VIEW_BUILT_BEFORE_CHANGE -> {
                                Assert.assertTrue(createIndex > -1 && !op.isColumnAdded);
                                final int buildIndex = Math.max(
                                        op.racers.indexOf(MatViewFuzzOpGenerator.RACER_RUN_REFRESH_JOB, createIndex, op.racers.size()),
                                        op.racers.indexOf(MatViewFuzzOpGenerator.RACER_FULL, createIndex, op.racers.size())
                                );
                                Assert.assertTrue("a racer that runs the refresh job after the CREATE", buildIndex > createIndex);
                                racedViewsBuiltBeforeChange++;
                            }
                            default -> {
                                Assert.assertEquals(MatViewFuzzOpGenerator.RACED_VIEW_BUILT_AFTER_CHANGE, op.racedViewOutcome);
                                Assert.assertTrue(createIndex > -1 && !op.isColumnAdded);
                                Assert.assertEquals(-1, op.racers.indexOf(MatViewFuzzOpGenerator.RACER_RUN_REFRESH_JOB, createIndex, op.racers.size()));
                                Assert.assertEquals(-1, op.racers.indexOf(MatViewFuzzOpGenerator.RACER_FULL, createIndex, op.racers.size()));
                                racedViewsBuiltAfterChange++;
                            }
                        }
                    }
                    case VIEW_RESUME -> {
                        switch (op.resumeOutcome) {
                            case MatViewFuzzOpGenerator.RESUME_REFUSED_SWITCH -> {
                                Assert.assertTrue(wasInvalid && generator.hasBaseCountColumn() && generator.isBareNameSwitchStanding());
                                if (op.isSkipBacklog) {
                                    resumeRefusedOnSwitchSkip++;
                                } else {
                                    resumeRefusedOnSwitchPlain++;
                                }
                                if (alterRecoveryStep == 1) {
                                    alterRecoveryStep = 2;
                                }
                            }
                            case MatViewFuzzOpGenerator.RESUME_REFUSED_COMPILE -> {
                                Assert.assertFalse(generator.hasBaseCountColumn());
                                resumeRefusedOnCompile++;
                            }
                            default -> {
                                Assert.assertFalse(generator.isBareNameViewInvalid());
                                if (wasInvalid && alterRecoveryStep == 3) {
                                    recoveredByAlter++;
                                }
                                if (wasInvalid && isSwitchCovered && sqlBefore == MatViewFuzzOpGenerator.BARE_NAME_SQL_BARE
                                        && generator.hasBaseCountColumn()) {
                                    resumedAfterCoveringFull++;
                                }
                                if (wasInvalid) {
                                    alterRecoveryStep = 0;
                                }
                            }
                        }
                    }
                    case VIEW_ALTER_AS -> {
                        if (op.isRefused) {
                            Assert.assertEquals(MatViewFuzzOpGenerator.BARE_NAME_SQL_QUALIFIED, op.bareNameSql);
                            Assert.assertFalse(generator.hasBaseCountColumn());
                            Assert.assertEquals(sqlBefore, generator.getBareNameSql());
                            alterRefused++;
                        } else {
                            Assert.assertEquals(op.bareNameSql, generator.getBareNameSql());
                            if (op.bareNameSql == MatViewFuzzOpGenerator.BARE_NAME_SQL_CALL) {
                                alterToCall++;
                                if (alterRecoveryStep == 2) {
                                    alterRecoveryStep = 3;
                                }
                            } else {
                                alterToQualified++;
                            }
                            if (!wasMixed && generator.isReadingMixedByAlter()) {
                                alterMixingReadings++;
                            }
                        }
                    }
                    case VIEW_FULL -> {
                        Assert.assertFalse(generator.isBareNameSwitchStanding());
                        Assert.assertFalse(generator.isReadingMixedByAlter());
                        isSwitchCovered |= sqlBefore == MatViewFuzzOpGenerator.BARE_NAME_SQL_BARE && generator.hasBaseCountColumn();
                    }
                    default -> {
                    }
                }
            }
        }
        final int total = BARE_NAME_ROUNDS * STEPS_PER_ROUND;
        int totalWeight = 0;
        for (MatViewFuzzOpGenerator.Kind kind : MatViewFuzzOpGenerator.Kind.values()) {
            totalWeight += MatViewFuzzOpGenerator.bareNameRoundWeight(kind);
        }
        for (MatViewFuzzOpGenerator.Kind kind : MatViewFuzzOpGenerator.Kind.values()) {
            final int weight = MatViewFuzzOpGenerator.bareNameRoundWeight(kind);
            if (weight == 0) {
                Assert.assertEquals(kind + " drawn in a bare-name round", 0, kindCounts[kind.ordinal()]);
            } else {
                final long expected = (long) total * weight / totalWeight;
                Assert.assertTrue(kind + " drawn " + kindCounts[kind.ordinal()] + " times, expected about " + expected, kindCounts[kind.ordinal()] > expected / 4);
            }
        }
        assertShare("plain RESUME refused on a switch", resumeRefusedOnSwitchPlain, 200);
        assertShare("RESUME SKIP BACKLOG refused on a switch", resumeRefusedOnSwitchSkip, 100);
        assertShare("RESUME refused on the compile", resumeRefusedOnCompile, 200);
        assertShare("ALTER ... AS to count()", alterToCall, 200);
        assertShare("ALTER ... AS to the qualified column", alterToQualified, 100);
        assertShare("ALTER ... AS refused for a qualified column the base lacks", alterRefused, 100);
        assertShare("ALTER ... AS that mixes the readings of the view's rows", alterMixingReadings, 100);
        assertShare("a switch while invalid, RESUME refused on it, ALTER ... AS to count(), RESUME accepted", recoveredByAlter, 40);
        assertShare("a switch that a FULL covers, RESUME accepted on the bare count", resumedAfterCoveringFull, 40);
        assertShare("a raced switch of the bare count", racedSwitches, 100);
        assertShare("a raced switch of the bare count with a FULL among its racers", racedSwitchesWithFull, 50);
        assertShare("a raced base column change under count()", racedChangesUnderCall, 50);
        assertShare("a deferred view's first build racing the round's first switch", racedFirstBuilds, 200);
        assertShare("a second view created while a raced DROP applies and built before the change", racedViewsBuiltBeforeChange, RACED_VIEW_SHAPE_MIN);
        assertShare("a second view created while a raced DROP applies and built after the change", racedViewsBuiltAfterChange, RACED_VIEW_SHAPE_MIN);
        assertShare("a second view whose CREATE a raced ADD refuses", racedViewsRefused, RACED_VIEW_SHAPE_MIN);
        assertShare("ALTER ... AS from count() to the bare count while a raced DROP applies", racedAltersFromCallToBare, RACED_VIEW_SHAPE_MIN);
        assertShare("ALTER ... AS to the bare count that a raced ADD refuses", racedAltersRefused, RACED_VIEW_SHAPE_MIN);
        for (int racer = 0; racer < MatViewFuzzOpGenerator.RACER_COUNT; racer++) {
            assertShare("racer " + racer, racerCounts[racer], 100);
        }
    }

    @Test
    public void testPositionalCopyRule() {
        // the rule ALTER MATERIALIZED VIEW ... AS applies to a SQL that copies by position, which the generator predicts
        final ObjList<String> view = new ObjList<>("ts", "sym", "price", "qty");
        Assert.assertFalse(MatViewFuzzOpGenerator.isRefusedByPositionalCopy(view, new ObjList<>("ts", "sym", "price", "qty")));
        // a moved, a renamed, a missing and an extra column
        Assert.assertTrue(MatViewFuzzOpGenerator.isRefusedByPositionalCopy(view, new ObjList<>("ts", "sym", "qty", "price")));
        Assert.assertTrue(MatViewFuzzOpGenerator.isRefusedByPositionalCopy(view, new ObjList<>("ts", "sym", "price", "cnt")));
        Assert.assertTrue(MatViewFuzzOpGenerator.isRefusedByPositionalCopy(view, new ObjList<>("ts", "sym", "price")));
        Assert.assertTrue(MatViewFuzzOpGenerator.isRefusedByPositionalCopy(view, new ObjList<>("ts", "sym", "price", "qty", "cnt")));
    }

    @Test
    public void testSamePositionRule() {
        // the rule ALTER MATERIALIZED VIEW ... AS applies, which the generator predicts
        final ObjList<String> view = new ObjList<>("ts", "sym", "price", "qty");
        Assert.assertFalse(MatViewFuzzOpGenerator.isRefusedBySamePositionRule(view, new ObjList<>("ts", "sym", "price", "qty")));
        // reordered, missing and extra columns
        Assert.assertFalse(MatViewFuzzOpGenerator.isRefusedBySamePositionRule(view, new ObjList<>("sym", "qty", "ts", "price")));
        Assert.assertFalse(MatViewFuzzOpGenerator.isRefusedBySamePositionRule(view, new ObjList<>("ts", "sym", "price")));
        Assert.assertFalse(MatViewFuzzOpGenerator.isRefusedBySamePositionRule(view, new ObjList<>("ts", "sym", "price", "qty", "cnt")));
        // a view column the SQL stops producing at the position of a SQL column the view lacks
        Assert.assertTrue(MatViewFuzzOpGenerator.isRefusedBySamePositionRule(view, new ObjList<>("ts", "sym", "price", "cnt")));
        Assert.assertTrue(MatViewFuzzOpGenerator.isRefusedBySamePositionRule(view, new ObjList<>("ts", "sym", "hi", "qty")));
        // ts is still produced, so the view's first position takes any SQL column
        Assert.assertFalse(MatViewFuzzOpGenerator.isRefusedBySamePositionRule(view, new ObjList<>("cnt", "ts", "sym", "price")));
        // the SQL stops producing qty, but the view has the column at that position
        Assert.assertFalse(MatViewFuzzOpGenerator.isRefusedBySamePositionRule(view, new ObjList<>("ts", "sym", "qty", "cnt")));
    }

    // Asserts that every kind a view of the round's kind has is drawn at least a quarter of the share its weight asks
    // for, and that a kind it does not have is never drawn.
    private static void assertKindShares(int[] kindCounts, boolean isPeriodView) {
        final int total = ROUNDS * STEPS_PER_ROUND;
        int totalWeight = 0;
        for (MatViewFuzzOpGenerator.Kind kind : MatViewFuzzOpGenerator.Kind.values()) {
            totalWeight += MatViewFuzzOpGenerator.weight(kind, isPeriodView);
        }
        for (MatViewFuzzOpGenerator.Kind kind : MatViewFuzzOpGenerator.Kind.values()) {
            final int weight = MatViewFuzzOpGenerator.weight(kind, isPeriodView);
            if (weight == 0) {
                Assert.assertEquals(kind + " drawn for a view that does not have it", 0, kindCounts[kind.ordinal()]);
                continue;
            }
            if (kind == MatViewFuzzOpGenerator.Kind.BASE_ATTACH_PARTITION || kind == MatViewFuzzOpGenerator.Kind.BASE_TRUNCATE_ATTACH_UPDATE) {
                // an ATTACH and a reload need a partition that a DETACH of the round left detached, and fall back to
                // INSERT without one; the callers pin their own shares
                continue;
            }
            // at least a quarter of the share the kind's weight asks for: a kind whose parameters the state cannot give,
            // such as a DETACH with no partition before the newest, falls back to INSERT, which must not starve it
            final long expected = (long) total * weight / totalWeight;
            Assert.assertTrue(kind + " drawn " + kindCounts[kind.ordinal()] + " times, expected about " + expected, kindCounts[kind.ordinal()] > expected / 4);
        }
    }

    private static void assertShare(String shape, int count, int min) {
        Assert.assertTrue(shape + " drawn " + count + " times, expected at least " + min, count >= min);
    }

    // A view operation that writes a transaction without view state to the view's WAL.
    private static boolean isViewOperationWithoutState(MatViewFuzzOpGenerator.Op op) {
        return switch (op.kind) {
            case VIEW_DROP_PARTITION_LIST, VIEW_DROP_PARTITION_WHERE, VIEW_DROP_EXPIRE -> true;
            case VIEW_SET_EXPIRE -> !op.isRefused;
            default -> false;
        };
    }

    // Whether MatViewFuzzTest applies the view's WAL while it runs the operation, see runOperationsFuzzOp(): the
    // operations that wait for the view's WAL or drain the queues, a view DROP PARTITION once the round added a view
    // column, a view ADD COLUMN or ALTER ... AS that waits for the definition event of an earlier ALTER ... AS, accepted
    // or refused, and a RESTART while such an event waits, which the fuzz applies first. A view DROP COLUMN and a SET
    // EXPIRE whose policy has a read probe apply it while a policy or column change waits in the view's WAL, which this
    // takes as always.
    private static boolean isViewWalApplied(MatViewFuzzOpGenerator.Op op, boolean wasDefinitionPending, boolean isViewColumnAdded) {
        return switch (op.kind) {
            case APPLY_WAL, BASE_DETACH_PARTITION, BASE_TRUNCATE_ATTACH_UPDATE, BASE_REPLACE_FULL_RESTART,
                 BASE_REPLACE_RESUME_RESTART, VIEW_DROP_COLUMN, VIEW_RANGE, VIEW_FULL, VIEW_RESUME -> true;
            case VIEW_DROP_PARTITION_LIST, VIEW_DROP_PARTITION_WHERE -> isViewColumnAdded;
            case VIEW_SET_EXPIRE -> op.isProbed;
            case VIEW_ADD_COLUMN, VIEW_ALTER_AS, RESTART -> wasDefinitionPending;
            // an invalidation that publishes the marker first applies the WAL before
            case BASE_UPDATE, BASE_ATTACH_PARTITION -> op.invalidationShape != MatViewFuzzOpGenerator.INVALIDATE_NONE;
            case VIEW_INVALIDATE -> op.invalidationShape != MatViewFuzzOpGenerator.INVALIDATE_TASK;
            default -> false;
        };
    }

    // Follows in the generator what MatViewFuzzTest tells it of the definition events that wait in the view's WAL: an
    // accepted ALTER ... AS issues one, and the fuzz applies the view's WAL in the operations of isViewWalApplied(),
    // before the statement's own change, and at the view check after every sixth operation.
    private static void trackDefinitionPending(
            MatViewFuzzOpGenerator generator,
            MatViewFuzzOpGenerator.Op op,
            boolean wasDefinitionPending,
            boolean isViewColumnAdded,
            int step
    ) {
        if (isViewWalApplied(op, wasDefinitionPending, isViewColumnAdded)) {
            generator.onViewWalApplied();
        }
        if (op.kind == MatViewFuzzOpGenerator.Kind.VIEW_ALTER_AS && !op.isRefused) {
            generator.onDefinitionChangeIssued();
        }
        if (step % CHECK_STEPS == CHECK_STEPS - 1) {
            generator.onViewWalApplied();
        }
    }
}
