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
 *     <li>a view ADD COLUMN of a column the SQL produces under another type than the SQL's, which name mapping casts,
 *     and of a column no SQL produces;</li>
 *     <li>an ATTACH of a partition the same round detached, and a row into a day whose base partition the round
 *     dropped, which recreates it;</li>
 *     <li>a reload of the base: TRUNCATE, the ATTACH of a partition the round detached and an UPDATE of the attached
 *     day, applied in one WAL apply run whose range task the refresh job takes before the apply job moves the base's
 *     SeqTxnTracker; in a period view's round, of a day whose period ended;</li>
 *     <li>RESUME with and without SKIP BACKLOG;</li>
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
    // each shape of an ALTER ... AS that copies by position, and the column changes it refuses
    private static final int POSITIONAL_ALTER_MIN = 20;
    // the rounds of the bare-name round's shapes, which share their draws with the raced base column change
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
            for (int step = 0; step < STEPS_PER_ROUND; step++) {
                final ObjList<String> sqlBefore = new ObjList<>();
                sqlBefore.addAll(generator.getSqlColumns());
                final boolean wasSqlPositional = generator.isSqlPositional();
                generator.next(rnd, op);
                kindCounts[op.kind.ordinal()]++;
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
                // the view's columns always start with ts and sym, which no statement drops
                Assert.assertEquals(MatViewFuzzOpGenerator.TS, generator.getViewColumns().getQuick(0));
                Assert.assertEquals(MatViewFuzzOpGenerator.SYM, generator.getViewColumns().getQuick(1));
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
    public void testBareNameRoundShapes() {
        // The shapes a bare-name round, which MatViewFuzzTest#testBareNameSwitchResume() runs, has to keep drawing:
        // every operation of the round and no other; a switch of the bare count while the view is invalid, which RESUME
        // refuses, plain and with SKIP BACKLOG, until ALTER ... AS writes count(), after which RESUME accepts the view;
        // a switch that a FULL then covers, after which RESUME accepts the invalid view, which still calls count bare
        // and reads the base's column; RESUME refused on the compile; ALTER ... AS to count() and to the qualified
        // column, and one refused for a qualified column the base lacks; and the view's rows mixed by such an ALTER. A
        // raced switch of the bare count, with every racer and with a FULL among its racers, and a raced change under
        // count(), which switches nothing.
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
        final MatViewFuzzOpGenerator.Op op = new MatViewFuzzOpGenerator.Op();
        for (int round = 0; round < BARE_NAME_ROUNDS; round++) {
            final MatViewFuzzOpGenerator generator = MatViewFuzzOpGenerator.forBareNameRound(DAY_COUNT);
            for (int day = 0; day < DAY_COUNT; day++) {
                Assert.assertTrue(generator.initRows(rnd, day, op).rowTs.size() > 0);
            }
            // the steps of the recovery with ALTER ... AS: a switch while the view is invalid, RESUME refused on it,
            // ALTER ... AS to count()
            int alterRecoveryStep = 0;
            // a FULL came after a switch, and no switch since
            boolean isSwitchCovered = false;
            for (int step = 0; step < STEPS_PER_ROUND; step++) {
                final boolean wasInvalid = generator.isBareNameViewInvalid();
                final int sqlBefore = generator.getBareNameSql();
                final boolean wasMixed = generator.isReadingMixedByAlter();
                generator.next(rnd, op);
                kindCounts[op.kind.ordinal()]++;
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
                        for (int i = 0, n = op.racers.size(); i < n; i++) {
                            racerCounts[op.racers.getQuick(i)]++;
                        }
                        if (sqlBefore == MatViewFuzzOpGenerator.BARE_NAME_SQL_BARE) {
                            // a FULL that raced the change rebuilt the view without it, so the switch still stands
                            Assert.assertTrue(generator.isBareNameSwitchStanding());
                            Assert.assertTrue(generator.isBareNameViewInvalid());
                            racedSwitches++;
                            if (op.racers.contains(MatViewFuzzOpGenerator.RACER_FULL)) {
                                racedSwitchesWithFull++;
                            }
                            alterRecoveryStep = wasInvalid ? 1 : 0;
                            isSwitchCovered = false;
                        } else {
                            // a SQL that reads the qualified column keeps it
                            Assert.assertEquals(MatViewFuzzOpGenerator.BARE_NAME_SQL_CALL, sqlBefore);
                            racedChangesUnderCall++;
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
        assertShare("plain RESUME refused on a switch", resumeRefusedOnSwitchPlain, 100);
        assertShare("RESUME SKIP BACKLOG refused on a switch", resumeRefusedOnSwitchSkip, 50);
        assertShare("RESUME refused on the compile", resumeRefusedOnCompile, 100);
        assertShare("ALTER ... AS to count()", alterToCall, 100);
        assertShare("ALTER ... AS to the qualified column", alterToQualified, 50);
        assertShare("ALTER ... AS refused for a qualified column the base lacks", alterRefused, 50);
        assertShare("ALTER ... AS that mixes the readings of the view's rows", alterMixingReadings, 50);
        assertShare("a switch while invalid, RESUME refused on it, ALTER ... AS to count(), RESUME accepted", recoveredByAlter, 20);
        assertShare("a switch that a FULL covers, RESUME accepted on the bare count", resumedAfterCoveringFull, 20);
        assertShare("a raced switch of the bare count", racedSwitches, 100);
        assertShare("a raced switch of the bare count with a FULL among its racers", racedSwitchesWithFull, 50);
        assertShare("a raced base column change under count()", racedChangesUnderCall, 50);
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
}
