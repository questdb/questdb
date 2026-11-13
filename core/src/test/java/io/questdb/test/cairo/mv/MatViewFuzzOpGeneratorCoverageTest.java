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
 *     <li>a view ADD COLUMN of a column the SQL produces under another type than the SQL's, which name mapping casts,
 *     and of a column no SQL produces;</li>
 *     <li>an ATTACH of a partition the same round detached, and a row into a day whose base partition the round
 *     dropped, which recreates it;</li>
 *     <li>RESUME with and without SKIP BACKLOG;</li>
 *     <li>a base replacement that leaves out the first day, so the view keeps the old base's rows there;</li>
 *     <li>a SET EXPIRE of each policy shape, one that reads two columns, and one over a column the view lacks, which
 *     the statement refuses;</li>
 *     <li>a view DROP COLUMN of a column the EXPIRE ROWS policy reads, which the statement refuses, and one of a
 *     column the policy does not read while the view has a policy.</li>
 * </ul>
 * The generator draws without an engine; the seed is fixed, so a failure reproduces.
 */
public class MatViewFuzzOpGeneratorCoverageTest {
    private static final int DAY_COUNT = 6;
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
        int insertIntoDropped = 0;
        int resumeSkip = 0;
        int resumePlain = 0;
        int replaceLeavesFirstDay = 0;
        int bulkInsert = 0;
        int setExpireRefused = 0;
        int setExpireTwoColumns = 0;
        int dropColumnRefused = 0;
        int dropColumnBesidePolicy = 0;
        final int[] policyShapes = new int[5];
        final MatViewFuzzOpGenerator.Op op = new MatViewFuzzOpGenerator.Op();
        for (int round = 0; round < ROUNDS; round++) {
            final MatViewFuzzOpGenerator generator = new MatViewFuzzOpGenerator(DAY_COUNT);
            generator.initView(rnd);
            final boolean[] isDetached = new boolean[DAY_COUNT];
            final boolean[] isDropped = new boolean[DAY_COUNT];
            for (int step = 0; step < STEPS_PER_ROUND; step++) {
                final ObjList<String> sqlBefore = new ObjList<>();
                sqlBefore.addAll(generator.getSqlColumns());
                generator.next(rnd, op);
                kindCounts[op.kind.ordinal()]++;
                switch (op.kind) {
                    case VIEW_ALTER_AS -> {
                        if (op.isRefused) {
                            alterRefused++;
                            Assert.assertEquals("a refused ALTER ... AS must keep the SQL", sqlBefore, generator.getSqlColumns());
                        } else {
                            alterAccepted++;
                            Assert.assertEquals(op.sqlColumns, generator.getSqlColumns());
                        }
                        if (!op.sqlColumns.getQuick(0).equals(MatViewFuzzOpGenerator.TS) || !op.sqlColumns.getQuick(1).equals(MatViewFuzzOpGenerator.SYM)) {
                            alterReordered++;
                        }
                    }
                    case VIEW_ADD_COLUMN -> {
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
                            dropColumnRefused++;
                            Assert.assertTrue("a refused DROP COLUMN keeps the column", generator.getViewColumns().indexOf(op.column) > -1);
                        } else {
                            Assert.assertTrue(generator.getViewColumns().indexOf(op.column) < 0);
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

        final int total = ROUNDS * STEPS_PER_ROUND;
        int totalWeight = 0;
        for (MatViewFuzzOpGenerator.Kind kind : MatViewFuzzOpGenerator.Kind.values()) {
            totalWeight += MatViewFuzzOpGenerator.weight(kind);
        }
        for (MatViewFuzzOpGenerator.Kind kind : MatViewFuzzOpGenerator.Kind.values()) {
            if (kind == MatViewFuzzOpGenerator.Kind.BASE_ATTACH_PARTITION) {
                // an ATTACH needs a partition that a DETACH of the round left detached, and falls back to INSERT
                // without one
                continue;
            }
            // at least a quarter of the share the kind's weight asks for: a kind whose parameters the state cannot give,
            // such as a DETACH with no partition before the newest, falls back to INSERT, which must not starve it
            final long expected = (long) total * MatViewFuzzOpGenerator.weight(kind) / totalWeight;
            Assert.assertTrue(kind + " drawn " + kindCounts[kind.ordinal()] + " times, expected about " + expected, kindCounts[kind.ordinal()] > expected / 4);
        }
        final int detachCount = kindCounts[MatViewFuzzOpGenerator.Kind.BASE_DETACH_PARTITION.ordinal()];
        assertShare("ATTACH of a partition the round detached, of " + detachCount + " DETACH", attachAfterDetach, detachCount / 4);
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

    private static void assertShare(String shape, int count, int min) {
        Assert.assertTrue(shape + " drawn " + count + " times, expected at least " + min, count >= min);
    }
}
