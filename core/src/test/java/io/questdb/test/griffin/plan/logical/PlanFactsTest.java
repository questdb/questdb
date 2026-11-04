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

package io.questdb.test.griffin.plan.logical;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.plan.logical.BuildSide;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.PlanFacts;
import io.questdb.griffin.plan.logical.PlanNodePools;
import io.questdb.griffin.plan.logical.PlanStage;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

public class PlanFactsTest extends AbstractCairoTest {
    private static final String SQL = "SELECT x FROM t WHERE x > 1 ORDER BY x LIMIT 3";

    @Test
    public void testClearedPoolObjectKeepsFact() {
        final PlanNodePools pools = new PlanNodePools(configuration);
        pools.scans.next().of(tableToken(), 2, 0).setHints(ScanPlan.HINT_NO_INDEX);
        pools.clear();
        pools.scans.peekQuick(0).setHints(ScanPlan.HINT_NO_INDEX);
        final AssertionError error = Assert.assertThrows(AssertionError.class, pools::clear);
        TestUtils.assertContains(error.getMessage(), PlanFacts.CLEARED_FACT + ": ScanPlan.hints");
    }

    @Test
    public void testRewriteAfterRewrittenStage() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignored = compiler.compile(SQL, sqlExecutionContext).getRecordCursorFactory()
            ) {
                final LogicalPlan plan = compiler.getPlanForTesting();
                final SortPlan sort = find(plan, SortPlan.class);
                final ScanPlan scan = find(plan, ScanPlan.class);
                assertLateRewrite(() -> sort.replaceInput(0, sort.getChild()), "UnaryPlan.replaceInput");
                assertLateRewrite(() -> scan.setLimit(null, null), "ScanPlan.setLimit");
                sort.setStage(PlanStage.REWRITTEN);
                assertLateRewrite(sort::deriveOutput, "ForwardingPlan.deriveOutput");
                sort.setStage(PlanStage.DECORRELATED);
                sort.replaceInput(0, sort.getChild());
            }
        });
    }

    @Test
    public void testUndecidedJoinStepFacts() {
        final JoinInput step = JoinInput.FACTORY.newInstance().of(new ScanPlan().of(tableToken(), 2, 0), JoinKind.INNER, "s", 0);
        assertUndecided(step::getKeyCheck, "JoinInput.keyCheck");
        assertUndecided(step::getNullKeyCheck, "JoinInput.nullKeyCheck");
        assertUndecided(step::getTimeFrameScan, "JoinInput.timeFrameScan");
        assertUndecided(step::getBuildSide, "JoinInput.buildSide");
        assertUndecided(step::getFilterAlgorithm, "JoinInput.filterAlgorithm");
        step.setAlgorithm(JoinInput.Algorithm.HASH);
        Assert.assertNull(step.getKeyCheck());
        Assert.assertNull(step.getFilterAlgorithm());
        Assert.assertNull(step.getTimeFrameScan());
        Assert.assertNull(step.getBuildSide());
        step.setAlgorithm(JoinInput.Algorithm.LIGHT_HASH);
        assertUndecided(step::getBuildSide, "JoinInput.buildSide");
        step.setBuildSide(BuildSide.SMALLER);
        Assert.assertEquals(BuildSide.SMALLER, step.getBuildSide());
        step.clear();
        assertUndecided(step::getBuildSide, "JoinInput.buildSide");
    }

    @Test
    public void testUndecidedPhysicalFacts() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignored = compiler.compile(SQL, sqlExecutionContext).getRecordCursorFactory()
            ) {
                final LogicalPlan plan = compiler.getPlanForTesting();
                final SortPlan sort = find(plan, SortPlan.class);
                final SortPlan.Algorithm algorithm = sort.getAlgorithm();
                Assert.assertNotNull(algorithm);
                sort.setStage(PlanStage.REWRITTEN);
                Assert.assertEquals(algorithm, sort.getAlgorithm());
                sort.setStage(PlanStage.DECORRELATED);
                assertUndecided(sort::getAlgorithm, "SortPlan.algorithm");
                final ScanPlan scan = find(plan, ScanPlan.class);
                Assert.assertEquals(ScanPlan.IndexRead.NONE, scan.getIndexRead());
                scan.clearAccessPath();
                Assert.assertNull(scan.getAccessPath());
                assertUndecided(scan::getIndexRead, "ScanPlan.indexRead");
                assertUndecided(scan::getResidual, "ScanPlan.residual");
                assertUndecided(scan::isTimestampDropped, "ScanPlan.isTimestampDropped");
            }
        });
    }

    @Test
    public void testUndecidedWindowOrder() {
        final WindowSpec spec = WindowSpec.FACTORY.newInstance();
        assertUndecided(spec::isOrderDelivered, "WindowSpec.orderSource");
        spec.setOrderDelivered(true);
        Assert.assertTrue(spec.isOrderDelivered());
        spec.setOrderDelivered(false);
        Assert.assertFalse(spec.isOrderDelivered());
        spec.clear();
        assertUndecided(spec::isOrderDelivered, "WindowSpec.orderSource");
    }

    private static void assertLateRewrite(ThrowingRunnable rewrite, String name) {
        final AssertionError error = Assert.assertThrows(AssertionError.class, rewrite);
        TestUtils.assertContains(error.getMessage(), PlanFacts.LATE_REWRITE + ": " + name);
    }

    private static void assertUndecided(ThrowingRunnable read, String fact) {
        final AssertionError error = Assert.assertThrows(AssertionError.class, read);
        TestUtils.assertContains(error.getMessage(), PlanFacts.UNDECIDED_READ + ": " + fact);
    }

    private static <T extends LogicalPlan> T find(LogicalPlan plan, Class<T> type) {
        if (type.isInstance(plan)) {
            return type.cast(plan);
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final T found = find(plan.inputAt(i), type);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static TableToken tableToken() {
        return new TableToken("trades", "trades~1", null, 1, false, false, false);
    }
}
