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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.IntList;
import org.junit.Assert;
import org.junit.Test;

public class WindowPlanTest {
    @Test
    public void testDeriveOutputFollowsInputAndKeepsResultAttributes() {
        final FunctionSourcePlan source = source();
        final WindowPlan window = window(source);
        final OutputSchema input = source.getOutput();
        final IntList kept = new IntList();
        kept.add(2);
        kept.add(0);
        input.retain(kept);
        input.setTimestampColumnId(3);
        input.setColumnName(1, "renamed", null);
        final OutputSchema scratch = new OutputSchema();
        window.deriveOutput(scratch);
        assertLayout(window.getOutput(), new int[]{3, 1, 4, 5}, "ts", "renamed", "w1", "w2");
        Assert.assertEquals(0, window.getOutput().getTimestampIndex());
        Assert.assertFalse(window.getOutput().isVisible(2));
        Assert.assertFalse(window.getOutput().isVisible(3));
        Assert.assertEquals(0, scratch.getColumnCount());
    }

    @Test
    public void testDeriveOutputKeepsOnlyTheRemainingResults() {
        final FunctionSourcePlan source = source();
        final WindowPlan window = window(source);
        window.getFunctionColumnIds().removeIndex(0);
        window.deriveOutput(new OutputSchema());
        assertLayout(window.getOutput(), new int[]{1, 2, 3, 5}, "a", "b", "ts", "w2");
        Assert.assertEquals(2, window.getOutput().getTimestampIndex());
    }

    @Test
    public void testDeriveOutputLeavesOutTheInputColumnAResultReplaced() {
        final FunctionSourcePlan source = source();
        final WindowPlan window = window(source);
        source.getOutput().add(9, "W1", ColumnType.LONG, true);
        window.deriveOutput(new OutputSchema());
        assertLayout(window.getOutput(), new int[]{1, 2, 3, 4, 5}, "a", "b", "ts", "w1", "w2");
    }

    private static void assertLayout(OutputSchema output, int[] columnIds, String... names) {
        Assert.assertEquals(columnIds.length, output.getColumnCount());
        for (int i = 0; i < columnIds.length; i++) {
            Assert.assertEquals(columnIds[i], output.getColumnId(i));
            Assert.assertEquals(names[i], output.getColumnName(i).toString());
        }
    }

    private static FunctionSourcePlan source() {
        final FunctionSourcePlan source = new FunctionSourcePlan().of(0);
        source.getOutput().add(1, "a", ColumnType.LONG, true);
        source.getOutput().add(2, "b", ColumnType.LONG, true);
        source.getOutput().add(3, "ts", ColumnType.TIMESTAMP, true);
        source.getOutput().setTimestampIndex(2);
        return source;
    }

    private static WindowPlan window(FunctionSourcePlan source) {
        final WindowPlan window = new WindowPlan().of(source, 0);
        window.getOutput().copyFrom(source.getOutput());
        window.getFunctionColumnIds().add(4);
        window.getFunctionColumnIds().add(5);
        window.getOutput().add(4, "w1", ColumnType.LONG, false);
        window.getOutput().add(5, "w2", ColumnType.LONG, false);
        return window;
    }
}
