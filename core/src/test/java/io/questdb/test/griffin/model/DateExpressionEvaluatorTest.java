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

package io.questdb.test.griffin.model;

import io.questdb.griffin.model.DateExpressionEvaluator;
import org.junit.Assert;
import org.junit.Test;

public class DateExpressionEvaluatorTest {

    @Test
    public void testContainsDateVariable() {
        // The interval scan, the Java IN filter and the JIT filter serializer all decide with this
        // check whether an interval expression resolves against the current time.
        assertContainsDateVariable(true, "$now");
        assertContainsDateVariable(true, "$today");
        assertContainsDateVariable(true, "$yesterday");
        assertContainsDateVariable(true, "$tomorrow");
        assertContainsDateVariable(true, "$TODAY");
        assertContainsDateVariable(true, "$Now");
        assertContainsDateVariable(true, "$now - 1h..$now");
        assertContainsDateVariable(true, "$today + 3bd");
        assertContainsDateVariable(true, "[$yesterday, 2024-01-02]");
        assertContainsDateVariable(true, "2024-01-02..$today");
        assertContainsDateVariable(true, "$today;1h");
        assertContainsDateVariable(true, "$today@Europe/London");
        assertContainsDateVariable(true, "$today#workday");
        // 'T' and a digit start a time override, not a longer name.
        assertContainsDateVariable(true, "$todayT10:00");

        assertContainsDateVariable(false, "");
        assertContainsDateVariable(false, "$");
        assertContainsDateVariable(false, "a$");
        assertContainsDateVariable(false, "$1");
        assertContainsDateVariable(false, "$no");
        assertContainsDateVariable(false, "$nowadays");
        assertContainsDateVariable(false, "$todays");
        assertContainsDateVariable(false, "$todayTx");
        assertContainsDateVariable(false, "$tomorrows");
        assertContainsDateVariable(false, "$yesterdays");
        assertContainsDateVariable(false, "today");
        assertContainsDateVariable(false, "2024-01-02");
        assertContainsDateVariable(false, "2024-01-02T10:00;1h;1d;3");
        assertContainsDateVariable(false, "[2024-01-02,2024-01-04]");
    }

    @Test
    public void testContainsDateVariableReadsOnlyTheGivenRange() {
        final String seq = "'$today'";
        Assert.assertTrue(DateExpressionEvaluator.containsDateVariable(seq, 1, 7));
        Assert.assertFalse(DateExpressionEvaluator.containsDateVariable(seq, 2, 7));
        Assert.assertFalse(DateExpressionEvaluator.containsDateVariable(seq, 1, 5));
        // The range ends right after the name, so the trailing letter doesn't count.
        Assert.assertTrue(DateExpressionEvaluator.containsDateVariable("$todays", 0, 6));
    }

    private static void assertContainsDateVariable(boolean expected, String seq) {
        Assert.assertEquals(seq, expected, DateExpressionEvaluator.containsDateVariable(seq, 0, seq.length()));
    }
}
