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

package io.questdb.test.griffin.engine;

import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.LimitBounds;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class LimitBoundsTest {
    private static final long NULL = Numbers.LONG_NULL;

    @Test
    public void testIsWithoutOffset() {
        Assert.assertTrue(LimitBounds.isWithoutOffset(5, 0));
        Assert.assertTrue(LimitBounds.isWithoutOffset(0, 5));
        Assert.assertTrue(LimitBounds.isWithoutOffset(-3, 0));
        Assert.assertTrue(LimitBounds.isWithoutOffset(0, 0));
        Assert.assertFalse(LimitBounds.isWithoutOffset(2, 5));
        Assert.assertFalse(LimitBounds.isWithoutOffset(-5, -2));
        Assert.assertFalse(LimitBounds.isWithoutOffset(2, -1));
        Assert.assertFalse(LimitBounds.isWithoutOffset(0, -3));
        Assert.assertFalse(LimitBounds.isWithoutOffset(NULL, 0));
        Assert.assertFalse(LimitBounds.isWithoutOffset(0, NULL));
        Assert.assertFalse(LimitBounds.isWithoutOffset(-3, NULL));
        Assert.assertFalse(LimitBounds.isWithoutOffset(NULL, NULL));
        Assert.assertTrue(LimitBounds.isWithoutOffset(Long.MIN_VALUE + 1, 0));
        Assert.assertTrue(LimitBounds.isWithoutOffset(Long.MAX_VALUE, 0));
    }

    @Test
    public void testLastRowCount() throws SqlException {
        assertLastRowCount(5, -5, 0, false);
        assertLastRowCount(-1, 5, 0, false);
        assertLastRowCount(-1, 0, 0, false);
        assertLastRowCount(-1, NULL, 0, false);
        assertLastRowCount(Long.MAX_VALUE, Long.MIN_VALUE + 1, 0, false);
        assertLastRowCount(5, -5, -2, true);
        assertLastRowCount(5, -2, -5, true);
        assertLastRowCount(3, -3, 0, true);
        assertLastRowCount(-1, 2, -1, true);
        assertLastRowCount(-1, NULL, -3, true);
        assertLastRowCount(-1, -3, NULL, true);
        assertLastRowCount(-1, NULL, NULL, true);
    }

    @Test
    public void testLoOf() {
        Assert.assertEquals(0, LimitBounds.loOf(NULL, 5));
        Assert.assertEquals(0, LimitBounds.loOf(NULL, -5));
        Assert.assertEquals(0, LimitBounds.loOf(NULL, 0));
        Assert.assertEquals(NULL, LimitBounds.loOf(NULL, NULL));
        Assert.assertEquals(3, LimitBounds.loOf(3, NULL));
        Assert.assertEquals(-1, LimitBounds.loOf(-1, 5));
    }

    private static void assertLastRowCount(long expected, long lo, long hi, boolean hasHi) throws SqlException {
        final LimitBounds.Values values = new LimitBounds.Values();
        values.of(lo, hi, hasHi, 0);
        Assert.assertEquals("LIMIT " + lo + (hasHi ? ", " + hi : ""), expected, values.getLastRowCount());
    }
}
