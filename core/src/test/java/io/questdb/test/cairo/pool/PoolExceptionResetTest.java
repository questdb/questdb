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

package io.questdb.test.cairo.pool;

import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.pool.ex.PoolClosedException;
import org.junit.Assert;
import org.junit.Test;

public class PoolExceptionResetTest {

    @Test
    public void testEntryLockedExceptionClearsFlagsFromPriorUse() {
        EntryLockedException first = EntryLockedException.instance("first");
        first.setOutOfMemory(true);
        Assert.assertTrue(first.isOutOfMemory());

        EntryLockedException second = EntryLockedException.instance("second");
        Assert.assertFalse("instance() must not inherit flags from the previous use", second.isOutOfMemory());
    }

    @Test
    public void testEntryLockedExceptionClearsPositionFromPriorUse() {
        // Same recycled-flyweight defect as LimitOverflowException, reached the same way:
        // SqlCompilerImpl stamps the statement position onto a caught CairoException in place on
        // the CREATE TABLE AS SELECT path, and without a full reset that position reappears on
        // every later table-busy error raised on the same carrier.
        EntryLockedException first = EntryLockedException.instance("first");
        first.position(42);
        Assert.assertEquals(42, first.getPosition());

        EntryLockedException second = EntryLockedException.instance("second");
        Assert.assertEquals("instance() must not inherit messagePosition from the previous use", 0, second.getPosition());
    }

    @Test
    public void testEntryLockedExceptionInstanceCarriesReason() {
        EntryLockedException ex = EntryLockedException.instance("because");
        Assert.assertEquals("table busy [reason=because]", ex.getFlyweightMessage().toString());
    }

    @Test
    public void testPoolClosedExceptionClearsPositionFromPriorUse() {
        // This used to be a single public static final INSTANCE with no reset hook at all, so a
        // position stamped onto it by one query stuck for the life of the process and on every
        // thread at once - strictly worse than the per-carrier flyweights above.
        PoolClosedException first = PoolClosedException.instance();
        first.position(42);
        Assert.assertEquals(42, first.getPosition());

        PoolClosedException second = PoolClosedException.instance();
        Assert.assertEquals("instance() must not inherit messagePosition from the previous use", 0, second.getPosition());
    }
}
