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

import io.questdb.cairo.CairoException;
import io.questdb.griffin.engine.LimitOverflowException;
import org.junit.Assert;
import org.junit.Test;

public class LimitOverflowExceptionTest {

    @Test
    public void testInstanceClearsFlagsFromPriorUse() {
        LimitOverflowException first = LimitOverflowException.instance();
        first.put("limit of 1 memory exceeded in FastMap");
        first.setOutOfMemory(true);
        Assert.assertTrue(first.isOutOfMemory());

        LimitOverflowException second = LimitOverflowException.instance();
        second.put("limit of 2 memory exceeded in FastMap");
        Assert.assertFalse("instance() must not inherit flags from the previous use", second.isOutOfMemory());
    }

    @Test
    public void testInstanceClearsMessageAndErrno() {
        LimitOverflowException first = LimitOverflowException.instance();
        first.put("limit of 1 memory exceeded in FastMap");
        Assert.assertEquals("limit of 1 memory exceeded in FastMap", first.getFlyweightMessage().toString());

        LimitOverflowException second = LimitOverflowException.instance();
        second.put("limit of 2 memory exceeded in FastMap");
        Assert.assertEquals("limit of 2 memory exceeded in FastMap", second.getFlyweightMessage().toString());
        Assert.assertEquals(CairoException.NON_CRITICAL, second.getErrno());
    }

    @Test
    public void testInstanceClearsPositionFromPriorUse() {
        // The flyweight is recycled per carrier, and callers mutate a caught CairoException in
        // place: SqlCompilerImpl stamps the statement position onto it on the CREATE ... AS SELECT
        // paths, which wrap the cursor copy and so can catch this exception directly, and on the
        // ALTER paths. A budget overflow under such a statement therefore leaves a non-zero
        // position behind, which every later limit overflow on the same carrier would report,
        // underlining the wrong token for PG and HTTP clients. Worse, compileAlterTable() and
        // compileAlterMatView() only stamp when the position still reads 0, so the stale value also
        // suppresses the position those two would otherwise set.
        LimitOverflowException first = LimitOverflowException.instance();
        first.put("limit of 1 memory exceeded in FastMap");
        first.position(42);
        Assert.assertEquals(42, first.getPosition());

        LimitOverflowException second = LimitOverflowException.instance();
        second.put("limit of 2 memory exceeded in FastMap");
        Assert.assertEquals("instance() must not inherit messagePosition from the previous use", 0, second.getPosition());
    }
}
