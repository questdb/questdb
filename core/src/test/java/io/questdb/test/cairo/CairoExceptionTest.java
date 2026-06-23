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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class CairoExceptionTest extends AbstractTest {

    // clear(errno) must fully reset CairoException state. The base instance() now allocates a fresh
    // object every call, but clear() is shared with subclasses that recycle a pooled flyweight (e.g.
    // LineProtocolException via ThreadLocal), so a write-once cancellation/preferences-out-of-date
    // flag must not survive a reset and leak onto the next exception built on the same flyweight.
    // These guards set a flag, invoke clear() directly, and assert the flag is back to false.
    @Test
    public void testClearResetsStickyCancellationFlag() throws Exception {
        CairoException ex = CairoException.queryCancelled();
        Assert.assertTrue(ex.isCancellation());
        Assert.assertTrue(ex.isInterruption());
        invokeClear(ex);
        Assert.assertFalse("clear() must reset the sticky cancellation flag", ex.isCancellation());
        Assert.assertFalse(ex.isInterruption());
    }

    @Test
    public void testClearResetsStickyPreferencesOutOfDateFlag() throws Exception {
        CairoException ex = CairoException.preferencesOutOfDate(1, 2);
        Assert.assertTrue(ex.isPreferencesOutOfDateError());
        invokeClear(ex);
        Assert.assertFalse("clear() must reset the sticky preferences-out-of-date flag", ex.isPreferencesOutOfDateError());
    }

    @Test
    public void testMatViewDoesNotExistIsNotCritical() {
        Assert.assertFalse(CairoException.matViewDoesNotExist("foo").isCritical());
    }

    @Test
    public void testTableDoesNotExistIsNotCritical() {
        Assert.assertFalse(CairoException.tableDoesNotExist("foo").isCritical());
    }

    @Test
    public void testTableDroppedIsNotCriticial() {
        Assert.assertFalse(CairoException.tableDropped(new TableToken("x", "x", null, 123, false, false, false)).isCritical());
    }

    private static void invokeClear(CairoException ex) throws Exception {
        Method clear = CairoException.class.getDeclaredMethod("clear", int.class);
        clear.setAccessible(true);
        clear.invoke(ex, CairoException.NON_CRITICAL);
    }
}
