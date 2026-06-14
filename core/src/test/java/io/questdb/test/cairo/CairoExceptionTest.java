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

    // CairoException is a reused thread-local flyweight: instance() calls clear(errno) to recycle it.
    // cancellation and preferencesOutOfDateError are only ever set true, so clear() MUST reset them;
    // otherwise a stale flag leaks onto an unrelated exception built later on the same worker thread
    // (e.g. a corrupt-parquet decode error inheriting cancellation=true from an earlier CANCEL QUERY).
    // These guards invoke clear() directly so they hold even under -ea, where instance() allocates fresh.
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
