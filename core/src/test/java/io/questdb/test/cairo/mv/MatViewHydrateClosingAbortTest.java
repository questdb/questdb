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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.NoOpMatViewStateStore;
import io.questdb.std.ObjHashSet;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Isolates the ENTRY-TIME closing abort in {@link CairoEngine#hydrateMatViewStateStore}:
 * a close that already won the flag-publication race must see the loader abort BEFORE its
 * first engine-state read (the {@code getTableTokens} registry walk). The per-token poll
 * inside the loop witnesses the same abort one iteration later, so this test pins the
 * ordering specifically -- a revert of only the entry check stays green under the existing
 * poll witnesses but fails here on the recorded registry walk.
 */
public class MatViewHydrateClosingAbortTest extends AbstractCairoTest {

    @Test
    public void hydrateAbortsBeforeFirstRegistryReadWhenEngineIsClosing() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean closing = new AtomicBoolean();
            final AtomicBoolean registryWalked = new AtomicBoolean();
            try (CairoEngine closingEngine = new CairoEngine(configuration) {
                @Override
                public void getTableTokens(ObjHashSet<TableToken> bucket, boolean includeDropped) {
                    registryWalked.set(true);
                    super.getTableTokens(bucket, includeDropped);
                }

                @Override
                public boolean isClosing() {
                    // Armed only after construction: boot-time registry walks are legitimate,
                    // and close() at the end of the try must run against the real flag.
                    return closing.get() || super.isClosing();
                }
            }) {
                closing.set(true);
                registryWalked.set(false);
                try {
                    closingEngine.hydrateMatViewStateStore(NoOpMatViewStateStore.INSTANCE);
                    Assert.fail("hydrate must abort when the engine is closing");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "engine is closing; mat-view hydration aborted");
                }
                Assert.assertFalse(
                        "the entry-time abort must fire BEFORE the first registry read",
                        registryWalked.get());
                closing.set(false);
            }
        });
    }
}
