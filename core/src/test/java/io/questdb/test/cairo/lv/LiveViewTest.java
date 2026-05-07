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

package io.questdb.test.cairo.lv;

import io.questdb.test.AbstractCairoTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Phase 1 placeholder. The prototype's tests survive in
 * {@code LiveViewTest.java.prototype.bak} for reference during the Phase 1 test
 * rewrite (delta plan task #8): the prototype assertions are tightly coupled to
 * the merge-buffer / cold-path / in-mem-only output that Phase 1 has removed.
 * <p>
 * The Phase 1 test surface that needs to land in this file:
 * <ul>
 *     <li>CREATE / DROP lifecycle including registry visibility transitions and
 *     restart recovery from {@code _lv} + {@code _lv.s}.</li>
 *     <li>Asserted-wording validation messages: FLUSH EVERY &lt; 100ms, IN MEMORY
 *     &lt; FLUSH EVERY, IN MEMORY &gt; cap, BACKFILL reject, missing FLUSH EVERY.</li>
 *     <li>Refresh end-to-end on a trivial query (SELECT * FROM base WHERE x &gt; 0,
 *     plus a row_number() / lag() smoke test once Phase 1's WindowFunction
 *     migration lands).</li>
 *     <li>{@code lv_consumed_seqTxn} floor advances and {@code WalPurgeJob} honors
 *     it (pending the WalPurgeJob hook).</li>
 *     <li>{@code live_views()} catalogue surface for the Phase 1 column set.</li>
 * </ul>
 */
public class LiveViewTest extends AbstractCairoTest {

    @Test
    @Ignore("Phase 1 test rewrite pending — see file header for the planned coverage")
    public void placeholder() {
        // intentionally empty — keeps the test class on the discovery path while the
        // Phase 1 test surface is being written.
    }
}
