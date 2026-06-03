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

package io.questdb.test.griffin.engine.functions.activity;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that the {@code memory_limit} column of {@code query_activity}
 * surfaces a configured per-query limit. The limit is applied per test in
 * {@link #setUp()} via {@code setProperty} so it survives the per-test override
 * reset; the provider reads it live on each tracker acquisition. The unlimited
 * counterpart (memory_limit NULL) lives in
 * {@link QueryActivityFunctionFactoryTest}, which runs with the default
 * unlimited config.
 */
public class QueryActivityMemoryTrackerTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        // 128 MiB: comfortably above anything query_activity itself allocates, so
        // the self-query never breaches. The point is to prove memory_limit
        // reports a configured (non-zero) cap, not to trigger a breach.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 128 * 1024 * 1024L);
    }

    @Test
    public void testMemoryLimitColumnReflectsConfiguredLimit() throws Exception {
        // query_activity reports the running query against itself; with a limit
        // configured, memory_limit shows the cap and memory_used is non-negative.
        assertQuery("select memory_used >= 0 used_ok, memory_limit from query_activity()")
                .noRandomAccess()
                .returns("used_ok\tmemory_limit\n" +
                        "true\t134217728\n");
    }
}
