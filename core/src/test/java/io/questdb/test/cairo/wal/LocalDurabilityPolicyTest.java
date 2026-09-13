/*******************************************************************************
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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * S5: the LocalDurabilityPolicy seam that lets Enterprise make the adaptive apply-side durable
 * epoch role-aware (skip on a replica). This tests only the seam; the gate behavior is in
 * AdaptiveReplicaEpochSkipTest and the role wiring is in the Enterprise suite.
 */
public class LocalDurabilityPolicyTest extends AbstractCairoTest {

    @Test
    public void testConstantsHaveExpectedPolarity() {
        Assert.assertTrue("ALWAYS_ON must enable local durability",
                LocalDurabilityPolicy.ALWAYS_ON.isLocalDurabilityEnabled());
        Assert.assertFalse("REPLICA_SKIP must disable local durability",
                LocalDurabilityPolicy.REPLICA_SKIP.isLocalDurabilityEnabled());
    }

    @Test
    public void testEngineDefaultIsAlwaysOn() {
        // Fail-safe default: a fresh engine (single-node / OSS) forces local durability.
        Assert.assertSame(LocalDurabilityPolicy.ALWAYS_ON, engine.getLocalDurabilityPolicy());
        Assert.assertTrue(engine.getLocalDurabilityPolicy().isLocalDurabilityEnabled());
    }

    @Test
    public void testEngineSetGetRoundTrips() {
        try {
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            Assert.assertSame(LocalDurabilityPolicy.REPLICA_SKIP, engine.getLocalDurabilityPolicy());
            Assert.assertFalse(engine.getLocalDurabilityPolicy().isLocalDurabilityEnabled());
        } finally {
            // engine is a static shared across the suite — restore the fail-safe default so this
            // test cannot leak REPLICA_SKIP into a sibling test.
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
        }
    }
}
