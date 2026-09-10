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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class CairoConfigurationWrapperTest extends AbstractTest {

    /**
     * {@code getBypassWalFdCache()} carries a default that derives an answer from the descriptor
     * cache size, so a wrapper that does not forward it answers from its own state and drops the
     * delegate's decision without a trace. Enterprise turns the WAL descriptor caches off on
     * replication nodes through exactly this method, and the engine reads its configuration through
     * a wrapper.
     */
    @Test
    public void testBypassWalFdCacheIsForwarded() {
        final CairoConfiguration delegate = new DefaultTestCairoConfiguration(root) {
            @Override
            public boolean getBypassWalFdCache() {
                return true;
            }

            @Override
            public int getWalMaxSegmentFileDescriptorsCache() {
                // Left at a caching value on purpose: the wrapper must report what the delegate
                // decided, not re-derive it from this.
                return 30;
            }
        };

        final CairoConfigurationWrapper wrapper = new CairoConfigurationWrapper(delegate);
        Assert.assertTrue(wrapper.getBypassWalFdCache());
        Assert.assertEquals(30, wrapper.getWalMaxSegmentFileDescriptorsCache());
    }
}
