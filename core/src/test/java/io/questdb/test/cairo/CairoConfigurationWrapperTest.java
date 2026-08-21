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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link CairoConfigurationWrapper} must forward every getter to its delegate. Any getter it
 * fails to override silently resolves to the {@link CairoConfiguration} interface default, which
 * makes the wrapper report a value the delegate never held. That is invisible in production but
 * fatal in tests, because {@link CairoTestConfiguration} extends the wrapper and is the only route
 * to the real {@code PropServerConfiguration}: a suite that sets such a property runs against the
 * default instead and quietly exercises the wrong configuration.
 */
public class CairoConfigurationWrapperTest extends AbstractTest {
    private static final double DELTA = 0.000_001;

    @Test
    public void testForwardsDerivedDefaultGetters() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoConfiguration delegate = newDelegate();
            CairoConfigurationWrapper wrapper = new CairoConfigurationWrapper(delegate);

            // Both of these have interface defaults derived from other getters the wrapper does
            // forward, so an un-overridden wrapper computes a plausible but wrong answer instead
            // of asking the delegate.
            Assert.assertTrue(delegate.getBypassWalFdCache());
            Assert.assertTrue(wrapper.getBypassWalFdCache());

            Assert.assertFalse(delegate.isLiveViewRefreshEnabled());
            Assert.assertFalse(wrapper.isLiveViewRefreshEnabled());
        });
    }

    @Test
    public void testForwardsPreviouslyMissingGetters() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoConfiguration delegate = newDelegate();
            CairoConfigurationWrapper wrapper = new CairoConfigurationWrapper(delegate);

            Assert.assertEquals(delegate.getBypassWalFdCache(), wrapper.getBypassWalFdCache());
            Assert.assertEquals(delegate.getO3LagDecreaseFactor(), wrapper.getO3LagDecreaseFactor(), DELTA);
            Assert.assertEquals(delegate.getO3LagIncreaseFactor(), wrapper.getO3LagIncreaseFactor(), DELTA);
            Assert.assertEquals(
                    delegate.getPostingIndexAlignedBitWidthThreshold(),
                    wrapper.getPostingIndexAlignedBitWidthThreshold(),
                    DELTA
            );
            Assert.assertEquals(delegate.getPostingIndexRowIdEncoding(), wrapper.getPostingIndexRowIdEncoding());
            Assert.assertEquals(delegate.isLiveViewRefreshEnabled(), wrapper.isLiveViewRefreshEnabled());
            Assert.assertEquals(delegate.isTtlWallClockEnabled(), wrapper.isTtlWallClockEnabled());
        });
    }

    /**
     * Negative control: the delegate values below must all differ from the {@link CairoConfiguration}
     * interface defaults, otherwise the assertions above would pass against a wrapper that forwards
     * nothing at all.
     */
    @Test
    public void testPostingIndexRowIdEncodingForwarded() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoConfiguration delegate = newDelegate();
            CairoConfigurationWrapper wrapper = new CairoConfigurationWrapper(delegate);

            Assert.assertEquals(PostingIndexUtils.ENCODING_EF, delegate.getPostingIndexRowIdEncoding());
            Assert.assertEquals(PostingIndexUtils.ENCODING_EF, wrapper.getPostingIndexRowIdEncoding());
            Assert.assertNotEquals(PostingIndexUtils.ENCODING_ADAPTIVE, wrapper.getPostingIndexRowIdEncoding());

            Assert.assertEquals(0.25, wrapper.getO3LagDecreaseFactor(), DELTA);
            Assert.assertEquals(2.5, wrapper.getO3LagIncreaseFactor(), DELTA);
            Assert.assertEquals(0.75, wrapper.getPostingIndexAlignedBitWidthThreshold(), DELTA);
            Assert.assertFalse(wrapper.isTtlWallClockEnabled());
        });
    }

    private static CairoConfiguration newDelegate() {
        return new DefaultCairoConfiguration(root) {
            @Override
            public boolean getBypassWalFdCache() {
                return true;
            }

            @Override
            public double getO3LagDecreaseFactor() {
                return 0.25;
            }

            @Override
            public double getO3LagIncreaseFactor() {
                return 2.5;
            }

            @Override
            public double getPostingIndexAlignedBitWidthThreshold() {
                return 0.75;
            }

            @Override
            public byte getPostingIndexRowIdEncoding() {
                return PostingIndexUtils.ENCODING_EF;
            }

            @Override
            public boolean isLiveViewRefreshEnabled() {
                return false;
            }

            @Override
            public boolean isTtlWallClockEnabled() {
                return false;
            }
        };
    }
}
