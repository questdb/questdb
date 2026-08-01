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

package io.questdb.test.cutlass.line.udp;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.line.udp.AbstractLineProtoUdpReceiver;
import io.questdb.cutlass.line.udp.DefaultLineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiver;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class LineUdpReceiverCloseTimeoutTest extends AbstractCairoTest {

    private static final LineUdpReceiverConfiguration RCVR_CONF = new DefaultLineUdpReceiverConfiguration() {
        @Override
        public int getPort() {
            return 0;
        }

        @Override
        public boolean isUnicast() {
            return true;
        }
    };

    @Test
    public void testCloseAfterThreadCreationFailure() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try {
                    new FailingThreadLineUdpReceiver(RCVR_CONF, engine);
                    Assert.fail();
                } catch (IllegalStateException e) {
                    Assert.assertEquals("thread creation failed", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testFailedStartCannotReuseCompletedLatches() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    FailingStartLineUdpReceiver receiver = new FailingStartLineUdpReceiver(RCVR_CONF, engine)
            ) {
                try {
                    receiver.start();
                    Assert.fail();
                } catch (IllegalStateException e) {
                    Assert.assertEquals("thread creation failed", e.getMessage());
                }
                receiver.start();
                Assert.assertEquals(1, receiver.getThreadCreationCount());
            }
        });
    }

    private static class FailingStartLineUdpReceiver extends AbstractLineProtoUdpReceiver {
        private int threadCreationCount;

        private FailingStartLineUdpReceiver(LineUdpReceiverConfiguration configuration, CairoEngine engine) {
            super(configuration, engine, null);
        }

        @Override
        protected Thread createThread(Runnable runnable) {
            threadCreationCount++;
            throw new IllegalStateException("thread creation failed");
        }

        @Override
        protected boolean runSerially() {
            return false;
        }

        private int getThreadCreationCount() {
            return threadCreationCount;
        }
    }

    private static class FailingThreadLineUdpReceiver extends LineUdpReceiver {
        private FailingThreadLineUdpReceiver(LineUdpReceiverConfiguration configuration, CairoEngine engine) {
            super(configuration, engine, null);
        }

        @Override
        protected Thread createThread(Runnable runnable) {
            throw new IllegalStateException("thread creation failed");
        }
    }
}
