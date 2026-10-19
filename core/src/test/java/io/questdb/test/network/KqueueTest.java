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

package io.questdb.test.network;

import io.questdb.log.Log;
import io.questdb.network.Kqueue;
import io.questdb.network.KqueueFacade;
import io.questdb.network.KqueueFacadeImpl;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class KqueueTest {

    @Test
    public void testCloseIsIdempotent() throws Exception {
        // KqueueAccessor's natives ship only in the macOS build.
        Assume.assumeTrue(Os.isOSX());
        TestUtils.assertMemoryLeak(() -> {
            final AtomicInteger descriptorCloses = new AtomicInteger();
            final NetworkFacade nf = new NetworkFacadeImpl() {
                @Override
                public void close(long fd, Log logger) {
                    descriptorCloses.incrementAndGet();
                    super.close(fd, logger);
                }
            };
            final KqueueFacade kqf = new KqueueFacade() {
                @Override
                public NetworkFacade getNetworkFacade() {
                    return nf;
                }

                @Override
                public int kevent(long kq, long changeList, int nChanges, long eventList, int nEvents, int timeout) {
                    return KqueueFacadeImpl.INSTANCE.kevent(kq, changeList, nChanges, eventList, nEvents, timeout);
                }

                @Override
                public int kqueue() {
                    return KqueueFacadeImpl.INSTANCE.kqueue();
                }
            };

            // The dispatcher closes its Kqueue on shutdown and again through the enclosing
            // try-with-resources. A second pass would free the change and event lists twice --
            // assertMemoryLeak sees that as a native accounting underflow -- and would hand the
            // reused descriptor id to the network facade.
            final Kqueue kqueue = new Kqueue(kqf, 4);
            kqueue.close();
            kqueue.close();

            Assert.assertEquals("close must release the kqueue descriptor once", 1, descriptorCloses.get());
        });
    }
}
