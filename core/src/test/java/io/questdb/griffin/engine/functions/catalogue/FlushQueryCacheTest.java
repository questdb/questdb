/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.mp.FanOut;
import io.questdb.mp.SCSequence;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class FlushQueryCacheTest extends AbstractGriffinTest {

    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "select flush_query_cache",
                sink,
                "flush_query_cache\n" +
                        "true\n"
        ));
    }

    @Test
    public void testFullQueue() throws Exception {
        assertMemoryLeak(() -> {
            // Subscribe to the FanOut, so that we have some consumers.
            final SCSequence queryCacheEventSubSeq = new SCSequence();
            final FanOut queryCacheEventFanOut = messageBus.getQueryCacheEventFanOut();
            queryCacheEventFanOut.and(queryCacheEventSubSeq);

            try {
                int queueSize = configuration.getQueryCacheEventQueueCapacity();

                for (int i = 0; i < queueSize; i++) {
                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "select flush_query_cache",
                            sink,
                            "flush_query_cache\n" +
                                    "true\n"
                    );
                }

                // The queue is full now, so we expect false value to be returned.
                TestUtils.assertSql(
                        compiler,
                        sqlExecutionContext,
                        "select flush_query_cache",
                        sink,
                        "flush_query_cache\n" +
                                "false\n"
                );
            } finally {
                messageBus.getQueryCacheEventFanOut().remove(queryCacheEventSubSeq);
                queryCacheEventSubSeq.clear();
            }
        });
    }
}
