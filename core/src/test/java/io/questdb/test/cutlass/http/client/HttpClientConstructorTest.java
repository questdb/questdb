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

package io.questdb.test.cutlass.http.client;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.HttpClientConfiguration;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.network.PlainSocketFactory;
import io.questdb.test.AbstractOomSweepTest;
import org.junit.Test;

public class HttpClientConstructorTest extends AbstractOomSweepTest {

    @Test
    public void testConstructorFailureFreesNativeAllocations() throws Exception {
        // The constructor takes the socket, then the request buffer, then the response-parser
        // buffer, then a ResponseHeaders whose own parser allocates again. A ceiling tripped at
        // any point after the first used to strand everything acquired before it: the caller
        // never receives the client, so close() never runs.
        //
        // The buffers are shrunk from their 64 KiB defaults so the sweep can step finely enough
        // to land between allocation points without running for thousands of iterations.
        final HttpClientConfiguration configuration = new DefaultHttpClientConfiguration() {
            @Override
            public int getInitialRequestBufferSize() {
                return 1024;
            }

            @Override
            public int getResponseBufferSize() {
                return 2048;
            }
        };

        assertMemoryLeak(() -> assertOomSweep(16 * 1024, 64, null, () -> {
            //noinspection EmptyTryBlock
            try (HttpClient ignore = HttpClientFactory.newInstance(configuration, PlainSocketFactory.INSTANCE)) {
                // built without tripping the ceiling; close() releases it
            }
        }));
    }
}
