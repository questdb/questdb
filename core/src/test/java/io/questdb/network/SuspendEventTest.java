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

package io.questdb.network;

import io.questdb.std.Os;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class SuspendEventTest {

    @Test
    public void testCreateAndClose() throws Exception {
        assertMemoryLeak(() -> {
            SuspendEvent event = SuspendEventFactory.newInstance(new DefaultIODispatcherConfiguration());
            if (Os.type != Os.WINDOWS) {
                // Read is non-blocking only on Linux, BSD and OS X.
                // The event is not yet triggered, so read has to fail.
                try {
                    event.checkTriggered();
                    Assert.fail();
                } catch (NetworkError e) {
                    MatcherAssert.assertThat(e.getMessage(), CoreMatchers.containsString("read value"));
                }
            }
            event.trigger();
            event.checkTriggered();
            // We need to close the event two times as if it's closed by both waiting and sending sides.
            event.close();
            event.close();
        });
    }
}
