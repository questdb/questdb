/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.SuspendEvent;
import io.questdb.network.SuspendEventFactory;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class SuspendEventTest {

    @Test
    public void testSmoke() throws Exception {
        assertMemoryLeak(() -> {
            SuspendEvent event = SuspendEventFactory.newInstance(DefaultIODispatcherConfiguration.INSTANCE);
            Assert.assertFalse(event.checkTriggered());
            event.trigger();
            Assert.assertTrue(event.checkTriggered());
            // We need to close the event two times as if it's closed by both waiting and sending sides.
            event.close();
            event.close();
        });
    }
}
