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

package io.questdb.cutlass.line.tcp;

import io.questdb.cutlass.line.LineChannel;
import io.questdb.cutlass.line.Sender;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.io.IOException;


public class DelegatingTlsChannelTest {

    @Test
    public void testNoLeakWhenHandshakeFail() throws Exception {
        LineChannel exceptionThrowingChannel = new LineChannel() {
            @Override
            public void send(long ptr, int len) {
                throw new UnsupportedOperationException("go away");
            }

            @Override
            public int receive(long ptr, int len) {
                throw new UnsupportedOperationException("go away");
            }

            @Override
            public int errno() {
                throw new UnsupportedOperationException("go away");
            }

            @Override
            public void close() throws IOException {
                throw new UnsupportedOperationException("go away, yes, I throw an exception even during close()");
            }
        };
        TestUtils.assertMemoryLeak(() -> {
            try {
                new DelegatingTlsChannel(exceptionThrowingChannel, null, null, Sender.TlsValidationMode.DEFAULT);
            } catch (Throwable ignored) { }
        });
    }
}