/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ha;

import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.ha.config.ClientConfig;
import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class ClientRecoveryTest extends AbstractTest {
    @Test
    public void testClientWriterRelease() throws Exception {
        JournalClient client = new JournalClient(new ClientConfig("localhost"), factory);
        client.subscribe(Quote.class);
        try {
            client.start();
            Assert.fail("Expect client to fail");
        } catch (JournalNetworkException e) {
            client.halt();
        }

        // should be able to get writer after client failure.
        JournalWriter<Quote> w = factory.writer(Quote.class);
        Assert.assertNotNull(w);
    }
}
