/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.net;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.net.config.ClientConfig;
import com.nfsdb.journal.net.config.ServerConfig;
import com.nfsdb.journal.test.model.Quote;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

public class SSLTest extends AbstractTest {

    @Test
    @Ignore
    public void testSingleKeySSL() throws Exception {

        int size = 100000;

        JournalServer server = new JournalServer(new ServerConfig() {{
            setIfName("lo");
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            getSslConfig().setSecure(true);
        }}, factory);

        JournalClient client = new JournalClient(ClientConfig.INSTANCE, factory);

        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote", size);
        server.export(remote);
        server.start();

        client.sync(Quote.class, "remote", "local", size);
        client.start();

        TestUtils.generateQuoteData(remote, size);

        client.halt();
        server.halt();
        Journal<Quote> local = factory.reader(Quote.class, "local");
        TestUtils.assertDataEquals(remote, local);
    }
}
