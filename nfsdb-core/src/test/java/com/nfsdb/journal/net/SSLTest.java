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
import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.net.config.ClientConfig;
import com.nfsdb.journal.net.config.ServerConfig;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestUtils;
import com.nfsdb.journal.tx.TxListener;
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
            setHostname("localhost");
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
        }}, factory);

        JournalClient client = new JournalClient(new ClientConfig() {{
            getSslConfig().setSecure(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setTrustStore("JKS", is, "changeit");
            }
        }}, factory);

        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        server.publish(remote);
        server.start();

        client.subscribe(Quote.class, "remote", "local", new TxListener() {
            @Override
            public void onCommit() {
                System.out.println("got commit");
            }
        });
        client.start();

        TestUtils.generateQuoteData(remote, size);

        System.out.println("done");
        Thread.sleep(3000);

        client.halt();
        System.out.println("here");
        server.halt();
        Journal<Quote> local = factory.reader(Quote.class, "local");
        TestUtils.assertDataEquals(remote, local);
    }
}
