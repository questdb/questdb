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
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

public class SSLTest extends AbstractTest {

    @Test
    public void testSingleKeySSL() throws Exception {

        int size = 1000000;

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
                getSslConfig().setTrustStore(is, "changeit");
            }
        }}, factory);

        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        server.publish(remote);
        server.start();

        client.subscribe(Quote.class, "remote", "local");
        client.start();

        TestUtils.generateQuoteData(remote, size);
        Thread.sleep(500);

        client.halt();
        server.halt();
        Journal<Quote> local = factory.reader(Quote.class, "local");
        TestUtils.assertDataEquals(remote, local);
    }

    @Test
    public void testAuthBothCertsMissing() throws Exception {

        JournalServer server = new JournalServer(new ServerConfig() {{
            setHostname("localhost");
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            getSslConfig().setRequireClientAuth(true);
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

        client.subscribe(Quote.class, "remote", "local");
        try {
            client.start();
            Assert.fail("Expect client not to start");
        } catch (Exception e) {
            // expect this
        } finally {
            client.halt();
        }

        Assert.assertEquals(0, server.getConnectedClients());
        server.halt();
    }

    @Test
    public void testClientAuth() throws Exception {

        int size = 1000000;

        JournalServer server = new JournalServer(new ServerConfig() {{
            setHostname("localhost");
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            getSslConfig().setRequireClientAuth(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setTrustStore(is, "changeit");
            }
        }}, factory);

        JournalClient client = new JournalClient(new ClientConfig() {{
            getSslConfig().setSecure(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setTrustStore(is, "changeit");
            }
        }}, factory);

        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        server.publish(remote);
        server.start();

        client.subscribe(Quote.class, "remote", "local");
        client.start();

        TestUtils.generateQuoteData(remote, size);
        Thread.sleep(1000);

        client.halt();
        server.halt();
        Journal<Quote> local = factory.reader(Quote.class, "local");
        TestUtils.assertDataEquals(remote, local);
    }

    @Test
    public void testServerTrustMissing() throws Exception {

        JournalServer server = new JournalServer(new ServerConfig() {{
            setHostname("localhost");
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            getSslConfig().setRequireClientAuth(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
        }}, factory);

        JournalClient client = new JournalClient(new ClientConfig() {{
            getSslConfig().setSecure(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setTrustStore(is, "changeit");
            }
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
        }}, factory);

        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        server.publish(remote);
        server.start();

        client.subscribe(Quote.class, "remote", "local");
        try {
            client.start();
            Assert.fail("Expect client not to start");
        } catch (Exception e) {
            // expect this
        } finally {
            client.halt();
        }

        Assert.assertEquals(0, server.getConnectedClients());
        server.halt();
    }


    @Test
    public void testNonAuthClientTrustMissing() throws Exception {

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
        }}, factory);

        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        server.publish(remote);
        server.start();

        client.subscribe(Quote.class, "remote", "local");
        try {
            client.start();
            Assert.fail("Expect client not to start");
        } catch (Exception e) {
            // expect this
        } finally {
            client.halt();
        }

        Assert.assertEquals(0, server.getConnectedClients());
        server.halt();
    }

    @Test
    public void testNoCertTrustAllSSL() throws Exception {

        int size = 1000000;

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
            getSslConfig().setTrustAll(true);
        }}, factory);

        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        server.publish(remote);
        server.start();

        client.subscribe(Quote.class, "remote", "local");
        client.start();

        TestUtils.generateQuoteData(remote, size);
        Thread.sleep(500);

        client.halt();
        server.halt();
        Journal<Quote> local = factory.reader(Quote.class, "local");
        TestUtils.assertDataEquals(remote, local);
    }

}
