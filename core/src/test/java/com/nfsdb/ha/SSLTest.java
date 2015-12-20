/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb.ha;

import com.nfsdb.Journal;
import com.nfsdb.JournalWriter;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.ha.config.ClientConfig;
import com.nfsdb.ha.config.ServerConfig;
import com.nfsdb.logging.Logger;
import com.nfsdb.misc.Files;
import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.JournalTestFactory;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

public class SSLTest {

    private static final Logger LOGGER = Logger.getLogger(SSLTest.class);

    @Rule
    public final JournalTestFactory factory = new JournalTestFactory(new JournalConfigurationBuilder() {{
        $(Quote.class).recordCountHint(2000)
                .$sym("sym").valueCountHint(20)
                .$sym("mode")
                .$sym("ex")
        ;


    }}.build(Files.makeTempDir()));

    @Test
    public void testAuthBothCertsMissing() throws Exception {

        LOGGER.info("testAuthBothCertsMissing()");

        JournalServer server = new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            getSslConfig().setRequireClientAuth(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            setEnableMultiCast(false);
            setHeartbeatFrequency(50);
        }}, factory);

        JournalClient client = new JournalClient(new ClientConfig("localhost") {{
            getSslConfig().setSecure(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setTrustStore(is, "changeit");
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

        Thread.sleep(500);
        Assert.assertEquals(0, server.getConnectedClients());
        server.halt();
        LOGGER.info("-------------");
    }

    @Test
    public void testClientAuth() throws Exception {

        LOGGER.info("testClientAuth()");

        int size = 2000;

        JournalServer server = new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            getSslConfig().setRequireClientAuth(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setTrustStore(is, "changeit");
            }
            setEnableMultiCast(false);
            setHeartbeatFrequency(50);
        }}, factory);

        JournalClient client = new JournalClient(new ClientConfig("localhost") {{
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
        LOGGER.info("-------------");
    }

    @Test
    public void testNoCertTrustAllSSL() throws Exception {

        LOGGER.info("testNoCertTrustAllSSL()");

        int size = 2000;

        JournalServer server = new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            setEnableMultiCast(false);
            setHeartbeatFrequency(50);
        }}, factory);

        JournalClient client = new JournalClient(new ClientConfig("localhost") {{
            getSslConfig().setSecure(true);
            getSslConfig().setTrustAll(true);
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
        LOGGER.info("-------------");
    }

    @Test
    public void testNonAuthClientTrustMissing() throws Exception {

        LOGGER.info("testNonAuthClientTrustMissing()");

        JournalServer server = new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            setEnableMultiCast(false);
            setHeartbeatFrequency(50);
        }}, factory);

        JournalClient client = new JournalClient(new ClientConfig("localhost") {{
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
        Thread.sleep(1000);
        Assert.assertEquals(0, server.getConnectedClients());
        server.halt();
        LOGGER.info("-------------");
    }

    @Test
    public void testServerTrustMissing() throws Exception {

        LOGGER.info("testServerTrustMissing()");

        JournalServer server = new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            getSslConfig().setRequireClientAuth(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            setEnableMultiCast(false);
            setHeartbeatFrequency(50);
        }}, factory);

        JournalClient client = new JournalClient(new ClientConfig("localhost") {{
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

        Thread.sleep(1000);

        Assert.assertEquals(0, server.getConnectedClients());
        server.halt();
        LOGGER.info("-------------");
    }

    @Test
    public void testSingleKeySSL() throws Exception {

        LOGGER.info("testSingleKeySSL()");

        int size = 1000;

        JournalServer server = new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(500));
            getSslConfig().setSecure(true);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setKeyStore(is, "changeit");
            }
            setEnableMultiCast(false);
            setHeartbeatFrequency(50);
        }}, factory);

        JournalClient client = new JournalClient(new ClientConfig("localhost") {{
            getSslConfig().setSecure(true);
            setTcpNoDelay(false);
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
        LOGGER.info("-------------");
    }
}
