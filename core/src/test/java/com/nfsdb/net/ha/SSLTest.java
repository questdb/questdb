/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.net.ha;

import com.nfsdb.Journal;
import com.nfsdb.JournalWriter;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.misc.Files;
import com.nfsdb.model.Quote;
import com.nfsdb.net.ha.config.ClientConfig;
import com.nfsdb.net.ha.config.ServerConfig;
import com.nfsdb.test.tools.JournalTestFactory;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

public class SSLTest {

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
    }

    @Test
    public void testClientAuth() throws Exception {
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
    }

    @Test
    public void testNoCertTrustAllSSL() throws Exception {
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
    }

    @Test
    public void testNonAuthClientTrustMissing() throws Exception {
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
    }

    @Test
    public void testServerTrustMissing() throws Exception {
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
    }

    @Test
    public void testSingleKeySSL() throws Exception {
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
            setTcpNoDelay(false);
            try (InputStream is = this.getClass().getResourceAsStream("/keystore/singlekey.ks")) {
                getSslConfig().setTrustStore(is, "changeit");
            }
            getSslConfig().setSecure(true);
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
