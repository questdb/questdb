/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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
 ******************************************************************************/

package com.questdb.net.ha;

import com.questdb.JournalWriter;
import com.questdb.model.Quote;
import com.questdb.net.ha.config.ClientConfig;
import com.questdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClientRecoveryTest extends AbstractTest {
    @Test
    public void testClientWriterRelease() throws Exception {
        final CountDownLatch serverError = new CountDownLatch(1);
        JournalClient client = new JournalClient(new ClientConfig("localhost"), factory, null, new JournalClient.Callback() {
            @Override
            public void onEvent(int evt) {
                if (evt == JournalClientEvents.EVT_SERVER_ERROR) {
                    serverError.countDown();
                }
            }
        });

        client.subscribe(Quote.class);
        client.start();

        Assert.assertTrue(serverError.await(5, TimeUnit.SECONDS));
        Assert.assertFalse(client.isRunning());

        // should be able to get writer after client failure.
        try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
            Assert.assertNotNull(w);
        }
    }
}
