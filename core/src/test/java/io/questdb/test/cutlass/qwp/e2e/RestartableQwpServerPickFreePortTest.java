/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.cutlass.qwp.e2e;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Covers the retry and exhaustion branches of {@link RestartableQwpServer#pickFreePort()}.
 * <p>
 * Both branches fire only when the candidate port is already occupied on the loopback address, and
 * the method's own javadoc records that nobody could establish whether an ephemeral allocator ever
 * hands out such a port. A test that waited for that condition would be flaky by construction, so
 * these tests drive the package-private
 * {@link RestartableQwpServer#pickFreePort(RestartableQwpServer.PortCandidateSupplier)} seam and
 * feed it a port the test itself holds a loopback listener on. The loopback probe under test still
 * runs for real -- the {@link IOException} it catches is a genuine bind failure from the kernel,
 * not a stubbed throw.
 * <p>
 * These tests allocate no native memory (plain {@code java.net} sockets on the JDK heap), so they
 * do not need {@code assertMemoryLeak()}. Every {@link ServerSocket} they open lives in a
 * try-with-resources block and closes on the assertion-failure path too.
 */
public class RestartableQwpServerPickFreePortTest {

    @Test
    public void testPickFreePortRetriesWhenCandidateIsOccupiedOnLoopback() throws Exception {
        try (ServerSocket occupied = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
            int occupiedPort = occupied.getLocalPort();
            AtomicInteger candidateCalls = new AtomicInteger();
            // The first candidate collides with the listener this test holds open for the whole
            // test, so the kernel refuses the probe's bind and pickFreePort() must ask for another
            // candidate. The second candidate is 0, which makes the probe bind an ephemeral port
            // the kernel always has to satisfy: the retry cannot fail, and the port it returns
            // cannot be the still-occupied one.
            int port = RestartableQwpServer.pickFreePort(() -> candidateCalls.incrementAndGet() > 1 ? 0 : occupiedPort);
            Assert.assertEquals("pickFreePort() must retry after the loopback bind fails", 2, candidateCalls.get());
            Assert.assertTrue("pickFreePort() must return the port its probe actually bound, was: " + port,
                    port > 0 && port != occupiedPort);
        }
    }

    @Test
    public void testPickFreePortReturnsUsablePortWithDefaultCandidateSupplier() throws Exception {
        int port = RestartableQwpServer.pickFreePort();
        Assert.assertTrue("pickFreePort() returned a port outside the TCP range: " + port,
                port > 0 && port <= 65_535);
        // The range check alone is not a signal: ServerSocket.getLocalPort() cannot violate it on
        // a bound socket. What the 14 callers need is that the returned port is actually FREE for
        // a server to take -- on the wildcard address HttpServer binds, and on the loopback
        // address the tests then dial. Both binds must land on the port the method returned, so a
        // candidate the method never released, or one it picked without vetting, fails here.
        try (ServerSocket wildcard = new ServerSocket(port)) {
            Assert.assertEquals("the returned port must be free to bind on the wildcard address",
                    port, wildcard.getLocalPort());
        }
        try (ServerSocket loopback = new ServerSocket(port, 0, InetAddress.getLoopbackAddress())) {
            Assert.assertEquals("the returned port must be free to bind on the loopback address",
                    port, loopback.getLocalPort());
        }
    }

    @Test
    public void testPickFreePortThrowsCarryingLastBindFailureAfterExhaustingAttempts() throws Exception {
        try (ServerSocket occupied = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
            int occupiedPort = occupied.getLocalPort();
            AtomicInteger candidateCalls = new AtomicInteger();
            try {
                RestartableQwpServer.pickFreePort(() -> {
                    candidateCalls.incrementAndGet();
                    return occupiedPort;
                });
                Assert.fail("pickFreePort() must give up when every candidate is occupied on loopback");
            } catch (IllegalStateException e) {
                Assert.assertEquals(RestartableQwpServer.PORT_PICK_ATTEMPTS, candidateCalls.get());
                Assert.assertTrue("exhaustion message must name the last candidate, was: " + e.getMessage(),
                        e.getMessage().contains("[lastPort=" + occupiedPort + ']'));
                Throwable cause = e.getCause();
                Assert.assertNotNull("the exhaustion throw must carry the last bind failure as its cause", cause);
                Assert.assertTrue("expected the cause to be the last IOException, was: " + cause,
                        cause instanceof IOException);
            }
        }
    }
}
