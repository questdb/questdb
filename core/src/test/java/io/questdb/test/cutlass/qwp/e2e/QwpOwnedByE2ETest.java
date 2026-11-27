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

import io.questdb.DefaultFactoryProvider;
import io.questdb.cairo.DefaultDdlListener;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.std.Chars;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

// The owned_by option of the pinned client against the QWP ingress server: the X-QWP-Owned-By header the client
// sends, the echo of the 101 response the client checks, and the reason of a refused upgrade.
public class QwpOwnedByE2ETest extends AbstractQwpWebSocketTest {
    // a name that both sides have to percent-encode: UTF-8, a space, '+' and '%'
    private static final String OWNER = "caf\u00e9 team+%1";

    @Test
    public void testOwnedByEchoedAndNamedOnAutoCreate() throws Exception {
        // A server that sets the owner echoes it in its canonical encoding, which the client compares with its own: the
        // connection comes up, and the table and the column it auto-creates are created for that owner.
        factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull SecurityContextFactory getSecurityContextFactory() {
                return (principalContext, interfaceId) -> new OwnerAcceptingSecurityContext();
            }
        };
        final StringBuffer createdObjects = new StringBuffer();
        engine.setDdlListener(new DefaultDdlListener() {
            @Override
            public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                createdObjects.append("column ").append(columnName)
                        .append(" for ").append(securityContext.getAutoCreateOwner()).append('\n');
            }

            @Override
            public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                createdObjects.append("table ").append(tableToken.getTableName())
                        .append(" for ").append(securityContext.getAutoCreateOwner()).append('\n');
            }
        });
        try {
            runInContext(port -> {
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";owned_by=" + OWNER + ";")) {
                    sender.table("owned").longColumn("v", 1).atNow();
                    sender.flush();
                    sender.table("owned").longColumn("v", 2).longColumn("w", 3).atNow();
                    sender.flush();
                }
                drainWalQueue();
                assertQuery("SELECT v, w FROM owned").noLeakCheck().expectSize().returns("""
                        v\tw
                        1\tnull
                        2\t3
                        """);
                TestUtils.assertEquals("table owned for " + OWNER + "\ncolumn w for " + OWNER + "\n", createdObjects);
            });
        } finally {
            engine.setDdlListener(DefaultDdlListener.INSTANCE);
        }
    }

    @Test
    public void testOwnedByRefusedWithoutAccessControl() throws Exception {
        // Without access control the server cannot set an owner, so the client fails at build, with the server's
        // reason, instead of at the first table it auto-creates. Unfragmented sends deliver the reason with the headers.
        sendChunk = Integer.MAX_VALUE;
        recvChunk = Integer.MAX_VALUE;
        runInContext(port -> {
            try (Sender ignored = Sender.fromConfig("ws::addr=localhost:" + port + ";owned_by=" + OWNER + ";")) {
                Assert.fail("expected the upgrade to fail");
            } catch (QwpAuthFailedException e) {
                Assert.assertEquals(403, e.getStatusCode());
                Assert.assertEquals("owned_by refused: the owner of auto-created tables and columns can only be set in QuestDB Enterprise",
                        e.getReason());
            }
        });
    }

    private static final class OwnerAcceptingSecurityContext extends AllowAllSecurityContext {
        private volatile String owner;

        @Override
        public CharSequence getAutoCreateOwner() {
            return owner;
        }

        @Override
        public void setAutoCreateOwner(@NotNull CharSequence ownerName) {
            owner = Chars.toString(ownerName);
        }
    }
}
