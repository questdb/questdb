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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the read-only REFUSAL-SHAPE contract on the QWP ingress path. A
 * read-only node refuses every write batch, but HOW it refuses must depend on
 * WHY it is read-only ({@code isStaticReadOnlyInstance()} in
 * {@link QwpIngressProcessorState}):
 * <ul>
 *   <li><b>Static</b> ({@code readonly=true} / {@code READ_ONLY_INSTANCE} in
 *       the server config): a permanent misconfiguration from the producer's
 *       point of view. The refusal must be the typed, terminal
 *       {@link QwpIngressProcessorState.Status#SECURITY_ERROR} NACK the client
 *       latches and surfaces loudly. The role-change close is wrong here
 *       because its 421 backstop does not exist: the upgrade gate rejects only
 *       {@code ROLE_REPLICA} / {@code ROLE_PRIMARY_CATCHUP}, and a statically
 *       read-only node keeps reporting its upgrade-eligible role (STANDALONE
 *       on OSS) forever — a store-and-forward client would classify each
 *       NORMAL_CLOSURE as orderly (no NACK, no poison strike, no terminal)
 *       and reconnect-replay in a silent infinite loop.</li>
 *   <li><b>Role-derived</b> (an enterprise engine widens
 *       {@code isReadOnlyMode()} with the dynamic replica leg while the static
 *       flag stays false): a TRANSIENT in-place PRIMARY-to-REPLICA demote.
 *       The refusal must take the reconnect-eligible role-change close
 *       (INVARIANT B): the reconnecting client meets the 421 role reject on
 *       the now-replica endpoint and retries from store-and-forward until a
 *       primary is reachable. This side of the boundary is fuzzed in depth by
 *       {@link QwpIngressDemoteRaceFuzzTest}; the deterministic case here
 *       pins it at the unit level alongside its complement.</li>
 *   <li><b>Both legs</b>: the static answer wins — the node refuses writes
 *       regardless of any future promote, so the terminal NACK is the
 *       truthful signal.</li>
 * </ul>
 * The batch driver mirrors {@code QwpIngressUpgradeProcessor.handleBinaryMessage}
 * exactly (addData → isDeferCommit → processMessage → commit calls → read
 * {@code isRoleChangeClosePending()} AFTER the commits). The static test uses
 * a fresh state per attempt, modelling the client's reconnect: on a static
 * node every fresh connection meets the identical refusal, which is why the
 * wrong refusal shape loops forever rather than self-correcting.
 */
public class QwpIngressReadOnlyRefusalShapeTest extends AbstractCairoTest {

    private static final int RECONNECT_ATTEMPTS = 3;

    @Test
    public void testRoleDerivedReadOnlyTakesRoleChangeClose() throws Exception {
        // Deterministic complement of QwpIngressDemoteRaceFuzzTest: role-derived
        // read-only (dynamic replica leg true, static config flag false) must
        // take the reconnect-eligible role-change close, never SECURITY_ERROR.
        assertMemoryLeak(() -> {
            final LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);

            try (CairoEngine roEngine = newRoleDerivedReadOnlyEngine()) {
                final QwpIngressProcessorState state =
                        new QwpIngressProcessorState(1024, 4096, roEngine, lineConfig);
                try {
                    state.of(1, AllowAllSecurityContext.INSTANCE);
                    final boolean roleChangeClose = driveBatch(state, zeroTableMessage());

                    Assert.assertFalse("role-derived: batch must be refused", state.isOk());
                    Assert.assertEquals(
                            "role-derived read-only is a transient demote — must map to the"
                                    + " reconnect-eligible status, never the terminal SECURITY_ERROR"
                                    + " a store-and-forward client latches as HALT (INVARIANT B)",
                            QwpIngressProcessorState.Status.NOT_ACCEPTING_WRITES,
                            state.getStatus()
                    );
                    Assert.assertTrue(
                            "role-derived read-only must flag the role-change close: the"
                                    + " reconnecting client is handed to the 421 role reject of the"
                                    + " now-replica endpoint",
                            roleChangeClose
                    );
                    TestUtils.assertContains(state.getErrorText(), CairoException.READ_ONLY_ACCESS_MESSAGE);
                } finally {
                    state.onDisconnected();
                    state.close();
                }
            }
        });
    }

    @Test
    public void testStaticReadOnlyRefusalStaysSecurityError() throws Exception {
        assertMemoryLeak(() -> {
            final LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);

            try (CairoEngine roEngine = newStaticReadOnlyEngine()) {
                // Premise of the whole contract: the OSS standalone role is NOT
                // rejected by the 421 upgrade gate (it rejects only ROLE_REPLICA /
                // ROLE_PRIMARY_CATCHUP), so a producer upgrades successfully against
                // a statically read-only node and its refusal shape is decided
                // per-batch in processMessage — there is no 421 backstop for the
                // role-change close to hand the client over to.
                Assert.assertEquals(
                        "OSS default provider must report STANDALONE (premise: upgrade succeeds, no 421 backstop)",
                        QwpEgressMsgKind.ROLE_STANDALONE,
                        roEngine.getQwpServerInfoProvider().role()
                );

                // Static readonly=true is boot-time state: it holds for the FIRST
                // batch of the FIRST connection, and identically for every
                // reconnect. Model the client's reconnect loop with a fresh state
                // (= fresh connection) per attempt.
                for (int attempt = 0; attempt < RECONNECT_ATTEMPTS; attempt++) {
                    final QwpIngressProcessorState state =
                            new QwpIngressProcessorState(1024, 4096, roEngine, lineConfig);
                    try {
                        state.of(1, AllowAllSecurityContext.INSTANCE);
                        final boolean roleChangeClose = driveBatch(state, zeroTableMessage());

                        final String where = "static readonly, connection #" + (attempt + 1);
                        Assert.assertFalse(where + ": batch must be refused", state.isOk());
                        Assert.assertEquals(
                                where + ": static readonly=true is a permanent misconfiguration, not a"
                                        + " transient demote — the refusal must stay the typed, terminal"
                                        + " SECURITY_ERROR NACK the client surfaces to its producer",
                                QwpIngressProcessorState.Status.SECURITY_ERROR,
                                state.getStatus()
                        );
                        Assert.assertFalse(
                                where + ": must NOT take the role-change close — on a standalone node the"
                                        + " role never changes, so the NORMAL_CLOSURE (orderly, no NACK, no"
                                        + " poison strike) sends the client into a silent infinite"
                                        + " reconnect-replay loop with data pinned in store-and-forward",
                                roleChangeClose
                        );
                        TestUtils.assertContains(state.getErrorText(), CairoException.READ_ONLY_ACCESS_MESSAGE);
                    } finally {
                        state.onDisconnected();
                        state.close();
                    }
                }
            }
        });
    }

    @Test
    public void testStaticWinsWhenBothReadOnlyLegsHold() throws Exception {
        // Tie-break: an enterprise node that is BOTH statically read-only and
        // role-derived read-only refuses writes regardless of any future
        // promote, so the terminal SECURITY_ERROR NACK is the truthful signal.
        // (Unreachable mid-connection in practice — a statically read-only node
        // refuses the very first batch, before any role flip can land — but the
        // tie-break must be pinned so a refactor cannot silently invert it.)
        assertMemoryLeak(() -> {
            final LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);

            try (CairoEngine roEngine = newStaticAndRoleDerivedReadOnlyEngine()) {
                final QwpIngressProcessorState state =
                        new QwpIngressProcessorState(1024, 4096, roEngine, lineConfig);
                try {
                    state.of(1, AllowAllSecurityContext.INSTANCE);
                    final boolean roleChangeClose = driveBatch(state, zeroTableMessage());

                    Assert.assertFalse("both legs: batch must be refused", state.isOk());
                    Assert.assertEquals(
                            "static read-only must win the tie-break: the node never accepts"
                                    + " writes regardless of role, so the refusal is terminal",
                            QwpIngressProcessorState.Status.SECURITY_ERROR,
                            state.getStatus()
                    );
                    Assert.assertFalse(
                            "static read-only must win the tie-break: no role-change close",
                            roleChangeClose
                    );
                    TestUtils.assertContains(state.getErrorText(), CairoException.READ_ONLY_ACCESS_MESSAGE);
                } finally {
                    state.onDisconnected();
                    state.close();
                }
            }
        });
    }

    private static void addNativeData(QwpIngressProcessorState state, byte[] data) {
        long ptr = Unsafe.malloc(data.length, MemoryTag.NATIVE_HTTP_CONN);
        try {
            for (int i = 0; i < data.length; i++) {
                Unsafe.putByte(ptr + i, data[i]);
            }
            state.addData(ptr, ptr + data.length);
        } finally {
            Unsafe.free(ptr, data.length, MemoryTag.NATIVE_HTTP_CONN);
        }
    }

    /**
     * Mirrors {@code QwpIngressUpgradeProcessor.handleBinaryMessage} exactly,
     * including reading {@code isRoleChangeClosePending()} AFTER the commit
     * calls (same driver contract as {@link QwpIngressDemoteRaceFuzzTest}).
     *
     * @return the roleChangeClose flag as the upgrade processor would observe it
     */
    private static boolean driveBatch(QwpIngressProcessorState state, byte[] message) {
        addNativeData(state, message);
        boolean deferCommit = state.isDeferCommit();
        state.processMessage();
        if (state.isOk() && !deferCommit) {
            state.commit();
        }
        if (state.isOk() && deferCommit) {
            state.commitIfMaxUncommittedRowsReached();
            if (state.isOk()) {
                // Mirrors the processor's deferred-ack containment: rows are
                // buffered but uncommitted, so the cumulative-ack watermark
                // must not advance past this frame until the group commits.
                state.markUncommittedDeferredRows();
            }
        }
        return state.isRoleChangeClosePending();
    }

    /**
     * Engine that is read-only through the DYNAMIC leg only: {@code isReadOnlyMode()}
     * is widened at the engine level — the same shape as the enterprise override
     * ({@code super.isReadOnlyMode() || isReadOnlyReplica()}) — while the static
     * {@code configuration.isReadOnlyInstance()} flag stays false.
     */
    private static CairoEngine newRoleDerivedReadOnlyEngine() {
        return new CairoEngine(new DefaultTestCairoConfiguration(root)) {
            @Override
            public boolean isReadOnlyMode() {
                return true;
            }
        };
    }

    /**
     * Enterprise both-legs shape: statically read-only AND role-derived read-only.
     */
    private static CairoEngine newStaticAndRoleDerivedReadOnlyEngine() {
        return new CairoEngine(new DefaultTestCairoConfiguration(root) {
            @Override
            public boolean isReadOnlyInstance() {
                return true;
            }
        }) {
            @Override
            public boolean isReadOnlyMode() {
                return true;
            }
        };
    }

    /**
     * Engine whose configuration reports {@code isReadOnlyInstance() == true}
     * from construction — the plain-OSS {@code readonly=true} shape. The base
     * {@code CairoEngine.isReadOnlyMode()} answers from exactly this flag, so
     * the top-of-processMessage gate refuses the very first batch of every
     * connection. Unlike the demotable engine of
     * {@link QwpIngressDemoteRaceFuzzTest} (which widens {@code isReadOnlyMode()}
     * dynamically to model the enterprise demote), the flag here is constant:
     * the node was never writable and never becomes writable.
     */
    private static CairoEngine newStaticReadOnlyEngine() {
        return new CairoEngine(new DefaultTestCairoConfiguration(root) {
            @Override
            public boolean isReadOnlyInstance() {
                return true;
            }
        });
    }

    /**
     * Well-formed QWP v1 message with zero table blocks: enough to enter
     * {@code processMessage} and hit the read-only gate at its top.
     */
    private static byte[] zeroTableMessage() {
        byte[] message = new byte[QwpConstants.HEADER_SIZE];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[4] = QwpConstants.VERSION;
        message[5] = 0; // flags
        message[6] = 0; // tableCount lo
        message[7] = 0; // tableCount hi
        message[8] = 0; // payloadLength (0)
        message[9] = 0;
        message[10] = 0;
        message[11] = 0;
        return message;
    }
}
