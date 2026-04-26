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

package io.questdb.cutlass.qwp.codec;

/**
 * Source of truth for the fields emitted on the QWP egress {@code SERVER_INFO}
 * frame. OSS ships with {@link DefaultQwpServerInfoProvider} which reports
 * {@code ROLE_STANDALONE}. Enterprise substitutes an implementation that pulls
 * the live replication role from its cluster configuration, so the same code
 * path serves both packagings without OSS depending on Enterprise types.
 * <p>
 * Implementations must be safe to call from any HTTP worker thread without
 * external synchronisation. Role and epoch are read on every new connection;
 * cluster id and node id are expected to be stable for the life of the process.
 * <p>
 * Null return values from {@link #getClusterId()} or {@link #getNodeId()} are
 * serialised as zero-length strings on the wire. The frame writer caps each
 * field at 65535 UTF-8 bytes to fit the {@code u16} length prefix and truncates
 * longer values silently.
 */
public interface QwpServerInfoProvider {

    /**
     * Bitfield of protocol extensions this server is willing to honour. Reserved
     * for future use (e.g. freshness-watermark reads, multi-query multiplexing);
     * zero in v2.
     */
    int getCapabilities();

    /**
     * Stable identifier for the logical cluster this node belongs to. Clients
     * use it to detect accidental cross-cluster misconfiguration (an endpoint
     * pointing at the wrong cluster returns a recognisably different id).
     */
    CharSequence getClusterId();

    /**
     * Monotonic epoch. Bumps whenever the role transitions (replica promoted,
     * primary demoted). Clients tracking a specific primary use the epoch to
     * refuse a stale reconnection that lands on a node which no longer believes
     * it is primary at the current cluster epoch.
     */
    long getEpoch();

    /**
     * Stable per-node identifier. Used by clients for sticky routing (reconnect
     * to the same replica when possible) and to surface in diagnostics which
     * physical node served a given query.
     */
    CharSequence getNodeId();

    /**
     * One of {@link QwpEgressMsgKind#ROLE_STANDALONE}, {@link
     * QwpEgressMsgKind#ROLE_PRIMARY}, {@link QwpEgressMsgKind#ROLE_REPLICA},
     * {@link QwpEgressMsgKind#ROLE_PRIMARY_CATCHUP}. Evaluated per connection
     * at handshake time; a role transition after the handshake is not reflected
     * on the already-open WebSocket (clients detect it on reconnect).
     */
    byte getRole();
}
