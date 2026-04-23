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
 * QWP egress message-kind discriminator. The first byte of every egress payload
 * identifies which of the egress message types it carries. See
 * {@code docs/QWP_EGRESS_EXTENSION.md} sec 5 for the authoritative list.
 */
public final class QwpEgressMsgKind {
    public static final byte CANCEL = 0x14;
    public static final byte CREDIT = 0x15;
    /**
     * Server-to-client ack for successful non-SELECT queries (DDL, INSERT,
     * UPDATE, etc.). Body: {@code request_id:u64, op_type:u8, rows_affected:varint}.
     * The op_type byte is the corresponding {@code CompiledQuery.TYPE_*} constant so
     * the client can surface it to the user alongside the affected-row count.
     */
    public static final byte EXEC_DONE = 0x16;
    public static final byte QUERY_ERROR = 0x13;
    public static final byte QUERY_REQUEST = 0x10;
    public static final byte RESULT_BATCH = 0x11;
    public static final byte RESULT_END = 0x12;
    /**
     * Role value on {@code SERVER_INFO.role}: the authoritative write node. Reads
     * here see the most recent commits without replication lag. A cluster has at
     * most one {@link #ROLE_PRIMARY} at any given epoch.
     */
    public static final byte ROLE_PRIMARY = 1;
    /**
     * Role value on {@code SERVER_INFO.role}: promotion-in-progress. The node
     * believes it is primary but is still uploading in-flight WAL segments to
     * the shared object store before accepting writes. Clients that insist on
     * primary-only reads may still route here; clients that need write-visible
     * reads should wait for {@link #ROLE_PRIMARY}.
     */
    public static final byte ROLE_PRIMARY_CATCHUP = 3;
    /**
     * Role value on {@code SERVER_INFO.role}: the node is a read-only replica
     * that pulls WAL segments from the shared object store. Reads may lag the
     * primary by the replication poll interval plus transport time.
     */
    public static final byte ROLE_REPLICA = 2;
    /**
     * Role value on {@code SERVER_INFO.role}: no replication is configured. The
     * standalone OSS default; behaves like a primary for routing purposes.
     */
    public static final byte ROLE_STANDALONE = 0;
    /**
     * Server-to-client. Unsolicited frame delivered as the first QWP message on
     * every v2 WebSocket connection (see {@code QwpConstants.VERSION_2}). Carries
     * the server's replication role, monotonic role epoch, cluster and node
     * identifiers, a capabilities bitfield, and the server's wall-clock
     * nanoseconds at send time.
     * <p>
     * Body layout (little-endian): {@code msg_kind:u8, role:u8, epoch:u64,
     * capabilities:u32, server_wall_ns:i64, cluster_id:u16_len+utf8,
     * node_id:u16_len+utf8}.
     */
    public static final byte SERVER_INFO = 0x17;
    public static final byte STATUS_CANCELLED = 0x0A;
    public static final byte STATUS_LIMIT_EXCEEDED = 0x0B;

    private QwpEgressMsgKind() {
    }
}
