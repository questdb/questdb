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

import java.nio.charset.StandardCharsets;

/**
 * QWP egress message-kind discriminator. The first byte of every egress payload
 * identifies which of the egress message types it carries. See the QWP egress
 * protocol reference at
 * {@code https://questdb.com/docs/connect/wire-protocols/qwp-egress-websocket/}
 * for the authoritative list.
 */
public final class QwpEgressMsgKind {
    /**
     * Server-to-client connection-cache reset. Body:
     * {@code reset_mask:u8} with bit 0 = SYMBOL dict.
     * Sent between result boundaries when a cache reaches its configured
     * soft cap. Recipient clears the indicated caches; subsequent RESULT_BATCH
     * frames assume a fresh starting state. See the QWP egress protocol
     * reference at
     * {@code https://questdb.com/docs/connect/wire-protocols/qwp-egress-websocket/}.
     */
    public static final byte CACHE_RESET = 0x17;
    public static final byte CANCEL = 0x14;
    /**
     * SERVER_INFO ends with compression_codec:u8, compression_level:u8.
     */
    public static final int CAP_COMPRESSION = 0x00000004;
    /**
     * {@code SERVER_INFO.capabilities} bit: the server honours the query-details
     * flags of {@code QUERY_REQUEST.query_flags} -- {@link #QUERY_FLAG_ERROR_POSITION},
     * {@link #QUERY_FLAG_STATS}, {@link #QUERY_FLAG_COUNT_ROWS},
     * {@link #QUERY_FLAG_SKIP_ROWS} and {@link #QUERY_FLAG_MAX_ROWS}. Clients
     * set those bits only when this bit is set: a server without it ignores
     * them and answers with the baseline frames.
     */
    public static final int CAP_QUERY_DETAILS = 0x00000008;
    /**
     * {@code SERVER_INFO.capabilities} bit: the server parses the optional
     * {@code query_flags:varint} trailer on {@code QUERY_REQUEST}. Clients append
     * the trailer only when this bit is set.
     */
    public static final int CAP_QUERY_FLAGS = 0x00000002;
    /**
     * {@code SERVER_INFO.capabilities} bit advertising that the frame ends with
     * an additional {@code zone_id:u16_len+utf8} field after {@code node_id}.
     * Servers set the bit when the operator has configured a zone; clients use
     * the value to prefer same-zone endpoints (see
     * {@code https://questdb.com/docs/high-availability/client-failover/concepts/}
     * and the QWP egress protocol reference at
     * {@code https://questdb.com/docs/connect/wire-protocols/qwp-egress-websocket/}).
     * When the bit is
     * unset, the trailer is absent entirely so a client built against the
     * zone-less layout sees the byte layout it expects.
     */
    public static final int CAP_ZONE = 0x00000001;
    public static final byte CREDIT = 0x15;
    /**
     * Server-to-client ack for successful non-SELECT queries (DDL, INSERT,
     * UPDATE, etc.). Body: {@code request_id:u64, op_type:u8, rows_affected:varint}.
     * The op_type byte is the corresponding {@code CompiledQuery.TYPE_*} constant so
     * the client can surface it to the user alongside the affected-row count.
     */
    public static final byte EXEC_DONE = 0x16;
    /**
     * {@code position} value of a {@code QUERY_ERROR} position trailer when the
     * error does not point into the SQL text: a protocol error, a cancellation,
     * a result that does not fit the send buffer.
     */
    public static final int NO_ERROR_POSITION = -1;
    public static final byte QUERY_ERROR = 0x13;
    /**
     * {@code QUERY_REQUEST.query_flags} bit: return the number of rows in the
     * whole result set, whatever {@link #QUERY_FLAG_SKIP_ROWS} and
     * {@link #QUERY_FLAG_MAX_ROWS} leave out of the stream. {@code RESULT_END}
     * then carries {@code row_count:varint} after {@code total_rows}. When the
     * stream stops at {@code max_rows} before the result ends, the server counts
     * the remaining rows without sending them. Ignored by statements that do not
     * produce rows.
     */
    public static final int QUERY_FLAG_COUNT_ROWS = 0x08;
    /**
     * {@code QUERY_REQUEST.query_flags} bit: report the error position. A
     * {@code QUERY_ERROR} for this request then carries the bare message,
     * without the {@code [n]} prefix of the baseline frame, and ends with
     * {@code position:i32}: the UTF-16 offset into the SQL text the error points
     * at, or {@link #NO_ERROR_POSITION}. The server answers with the baseline
     * frame when it has not read the request's flags: a request it cannot
     * decode, or one it rejects because another query is in flight.
     */
    public static final int QUERY_FLAG_ERROR_POSITION = 0x02;
    /**
     * {@code QUERY_REQUEST.query_flags} bit: stream at most {@code max_rows}
     * rows. The request carries {@code max_rows:varint} after {@code query_flags}
     * (and after {@code skip_rows} when {@link #QUERY_FLAG_SKIP_ROWS} is set too).
     * Zero streams no rows, only the schema. Ignored by statements that do not
     * produce rows.
     */
    public static final int QUERY_FLAG_MAX_ROWS = 0x20;
    /**
     * {@code QUERY_REQUEST.query_flags} bit: reset the SYMBOL dict before this
     * query, scoping it to the query rather than the connection. Reuses the
     * {@link #RESET_MASK_DICT} path: the dict clears at the query boundary and
     * the {@code CACHE_RESET} frame precedes the next {@code RESULT_BATCH}, so a
     * non-SELECT carrying the flag clears the dict at once but defers the
     * client-visible reset to the following result-producing query. Declared
     * {@code int} so masking against the 64-bit {@code query_flags} varint never
     * sign-extends a flag constant.
     */
    public static final int QUERY_FLAG_RESET_DICT = 0x01;
    /**
     * {@code QUERY_REQUEST.query_flags} bit: leave out the first
     * {@code skip_rows} rows of the result set. The request carries
     * {@code skip_rows:varint} right after {@code query_flags}. Ignored by
     * statements that do not produce rows.
     */
    public static final int QUERY_FLAG_SKIP_ROWS = 0x10;
    /**
     * {@code QUERY_REQUEST.query_flags} bit: report execution statistics.
     * {@code RESULT_END} and {@code EXEC_DONE} then end with the statistics block
     * {@code compile_ns:varint, execute_ns:varint, count_ns:varint,
     * client_wait_ns:varint, stats_flags:u8}, following {@code row_count} when
     * {@link #QUERY_FLAG_COUNT_ROWS} is set too. {@code execute_ns} runs from the
     * arrival of the request to the terminator and includes the other three
     * durations. {@code client_wait_ns} is the time the query waited for the
     * client to read a result or grant credit. See {@link #STATS_FLAG_JIT_COMPILED}.
     */
    public static final int QUERY_FLAG_STATS = 0x04;
    public static final byte QUERY_REQUEST = 0x10;
    /**
     * Reset mask bit: clear the connection-scoped SYMBOL dict.
     * After receiving, the peer's dict size returns to 0 and the next
     * {@code RESULT_BATCH} delta section starts at {@code deltaStart=0}.
     */
    public static final byte RESET_MASK_DICT = 0x01;
    public static final byte RESULT_BATCH = 0x11;
    public static final byte RESULT_END = 0x12;
    /**
     * Role value on {@code SERVER_INFO.role}: the authoritative write node. Reads
     * here see the most recent commits without replication lag. A cluster has at
     * most one ROLE_PRIMARY at any given epoch.
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
     * every WebSocket connection. Carries the server's replication role,
     * monotonic role epoch, cluster and node
     * identifiers, a capabilities bitfield, and the server's wall-clock
     * nanoseconds at send time.
     * <p>
     * Body layout (little-endian): {@code msg_kind:u8, role:u8, epoch:u64,
     * capabilities:u32, server_wall_ns:i64, cluster_id:u16_len+utf8,
     * node_id:u16_len+utf8} followed by an optional {@code zone_id:u16_len+utf8}
     * gated by the {@link #CAP_ZONE} bit in {@code capabilities}. The byte-value
     * 0x17 is claimed by {@link #CACHE_RESET}; SERVER_INFO lives at 0x18.
     */
    public static final byte SERVER_INFO = 0x18;
    /**
     * {@code stats_flags} bit of the {@link #QUERY_FLAG_STATS} block: the query
     * filter runs as JIT-compiled native code.
     */
    public static final byte STATS_FLAG_JIT_COMPILED = 0x01;
    private static final byte[] ROLE_NAME_BYTES_PRIMARY = "PRIMARY".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ROLE_NAME_BYTES_PRIMARY_CATCHUP = "PRIMARY_CATCHUP".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ROLE_NAME_BYTES_REPLICA = "REPLICA".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ROLE_NAME_BYTES_STANDALONE = "STANDALONE".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ROLE_NAME_BYTES_UNKNOWN = "UNKNOWN".getBytes(StandardCharsets.US_ASCII);

    private QwpEgressMsgKind() {
    }

    public static String roleName(byte role) {
        return switch (role) {
            case ROLE_STANDALONE -> "STANDALONE";
            case ROLE_PRIMARY -> "PRIMARY";
            case ROLE_REPLICA -> "REPLICA";
            case ROLE_PRIMARY_CATCHUP -> "PRIMARY_CATCHUP";
            default -> "UNKNOWN";
        };
    }

    public static byte[] roleNameBytes(byte role) {
        return switch (role) {
            case ROLE_STANDALONE -> ROLE_NAME_BYTES_STANDALONE;
            case ROLE_PRIMARY -> ROLE_NAME_BYTES_PRIMARY;
            case ROLE_REPLICA -> ROLE_NAME_BYTES_REPLICA;
            case ROLE_PRIMARY_CATCHUP -> ROLE_NAME_BYTES_PRIMARY_CATCHUP;
            default -> ROLE_NAME_BYTES_UNKNOWN;
        };
    }
}
