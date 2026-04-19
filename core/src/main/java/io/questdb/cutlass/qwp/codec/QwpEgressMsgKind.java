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

    // Egress-specific status codes (extend ingress QwpConstants.STATUS_* namespace).
    public static final byte STATUS_CANCELLED = 0x0A;
    public static final byte STATUS_LIMIT_EXCEEDED = 0x0B;

    private QwpEgressMsgKind() {
    }
}
