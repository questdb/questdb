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

import io.questdb.cairo.sql.Function;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.cutlass.qwp.server.egress.QwpEgressRequestDecoder;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * Unit tests for {@link QwpEgressRequestDecoder}: build a QUERY_REQUEST payload
 * directly in native memory and verify the decoded fields. These tests pin the
 * functional behavior so the C4 zero-alloc refactor (removing per-bind allocations)
 * can be made safely.
 */
public class QwpEgressRequestDecoderTest {

    @Test
    public void testDecodeNoBinds() throws Exception {
        BindVariableServiceImpl bindVars = newBindVars();
        QwpEgressRequestDecoder decoder = new QwpEgressRequestDecoder();

        long buf = 0;
        int bufSize = 256;
        try {
            buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            byte[] sql = "SELECT 1".getBytes(StandardCharsets.UTF_8);
            int len = writeQueryRequest(buf, /*requestId=*/ 42, sql, /*initialCredit=*/ 0, /*bindCount=*/ 0);
            decoder.decodeQueryRequest(buf, len, bindVars);

            Assert.assertEquals(42L, decoder.requestId);
            Assert.assertEquals(0L, decoder.initialCredit);
            Assert.assertEquals("SELECT 1", decoder.sql.toString());
        } finally {
            if (buf != 0) Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeStringBind() throws Exception {
        BindVariableServiceImpl bindVars = newBindVars();
        QwpEgressRequestDecoder decoder = new QwpEgressRequestDecoder();

        long buf = 0;
        int bufSize = 256;
        try {
            buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            byte[] sql = "SELECT ?".getBytes(StandardCharsets.UTF_8);
            byte[] bindStr = "hello".getBytes(StandardCharsets.UTF_8);

            // Header: msg_kind + requestId + sql_len + sql + initial_credit + bind_count
            // Bind: type_code + null_flag(0) + offsets[2 × i32] + bytes
            int len = writeQueryRequest(buf, 7, sql, 0, 1);
            // Append the single STRING bind starting at the current end of the request.
            // writeQueryRequest already wrote bind_count = 1; we now append the bind body.
            long p = buf + len;
            Unsafe.getUnsafe().putByte(p++, QwpConstants.TYPE_STRING);
            Unsafe.getUnsafe().putByte(p++, (byte) 0); // null_flag = 0 (non-null)
            Unsafe.getUnsafe().putInt(p, 0);            // offset[0] = 0
            p += 4;
            Unsafe.getUnsafe().putInt(p, bindStr.length); // offset[1]
            p += 4;
            for (byte b : bindStr) Unsafe.getUnsafe().putByte(p++, b);
            int totalLen = (int) (p - buf);

            decoder.decodeQueryRequest(buf, totalLen, bindVars);

            Assert.assertEquals(7L, decoder.requestId);
            Assert.assertEquals("SELECT ?", decoder.sql.toString());
            Function bind0 = bindVars.getFunction(0);
            Assert.assertNotNull("bind 0 must be set", bind0);
            CharSequence got = bind0.getStrA(null);
            Assert.assertNotNull("bind 0 value must be non-null", got);
            Assert.assertEquals("hello", got.toString());
        } finally {
            if (buf != 0) Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeVarcharBind() throws Exception {
        BindVariableServiceImpl bindVars = newBindVars();
        QwpEgressRequestDecoder decoder = new QwpEgressRequestDecoder();

        long buf = 0;
        int bufSize = 256;
        try {
            buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            byte[] sql = "SELECT ?".getBytes(StandardCharsets.UTF_8);
            byte[] bindStr = "world".getBytes(StandardCharsets.UTF_8);

            int len = writeQueryRequest(buf, 8, sql, 0, 1);
            long p = buf + len;
            Unsafe.getUnsafe().putByte(p++, QwpConstants.TYPE_VARCHAR);
            Unsafe.getUnsafe().putByte(p++, (byte) 0);
            Unsafe.getUnsafe().putInt(p, 0);
            p += 4;
            Unsafe.getUnsafe().putInt(p, bindStr.length);
            p += 4;
            for (byte b : bindStr) Unsafe.getUnsafe().putByte(p++, b);
            int totalLen = (int) (p - buf);

            decoder.decodeQueryRequest(buf, totalLen, bindVars);

            Assert.assertEquals(8L, decoder.requestId);
            Function bind0 = bindVars.getFunction(0);
            Assert.assertNotNull("bind 0 must be set", bind0);
            // VARCHAR bind round-trips as a UTF-8 sequence
            io.questdb.std.str.Utf8Sequence got = bind0.getVarcharA(null);
            Assert.assertNotNull("bind 0 value must be non-null", got);
            Assert.assertEquals(bindStr.length, got.size());
            for (int i = 0; i < bindStr.length; i++) {
                Assert.assertEquals("byte " + i, bindStr[i], got.byteAt(i));
            }
        } finally {
            if (buf != 0) Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeNullStringBind() throws Exception {
        BindVariableServiceImpl bindVars = newBindVars();
        QwpEgressRequestDecoder decoder = new QwpEgressRequestDecoder();

        long buf = 0;
        int bufSize = 128;
        try {
            buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            byte[] sql = "SELECT ?".getBytes(StandardCharsets.UTF_8);

            int len = writeQueryRequest(buf, 9, sql, 0, 1);
            long p = buf + len;
            Unsafe.getUnsafe().putByte(p++, QwpConstants.TYPE_STRING);
            Unsafe.getUnsafe().putByte(p++, (byte) 1);  // null_flag nonzero -> bitmap follows
            Unsafe.getUnsafe().putByte(p++, (byte) 1);  // bitmap byte: bit 0 set -> NULL
            int totalLen = (int) (p - buf);

            decoder.decodeQueryRequest(buf, totalLen, bindVars);

            Function bind0 = bindVars.getFunction(0);
            Assert.assertNotNull(bind0);
            Assert.assertNull("NULL string bind must surface as null", bind0.getStrA(null));
        } finally {
            if (buf != 0) Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeMixedBinds() throws Exception {
        BindVariableServiceImpl bindVars = newBindVars();
        QwpEgressRequestDecoder decoder = new QwpEgressRequestDecoder();

        long buf = 0;
        int bufSize = 256;
        try {
            buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            byte[] sql = "SELECT ?, ?, ?".getBytes(StandardCharsets.UTF_8);
            byte[] s = "abc".getBytes(StandardCharsets.UTF_8);

            // 3 binds: LONG=42, STRING="abc", LONG=NULL
            int len = writeQueryRequest(buf, 100, sql, 0, 3);
            long p = buf + len;

            // bind 0: LONG = 42
            Unsafe.getUnsafe().putByte(p++, QwpConstants.TYPE_LONG);
            Unsafe.getUnsafe().putByte(p++, (byte) 0);
            Unsafe.getUnsafe().putLong(p, 42L);
            p += 8;

            // bind 1: STRING = "abc"
            Unsafe.getUnsafe().putByte(p++, QwpConstants.TYPE_STRING);
            Unsafe.getUnsafe().putByte(p++, (byte) 0);
            Unsafe.getUnsafe().putInt(p, 0);
            p += 4;
            Unsafe.getUnsafe().putInt(p, s.length);
            p += 4;
            for (byte b : s) Unsafe.getUnsafe().putByte(p++, b);

            // bind 2: LONG = NULL
            Unsafe.getUnsafe().putByte(p++, QwpConstants.TYPE_LONG);
            Unsafe.getUnsafe().putByte(p++, (byte) 1);  // null
            Unsafe.getUnsafe().putByte(p++, (byte) 1);  // bitmap: bit 0 set

            int totalLen = (int) (p - buf);
            decoder.decodeQueryRequest(buf, totalLen, bindVars);

            Assert.assertEquals(100L, decoder.requestId);
            Assert.assertEquals(42L, bindVars.getFunction(0).getLong(null));
            Assert.assertEquals("abc", bindVars.getFunction(1).getStrA(null).toString());
            Assert.assertTrue("bind 2 LONG NULL must report Long.MIN_VALUE",
                    bindVars.getFunction(2).getLong(null) == io.questdb.std.Numbers.LONG_NULL);
        } finally {
            if (buf != 0) Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static BindVariableServiceImpl newBindVars() {
        return new BindVariableServiceImpl(new DefaultTestCairoConfiguration("/tmp/qwp-decoder-test"));
    }

    /**
     * Writes the QUERY_REQUEST header (msg_kind + requestId + sql_len + sql + initial_credit + bind_count)
     * to {@code buf}. Returns the number of bytes written. Bind bodies must be appended by the caller.
     */
    private static int writeQueryRequest(long buf, long requestId, byte[] sql, long initialCredit, int bindCount) {
        long p = buf;
        Unsafe.getUnsafe().putByte(p++, QwpEgressMsgKind.QUERY_REQUEST);
        Unsafe.getUnsafe().putLong(p, requestId);
        p += 8;
        p = QwpVarint.encode(p, sql.length);
        for (byte b : sql) Unsafe.getUnsafe().putByte(p++, b);
        p = QwpVarint.encode(p, initialCredit);
        p = QwpVarint.encode(p, bindCount);
        return (int) (p - buf);
    }
}
