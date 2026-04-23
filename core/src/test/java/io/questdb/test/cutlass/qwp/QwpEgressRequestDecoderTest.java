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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.cutlass.qwp.server.egress.QwpEgressRequestDecoder;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;

/**
 * Unit tests for {@link QwpEgressRequestDecoder}: build a QUERY_REQUEST (or CANCEL /
 * CREDIT) payload directly in native memory and verify the decoded fields.
 * <p>
 * Every test runs inside {@link TestUtils#assertMemoryLeak} -- the decoder operates on
 * native memory and any allocation that escapes the test must fail the build.
 */
public class QwpEgressRequestDecoderTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testCancelBody() throws Exception {
        runWithBuf(32, (buf, bindVars, decoder) -> {
            long p = buf;
            Unsafe.getUnsafe().putByte(p++, QwpEgressMsgKind.CANCEL);
            Unsafe.getUnsafe().putLong(p, 0xCAFEBABE_DEADBEEFL);
            int totalLen = (int) (p + 8 - buf);
            Assert.assertEquals(0xCAFEBABE_DEADBEEFL, decoder.decodeCancel(buf, totalLen));
        });
    }

    @Test
    public void testCancelRejectsTruncated() throws Exception {
        runWithBuf(32, (buf, bindVars, decoder) -> {
            Unsafe.getUnsafe().putByte(buf, QwpEgressMsgKind.CANCEL);
            try {
                decoder.decodeCancel(buf, 8); // need at least 9 bytes
                Assert.fail("expected QwpParseException for short CANCEL");
            } catch (QwpParseException expected) {
                Assert.assertTrue(expected.getFlyweightMessage().toString().contains("CANCEL"));
            }
        });
    }

    @Test
    public void testCreditBody() throws Exception {
        runWithBuf(32, (buf, bindVars, decoder) -> {
            long p = buf;
            Unsafe.getUnsafe().putByte(p++, QwpEgressMsgKind.CREDIT);
            Unsafe.getUnsafe().putLong(p, 1L);
            p += 8;
            p = QwpVarint.encode(p, 4096L);
            int totalLen = (int) (p - buf);
            Assert.assertEquals(4096L, decoder.decodeCredit(buf, totalLen));
        });
    }

    @Test
    public void testCreditRejectsTruncated() throws Exception {
        runWithBuf(32, (buf, bindVars, decoder) -> {
            try {
                decoder.decodeCredit(buf, 9);
                Assert.fail("expected QwpParseException for short CREDIT");
            } catch (QwpParseException expected) {
                Assert.assertTrue(expected.getFlyweightMessage().toString().contains("CREDIT"));
            }
        });
    }

    /**
     * Bindless queries use the SQL text verbatim as the select-cache key,
     * preserving the existing cache shape for the 99% path.
     */
    @Test
    public void testSelectCacheKeyNoBindsEqualsSql() throws Exception {
        runWithBuf(64, (buf, bindVars, decoder) -> {
            int total = writeBindScaffold(buf, 0);
            decoder.decodeQueryRequest(buf, total, bindVars);
            CharSequence key = decoder.buildSelectCacheKey(bindVars);
            Assert.assertSame("bindless key must be the sql sink itself (no composition)",
                    decoder.sql, key);
        });
    }

    /**
     * Same SQL with the same bind type must produce the same composite key, so
     * repeated {@code WHERE x = $1} queries with different INT values hit the
     * select cache instead of recompiling each time.
     */
    @Test
    public void testSelectCacheKeyStableAcrossSameTypeBinds() throws Exception {
        runWithBuf(128, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNonNullBind(p, QwpConstants.TYPE_INT);
            Unsafe.getUnsafe().putInt(p, 1);
            decoder.decodeQueryRequest(buf, (int) (p + 4 - buf), bindVars);
            String key1 = decoder.buildSelectCacheKey(bindVars).toString();

            // Decode the same request shape again with a different bind VALUE
            // but the same TYPE. decodeQueryRequest clears bindVars internally.
            Unsafe.getUnsafe().putInt(p, 42);
            decoder.decodeQueryRequest(buf, (int) (p + 4 - buf), bindVars);
            String key2 = decoder.buildSelectCacheKey(bindVars).toString();

            Assert.assertEquals("same SQL + same bind type must produce the same cache key",
                    key1, key2);
            Assert.assertTrue("composite key must carry a type prefix [...] on bound queries: " + key1,
                    key1.startsWith("["));
        });
    }

    /**
     * Same SQL with a different bind type must produce a different composite key,
     * so a factory compiled for {@code WHERE x = $1::INT} is never reused for
     * {@code WHERE x = $1::VARCHAR}.
     */
    @Test
    public void testSelectCacheKeyDiffersByBindType() throws Exception {
        runWithBuf(128, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            long saved = p;
            p = writeNonNullBind(p, QwpConstants.TYPE_INT);
            Unsafe.getUnsafe().putInt(p, 7);
            decoder.decodeQueryRequest(buf, (int) (p + 4 - buf), bindVars);
            String keyInt = decoder.buildSelectCacheKey(bindVars).toString();

            // Rewrite the bind section as TYPE_LONG at the same offset.
            p = writeNonNullBind(saved, QwpConstants.TYPE_LONG);
            Unsafe.getUnsafe().putLong(p, 7L);
            decoder.decodeQueryRequest(buf, (int) (p + 8 - buf), bindVars);
            String keyLong = decoder.buildSelectCacheKey(bindVars).toString();

            Assert.assertNotEquals("INT and LONG bind signatures must produce distinct cache keys",
                    keyInt, keyLong);
        });
    }

    @Test
    public void testDecodeBooleanBind() throws Exception {
        runWithBuf(128, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNonNullBind(p, QwpConstants.TYPE_BOOLEAN);
            Unsafe.getUnsafe().putByte(p++, (byte) 0x01);
            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
            Assert.assertTrue(bindVars.getFunction(0).getBool(null));
        });
    }

    @Test
    public void testDecodeByteShortCharInt() throws Exception {
        runWithBuf(256, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 4);
            long p = buf + totalLen;
            p = writeNonNullBind(p, QwpConstants.TYPE_BYTE);
            Unsafe.getUnsafe().putByte(p++, (byte) 0x7F);
            p = writeNonNullBind(p, QwpConstants.TYPE_SHORT);
            Unsafe.getUnsafe().putShort(p, (short) -12_345);
            p += 2;
            p = writeNonNullBind(p, QwpConstants.TYPE_CHAR);
            Unsafe.getUnsafe().putShort(p, (short) 'Q');
            p += 2;
            p = writeNonNullBind(p, QwpConstants.TYPE_INT);
            Unsafe.getUnsafe().putInt(p, -1_234_567);
            p += 4;
            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
            Assert.assertEquals((byte) 0x7F, bindVars.getFunction(0).getByte(null));
            Assert.assertEquals((short) -12_345, bindVars.getFunction(1).getShort(null));
            Assert.assertEquals('Q', bindVars.getFunction(2).getChar(null));
            Assert.assertEquals(-1_234_567, bindVars.getFunction(3).getInt(null));
        });
    }

    @Test
    public void testDecodeCancelMsgKindRoutingViaPeek() throws Exception {
        runWithBuf(16, (buf, bindVars, decoder) -> {
            Unsafe.getUnsafe().putByte(buf, QwpEgressMsgKind.CANCEL);
            Assert.assertEquals(QwpEgressMsgKind.CANCEL, decoder.peekMsgKind(buf));
        });
    }

    /**
     * Regression for DECIMAL128 NULL bind: the NULL sentinel must be recognised as NULL
     * by Decimals.isNull. Prior to the fix, the decoder wrote LONG_NULL to both the HI
     * and LO halves, producing a value that was not the canonical DECIMAL128 NULL.
     */
    @Test
    public void testDecodeDecimal128NullBind() throws Exception {
        runWithBuf(64, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNullBind(p, QwpConstants.TYPE_DECIMAL128);
            Unsafe.getUnsafe().putByte(p++, (byte) 2); // scale
            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);

            Function f = bindVars.getFunction(0);
            Decimal128 sink = new Decimal128();
            f.getDecimal128(null, sink);
            Assert.assertTrue("DECIMAL128 NULL sentinel must be recognised as null", sink.isNull());
        });
    }

    @Test
    public void testDecodeDecimal128RoundTrip() throws Exception {
        runWithBuf(128, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNonNullBind(p, QwpConstants.TYPE_DECIMAL128);
            Unsafe.getUnsafe().putByte(p++, (byte) 6); // scale
            Unsafe.getUnsafe().putLong(p, 0x0123456789ABCDEFL); // lo
            p += 8;
            Unsafe.getUnsafe().putLong(p, 0x0011223344556677L); // hi
            p += 8;
            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);

            Decimal128 sink = new Decimal128();
            bindVars.getFunction(0).getDecimal128(null, sink);
            Assert.assertFalse("non-null DECIMAL128 must not be flagged as null", sink.isNull());
            Assert.assertEquals(0x0011223344556677L, sink.getHigh());
            Assert.assertEquals(0x0123456789ABCDEFL, sink.getLow());
        });
    }

    /**
     * Regression: DECIMAL128 scale byte must be validated against Decimals.MAX_SCALE.
     * Prior to the fix the decoder forwarded the raw byte (0..255) into
     * {@code ColumnType.getDecimalType}, which guards the range with {@code assert} only.
     */
    @Test
    public void testDecodeDecimal128ScaleOutOfRange() throws Exception {
        runWithBuf(64, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNullBind(p, QwpConstants.TYPE_DECIMAL128);
            Unsafe.getUnsafe().putByte(p++, (byte) 200); // scale -- outside 0..76
            try {
                decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
                Assert.fail("expected QwpParseException for DECIMAL128 scale=200");
            } catch (QwpParseException expected) {
                Assert.assertTrue("error must mention scale: " + expected.getFlyweightMessage(),
                        expected.getFlyweightMessage().toString().contains("scale"));
            }
        });
    }

    /**
     * Regression for DECIMAL256 NULL bind: all four legs passing LONG_NULL produced a
     * different 256-bit value that {@link Decimal256#isNull()} does not recognise.
     */
    @Test
    public void testDecodeDecimal256NullBind() throws Exception {
        runWithBuf(64, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNullBind(p, QwpConstants.TYPE_DECIMAL256);
            Unsafe.getUnsafe().putByte(p++, (byte) 10); // scale
            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);

            Function f = bindVars.getFunction(0);
            Decimal256 sink = new Decimal256();
            f.getDecimal256(null, sink);
            Assert.assertTrue("DECIMAL256 NULL sentinel must be recognised as null", sink.isNull());
        });
    }

    /**
     * Regression: DECIMAL256 scale byte must be validated against Decimals.MAX_SCALE.
     * See {@link #testDecodeDecimal128ScaleOutOfRange} for the underlying rationale.
     */
    @Test
    public void testDecodeDecimal256ScaleOutOfRange() throws Exception {
        runWithBuf(64, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNullBind(p, QwpConstants.TYPE_DECIMAL256);
            Unsafe.getUnsafe().putByte(p++, (byte) 200); // scale -- outside 0..76
            try {
                decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
                Assert.fail("expected QwpParseException for DECIMAL256 scale=200");
            } catch (QwpParseException expected) {
                Assert.assertTrue("error must mention scale: " + expected.getFlyweightMessage(),
                        expected.getFlyweightMessage().toString().contains("scale"));
            }
        });
    }

    @Test
    public void testDecodeDecimal64NullBind() throws Exception {
        runWithBuf(64, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNullBind(p, QwpConstants.TYPE_DECIMAL64);
            Unsafe.getUnsafe().putByte(p++, (byte) 4); // scale
            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);

            Assert.assertEquals(Numbers.LONG_NULL, bindVars.getFunction(0).getDecimal64(null));
        });
    }

    /**
     * Regression: DECIMAL64 scale byte must be validated against Decimals.MAX_SCALE.
     * See {@link #testDecodeDecimal128ScaleOutOfRange} for the underlying rationale.
     */
    @Test
    public void testDecodeDecimal64ScaleOutOfRange() throws Exception {
        runWithBuf(64, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNullBind(p, QwpConstants.TYPE_DECIMAL64);
            Unsafe.getUnsafe().putByte(p++, (byte) 200); // scale -- outside 0..76
            try {
                decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
                Assert.fail("expected QwpParseException for DECIMAL64 scale=200");
            } catch (QwpParseException expected) {
                Assert.assertTrue("error must mention scale: " + expected.getFlyweightMessage(),
                        expected.getFlyweightMessage().toString().contains("scale"));
            }
        });
    }

    @Test
    public void testDecodeFloatAndDoubleBind() throws Exception {
        runWithBuf(128, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 2);
            long p = buf + totalLen;
            p = writeNonNullBind(p, QwpConstants.TYPE_FLOAT);
            Unsafe.getUnsafe().putInt(p, Float.floatToRawIntBits(3.14f));
            p += 4;
            p = writeNonNullBind(p, QwpConstants.TYPE_DOUBLE);
            Unsafe.getUnsafe().putLong(p, Double.doubleToRawLongBits(2.718281828));
            p += 8;
            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
            Assert.assertEquals(3.14f, bindVars.getFunction(0).getFloat(null), 0.0f);
            Assert.assertEquals(2.718281828, bindVars.getFunction(1).getDouble(null), 0.0);
        });
    }

    /**
     * Regression for GEOHASH bind: precision_bits of 0 or >60 must be rejected with a
     * QwpParseException, not propagated into ColumnType.getGeoHashTypeWithBits which
     * is assert-only.
     */
    @Test
    public void testDecodeGeohashBindRejectsOutOfRangePrecision() throws Exception {
        runWithBuf(64, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNullBind(p, QwpConstants.TYPE_GEOHASH);
            p = QwpVarint.encode(p, 0); // precision_bits = 0 -- invalid
            try {
                decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
                Assert.fail("expected QwpParseException for precision_bits=0");
            } catch (QwpParseException expected) {
                Assert.assertTrue("error must mention precision_bits: " + expected.getFlyweightMessage(),
                        expected.getFlyweightMessage().toString().contains("precision_bits"));
            }
        });
    }

    @Test
    public void testDecodeGeohashBindRejectsPrecisionAboveMax() throws Exception {
        runWithBuf(64, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNullBind(p, QwpConstants.TYPE_GEOHASH);
            p = QwpVarint.encode(p, ColumnType.GEOLONG_MAX_BITS + 1); // 61
            try {
                decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
                Assert.fail("expected QwpParseException for precision_bits=61");
            } catch (QwpParseException expected) {
                Assert.assertTrue(expected.getFlyweightMessage().toString().contains("precision_bits"));
            }
        });
    }

    @Test
    public void testDecodeGeohashBindRoundTripAtMaxPrecision() throws Exception {
        runWithBuf(64, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNonNullBind(p, QwpConstants.TYPE_GEOHASH);
            p = QwpVarint.encode(p, ColumnType.GEOLONG_MAX_BITS); // 60 bits => 8 bytes
            Unsafe.getUnsafe().putLong(p, 0x0FFF_FFFF_FFFF_FFFFL);
            p += 8;
            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
            Assert.assertEquals(0x0FFF_FFFF_FFFF_FFFFL, bindVars.getFunction(0).getGeoLong(null));
        });
    }

    @Test
    public void testDecodeLong256Bind() throws Exception {
        runWithBuf(128, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNonNullBind(p, QwpConstants.TYPE_LONG256);
            Unsafe.getUnsafe().putLong(p, 0x11L);
            p += 8;
            Unsafe.getUnsafe().putLong(p, 0x22L);
            p += 8;
            Unsafe.getUnsafe().putLong(p, 0x33L);
            p += 8;
            Unsafe.getUnsafe().putLong(p, 0x44L);
            p += 8;
            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
            io.questdb.std.Long256 v = bindVars.getFunction(0).getLong256A(null);
            Assert.assertEquals(0x11L, v.getLong0());
            Assert.assertEquals(0x22L, v.getLong1());
            Assert.assertEquals(0x33L, v.getLong2());
            Assert.assertEquals(0x44L, v.getLong3());
        });
    }

    @Test
    public void testDecodeMixedBinds() throws Exception {
        runWithBuf(256, (buf, bindVars, decoder) -> {
            byte[] sql = "SELECT ?, ?, ?".getBytes(StandardCharsets.UTF_8);
            byte[] s = "abc".getBytes(StandardCharsets.UTF_8);

            int len = writeQueryRequest(buf, 100, sql, 0, 3);
            long p = buf + len;

            p = writeNonNullBind(p, QwpConstants.TYPE_LONG);
            Unsafe.getUnsafe().putLong(p, 42L);
            p += 8;

            p = writeNonNullBind(p, QwpConstants.TYPE_VARCHAR);
            Unsafe.getUnsafe().putInt(p, 0);
            p += 4;
            Unsafe.getUnsafe().putInt(p, s.length);
            p += 4;
            for (byte b : s) Unsafe.getUnsafe().putByte(p++, b);

            p = writeNullBind(p, QwpConstants.TYPE_LONG);

            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
            Assert.assertEquals(100L, decoder.requestId);
            Assert.assertEquals(42L, bindVars.getFunction(0).getLong(null));
            Assert.assertEquals("abc", bindVars.getFunction(1).getStrA(null).toString());
            Assert.assertEquals(Numbers.LONG_NULL, bindVars.getFunction(2).getLong(null));
        });
    }

    @Test
    public void testDecodeNoBinds() throws Exception {
        runWithBuf(256, (buf, bindVars, decoder) -> {
            byte[] sql = "SELECT 1".getBytes(StandardCharsets.UTF_8);
            int len = writeQueryRequest(buf, 42, sql, 0, 0);
            decoder.decodeQueryRequest(buf, len, bindVars);
            Assert.assertEquals(42L, decoder.requestId);
            Assert.assertEquals(0L, decoder.initialCredit);
            Assert.assertEquals("SELECT 1", decoder.sql.toString());
        });
    }

    @Test
    public void testDecodeNullStringBind() throws Exception {
        runWithBuf(128, (buf, bindVars, decoder) -> {
            byte[] sql = "SELECT ?".getBytes(StandardCharsets.UTF_8);
            int len = writeQueryRequest(buf, 9, sql, 0, 1);
            long p = buf + len;
            p = writeNullBind(p, QwpConstants.TYPE_VARCHAR);
            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);

            Function bind0 = bindVars.getFunction(0);
            Assert.assertNotNull(bind0);
            Assert.assertNull("NULL string bind must surface as null", bind0.getStrA(null));
        });
    }

    @Test
    public void testDecodeStringBind() throws Exception {
        runWithBuf(256, (buf, bindVars, decoder) -> {
            byte[] sql = "SELECT ?".getBytes(StandardCharsets.UTF_8);
            byte[] bindStr = "hello".getBytes(StandardCharsets.UTF_8);

            int len = writeQueryRequest(buf, 7, sql, 0, 1);
            long p = buf + len;
            p = writeNonNullBind(p, QwpConstants.TYPE_VARCHAR);
            Unsafe.getUnsafe().putInt(p, 0);
            p += 4;
            Unsafe.getUnsafe().putInt(p, bindStr.length);
            p += 4;
            for (byte b : bindStr) Unsafe.getUnsafe().putByte(p++, b);

            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
            Assert.assertEquals(7L, decoder.requestId);
            Assert.assertEquals("hello", bindVars.getFunction(0).getStrA(null).toString());
        });
    }

    @Test
    public void testDecodeTimestampAndDateBind() throws Exception {
        runWithBuf(128, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 2);
            long p = buf + totalLen;
            p = writeNonNullBind(p, QwpConstants.TYPE_TIMESTAMP);
            Unsafe.getUnsafe().putLong(p, 1_700_000_000_000_000L);
            p += 8;
            p = writeNonNullBind(p, QwpConstants.TYPE_DATE);
            Unsafe.getUnsafe().putLong(p, 1_700_000_000_000L);
            p += 8;
            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);

            Function ts = bindVars.getFunction(0);
            Assert.assertEquals(1_700_000_000_000_000L, ts.getTimestamp(null));
            Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, ts.getType());
            Assert.assertEquals(1_700_000_000_000L, bindVars.getFunction(1).getDate(null));
        });
    }

    /**
     * Regression for TIMESTAMP_NANOS bind unit loss: before the fix, the decoder
     * routed TYPE_TIMESTAMP_NANOS through setTimestamp() (same as TYPE_TIMESTAMP),
     * which tagged the bind variable as TIMESTAMP_MICRO. Downstream coercion would
     * then multiply the value by 1000 when the placeholder is TIMESTAMP_NANO. The
     * type tag on the bind is the load-bearing distinction.
     */
    @Test
    public void testDecodeTimestampNanosBindIsTaggedAsNanos() throws Exception {
        runWithBuf(128, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNonNullBind(p, QwpConstants.TYPE_TIMESTAMP_NANOS);
            Unsafe.getUnsafe().putLong(p, 1_700_000_000_000_000_000L);
            p += 8;
            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);

            Function ts = bindVars.getFunction(0);
            Assert.assertEquals("TIMESTAMP_NANOS bind must tag as TIMESTAMP_NANO",
                    ColumnType.TIMESTAMP_NANO, ts.getType());
            Assert.assertEquals(1_700_000_000_000_000_000L, ts.getTimestamp(null));
        });
    }

    @Test
    public void testDecodeTimestampNanosNullBind() throws Exception {
        runWithBuf(64, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNullBind(p, QwpConstants.TYPE_TIMESTAMP_NANOS);
            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);

            Function ts = bindVars.getFunction(0);
            Assert.assertEquals(ColumnType.TIMESTAMP_NANO, ts.getType());
            Assert.assertEquals(Numbers.LONG_NULL, ts.getTimestamp(null));
        });
    }

    @Test
    public void testDecodeUuidBind() throws Exception {
        runWithBuf(128, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            p = writeNonNullBind(p, QwpConstants.TYPE_UUID);
            Unsafe.getUnsafe().putLong(p, 0xA0EE_0000_0000_0001L);
            p += 8;
            Unsafe.getUnsafe().putLong(p, 0xFFFF_FFFF_FFFF_FFFEL);
            p += 8;
            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
            Function f = bindVars.getFunction(0);
            Assert.assertEquals(0xA0EE_0000_0000_0001L, f.getLong128Lo(null));
            Assert.assertEquals(0xFFFF_FFFF_FFFF_FFFEL, f.getLong128Hi(null));
        });
    }

    @Test
    public void testDecodeVarcharBind() throws Exception {
        runWithBuf(256, (buf, bindVars, decoder) -> {
            byte[] sql = "SELECT ?".getBytes(StandardCharsets.UTF_8);
            byte[] bindStr = "world".getBytes(StandardCharsets.UTF_8);

            int len = writeQueryRequest(buf, 8, sql, 0, 1);
            long p = buf + len;
            p = writeNonNullBind(p, QwpConstants.TYPE_VARCHAR);
            Unsafe.getUnsafe().putInt(p, 0);
            p += 4;
            Unsafe.getUnsafe().putInt(p, bindStr.length);
            p += 4;
            for (byte b : bindStr) Unsafe.getUnsafe().putByte(p++, b);

            decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
            Assert.assertEquals(8L, decoder.requestId);
            io.questdb.std.str.Utf8Sequence got = bindVars.getFunction(0).getVarcharA(null);
            Assert.assertNotNull("non-null VARCHAR bind must not decode as null", got);
            Assert.assertEquals(bindStr.length, got.size());
            for (int i = 0; i < bindStr.length; i++) {
                Assert.assertEquals("byte " + i, bindStr[i], got.byteAt(i));
            }
        });
    }

    @Test
    public void testRejectsArrayBind() throws Exception {
        runWithBuf(128, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            Unsafe.getUnsafe().putByte(p++, QwpConstants.TYPE_DOUBLE_ARRAY);
            Unsafe.getUnsafe().putByte(p++, (byte) 0); // null_flag = 0
            try {
                decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
                Assert.fail("expected QwpParseException rejecting ARRAY bind");
            } catch (QwpParseException expected) {
                Assert.assertTrue(expected.getFlyweightMessage().toString().contains("ARRAY"));
            }
        });
    }

    /**
     * Regression: initial_credit must be rejected if negative. A varint with the sign
     * bit set would otherwise propagate to flow-control accounting.
     */
    @Test
    public void testRejectsNegativeInitialCredit() throws Exception {
        runWithBuf(64, (buf, bindVars, decoder) -> {
            byte[] sql = "OK".getBytes(StandardCharsets.UTF_8);
            long p = buf;
            Unsafe.getUnsafe().putByte(p++, QwpEgressMsgKind.QUERY_REQUEST);
            Unsafe.getUnsafe().putLong(p, 1L);
            p += 8;
            p = QwpVarint.encode(p, sql.length);
            for (byte b : sql) Unsafe.getUnsafe().putByte(p++, b);
            for (int i = 0; i < 9; i++) Unsafe.getUnsafe().putByte(p++, (byte) 0xFF);
            Unsafe.getUnsafe().putByte(p++, (byte) 0x01);
            try {
                decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
                Assert.fail("expected QwpParseException for negative initial_credit");
            } catch (QwpParseException expected) {
                Assert.assertTrue(expected.getFlyweightMessage().toString().contains("initial_credit"));
            }
        });
    }

    /**
     * Regression: a varint sql_len whose sign bit is set must be rejected, not allowed
     * past the bounds check via long-arithmetic wrap.
     */
    @Test
    public void testRejectsNegativeSqlLen() throws Exception {
        runWithBuf(64, (buf, bindVars, decoder) -> {
            long p = buf;
            Unsafe.getUnsafe().putByte(p++, QwpEgressMsgKind.QUERY_REQUEST);
            Unsafe.getUnsafe().putLong(p, 1L);
            p += 8;
            for (int i = 0; i < 9; i++) Unsafe.getUnsafe().putByte(p++, (byte) 0xFF);
            Unsafe.getUnsafe().putByte(p++, (byte) 0x01);
            try {
                decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
                Assert.fail("expected QwpParseException for negative sql_len");
            } catch (QwpParseException expected) {
                Assert.assertTrue(expected.getFlyweightMessage().toString().contains("sql_len"));
            }
        });
    }

    @Test
    public void testRejectsOversizedSqlLen() throws Exception {
        runWithBuf(32, (buf, bindVars, decoder) -> {
            long p = buf;
            Unsafe.getUnsafe().putByte(p++, QwpEgressMsgKind.QUERY_REQUEST);
            Unsafe.getUnsafe().putLong(p, 1L);
            p += 8;
            p = QwpVarint.encode(p, 1024); // way past buffer remainder
            try {
                decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
                Assert.fail("expected QwpParseException for oversized sql_len");
            } catch (QwpParseException expected) {
                Assert.assertTrue(expected.getFlyweightMessage().toString().contains("sql_len"));
            }
        });
    }

    @Test
    public void testRejectsTruncatedBindBody() throws Exception {
        runWithBuf(64, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            Unsafe.getUnsafe().putByte(p++, QwpConstants.TYPE_LONG);
            Unsafe.getUnsafe().putByte(p++, (byte) 0); // null_flag = 0 (non-null)
            // Missing the 8-byte long body.
            try {
                decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
                Assert.fail("expected QwpParseException for truncated LONG bind");
            } catch (QwpParseException expected) {
                Assert.assertTrue(expected.getFlyweightMessage().toString().contains("LONG"));
            }
        });
    }

    @Test
    public void testRejectsUnknownBindType() throws Exception {
        runWithBuf(64, (buf, bindVars, decoder) -> {
            int totalLen = writeBindScaffold(buf, 1);
            long p = buf + totalLen;
            Unsafe.getUnsafe().putByte(p++, (byte) 0x7F); // unassigned type code
            Unsafe.getUnsafe().putByte(p++, (byte) 0);
            try {
                decoder.decodeQueryRequest(buf, (int) (p - buf), bindVars);
                Assert.fail("expected QwpParseException for unknown bind type");
            } catch (QwpParseException expected) {
                Assert.assertTrue(expected.getFlyweightMessage().toString().contains("unsupported wire type"));
            }
        });
    }

    private static BindVariableServiceImpl newBindVars() {
        return new BindVariableServiceImpl(new DefaultTestCairoConfiguration(temp.getRoot().getAbsolutePath()));
    }

    /**
     * Allocates a native buffer, pre-writes a QUERY_REQUEST scaffold for N binds with
     * a fixed 8-byte SQL and bind_count = N, runs the callback, and frees the buffer.
     * All wrapped in assertMemoryLeak.
     */
    private static void runWithBuf(int bufSize, DecoderBody body) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            BindVariableServiceImpl bindVars = newBindVars();
            QwpEgressRequestDecoder decoder = new QwpEgressRequestDecoder();
            long buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            try {
                body.run(buf, bindVars, decoder);
            } finally {
                Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static int writeBindScaffold(long buf, int bindCount) {
        byte[] sql = "SELECT 1".getBytes(StandardCharsets.UTF_8);
        return writeQueryRequest(buf, 1L, sql, 0L, bindCount);
    }

    private static long writeNonNullBind(long p, byte type) {
        Unsafe.getUnsafe().putByte(p++, type);
        Unsafe.getUnsafe().putByte(p++, (byte) 0);
        return p;
    }

    private static long writeNullBind(long p, byte type) {
        Unsafe.getUnsafe().putByte(p++, type);
        Unsafe.getUnsafe().putByte(p++, (byte) 1); // null_flag
        Unsafe.getUnsafe().putByte(p++, (byte) 1); // bitmap bit 0 set
        return p;
    }

    /**
     * Writes msg_kind + requestId + sql_len + sql + initial_credit + bind_count.
     * Bind bodies are appended by the caller.
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

    @FunctionalInterface
    private interface DecoderBody {
        void run(long buf, BindVariableServiceImpl bindVars, QwpEgressRequestDecoder decoder) throws Exception;
    }
}
