/*******************************************************************************
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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cutlass.pgwire.PGMessageProcessingException;
import io.questdb.cutlass.pgwire.PGUtils;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;

/**
 * Cross-checks the decimal branches of {@link PGUtils#calculateColumnBinSize} against an independent model of the
 * PostgreSQL NUMERIC binary layout.
 * <p>
 * The end-to-end pgwire tests cannot do this job: pgjdbc drives its read loop entirely from the per-field length
 * prefixes and ignores the DataRow envelope length, so a wrong size computed here is invisible to it. Clients that
 * do frame on the declared length (Npgsql, libpq, asyncpg) see a corrupt stream instead. This test is therefore the
 * only thing in the suite that fails when the size arithmetic is wrong rather than merely missing.
 */
public class PGUtilsTest extends AbstractCairoTest {
    private static final BigInteger MASK_256 = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);
    private static final int PG_NUMERIC_EMPTY_BIN_SIZE = Integer.BYTES + 4 * Short.BYTES;
    private static final BigInteger[] POWERS_OF_TEN = new BigInteger[Decimals.MAX_PRECISION + 1];

    static {
        POWERS_OF_TEN[0] = BigInteger.ONE;
        for (int i = 1; i < POWERS_OF_TEN.length; i++) {
            POWERS_OF_TEN[i] = POWERS_OF_TEN[i - 1].multiply(BigInteger.TEN);
        }
    }

    @Test
    public void testDecimalBinSizeMatchesPgNumericLayout() throws Exception {
        assertMemoryLeak(() -> {
            final DecimalRecord record = new DecimalRecord();
            for (int precision = 1; precision <= Decimals.MAX_PRECISION; precision++) {
                for (int scale = 0; scale <= precision; scale++) {
                    final int columnType = ColumnType.getDecimalType(precision, scale);
                    assertBinSize(record, columnType, BigInteger.ZERO);
                    for (int power = 0; power < precision; power++) {
                        // 10^power and 10^(power+1)-1 are the two ends of the run of values sharing this leading
                        // digit position, so together they straddle every base-10000 group boundary.
                        final BigInteger powerOfTen = POWERS_OF_TEN[power];
                        final BigInteger allNines = POWERS_OF_TEN[power + 1].subtract(BigInteger.ONE);
                        assertBinSize(record, columnType, powerOfTen);
                        assertBinSize(record, columnType, powerOfTen.negate());
                        assertBinSize(record, columnType, allNines);
                        assertBinSize(record, columnType, allNines.negate());
                    }
                }
            }
        });
    }

    @Test
    public void testDecimalNullBinSize() throws Exception {
        assertMemoryLeak(() -> {
            final DecimalRecord record = new DecimalRecord();
            final int[] precisions = {2, 4, 9, 18, 38, Decimals.MAX_PRECISION};
            for (int i = 0, n = precisions.length; i < n; i++) {
                final int columnType = ColumnType.getDecimalType(precisions[i], 1);
                record.ofNull(ColumnType.tagOf(columnType));
                Assert.assertEquals(
                        "NULL size mismatch [precision=" + precisions[i] + ']',
                        Integer.BYTES,
                        calculateBinSize(record, columnType)
                );
            }
        });
    }

    private static void assertBinSize(DecimalRecord record, int columnType, BigInteger unscaled) throws PGMessageProcessingException {
        final int precision = ColumnType.getDecimalPrecision(columnType);
        final int scale = ColumnType.getDecimalScale(columnType);
        record.of(unscaled, scale);
        final int actual = calculateBinSize(record, columnType);
        final int expected = expectedBinSize(unscaled, scale);
        if (expected != actual) {
            Assert.fail("size mismatch [precision=" + precision + ", scale=" + scale + ", unscaled=" + unscaled
                    + ", expected=" + expected + ", actual=" + actual + ']');
        }
    }

    private static int calculateBinSize(DecimalRecord record, int columnType) throws PGMessageProcessingException {
        return PGUtils.calculateColumnBinSize(null, sqlExecutionContext, record, 0, columnType, 0, Long.MAX_VALUE, -1);
    }

    /**
     * Models the wire size of one NUMERIC field independently of the production formula: an int32 length prefix, the
     * four int16 header words (ndigits, weight, sign, dscale) and one int16 per base-10000 digit group.
     * <p>
     * The groups are aligned on the decimal point, so the integer part pads on the left and the fraction - always
     * carried to the full declared scale - pads on the right. QuestDB's encoder starts writing at the first non-zero
     * group and then writes every group down to the end of the scale; unlike PostgreSQL itself it does not strip
     * trailing zero groups, so the model keeps them.
     */
    private static int expectedBinSize(BigInteger unscaled, int scale) {
        if (unscaled.signum() == 0) {
            return PG_NUMERIC_EMPTY_BIN_SIZE;
        }
        final String digits = unscaled.abs().toString();
        final int intLen = Math.max(0, digits.length() - scale);
        final StringBuilder sb = new StringBuilder();
        for (int i = 0, n = (4 - intLen % 4) % 4; i < n; i++) {
            sb.append('0');
        }
        sb.append(digits, 0, intLen);
        for (int i = 0, n = scale - (digits.length() - intLen); i < n; i++) {
            sb.append('0');
        }
        sb.append(digits, intLen, digits.length());
        for (int i = 0, n = (4 - scale % 4) % 4; i < n; i++) {
            sb.append('0');
        }

        final int groupCount = sb.length() / 4;
        int firstGroup = 0;
        while (firstGroup < groupCount && isZeroGroup(sb, firstGroup)) {
            firstGroup++;
        }
        return PG_NUMERIC_EMPTY_BIN_SIZE + (groupCount - firstGroup) * Short.BYTES;
    }

    private static boolean isZeroGroup(StringBuilder digits, int group) {
        for (int i = group * 4, n = i + 4; i < n; i++) {
            if (digits.charAt(i) != '0') {
                return false;
            }
        }
        return true;
    }

    private static class DecimalRecord implements Record {
        private long hh;
        private long hl;
        private long lh;
        private long ll;
        private int scale;

        @Override
        public void getDecimal128(int col, Decimal128 sink) {
            sink.of(lh, ll, scale);
        }

        @Override
        public short getDecimal16(int col) {
            return (short) ll;
        }

        @Override
        public void getDecimal256(int col, Decimal256 sink) {
            sink.of(hh, hl, lh, ll, scale);
        }

        @Override
        public int getDecimal32(int col) {
            return (int) ll;
        }

        @Override
        public long getDecimal64(int col) {
            return ll;
        }

        @Override
        public byte getDecimal8(int col) {
            return (byte) ll;
        }

        void of(BigInteger unscaled, int scale) {
            final BigInteger bits = unscaled.and(MASK_256);
            this.ll = bits.longValue();
            this.lh = bits.shiftRight(64).longValue();
            this.hl = bits.shiftRight(128).longValue();
            this.hh = bits.shiftRight(192).longValue();
            this.scale = scale;
        }

        void ofNull(short typeTag) {
            this.hh = 0;
            this.hl = 0;
            this.lh = 0;
            this.ll = 0;
            this.scale = 0;
            switch (typeTag) {
                case ColumnType.DECIMAL8 -> this.ll = Decimals.DECIMAL8_NULL;
                case ColumnType.DECIMAL16 -> this.ll = Decimals.DECIMAL16_NULL;
                case ColumnType.DECIMAL32 -> this.ll = Decimals.DECIMAL32_NULL;
                case ColumnType.DECIMAL64 -> this.ll = Decimals.DECIMAL64_NULL;
                case ColumnType.DECIMAL128 -> {
                    this.lh = Decimals.DECIMAL128_HI_NULL;
                    this.ll = Decimals.DECIMAL128_LO_NULL;
                }
                case ColumnType.DECIMAL256 -> {
                    this.hh = Decimals.DECIMAL256_HH_NULL;
                    this.hl = Decimals.DECIMAL256_HL_NULL;
                    this.lh = Decimals.DECIMAL256_LH_NULL;
                    this.ll = Decimals.DECIMAL256_LL_NULL;
                }
                default -> throw new AssertionError("not a decimal type tag: " + typeTag);
            }
        }
    }
}
