package io.questdb.std;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * Decimal128 - a mutable decimal number implementation. The value is a signed number with
 * two's complement representation.
 * <p>
 * This class represents decimal numbers with a fixed scale (number of decimal places)
 * using 128-bit integer arithmetic for precise calculations. All operations are
 * performed in-place to eliminate object allocation and improve performance.
 * </p>
 * <p>
 * This type tries to but doesn't follow IEEE 754; one of the main goals is using 128 bits to store
 * the sign and trailing significant field (T).
 * Using 1 bit for the sign, we have 127 bits for T, which gives us 38 digits of precision.
 * </p>
 */
public class Decimal128 implements Sinkable {
    /**
     * Maximum allowed scale (number of decimal places)
     */
    public static final int MAX_SCALE = 38;
    public static final Decimal128 MAX_VALUE = new Decimal128(5421010862427522170L, 687399551400673279L);
    public static final Decimal128 MIN_VALUE = new Decimal128(-5421010862427522171L, -687399551400673279L);
    static final long LONG_MASK = 0xffffffffL;
    private static final long[] TEN_POWERS_TABLE_HIGH = { // High 64-bit part of the ten powers table from 10^20 to 10^38
            5L, // 10^20
            54L, // 10^21
            542L, // 10^22
            5421L, // 10^23
            54210L, // 10^24
            542101L, // 10^25
            5421010L, // 10^26
            54210108L, // 10^27
            542101086L, // 10^28
            5421010862L, // 10^29
            54210108624L, // 10^30
            542101086242L, // 10^31
            5421010862427L, // 10^32
            54210108624275L, // 10^33
            542101086242752L, // 10^34
            5421010862427522L, // 10^35
            54210108624275221L, // 10^36
            542101086242752217L, // 10^37
            5421010862427522170L, // 10^38
    };
    private static final long[] TEN_POWERS_TABLE_LOW = { // Low 64-bit part of the ten powers table from 10^0 to 10^38
            1L, // 10^0
            10L, // 10^1
            100L, // 10^2
            1000L, // 10^3
            10000L, // 10^4
            100000L, // 10^5
            1000000L, // 10^6
            10000000L, // 10^7
            100000000L, // 10^8
            1000000000L, // 10^9
            10000000000L, // 10^10
            100000000000L, // 10^11
            1000000000000L, // 10^12
            10000000000000L, // 10^13
            100000000000000L, // 10^14
            1000000000000000L, // 10^15
            10000000000000000L, // 10^16
            100000000000000000L, // 10^17
            1000000000000000000L, // 10^18
            -8446744073709551616L, // 10^19
            7766279631452241920L, // 10^20
            3875820019684212736L, // 10^21
            1864712049423024128L, // 10^22
            200376420520689664L, // 10^23
            2003764205206896640L, // 10^24
            1590897978359414784L, // 10^25
            -2537764290115403776L, // 10^26
            -6930898827444486144L, // 10^27
            4477988020393345024L, // 10^28
            7886392056514347008L, // 10^29
            5076944270305263616L, // 10^30
            -4570789518076018688L, // 10^31
            -8814407033341083648L, // 10^32
            4089650035136921600L, // 10^33
            4003012203950112768L, // 10^34
            3136633892082024448L, // 10^35
            -5527149226598858752L, // 10^36
            68739955140067328L, // 10^37
            687399551400673280L, // 10^38
    };
    // High 64-bit part of the maximum value that 10^x can multiply without overflowing
    private static final long[] TEN_POWERS_TABLE_THRESHOLD_HIGH = {
            9223372036854775807L, // 10^0
            922337203685477580L, // 10^1
            92233720368547758L, // 10^2
            9223372036854775L, // 10^3
            922337203685477L, // 10^4
            92233720368547L, // 10^5
            9223372036854L, // 10^6
            922337203685L, // 10^7
            92233720368L, // 10^8
            9223372036L, // 10^9
            922337203L, // 10^10
            92233720L, // 10^11
            9223372L, // 10^12
            922337L, // 10^13
            92233L, // 10^14
            9223L, // 10^15
            922L, // 10^16
            92L, // 10^17
            9L, // 10^18
    };
    // Low 64-bit part of the maximum value that 10^x can multiply without overflowing
    private static final long[] TEN_POWERS_TABLE_THRESHOLD_LOW = {
            -9223372036854775808L, // 10^0
            -4611686018427387904L, // 10^1
            1383505805528216371L, // 10^2
            -3550998234189088687L, // 10^3
            -7733797452902729516L, // 10^4
            -4462728560032183275L, // 10^5
            -4135621670745128651L, // 10^6
            8809809869780262942L, // 10^7
            -8342391049876749514L, // 10^8
            -2678913512358630113L, // 10^9
            -5801914573348728497L, // 10^10
            6798506172148947796L, // 10^11
            679850617214894779L, // 10^12
            3757333876463399801L, // 10^13
            -5158289834466525505L, // 10^14
            6862868646037168095L, // 10^15
            6220310086716582294L, // 10^16
            4311379823413568552L, // 10^17
            4120486797083267178L, // 10^18
            -1432625727662628444L, // 10^19
            1701411834604692317L, // 10^20
            170141183460469231L, // 10^21
            17014118346046923L, // 10^22
            1701411834604692L, // 10^23
            170141183460469L, // 10^24
            17014118346046L, // 10^25
            1701411834604L, // 10^26
            170141183460L, // 10^27
            17014118346L, // 10^28
            1701411834L, // 10^29
            170141183L, // 10^30
            17014118L, // 10^31
            1701411L, // 10^32
            170141L, // 10^33
            17014L, // 10^34
            1701L, // 10^35
            170L, // 10^36
            17L, // 10^37
            1L, // 10^38
    };
    // holders for in-place mutations that doesn't have a mutable structure available
    private static final ThreadLocal<Decimal128> tl = new ThreadLocal<>(Decimal128::new);
    private final DecimalKnuthDivider divider = new DecimalKnuthDivider();
    private long high;  // High 64 bits
    private long low;   // Low 64 bits
    private int scale;  // Number of decimal places

    /**
     * Default constructor - creates zero with scale 0
     */
    public Decimal128() {
        this.high = 0;
        this.low = 0;
        this.scale = 0;
    }

    public Decimal128(long high, long low) {
        this.high = high;
        this.low = low;
        this.scale = 0;
    }

    /**
     * Constructor with initial values.
     *
     * @param high  the high 64 bits of the decimal value
     * @param low   the low 64 bits of the decimal value
     * @param scale the number of decimal places
     * @throws NumericException if scale is invalid
     */
    public Decimal128(long high, long low, int scale) {
        validateScale(scale);
        this.high = high;
        this.low = low;
        this.scale = scale;
        if (hasUnsignOverflowed()) {
            throw NumericException.instance().put("Overflow in multiplication: result exceeds maximum precision");
        }
    }

    /**
     * Add two Decimal128 numbers and store the result in sink
     *
     * @param a    First operand
     * @param b    Second operand
     * @param sink Destination for the result
     */
    public static void add(Decimal128 a, Decimal128 b, Decimal128 sink) {
        sink.copyFrom(a);
        sink.add(b);
    }

    /**
     * Divide two Decimal128 numbers and store the result in sink (a / b -> sink)
     * Uses optimal precision calculation up to MAX_SCALE
     *
     * @param a            First operand (dividend)
     * @param b            Second operand (divisor)
     * @param sink         Destination for the result
     * @param scale        Result scale
     * @param roundingMode Rounding Mode used to round the result
     */
    public static void divide(Decimal128 a, Decimal128 b, Decimal128 sink, int scale, RoundingMode roundingMode) {
        sink.copyFrom(a);
        sink.divide(b, scale, roundingMode);
    }

    /**
     * Create a Decimal128 from a BigDecimal value.
     *
     * @param value the BigDecimal value to convert
     * @return a new Decimal128 representing the BigDecimal value
     * @throws NumericException if scale is invalid
     */
    public static Decimal128 fromBigDecimal(BigDecimal value) {
        if (value == null) {
            throw NumericException.instance().put("BigDecimal value cannot be null");
        }

        int scale = value.scale();
        long hi;
        long lo;
        BigInteger bi = value.unscaledValue();
        if (scale < 0) {
            // We don't support negative scale, we must transform the value to match
            // our format.
            bi = bi.multiply(new BigInteger("10").pow(-scale));
            scale = 0;
        }
        if (bi.bitLength() > 127) {
            throw NumericException.instance().put("Overflow: BigDecimal value exceeds 128-bit capacity during conversion");
        }
        lo = bi.longValue();
        hi = bi.shiftRight(64).longValue();
        validateScale(scale);
        return new Decimal128(hi, lo, scale);
    }

    /**
     * Create a Decimal128 from a double value, using HALF_UP when rounding is needed.
     *
     * @param value the double value to convert
     * @param scale the number of decimal places
     * @return a new Decimal128 representing the double value
     * @throws NumericException if scale is invalid
     */
    public static Decimal128 fromDouble(double value, int scale) {
        validateScale(scale);
        BigDecimal bd = new BigDecimal(value).setScale(scale, RoundingMode.HALF_UP);
        return fromBigDecimal(bd);
    }

    /**
     * Create a Decimal128 from a long value.
     *
     * @param value the long value to convert
     * @param scale the number of decimal places
     * @return a new Decimal128 representing the long value
     * @throws NumericException if scale is invalid
     */
    public static Decimal128 fromLong(long value, int scale) {
        validateScale(scale);
        long h = value < 0 ? -1L : 0L;
        return new Decimal128(h, value, scale);
    }

    /**
     * Check whether the given decimal128 is null or not.
     *
     * @return true if the value is null.
     */
    public static boolean isNull(long hi, long lo) {
        return hi == Decimals.DECIMAL128_HI_NULL && lo == Decimals.DECIMAL128_LO_NULL;
    }

    /**
     * Calculate modulo of two Decimal128 numbers and store the result in sink (a % b -> sink)
     *
     * @param a    First operand (dividend)
     * @param b    Second operand (divisor)
     * @param sink Destination for the result
     */
    public static void modulo(Decimal128 a, Decimal128 b, Decimal128 sink) {
        sink.copyFrom(a);
        sink.modulo(b);
    }

    /**
     * Multiply two Decimal128 numbers and store the result in sink
     *
     * @param a    First operand
     * @param b    Second operand
     * @param sink Destination for the result
     */
    public static void multiply(Decimal128 a, Decimal128 b, Decimal128 sink) {
        sink.copyFrom(a);
        sink.multiply(b);
    }

    /**
     * Negate a Decimal128 number and store the result in sink
     *
     * @param a    Input operand to negate
     * @param sink Destination for the result
     */
    public static void negate(Decimal128 a, Decimal128 sink) {
        sink.copyFrom(a);
        sink.negate();
    }

    /**
     * Subtract two Decimal128 numbers and store the result in sink (a - b -> sink)
     *
     * @param a    First operand (minuend)
     * @param b    Second operand (subtrahend)
     * @param sink Destination for the result
     */
    public static void subtract(Decimal128 a, Decimal128 b, Decimal128 sink) {
        sink.copyFrom(a);
        sink.subtract(b);
    }

    /**
     * Add another Decimal128 to this one (in-place)
     *
     * @param other The Decimal128 to add
     */
    public void add(Decimal128 other) {
        add(this, this.high, this.low, this.scale, other.high, other.low, other.scale);
    }

    public int compareTo(long otherHi, long otherLo) {
        if (this.high != otherHi) {
            return Long.compare(this.high, otherHi);
        }
        return Long.compareUnsigned(this.low, otherLo);
    }

    /**
     * Compare this to another Decimal128 (handles different scales).
     *
     * @param other the Decimal128 to compare with
     * @return -1 if this decimal is less than other, 0 if equal, 1 if greater than other
     */
    public int compareTo(Decimal128 other) {
        boolean aNeg = isNegative();
        boolean bNeg = other.isNegative();
        if (aNeg != bNeg) {
            return aNeg ? -1 : 1;
        }

        // Stores the coefficient to apply to the response, if both numbers are negative, then
        // we have to reverse the result
        int diffQ = aNeg ? -1 : 1;

        if (this.scale == other.scale) {
            // Same scale - direct comparison
            if (this.high != other.high) {
                return Long.compare(this.high, other.high);
            }
            return Long.compareUnsigned(this.low, other.low);
        }

        // We need to make both operands positive to detect overflows when scaling them
        long aH = this.high;
        long aL = this.low;
        long bH = other.high;
        long bL = other.low;
        if (aNeg) {
            aL = ~aL + 1;
            aH = ~aH + (aL == 0 ? 1L : 0L);

            // Negate b
            bL = ~bL + 1;
            bH = ~bH + (bL == 0 ? 1L : 0L);
        }

        // Different scales - need to align for comparison
        // We'll scale up the one with smaller scale
        Decimal128 holder = tl.get();
        if (this.scale < other.scale) {
            holder.of(aH, aL, this.scale);
            holder.multiplyByPowerOf10InPlace(other.scale - this.scale);
            aH = holder.high;
            aL = holder.low;
        } else {
            holder.of(bH, bL, other.scale);
            holder.multiplyByPowerOf10InPlace(this.scale - other.scale);
            bH = holder.high;
            bL = holder.low;
        }

        // Compare this with scaled other
        if (aH != bH) {
            return Long.compare(aH, bH) * diffQ;
        }
        return Long.compareUnsigned(aL, bL) * diffQ;
    }

    /**
     * Copy values from another Decimal128
     */
    public void copyFrom(Decimal128 source) {
        this.high = source.high;
        this.low = source.low;
        this.scale = source.scale;
    }

    /**
     * Divide this Decimal128 by another (in-place) with optimal precision
     *
     * @param divisor      The Decimal128 to divide by
     * @param scale        The decimal place
     * @param roundingMode The Rounding mode to use if the remainder is non-zero
     */
    public void divide(Decimal128 divisor, int scale, RoundingMode roundingMode) {
        divide(divisor.high, divisor.low, divisor.scale, scale, roundingMode);
    }

    /**
     * Divide this Decimal128 by another (in-place) with optimal precision
     *
     * @param scale        The decimal place
     * @param roundingMode The Rounding mode to use if the remainder is non-zero
     */
    public void divide(long divisorHigh, long divisorLow, int divisorScale, int scale, RoundingMode roundingMode) {
        validateScale(scale);
        // Compute the delta: how much power of 10 we should raise either the dividend or divisor.
        int delta = scale + (divisorScale - this.scale);

        // Fail early if we're sure to overflow.
        if (delta > 0 && (this.scale + delta) > MAX_SCALE) {
            throw NumericException.instance().put("Overflow in division: resulting scale would exceed maximum (" + MAX_SCALE + ")");
        }

        final boolean negResult = (divisorHigh < 0) ^ isNegative();

        // We're allowed to modify dividend as it will contain our result
        if (isNegative()) {
            negate();
        }

        // We cannot do the same for divisor, so we must reuse negate logic directly in our code to avoid
        // allocations.
        if (divisorHigh < 0) {
            divisorLow = ~divisorLow + 1;
            divisorHigh = ~divisorHigh + (divisorLow == 0 ? 1 : 0);
        }

        if (delta > 0) {
            // raise dividend to 10^delta
            multiplyByPowerOf10InPlace(delta);
        } else if (delta < 0) {
            // raise divisor to 10^(-delta), as we cannot modify the divisor, we use dividend to do it.
            long dividendHigh = this.high;
            long dividendLow = this.low;
            this.high = divisorHigh;
            this.low = divisorLow;
            multiplyByPowerOf10InPlace(-delta);
            divisorHigh = this.high;
            divisorLow = this.low;
            this.high = dividendHigh;
            this.low = dividendLow;
        }

        divider.clear();
        divider.ofDividend(high, low);
        divider.ofDivisor(divisorHigh, divisorLow);
        divider.divide(negResult, roundingMode);
        divider.sink(this, scale);

        if (negResult) {
            negate();
        }

    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Decimal128)) return false;
        Decimal128 other = (Decimal128) obj;
        return this.high == other.high &&
                this.low == other.low &&
                this.scale == other.scale;
    }

    /**
     * Gets the high 64 bits of the 128-bit decimal value.
     *
     * @return the high 64 bits of the decimal value
     */
    public long getHigh() {
        return high;
    }

    /**
     * Gets the low 64 bits of the 128-bit decimal value.
     *
     * @return the low 64 bits of the decimal value
     */
    public long getLow() {
        return low;
    }

    /**
     * Gets the scale (number of decimal places) of this decimal value.
     *
     * @return the scale of this decimal value
     */
    public int getScale() {
        return scale;
    }

    /**
     * Returns a hash code for this decimal value.
     *
     * @return a hash code value for this object
     */
    @Override
    public int hashCode() {
        return Long.hashCode(high) ^ Long.hashCode(low) ^ Integer.hashCode(scale);
    }

    /**
     * Check if this number is negative
     */
    public boolean isNegative() {
        return high < 0;
    }

    /**
     * Check if this number is zero
     */
    public boolean isZero() {
        return high == 0 && low == 0;
    }

    /**
     * Calculate modulo in-place.
     *
     * @param divisor the divisor
     * @throws NumericException if divisor is zero
     */
    public void modulo(Decimal128 divisor) {
        if (divisor.isZero()) {
            throw NumericException.instance().put("Division by zero");
        }

        // Result scale should be the larger of the two scales
        int resultScale = Math.max(this.scale, divisor.scale);

        // Use simple repeated subtraction for modulo: a % b = a - (a / b) * b
        // First compute integer division (a / b)
        // We store this for later usage
        long thisH = this.high;
        long thisL = this.low;
        int thisScale = this.scale;

        this.divide(divisor, 0, RoundingMode.DOWN);

        // Now compute this * divisor
        this.multiply(divisor);

        long qH = this.high;
        long qL = this.low;
        int qScale = this.scale;
        // restore this as a
        this.set(thisH, thisL, thisScale);
        // Finally compute remainder: a - (a / b) * b
        this.subtract(qH, qL, qScale);

        // Handle scale adjustment
        if (this.scale != resultScale) {
            rescale0(resultScale);
        }
    }

    /**
     * Multiply this Decimal128 by another (in-place)
     *
     * @param other The Decimal128 to multiply by
     */
    public void multiply(Decimal128 other) {
        multiply(other.high, other.low, other.scale);
    }

    /**
     * Negate this number in-place
     */
    public void negate() {
        // Two's complement: invert all bits and add 1
        this.low = ~this.low + 1;
        this.high = ~this.high + (this.low == 0 ? 1 : 0);
    }

    /**
     * Sets this Decimal128 to the specified 128-bit value and scale.
     *
     * @param high  the high 64 bits (bits 64-127)
     * @param low   the low 64 bits (bits 0-63)
     * @param scale the number of decimal places
     */
    public void of(long high, long low, int scale) {
        this.high = high;
        this.low = low;
        this.scale = scale;
    }

    /**
     * Set this Decimal128 to the null value.
     */
    public void ofNull() {
        high = Decimals.DECIMAL128_HI_NULL;
        low = Decimals.DECIMAL128_LO_NULL;
        scale = 0;
    }

    /**
     * Rescale this Decimal128 in place
     *
     * @param newScale The new scale (must be >= current scale)
     */
    public void rescale(int newScale) {
        validateScale(newScale);
        if (newScale < this.scale) {
            throw NumericException.instance().put("New scale must be >= current scale");
        }
        rescale0(newScale);
    }

    /**
     * Round this Decimal128 to the specified scale using the given rounding mode.
     * This method performs in-place rounding without requiring a divisor.
     *
     * @param targetScale  the desired scale (number of decimal places)
     * @param roundingMode the rounding mode to use
     * @throws NumericException if targetScale is invalid
     * @throws NumericException if roundingMode is UNNECESSARY and rounding is required
     */
    public void round(int targetScale, RoundingMode roundingMode) {
        if (targetScale == this.scale) {
            // No rounding needed
            return;
        }

        validateScale(targetScale);

        // UNNECESSARY mode should be a complete no-op
        if (roundingMode == RoundingMode.UNNECESSARY) {
            return;
        }

        // Handle zero specially
        if (isZero()) {
            this.scale = targetScale;
            return;
        }

        if (this.scale < targetScale) {
            boolean isNegative = isNegative();
            if (isNegative) {
                negate();
            }

            // Need to increase scale (add trailing zeros)
            int scaleIncrease = targetScale - this.scale;
            multiplyByPowerOf10InPlace(scaleIncrease);
            this.scale = targetScale;

            if (isNegative) {
                negate();
            }
            return;
        }

        divide(0, 1, 0, targetScale, roundingMode);
    }

    /**
     * Set values directly.
     *
     * @param high  the high 64 bits of the decimal value
     * @param low   the low 64 bits of the decimal value
     * @param scale the number of decimal places
     */
    public void set(long high, long low, int scale) {
        this.high = high;
        this.low = low;
        this.scale = scale;
    }

    /**
     * Set from a long value.
     *
     * @param value the long value to set
     * @param scale the number of decimal places
     */
    public void setFromLong(long value, int scale) {
        validateScale(scale);
        this.high = value < 0 ? -1L : 0L;
        this.low = value;
        this.scale = scale;
    }

    /**
     * Subtract another Decimal128 from this one (in-place)
     *
     * @param other The Decimal128 to subtract
     */
    public void subtract(Decimal128 other) {
        subtract(other.high, other.low, other.scale);
    }

    /**
     * Subtract another Decimal128 from this one (in-place)
     *
     * @param bH     the high 64 bits of the other Decimal128
     * @param bL     the low 64 bits of the other Decimal128
     * @param bScale the number of decimal places of the other Decimal128
     */
    public void subtract(long bH, long bL, int bScale) {
        // Negate other and perform addition
        if (bH != 0 || bL != 0) {
            // Two's complement: invert all bits and add 1
            bL = ~bL + 1;
            bH = ~bH + (bL == 0 ? 1 : 0);

            add(this, this.high, this.low, this.scale, bH, bL, bScale);
        }
    }

    /**
     * Convert to BigDecimal with full precision
     *
     * @return BigDecimal representation of this Decimal128
     */
    public java.math.BigDecimal toBigDecimal() {
        // Direct BigInteger construction is more efficient than string conversion
        byte[] bytes = new byte[16];
        // Convert to big-endian byte array
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (this.high >>> (56 - 8 * i));
            bytes[8 + i] = (byte) (this.low >>> (56 - 8 * i));
        }

        BigInteger unscaled = new BigInteger(bytes);
        return new BigDecimal(unscaled, this.scale);
    }

    /**
     * Convert to double (may lose precision).
     *
     * @return double representation
     */
    public double toDouble() {
        return toBigDecimal().doubleValue();
    }

    /**
     * Writes the string representation of this Decimal128 to the specified CharSink.
     * The output format is a plain decimal string without scientific notation.
     *
     * @param sink the CharSink to write to
     */
    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        BigDecimal bd = toBigDecimal();
        sink.put(bd.toPlainString());
    }

    /**
     * Returns the string representation of this Decimal128.
     * The output format is a plain decimal string without scientific notation.
     *
     * @return string representation of this decimal number
     */
    @Override
    public String toString() {
        return toBigDecimal().toPlainString();
    }

    /**
     * Generic function to make a 128-bit addition.
     *
     * @param result Decimal128 that will store the result of the operation
     * @param aH     High 64-bit part of the first operand.
     * @param aL     Low 64-bit part of the first operand.
     * @param aScale Scale of the first operand.
     * @param bH     High 64-bit part of the second operand.
     * @param bL     Low 64-bit part of the second operand.
     * @param bScale Scale of the second operand.
     */
    private static void add(Decimal128 result, long aH, long aL, int aScale, long bH, long bL, int bScale) {
        result.scale = aScale;
        if (aScale < bScale) {
            // We need to rescale A to the same scale as B
            result.high = aH;
            result.low = aL;
            result.rescale0(bScale);
            aH = result.high;
            aL = result.low;
        } else if (aScale > bScale) {
            // We need to rescale B to the same scale as A
            result.high = bH;
            result.low = bL;
            result.scale = bScale;
            result.rescale0(aScale);
            bH = result.high;
            bL = result.low;
        }

        // Perform 128-bit addition
        long sumLow = aL + bL;

        // Check for carry
        long carry = hasCarry(aL, bL, sumLow) ? 1 : 0;

        result.low = sumLow;
        try {
            result.high = Math.addExact(aH, Math.addExact(bH, carry));
        } catch (ArithmeticException e) {
            throw NumericException.instance().put("Overflow in addition: result exceeds 128-bit capacity");
        }
        if (result.hasOverflowed()) {
            throw NumericException.instance().put("Overflow in addition: result exceeds maximum precision");
        }
    }

    /**
     * Returns the max high 64-bits part that can fit in the given precision.
     */
    private static long getMaxHi(int precision) {
        return 0;
    }

    /**
     * Returns the max low 64-bits part that can fit in the given precision.
     */
    private static long getMaxLo(int precision) {
        return 0;
    }

    /**
     * Returns the min high 64-bits part that can fit in the given precision.
     */
    private static long getMinHi(int precision) {
        return 0;
    }

    /**
     * Returns the min low 64-bits part that can fit in the given precision.
     */
    private static long getMinLo(int precision) {
        return 0;
    }

    /**
     * Compare two longs as if they were unsigned.
     * Returns true iff one is bigger than two.
     */
    private static boolean unsignedLongCompare(long one, long two) {
        return (one + Long.MIN_VALUE) > (two + Long.MIN_VALUE);
    }

    /**
     * Validates that the scale is within allowed bounds
     */
    private static void validateScale(int scale) {
        // Use unsigned comparison for faster bounds check
        if (Integer.compareUnsigned(scale, MAX_SCALE) > 0) {
            throw NumericException.instance().put("Scale must be between 0 and " + MAX_SCALE + ", got: " + scale);
        }
    }

    private boolean hasOverflowed() {
        return compareTo(MAX_VALUE.high, MAX_VALUE.low) > 0 ||
                compareTo(MIN_VALUE.high, MIN_VALUE.low) < 0;
    }

    private boolean hasUnsignOverflowed() {
        return compareTo(MAX_VALUE.high, MAX_VALUE.low) > 0;
    }

    /**
     * Multiply this Decimal128 by another (in-place)
     *
     * @param bH     the high 64 bits of the other Decimal128
     * @param bL     the low 64 bits of the other Decimal128
     * @param bScale the number of decimal places of the other Decimal128
     */
    private void multiply(long bH, long bL, int bScale) {
        // Result scale is sum of scales
        int resultScale = this.scale + bScale;

        // Save the original signs before we modify anything
        boolean thisNegative = this.isNegative();
        boolean bNegative = bH < 0;

        // Convert to positive values for multiplication algorithm
        if (thisNegative) {
            negate();
        }

        // Get absolute value of other
        if (bNegative) {
            // Negate other's values
            bL = ~bL + 1;
            bH = ~bH + (bL == 0 ? 1 : 0);
        }

        if (bH == 0 && bL >= 0) {
            if (high == 0 && low >= 0) {
                multiply64By64Bit(bL);
            } else {
                multiplyBy64Bit(bL);
            }
        } else {
            multiplyBy128Bit(bH, bL);
        }

        if (hasUnsignOverflowed()) {
            throw NumericException.instance().put("Overflow in multiplication: result exceeds maximum precision");
        }

        // Handle sign - use the saved original signs
        boolean negative = (thisNegative != bNegative);
        if (negative) {
            // Negate result
            this.low = ~this.low + 1;
            this.high = ~this.high + (this.low == 0 ? 1 : 0);
        }

        this.scale = resultScale;
    }

    /**
     * Multiply this unsigned 64-bit value by an unsigned 64-bit value in place
     */
    private void multiply64By64Bit(long multiplier) {
        // Perform 128-bit × 64-bit multiplication
        // Result is at most 192 bits, but we keep only the lower 128 bits

        // Split multiplier into two 32-bit parts
        long m1 = multiplier >>> 32;
        long m0 = multiplier & 0xFFFFFFFFL;

        // Split this into four 32-bit parts
        long a1 = low >>> 32;
        long a0 = low & 0xFFFFFFFFL;

        // Compute partial products
        long p0 = a0 * m0;
        long p1 = a0 * m1 + a1 * m0;
        long p2 = a1 * m1;

        // Accumulate results
        long r0 = p0 & 0xFFFFFFFFL;
        long r1 = (p0 >>> 32) + (p1 & 0xFFFFFFFFL);
        long r2 = (r1 >>> 32) + (p1 >>> 32) + (p2 & 0xFFFFFFFFL);
        long r3 = (r2 >>> 32) + (p2 >>> 32);

        this.low = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.high = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
    }

    /**
     * Multiply this unsigned 128-bit value by an unsigned 128-bit value in place
     */
    private void multiplyBy128Bit(long multiplierHigh, long multiplierLow) {
        // Perform multiplication using the algorithm from Decimal128
        // This is complex but avoids allocations
        long a3 = this.high >>> 32;
        long a2 = this.high & 0xFFFFFFFFL;
        long a1 = this.low >>> 32;
        long a0 = this.low & 0xFFFFFFFFL;

        long b3 = multiplierHigh >>> 32;
        long b2 = multiplierHigh & 0xFFFFFFFFL;
        long b1 = multiplierLow >>> 32;
        long b0 = multiplierLow & 0xFFFFFFFFL;

        // Compute all partial products
        long p00 = a0 * b0;
        long p01 = a0 * b1;
        long p02 = a0 * b2;
        long p03 = a0 * b3;
        long p10 = a1 * b0;
        long p11 = a1 * b1;
        long p12 = a1 * b2;
        long p13 = a1 * b3;
        long p20 = a2 * b0;
        long p21 = a2 * b1;
        long p22 = a2 * b2;
        long p23 = a2 * b3;
        long p30 = a3 * b0;
        long p31 = a3 * b1;
        long p32 = a3 * b2;
        long p33 = a3 * b3;

        // Gather results into 128-bit result
        long r0 = (p00 & LONG_MASK);
        long r1 = (p00 >>> 32) + (p01 & LONG_MASK) + (p10 & LONG_MASK);
        long r2 = (r1 >>> 32) + (p01 >>> 32) + (p10 >>> 32) +
                (p02 & LONG_MASK) + (p11 & LONG_MASK) + (p20 & LONG_MASK);
        long r3 = (r2 >>> 32) + (p02 >>> 32) + (p11 >>> 32) + (p20 >>> 32) +
                (p03 & LONG_MASK) + (p12 & LONG_MASK) + (p21 & LONG_MASK) + (p30 & LONG_MASK);
        long r4 = (r3 >>> 32) + (p03 >>> 32) + (p12 >>> 32) + (p21 >>> 32) + (p30 >>> 32) +
                (p13 & LONG_MASK) + (p22 & LONG_MASK) + (p31 & LONG_MASK);
        long r5 = (r4 >>> 32) + (p13 >>> 32) + (p22 >>> 32) + (p31 >>> 32) +
                (p23 & LONG_MASK) + (p32 & LONG_MASK);
        long r6 = (r5 >>> 32) + (p23 >>> 32) + (p32 >>> 32) +
                (p33 & LONG_MASK);

        if ((r3 >>> 31) != 0 || r4 != 0 || r5 != 0 || r6 != 0) {
            throw NumericException.instance().put("Overflow in multiplication (128-bit × 128-bit): product exceeds 128-bit capacity");
        }

        this.low = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.high = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
    }

    /**
     * Multiply this unsigned 128-bit value by an unsigned 128-bit value in place without checking for overflow
     */
    private void multiplyBy128BitUnchecked(long multiplierHigh, long multiplierLow) {
        // Perform multiplication using the algorithm from Decimal128
        // This is complex but avoids allocations
        long a3 = this.high >>> 32;
        long a2 = this.high & 0xFFFFFFFFL;
        long a1 = this.low >>> 32;
        long a0 = this.low & 0xFFFFFFFFL;

        long b3 = multiplierHigh >>> 32;
        long b2 = multiplierHigh & 0xFFFFFFFFL;
        long b1 = multiplierLow >>> 32;
        long b0 = multiplierLow & 0xFFFFFFFFL;

        // Compute all partial products
        long p00 = a0 * b0;
        long p01 = a0 * b1;
        long p02 = a0 * b2;
        long p03 = a0 * b3;
        long p10 = a1 * b0;
        long p11 = a1 * b1;
        long p12 = a1 * b2;
        long p20 = a2 * b0;
        long p21 = a2 * b1;
        long p30 = a3 * b0;

        // Gather results into 128-bit result
        long r0 = (p00 & LONG_MASK);
        long r1 = (p00 >>> 32) + (p01 & LONG_MASK) + (p10 & LONG_MASK);
        long r2 = (r1 >>> 32) + (p01 >>> 32) + (p10 >>> 32) +
                (p02 & LONG_MASK) + (p11 & LONG_MASK) + (p20 & LONG_MASK);
        long r3 = (r2 >>> 32) + (p02 >>> 32) + (p11 >>> 32) + (p20 >>> 32) +
                (p03 & LONG_MASK) + (p12 & LONG_MASK) + (p21 & LONG_MASK) + (p30 & LONG_MASK);

        this.low = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.high = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
    }

    /**
     * Multiply this unsigned 128-bit value by an unsigned 64-bit value in place
     */
    private void multiplyBy64Bit(long multiplier) {
        // Perform 128-bit × 64-bit multiplication
        // Result is at most 192 bits, but we keep only the lower 128 bits

        // Split multiplier into two 32-bit parts
        long m1 = multiplier >>> 32;
        long m0 = multiplier & 0xFFFFFFFFL;

        // Split this into four 32-bit parts
        long a3 = high >>> 32;
        long a2 = high & 0xFFFFFFFFL;
        long a1 = low >>> 32;
        long a0 = low & 0xFFFFFFFFL;

        // Compute partial products
        long p0 = a0 * m0;
        long p1 = a0 * m1 + a1 * m0;
        long p2 = a1 * m1 + a2 * m0;
        long p3 = a2 * m1 + a3 * m0;
        long p4 = a3 * m1;

        // Accumulate results
        long r0 = p0 & 0xFFFFFFFFL;
        long r1 = (p0 >>> 32) + (p1 & 0xFFFFFFFFL);
        long r2 = (r1 >>> 32) + (p1 >>> 32) + (p2 & 0xFFFFFFFFL);
        long r3 = (r2 >>> 32) + (p2 >>> 32) + (p3 & 0xFFFFFFFFL);
        long r4 = (r3 >>> 32) + (p3 >>> 32) + p4;

        // Check for overflow: if r4 has significant bits, the result exceeds 128 bits
        if (r4 != 0 || (r3 >> 31) != 0) {
            throw NumericException.instance().put("Overflow in multiplication (128-bit × 64-bit): product exceeds 128-bit capacity");
        }

        this.low = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.high = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
    }

    /**
     * Multiply this unsigned 128-bit value by an unsigned 64-bit value in place without checking for overflow
     */
    private void multiplyBy64BitUnchecked(long multiplier) {
        // Perform 128-bit × 64-bit multiplication
        // Result is at most 192 bits, but we keep only the lower 128 bits

        // Split multiplier into two 32-bit parts
        long m1 = multiplier >>> 32;
        long m0 = multiplier & 0xFFFFFFFFL;

        // Split this into four 32-bit parts
        long a3 = high >>> 32;
        long a2 = high & 0xFFFFFFFFL;
        long a1 = low >>> 32;
        long a0 = low & 0xFFFFFFFFL;

        // Compute partial products
        long p0 = a0 * m0;
        long p1 = a0 * m1 + a1 * m0;
        long p2 = a1 * m1 + a2 * m0;
        long p3 = a2 * m1 + a3 * m0;

        // Accumulate results
        long r0 = p0 & 0xFFFFFFFFL;
        long r1 = (p0 >>> 32) + (p1 & 0xFFFFFFFFL);
        long r2 = (r1 >>> 32) + (p1 >>> 32) + (p2 & 0xFFFFFFFFL);
        long r3 = (r2 >>> 32) + (p2 >>> 32) + (p3 & 0xFFFFFFFFL);

        this.low = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.high = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
    }

    /**
     * Multiply this (unsigned) by 10^n in place
     */
    private void multiplyByPowerOf10InPlace(int n) {
        if (n <= 0 || (high == 0 && low == 0)) {
            return;
        }

        // For larger powers, break down into smaller chunks, first check the threshold to ensure that we won't overflow
        // and then apply either a fast multiplyBy64 or multiplyBy128 depending on high/low.
        // The bound checks for these tables already happens at the beginning of the method.
        final long thresholdH = n >= TEN_POWERS_TABLE_THRESHOLD_HIGH.length ? 0 : TEN_POWERS_TABLE_THRESHOLD_HIGH[n];
        final long thresholdL = TEN_POWERS_TABLE_THRESHOLD_LOW[n];

        if (high > thresholdH || (high == thresholdH && unsignedLongCompare(low, thresholdL))) {
            throw NumericException.instance().put("Overflow in scale adjustment: multiplying by 10^" + n + " exceeds 128-bit capacity");
        }

        final long multiplierHigh = n >= 20 ? TEN_POWERS_TABLE_HIGH[n - 20] : 0L;
        final long multiplierLow = TEN_POWERS_TABLE_LOW[n];
        if (multiplierHigh == 0L && multiplierLow >= 0L) {
            if (high == 0 && low >= 0L) {
                multiply64By64Bit(multiplierLow);
            } else {
                multiplyBy64BitUnchecked(multiplierLow);
            }
        } else {
            multiplyBy128BitUnchecked(multiplierHigh, multiplierLow);
        }
    }

    private void rescale0(int resultScale) {
        int scaleUp = resultScale - this.scale;
        boolean isNegative = isNegative();
        if (isNegative) {
            negate();
        }
        multiplyByPowerOf10InPlace(scaleUp);
        if (isNegative) {
            negate();
        }
        this.scale = resultScale;
    }

    /**
     * Check if addition resulted in a carry
     * When adding two unsigned numbers a + b = sum, carry occurs iff sum < a (or sum < b)
     * This works because:
     * - No carry: sum = a + b, so sum >= a and sum >= b
     * - Carry: sum = a + b - 2^64, so sum < a and sum < b
     */
    static boolean hasCarry(long a, @SuppressWarnings("unused") long b, long sum) {
        // We can check against either a or b - both work
        // Using a for consistency, b parameter kept for clarity
        return Long.compareUnsigned(sum, a) < 0;
    }
}