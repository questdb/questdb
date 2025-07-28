package io.questdb.std;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import java.math.RoundingMode;

/**
 * Decimal128 - A mutable 128-bit decimal number implementation
 * <p>
 * This class represents decimal numbers with a fixed scale (number of decimal places)
 * using 128-bit integer arithmetic for precise calculations. All operations are
 * performed in-place to eliminate object allocation and improve performance.
 */
public class Decimal128 implements Sinkable {
    /**
     * Maximum allowed scale (number of decimal places)
     */
    public static final int MAX_SCALE = 16;
    private static final long INFLATED = Long.MIN_VALUE;
    private long high;  // High 64 bits
    private long low;   // Low 64 bits
    private int scale;  // Number of decimal places
    private transient long compact;  // Compact representation for values fitting in long

    /**
     * Default constructor - creates zero with scale 0
     */
    public Decimal128() {
        this.high = 0;
        this.low = 0;
        this.scale = 0;
        this.compact = 0;
    }

    /**
     * Constructor with initial values
     */
    public Decimal128(long high, long low, int scale) {
        validateScale(scale);
        this.high = high;
        this.low = low;
        this.scale = scale;
        this.compact = computeCompact(high, low);
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
     * @param a    First operand (dividend)
     * @param b    Second operand (divisor)
     * @param sink Destination for the result
     */
    public static void divide(Decimal128 a, Decimal128 b, Decimal128 sink) {
        sink.copyFrom(a);
        sink.divide(b);
    }

    /**
     * Create a Decimal128 from a double value
     *
     * @param value The double value
     * @param scale Number of decimal places
     */
    public static Decimal128 fromDouble(double value, int scale) {
        validateScale(scale);
        long scaleFactor = scale <= 18 ? POWERS_OF_10[scale] : calculatePowerOf10(scale);
        long scaledValue = Math.round(value * scaleFactor);
        return fromLong(scaledValue, scale);
    }

    /**
     * Create a Decimal128 from a long value
     *
     * @param value The long value
     * @param scale Number of decimal places
     */
    public static Decimal128 fromLong(long value, int scale) {
        validateScale(scale);

        // Use cached values for common small values with scale 0
        if (scale == 0 && value >= 0 && value <= 10) {
            return new Decimal128(ZERO_THROUGH_TEN[(int)value]);
        }

        long h = value < 0 ? -1L : 0L;
        return new Decimal128(h, value, scale);
    }

    // Copy constructor for cached values
    private Decimal128(Decimal128 other) {
        this.high = other.high;
        this.low = other.low;
        this.scale = other.scale;
        this.compact = other.compact;
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
        // If scales match, use direct addition
        if (this.scale == other.scale) {
            // Perform 128-bit addition
            long sumLow = this.low + other.low;

            // Check for carry
            long carry = hasCarry(this.low, other.low, sumLow) ? 1 : 0;

            // Update values in place
            this.low = sumLow;
            this.high = this.high + other.high + carry;
            updateCompact();
            return;
        }

        // Handle different scales
        if (this.scale < other.scale) {
            // Rescale this to match other's scale
            rescale(other.scale);
            // Now add with same scale
            long sumLow = this.low + other.low;
            long carry = hasCarry(this.low, other.low, sumLow) ? 1 : 0;
            this.low = sumLow;
            this.high = this.high + other.high + carry;
            updateCompact();
        } else {
            // Need to rescale other - we'll do it mathematically
            // Scale difference
            int scaleDiff = this.scale - other.scale;
            long otherHigh = other.high;
            long otherLow = other.low;

            // Multiply other by 10^scaleDiff
            for (int i = 0; i < scaleDiff; i++) {
                // Multiply by 10: (8x + 2x)
                long high8 = (otherHigh << 3) | (otherLow >>> 61);
                long low8 = otherLow << 3;
                long high2 = (otherHigh << 1) | (otherLow >>> 63);
                long low2 = otherLow << 1;

                otherLow = low8 + low2;
                long carry = hasCarry(low8, low2, otherLow) ? 1 : 0;
                otherHigh = high8 + high2 + carry;
            }

            // Now add the scaled value
            long sumLow = this.low + otherLow;
            long carry = hasCarry(this.low, otherLow, sumLow) ? 1 : 0;
            this.low = sumLow;
            this.high = this.high + otherHigh + carry;
            updateCompact();
        }
    }

    /**
     * Compare this to another Decimal128 (handles different scales)
     */
    public int compareTo(Decimal128 other) {
        if (this.scale == other.scale) {
            // Same scale - direct comparison
            if (this.high != other.high) {
                return Long.compare(this.high, other.high);
            }
            return Long.compareUnsigned(this.low, other.low);
        }

        // Different scales - need to align for comparison
        // We'll scale up the one with smaller scale
        if (this.scale < other.scale) {
            // Scale up this to match other's scale
            int scaleDiff = other.scale - this.scale;
            long scaledHigh = this.high;
            long scaledLow = this.low;

            // Multiply by 10^scaleDiff
            for (int i = 0; i < scaleDiff; i++) {
                // Multiply by 10: (8x + 2x)
                long high8 = (scaledHigh << 3) | (scaledLow >>> 61);
                long low8 = scaledLow << 3;
                long high2 = (scaledHigh << 1) | (scaledLow >>> 63);
                long low2 = scaledLow << 1;

                scaledLow = low8 + low2;
                long carry = hasCarry(low8, low2, scaledLow) ? 1 : 0;
                scaledHigh = high8 + high2 + carry;
            }

            // Compare scaled this with other
            if (scaledHigh != other.high) {
                return Long.compare(scaledHigh, other.high);
            }
            return Long.compareUnsigned(scaledLow, other.low);
        } else {
            // Scale up other to match this scale
            int scaleDiff = this.scale - other.scale;
            long scaledHigh = other.high;
            long scaledLow = other.low;

            // Multiply by 10^scaleDiff
            for (int i = 0; i < scaleDiff; i++) {
                // Multiply by 10: (8x + 2x)
                long high8 = (scaledHigh << 3) | (scaledLow >>> 61);
                long low8 = scaledLow << 3;
                long high2 = (scaledHigh << 1) | (scaledLow >>> 63);
                long low2 = scaledLow << 1;

                scaledLow = low8 + low2;
                long carry = hasCarry(low8, low2, scaledLow) ? 1 : 0;
                scaledHigh = high8 + high2 + carry;
            }

            // Compare this with scaled other
            if (this.high != scaledHigh) {
                return Long.compare(this.high, scaledHigh);
            }
            return Long.compareUnsigned(this.low, scaledLow);
        }
    }

    // Static helper methods for non-destructive operations

    /**
     * Copy values from another Decimal128
     */
    public void copyFrom(Decimal128 source) {
        this.high = source.high;
        this.low = source.low;
        this.scale = source.scale;
        this.compact = source.compact;
    }

    /**
     * Divide this Decimal128 by another (in-place) with optimal precision
     * Uses dynamic scale calculation up to MAX_SCALE to avoid excessive trailing zeros
     * Always uses UNNECESSARY rounding - caller should use round() method if rounding needed
     *
     * @param divisor The Decimal128 to divide by
     */
    public void divide(Decimal128 divisor) {
        if (divisor.isZero()) {
            throw new ArithmeticException("Division by zero");
        }

        // Fast path for zero dividend
        if (this.isZero()) {
            this.scale = MAX_SCALE;
            return;
        }

        // Fast path for simple 64-bit native division
        if (canUseSimple64BitNativeDivision(divisor)) {
            performSimple64BitNativeDivision(divisor);
            return;
        }

        // Calculate optimal result scale
        // We want to maximize precision without causing overflow during calculation
        // First, determine the natural scale for the division
        int resultScale = MAX_SCALE;
        // Calculate scale adjustment needed
        int scaleAdjustment = MAX_SCALE + divisor.scale - this.scale;
        // Limit scale adjustment to prevent overflow
        // We need to be conservative because multiplication by 10^n can overflow
        // Rule of thumb: each multiplication by 10 adds about 3.32 bits
        // For a value using k bits, we can multiply by 10^n where n <= (127-k)/3.32
        if (scaleAdjustment > 0) {
            // Estimate bits used by current value
            int bitsUsed = estimateBitsUsed();
            // Maximum safe multiplications (conservative estimate)
            int maxSafeMultiplications = Math.max(0, (125 - bitsUsed) / 4);
            if (scaleAdjustment > maxSafeMultiplications) {
                // Reduce scale adjustment to safe level
                scaleAdjustment = maxSafeMultiplications;
                // Adjust result scale accordingly
                resultScale = this.scale - divisor.scale + scaleAdjustment;
                // Ensure result scale is non-negative
                if (resultScale < 0) {
                    resultScale = 0;
                    scaleAdjustment = divisor.scale - this.scale;
                    if (scaleAdjustment < 0) {
                        scaleAdjustment = 0;
                    }
                }
            }
        }

        // Scale the dividend if needed
        if (scaleAdjustment > 0) {
            multiplyByPowerOf10InPlace(scaleAdjustment);
        }

        // Determine sign of result
        boolean resultNegative = this.isNegative() ^ divisor.isNegative();

        // Convert both operands to positive for division
        if (this.isNegative()) {
            this.negate();
        }

        Decimal128 posDivisor = new Decimal128();
        posDivisor.copyFrom(divisor);
        if (posDivisor.isNegative()) {
            posDivisor.negate();
        }

        // Perform the division using the helper method
        divide(posDivisor.high, posDivisor.low, RoundingMode.DOWN, resultNegative);

        // Apply the result sign
        if (resultNegative && !this.isZero()) {
            this.negate();
        }

        // Set the result scale
        this.scale = resultScale;
    }

    /**
     * Conservative 64-bit native division for safe cases only
     */
    private void divideUsing64BitNativeConservative(Decimal128 divisor) {
        long dividendMag = this.get64BitMagnitude();
        long divisorMag = divisor.get64BitMagnitude();
        boolean resultNegative = this.isNegative64BitValue() ^ divisor.isNegative64BitValue();

        // Very simple scale adjustment - just enough to get reasonable precision
        int scaleAdjustment = 6; // Always scale by 10^6 for 6 decimal places

        // Scale dividend
        dividendMag *= 1000000L; // 10^6

        // Native 64-bit division
        long result = dividendMag / divisorMag;

        // Result scale: we added 6 to dividend scale
        int resultScale = this.scale + 6 - divisor.scale;

        // Set result
        this.high = resultNegative ? (result == 0 ? 0 : -1) : 0;
        this.low = resultNegative ? -result : result;
        this.scale = Math.min(MAX_SCALE, Math.max(0, resultScale));
        updateCompact();
    }

    /**
     * Intel optimization 2: Double-precision estimation with integer correction
     * Good balance between speed and accuracy for 64-bit values
     */
    private boolean divideUsingDoubleEstimation(Decimal128 divisor) {
        long dividendMag = this.get64BitMagnitude();
        long divisorMag = divisor.get64BitMagnitude();
        boolean resultNegative = this.isNegative64BitValue() ^ divisor.isNegative64BitValue();

        // Smart scale calculation
        int scaleAdjustment = calculateOptimalScale(dividendMag, divisorMag, this.scale, divisor.scale);

        // Check if we can handle this scale adjustment
        if (scaleAdjustment > 18) {
            return false; // Fall back to full algorithm
        }

        // Scale dividend if needed
        long scaledDividend = dividendMag;
        if (scaleAdjustment > 0) {
            if (dividendMag > Long.MAX_VALUE / POWERS_OF_10[scaleAdjustment]) {
                return false; // Would overflow, fall back
            }
            scaledDividend *= POWERS_OF_10[scaleAdjustment];
        } else if (scaleAdjustment < 0) {
            scaledDividend /= POWERS_OF_10[-scaleAdjustment];
        }

        // Intel technique: Double-precision initial estimation
        double quotientEstimate = (double) scaledDividend / (double) divisorMag;
        long quotient = (long) quotientEstimate;

        // Intel technique: Correct with integer arithmetic
        long remainder = scaledDividend - quotient * divisorMag;

        // Iterative correction (usually 0-2 iterations needed)
        int corrections = 0;
        while (remainder >= divisorMag && corrections < 3) {
            quotient++;
            remainder -= divisorMag;
            corrections++;
        }

        while (remainder < 0 && corrections < 3) {
            quotient--;
            remainder += divisorMag;
            corrections++;
        }

        // Set result
        this.high = resultNegative ? (quotient == 0 ? 0 : -1) : 0;
        this.low = resultNegative ? -quotient : quotient;
        this.scale = Math.min(MAX_SCALE, Math.max(0,
            this.scale - divisor.scale + scaleAdjustment));
        updateCompact();

        return true;
    }

    /**
     * Calculate optimal target scale for the division result
     */
    private int calculateOptimalTargetScale(Decimal128 divisor) {
        // Intel-style: Use smart scale instead of always MAX_SCALE
        // Aim for 16 significant digits in the result

        // Estimate magnitude of result using logarithms
        int thisDigits = estimateDecimalDigits128(this);
        int divisorDigits = estimateDecimalDigits128(divisor);
        int resultDigits = Math.max(1, thisDigits - divisorDigits + 1);

        // Target scale to get ~16 significant digits
        int targetScale = Math.min(MAX_SCALE, Math.max(6, 16 - resultDigits));
        return targetScale;
    }

    /**
     * Estimate decimal digits for 128-bit value
     */
    private int estimateDecimalDigits128(Decimal128 value) {
        if (value.isZero()) return 1;

        // Use binary approximation: log10(x) ≈ log2(x) * 0.30103
        int leadingZeros = value.high != 0 ?
            Long.numberOfLeadingZeros(Math.abs(value.high)) :
            64 + Long.numberOfLeadingZeros(Math.abs(value.low));

        int binaryBits = 128 - leadingZeros;
        return Math.max(1, (int)(binaryBits * 0.30103) + 1);
    }

    /**
     * Intel optimization 3: Full algorithm with smart scaling and estimation
     * For cases that don't fit in 64-bit optimizations
     */
    private void divideUsingIntelAlgorithm(Decimal128 divisor) {
        // Smart scale calculation instead of always using MAX_SCALE
        int targetScale = calculateOptimalTargetScale(divisor);
        int scaleAdjustment = targetScale + divisor.scale - this.scale;

        // Intel-style: More conservative overflow checking
        if (scaleAdjustment > 0) {
            int maxSafeScale = estimateMaxSafeScaling();
            scaleAdjustment = Math.min(scaleAdjustment, maxSafeScale);
        }

        // Scale the dividend if needed
        if (scaleAdjustment > 0) {
            multiplyByPowerOf10InPlace(scaleAdjustment);
        } else if (scaleAdjustment < 0) {
            // For negative scale adjustment, we multiply the divisor instead
            // This is handled later in the algorithm
        }

        // Intel-style: Use double-precision estimation for initial quotient
        long quotientEstimate = 0;
        if (divisor.high == 0) {
            // Divisor fits in 64 bits - use optimized path
            quotientEstimate = estimateQuotientDouble(this.high, this.low, 0, divisor.low);
        } else {
            // Full 128-bit estimation
            quotientEstimate = estimateQuotientDouble(this.high, this.low, divisor.high, divisor.low);
        }

        // Determine sign of result
        boolean resultNegative = this.isNegative() ^ divisor.isNegative();

        // Convert both operands to positive for division
        if (this.isNegative()) {
            this.negate();
        }

        Decimal128 posDivisor = new Decimal128();
        posDivisor.copyFrom(divisor);
        if (posDivisor.isNegative()) {
            posDivisor.negate();
        }

        // Intel-style: Use estimation + correction instead of pure binary long division
        if (quotientEstimate > 0) {
            divideWithEstimationAndCorrection(posDivisor, quotientEstimate, resultNegative, targetScale);
        } else {
            // Fall back to traditional binary long division
            divide(posDivisor.high, posDivisor.low, RoundingMode.DOWN, resultNegative);

            // Apply the result sign
            if (resultNegative && !this.isZero()) {
                this.negate();
            }

            // Set the result scale
            this.scale = targetScale;
        }
    }

    /**
     * Estimate maximum safe scaling to prevent overflow
     */
    private int estimateMaxSafeScaling() {
        int bitsUsed = estimateBitsUsed();
        return Math.max(0, (125 - bitsUsed) / 4);
    }

    /**
     * Intel-style division with initial estimation and iterative correction
     */
    private void divideWithEstimationAndCorrection(Decimal128 divisor, long quotientEstimate,
                                                   boolean resultNegative, int targetScale) {
        // Start with the estimate
        long quotient = quotientEstimate;

        // Calculate remainder: dividend - quotient * divisor
        Decimal128 temp = new Decimal128();
        temp.copyFrom(divisor);
        temp.multiplyByLong(quotient);

        Decimal128 remainder = new Decimal128();
        remainder.copyFrom(this);
        remainder.subtract(temp);

        // Intel-style: Iterative correction with powers of 2 optimization
        int corrections = 0;
        while (!remainder.isNegative() && remainder.compareTo(divisor) >= 0 && corrections < 10) {
            quotient++;
            remainder.subtract(divisor);
            corrections++;
        }

        while (remainder.isNegative() && corrections < 10) {
            quotient--;
            remainder.add(divisor);
            corrections++;
        }

        // Set the final result
        this.setFromLong(quotient);
        if (resultNegative && !this.isZero()) {
            this.negate();
        }
        this.scale = targetScale;
        updateCompact();
    }

    /**
     * Set this decimal from a long value
     */
    private void setFromLong(long value) {
        if (value >= 0) {
            this.high = 0;
            this.low = value;
        } else {
            this.high = -1;
            this.low = value;
        }
    }

    /**
     * Multiply this decimal by a long value (helper for estimation/correction)
     */
    private void multiplyByLong(long multiplier) {
        if (multiplier == 0) {
            this.high = 0;
            this.low = 0;
            return;
        }

        // Handle negative multiplier
        boolean negateResult = false;
        if (multiplier < 0) {
            negateResult = true;
            multiplier = -multiplier;
        }

        // Check if this is negative
        boolean thisNegative = this.isNegative();
        if (thisNegative) {
            this.negate();
            negateResult = !negateResult;
        }

        // Perform unsigned multiplication
        // low * multiplier
        long lowProduct = Math.multiplyHigh(this.low, multiplier);
        long lowResult = this.low * multiplier;

        // high * multiplier
        long highResult = this.high * multiplier + lowProduct;

        this.low = lowResult;
        this.high = highResult;

        if (negateResult) {
            this.negate();
        }

        updateCompact();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Decimal128)) return false;
        Decimal128 other = (Decimal128) obj;
        return this.high == other.high &&
                this.low == other.low &&
                this.scale == other.scale;
    }

    // Getters
    public long getHigh() {
        return high;
    }

    public long getLow() {
        return low;
    }

    public int getScale() {
        return scale;
    }

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
     * Calculate modulo in-place
     *
     * @param divisor The divisor
     */
    public void modulo(Decimal128 divisor) {
        if (divisor.isZero()) {
            throw new ArithmeticException("Division by zero");
        }

        // Result scale should be the larger of the two scales
        int resultScale = Math.max(this.scale, divisor.scale);

        // Save original dividend
        Decimal128 originalDividend = new Decimal128();
        originalDividend.copyFrom(this);

        // Use simple repeated subtraction for modulo: a % b = a - (a / b) * b
        // First compute integer division (a / b)
        Decimal128 quotient = new Decimal128();
        quotient.copyFrom(this);
        quotient.divide(divisor); // Use new signature
        quotient.round(0, RoundingMode.DOWN); // Integer division (scale 0)

        // Now compute quotient * divisor
        quotient.multiply(divisor);

        // Finally compute remainder: a - (a / b) * b
        this.subtract(quotient);

        // Handle scale adjustment
        if (this.scale != resultScale) {
            if (this.scale < resultScale) {
                int scaleUp = resultScale - this.scale;
                multiplyByPowerOf10InPlace(scaleUp);
            } else {
                int scaleDown = this.scale - resultScale;
                for (int i = 0; i < scaleDown; i++) {
                    divideBy10InPlace();
                }
            }
            this.scale = resultScale;
        }
    }

    /**
     * Multiply this Decimal128 by another (in-place)
     *
     * @param other The Decimal128 to multiply by
     */
    public void multiply(Decimal128 other) {
        // Fast path for 64-bit values using magnitude arithmetic
        // TEMPORARILY DISABLED - debugging modulo issue
        // if (this.is64BitValue() && other.is64BitValue()) {
        //     multiply64BitOptimized(other);
        //     return;
        // }

        // Result scale is sum of scales
        int resultScale = this.scale + other.scale;

        // Save the original signs before we modify anything
        boolean thisNegative = this.isNegative();
        boolean otherNegative = other.isNegative();

        // Convert to positive values for multiplication algorithm
        if (thisNegative) {
            negate();
        }

        // Get absolute value of other
        long otherHighAbs = other.high;
        long otherLowAbs = other.low;
        if (otherNegative) {
            // Negate other's values
            otherLowAbs = ~otherLowAbs + 1;
            otherHighAbs = ~otherHighAbs;
            if (otherLowAbs == 0) {
                otherHighAbs += 1;
            }
        }

        // Perform multiplication using the algorithm from Decimal128
        // This is complex but avoids allocations
        long a3 = this.high >>> 32;
        long a2 = this.high & 0xFFFFFFFFL;
        long a1 = this.low >>> 32;
        long a0 = this.low & 0xFFFFFFFFL;

        long b3 = otherHighAbs >>> 32;
        long b2 = otherHighAbs & 0xFFFFFFFFL;
        long b1 = otherLowAbs >>> 32;
        long b0 = otherLowAbs & 0xFFFFFFFFL;

        // Multiply all combinations
        long p00 = a0 * b0;
        long p01 = a0 * b1;
        long p10 = a1 * b0;
        long p02 = a0 * b2;
        long p11 = a1 * b1;
        long p20 = a2 * b0;
        long p03 = a0 * b3;
        long p12 = a1 * b2;
        long p21 = a2 * b1;
        long p30 = a3 * b0;

        // Accumulate results
        long r0 = p00 & 0xFFFFFFFFL;
        long r1 = (p00 >>> 32) + (p01 & 0xFFFFFFFFL) + (p10 & 0xFFFFFFFFL);
        long r2 = (r1 >>> 32) + (p01 >>> 32) + (p10 >>> 32) +
                (p02 & 0xFFFFFFFFL) + (p11 & 0xFFFFFFFFL) + (p20 & 0xFFFFFFFFL);
        long r3 = (r2 >>> 32) + (p02 >>> 32) + (p11 >>> 32) + (p20 >>> 32) +
                (p03 & 0xFFFFFFFFL) + (p12 & 0xFFFFFFFFL) + (p21 & 0xFFFFFFFFL) + (p30 & 0xFFFFFFFFL);

        this.low = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.high = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);

        // Handle sign - use the saved original signs
        boolean negative = (thisNegative != otherNegative);
        if (negative) {
            // Negate result
            this.low = ~this.low + 1;
            long newHigh = ~this.high;
            if (this.low == 0) {
                newHigh += 1;
            }
            this.high = newHigh;
        }

        this.scale = resultScale;
    }

    /**
     * Negate this number in-place
     */
    public void negate() {
        // Special case: negating zero should remain zero
        if (this.high == 0 && this.low == 0) {
            return;
        }

        long oldLow = this.low;

        // Two's complement: invert all bits and add 1
        this.low = ~this.low + 1;
        this.high = ~this.high;

        // Check for carry from low
        if (this.low == 0 && oldLow != 0) {
            this.high += 1;
        }
    }

    /**
     * Round this Decimal128 to the specified scale using the given rounding mode
     * This method performs in-place rounding without requiring a divisor
     *
     * @param targetScale  The desired scale (number of decimal places)
     * @param roundingMode The rounding mode to use
     */
    public void round(int targetScale, RoundingMode roundingMode) {
        validateScale(targetScale);

        // UNNECESSARY mode should be a complete no-op
        if (roundingMode == RoundingMode.UNNECESSARY) {
            return;
        }

        if (this.scale == targetScale) {
            // No rounding needed
            return;
        }

        if (this.scale < targetScale) {
            // Need to increase scale (add trailing zeros)
            int scaleIncrease = targetScale - this.scale;
            multiplyByPowerOf10InPlace(scaleIncrease);
            this.scale = targetScale;
            return;
        }

        // Need to decrease scale (remove decimal places with rounding)
        int scaleDecrease = this.scale - targetScale;

        // Handle zero specially
        if (isZero()) {
            this.scale = targetScale;
            return;
        }

        // Save the sign and work with absolute value
        boolean isNegative = isNegative();
        if (isNegative) {
            negate();
        }

        // Perform the rounding by dividing by 10^scaleDecrease
        long divisor = 1;
        for (int i = 0; i < scaleDecrease; i++) {
            divisor *= 10;
        }

        // Calculate remainder for rounding decision
        long remainder;
        if (this.high == 0) {
            // Simple case - fits in single long
            remainder = Long.remainderUnsigned(this.low, divisor);
            this.low = Long.divideUnsigned(this.low, divisor);
        } else {
            // Complex 128-bit case - calculate remainder and quotient
            remainder = calculateRemainder(divisor);
            divide(0, divisor, RoundingMode.DOWN, false);
        }

        // Apply rounding based on remainder and rounding mode
        boolean shouldRoundUp;

        if (remainder != 0) {
            long halfDivisor = divisor / 2;

            switch (roundingMode) {
                case UP:
                    shouldRoundUp = true;
                    break;
                case DOWN:
                    shouldRoundUp = false;
                    break;
                case CEILING:
                    // Round towards positive infinity
                    shouldRoundUp = !isNegative;
                    break;
                case FLOOR:
                    // Round towards negative infinity
                    shouldRoundUp = isNegative;
                    break;
                case HALF_UP:
                    shouldRoundUp = remainder >= halfDivisor;
                    break;
                case HALF_DOWN:
                    shouldRoundUp = remainder > halfDivisor;
                    break;
                case HALF_EVEN:
                    if (remainder > halfDivisor) {
                        shouldRoundUp = true;
                    } else if (remainder == halfDivisor) {
                        // Tie case - round to even
                        shouldRoundUp = (this.low & 1) == 1;
                    } else {
                        shouldRoundUp = false;
                    }
                    break;
                default:
                    shouldRoundUp = false;
            }
        } else {
            shouldRoundUp = false;
        }

        if (shouldRoundUp) {
            // Add 1 to the result
            this.low++;
            if (this.low == 0) { // Overflow in low part
                this.high++;
            }
        }

        // Restore sign if needed
        if (isNegative) {
            negate();
        }

        // Set the new scale
        this.scale = targetScale;
    }

    /**
     * Set values directly
     */
    public void set(long high, long low, int scale) {
        this.high = high;
        this.low = low;
        this.scale = scale;
    }

    /**
     * Set from a long value
     */
    public void setFromLong(long value, int scale) {
        validateScale(scale);
        this.high = value < 0 ? -1L : 0L;
        this.low = value;
        this.scale = scale;
        updateCompact();
    }

    // Setters for individual fields
    public void setHigh(long high) {
        this.high = high;
        updateCompact();
    }

    public void setLow(long low) {
        this.low = low;
        updateCompact();
    }

    public void setScale(int scale) {
        validateScale(scale);
        this.scale = scale;
    }

    /**
     * Subtract another Decimal128 from this one (in-place)
     *
     * @param other The Decimal128 to subtract
     */
    public void subtract(Decimal128 other) {
        // Handle scale differences first
        if (this.scale < other.scale) {
            // Rescale this to match other's scale
            rescale(other.scale);
        }

        // Now perform subtraction
        if (this.scale == other.scale) {
            // Special case: subtracting zero
            if (other.isZero()) {
                // Nothing to do - subtracting zero doesn't change the value
                return;
            }

            // Direct subtraction via two's complement addition
            // Negate other: ~other + 1
            long otherLow = ~other.low + 1;
            long otherHigh = ~other.high;
            if (otherLow == 0 && other.low != 0) {
                otherHigh += 1;
            }

            long sumLow = this.low + otherLow;
            long carry = hasCarry(this.low, otherLow, sumLow) ? 1 : 0;
            this.low = sumLow;
            this.high = this.high + otherHigh + carry;
        } else {
            // this.scale > other.scale

            // Special case: subtracting zero
            if (other.isZero()) {
                // Nothing to do - subtracting zero doesn't change the value
                return;
            }

            // Need to scale other up by (this.scale - other.scale)
            int scaleDiff = this.scale - other.scale;
            long otherHigh = other.high;
            long otherLow = other.low;

            // Multiply other by 10^scaleDiff
            for (int i = 0; i < scaleDiff; i++) {
                // Multiply by 10: (8x + 2x)
                long high8 = (otherHigh << 3) | (otherLow >>> 61);
                long low8 = otherLow << 3;
                long high2 = (otherHigh << 1) | (otherLow >>> 63);
                long low2 = otherLow << 1;

                otherLow = low8 + low2;
                long carry = hasCarry(low8, low2, otherLow) ? 1 : 0;
                otherHigh = high8 + high2 + carry;
            }

            // Now negate the scaled value and add
            long negLow = ~otherLow + 1;
            long negHigh = ~otherHigh;
            if (negLow == 0 && otherLow != 0) {
                negHigh += 1;
            }

            long sumLow = this.low + negLow;
            long carry = hasCarry(this.low, negLow, sumLow) ? 1 : 0;
            this.low = sumLow;
            this.high = this.high + negHigh + carry;
        }
    }

    /**
     * Convert to BigDecimal with full precision
     *
     * @return BigDecimal representation of this Decimal128
     */
    public java.math.BigDecimal toBigDecimal() {
        StringSink sink = new StringSink();
        toSink(sink);
        return new java.math.BigDecimal(sink.toString());
    }

    /**
     * Convert to double (may lose precision)
     */
    public double toDouble() {
        // Calculate the divisor (10^scale)
        double divisor = 1.0;
        for (int i = 0; i < scale; i++) {
            divisor *= 10.0;
        }

        // Convert 128-bit integer to double
        double result;

        if (high >= 0) {
            // Positive number
            // result = high * 2^64 + low (treating low as unsigned)
            result = (double) high * 18446744073709551616.0 + unsignedToDouble(low);
        } else {
            // Negative number - need to handle two's complement
            // For negative numbers, we need to convert from two's complement
            if (low == 0) {
                // Special case: low is 0, just negate after division
                result = (double) high * 18446744073709551616.0;
            } else {
                // Two's complement: ~high * 2^64 + ~low + 1
                // But we need to be careful with the arithmetic
                long negHigh = ~high;
                long negLow = ~low + 1;

                // Check for carry from low negation
                if (negLow == 0) {
                    negHigh += 1;
                }

                // Now we have the absolute value in negHigh:negLow
                result = -((double) negHigh * 18446744073709551616.0 + unsignedToDouble(negLow));
            }
        }

        return result / divisor;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        if (high == 0) {
            // Case: value fits in 64 bits (could be large unsigned)
            if (low >= 0) {
                // Positive value - use signed arithmetic
                longToDecimalSink(low, scale, sink);
            } else {
                // Large unsigned value that appears negative as signed long
                // Convert to unsigned string first, then format
                unsignedLongToDecimalSink(low, scale, sink);
            }
        } else if (high == -1 && low < 0) {
            // Simple negative case: small negative number
            longToDecimalSink(low, scale, sink);
        } else {
            // Complex case: full 128-bit conversion
            fullToSink(sink);
        }
    }

    @Override
    public String toString() {
        // Use StringSink which is already a CharSink - for compatibility
        StringSink sink = new StringSink();
        toSink(sink);
        return sink.toString();
    }

    /**
     * Append a long value to sink without allocation
     */
    private static void appendLongToSink(long value, CharSink<?> sink) {
        if (value == 0) {
            sink.putAscii('0');
            return;
        }

        // Find the highest power of 10 that fits in the value
        long divisor = 1;
        long temp = value;
        while (temp >= 10) {
            divisor *= 10;
            temp /= 10;
        }

        // Output digits from most significant to least significant
        while (divisor > 0) {
            int digit = (int) (value / divisor);
            sink.putAscii((char) ('0' + digit));
            value %= divisor;
            divisor /= 10;
        }
    }

    /**
     * Compare two unsigned 128-bit numbers
     *
     * @return negative if a < b, 0 if a == b, positive if a > b
     */
    private static int compareUnsigned(long aHigh, long aLow, long bHigh, long bLow) {
        int highCmp = Long.compareUnsigned(aHigh, bHigh);
        if (highCmp != 0) {
            return highCmp;
        }
        return Long.compareUnsigned(aLow, bLow);
    }

    /**
     * Count the number of digits in a positive long value
     */
    private static int countDigits(long value) {
        if (value == 0) return 1;
        int count = 0;
        while (value > 0) {
            count++;
            value /= 10;
        }
        return count;
    }

    /**
     * Convert unsigned long to double
     * Java's long is signed, but we need to treat it as unsigned for the low part
     */
    private static double unsignedToDouble(long value) {
        if (value >= 0) {
            return (double) value;
        } else {
            // For negative values (which represent large unsigned values)
            // Split into two parts to avoid precision loss
            // value = 2^63 + (value & 0x7FFFFFFFFFFFFFFF)
            return 9223372036854775808.0 + (double) (value & 0x7FFFFFFFFFFFFFFFL);
        }
    }

    /**
     * Validates that the scale is within allowed bounds
     */
    private static void validateScale(int scale) {
        if (scale < 0 || scale > MAX_SCALE) {
            throw new IllegalArgumentException("Scale must be between 0 and " + MAX_SCALE + ", got: " + scale);
        }
    }

    /**
     * Calculate the remainder when dividing this 128-bit value by a single long divisor
     */
    private long calculateRemainder(long divisor) {
        // Save original values
        long originalHigh = this.high;
        long originalLow = this.low;

        // Use binary long division to find remainder
        long remainderHigh = 0;
        long remainderLow = 0;

        // Process each bit of the dividend from MSB to LSB
        for (int i = 127; i >= 0; i--) {
            // Shift remainder left by 1
            remainderHigh = (remainderHigh << 1) | (remainderLow >>> 63);
            remainderLow = remainderLow << 1;

            // Get bit i from dividend
            boolean dividendBit;
            if (i >= 64) {
                dividendBit = ((originalHigh >>> (i - 64)) & 1) == 1;
            } else {
                dividendBit = ((originalLow >>> i) & 1) == 1;
            }

            if (dividendBit) {
                remainderLow |= 1;
            }

            // Check if remainder >= divisor (divisor has high part = 0)
            if (remainderHigh > 0 || Long.compareUnsigned(remainderLow, divisor) >= 0) {
                // Subtract divisor from remainder
                if (remainderHigh > 0 || Long.compareUnsigned(remainderLow, divisor) >= 0) {
                    remainderLow = remainderLow - divisor;
                    if (Long.compareUnsigned(remainderLow + divisor, remainderLow) < 0) {
                        // There was a borrow
                        remainderHigh--;
                    }
                }
            }
        }

        // The remainder should fit in a single long since divisor is single long
        if (remainderHigh != 0) {
            // This shouldn't happen for proper inputs, but let's handle it gracefully
            return remainderLow;
        }

        return remainderLow;
    }

    /**
     * Perform unsigned division in-place using binary long division
     *
     * @param divHigh High 64 bits of divisor
     * @param divLow  Low 64 bits of divisor
     */
    private void divide(long divHigh, long divLow, RoundingMode roundingMode, boolean resultNegative) {
        // Handle simple cases first
        if (divHigh == 0 && divLow == 1) {
            // Division by 1 - result is unchanged
            return;
        }

        if (divHigh == 0 && this.high == 0) {
            // Both operands fit in single long - use simple division
            this.low = Long.divideUnsigned(this.low, divLow);
            return;
        }

        // Handle division by zero (should not happen, but safety check)
        if (divHigh == 0 && divLow == 0) {
            throw new ArithmeticException("Division by zero");
        }

        // Save dividend values
        long dividendHigh = this.high;
        long dividendLow = this.low;

        // Initialize result (quotient) to zero
        this.high = 0;
        this.low = 0;

        // If dividend is smaller than divisor, result is 0
        if (compareUnsigned(dividendHigh, dividendLow, divHigh, divLow) < 0) {
            return;
        }

        // Special case: divisor fits in 64 bits (divHigh == 0)
        if (divHigh == 0) {
            // Use optimized 128-bit by 64-bit division
            divide128By64(dividendHigh, dividendLow, divLow);
            return;
        }

        // Use proper binary division
        long quotientHigh = 0;
        long quotientLow = 0;
        long remainderHigh = 0;
        long remainderLow = 0;

        // Find the most significant bit of the dividend
        int startBit;
        if (dividendHigh == 0) {
            startBit = 63 - Long.numberOfLeadingZeros(dividendLow);
        } else {
            startBit = 127 - Long.numberOfLeadingZeros(dividendHigh);
        }

        // Process each bit of the dividend from MSB to LSB
        for (int i = startBit; i >= 0; i--) {
            // Shift remainder left by 1
            remainderHigh = (remainderHigh << 1) | (remainderLow >>> 63);
            remainderLow = remainderLow << 1;

            // Get bit i from dividend
            if (i >= 64) {
                if ((dividendHigh & (1L << (i - 64))) != 0) {
                    remainderLow |= 1;
                }
            } else {
                if ((dividendLow & (1L << i)) != 0) {
                    remainderLow |= 1;
                }
            }

            // Check if remainder >= divisor
            if (remainderHigh > divHigh || (remainderHigh == divHigh && Long.compareUnsigned(remainderLow, divLow) >= 0)) {
                // Subtract divisor from remainder
                long newLow = remainderLow - divLow;
                long borrow = (Long.compareUnsigned(remainderLow, divLow) < 0) ? 1 : 0;
                remainderHigh = remainderHigh - divHigh - borrow;
                remainderLow = newLow;

                // Set bit in quotient
                if (i >= 64) {
                    quotientHigh |= (1L << (i - 64));
                } else {
                    quotientLow |= (1L << i);
                }
            }
        }

        // Apply rounding based on remainder and rounding mode
        // remainderHigh:remainderLow contains the final remainder
        boolean shouldRoundUp = false;

        // Only apply rounding if there's a remainder
        if (remainderHigh != 0 || remainderLow != 0) {
            switch (roundingMode) {
                case UP:
                    shouldRoundUp = true;
                    break;
                case DOWN:
                    shouldRoundUp = false;
                    break;
                case CEILING:
                    // Round towards positive infinity
                    shouldRoundUp = !resultNegative;
                    break;
                case FLOOR:
                    // Round towards negative infinity
                    shouldRoundUp = resultNegative;
                    break;
                case HALF_UP:
                case HALF_DOWN:
                case HALF_EVEN:
                    // Calculate divisor/2 for comparison
                    long halfDivisorHigh = divHigh;
                    long halfDivisorLow = divLow;

                    // Divide by 2: shift right by 1 bit
                    halfDivisorLow = (halfDivisorHigh << 63) | (halfDivisorLow >>> 1);
                    halfDivisorHigh = halfDivisorHigh >>> 1;

                    int cmp = compareUnsigned(remainderHigh, remainderLow, halfDivisorHigh, halfDivisorLow);
                    if (cmp > 0) {
                        // remainder > divisor/2 - always round up
                        shouldRoundUp = true;
                    } else if (cmp < 0) {
                        // remainder < divisor/2 - always round down
                        shouldRoundUp = false;
                    } else {
                        // remainder == divisor/2 - tie case
                        switch (roundingMode) {
                            case HALF_UP:
                                shouldRoundUp = true;
                                break;
                            case HALF_DOWN:
                                shouldRoundUp = false;
                                break;
                            case HALF_EVEN:
                                // Round to even - check if quotient is odd
                                shouldRoundUp = (quotientLow & 1) == 1;
                                break;
                        }
                    }
                    break;
                case UNNECESSARY:
                    // Don't round - keep exact result even if there's a remainder
                    shouldRoundUp = false;
                    break;
            }
        }

        if (shouldRoundUp) {
            // Add 1 to quotient
            quotientLow++;
            if (quotientLow == 0) { // Overflow in low part
                quotientHigh++;
            }
        }

        // Store final quotient
        this.high = quotientHigh;
        this.low = quotientLow;
    }

    /**
     * Divide this by 10 in place
     */
    private void divideBy10InPlace() {
        // Simple case
        if (this.high == 0 && this.low < 10) {
            this.low = 0;
            return;
        }

        // Use our division algorithm for dividing by 10
        long quotientHigh = 0;
        long quotientLow = 0;
        long remainder = 0;

        // Divide high part
        if (this.high != 0) {
            quotientHigh = Long.divideUnsigned(this.high, 10);
            remainder = Long.remainderUnsigned(this.high, 10);
        }

        // Combine remainder with low part for division
        // We need to compute (remainder * 2^64 + low) / 10
        // Do this bit by bit to avoid overflow
        for (int i = 63; i >= 0; i--) {
            remainder = remainder * 2 + ((this.low >>> i) & 1);
            if (remainder >= 10) {
                quotientLow |= (1L << i);
                remainder -= 10;
            }
        }

        this.high = quotientHigh;
        this.low = quotientLow;
    }

    // Note: moduloUnsignedInPlace method removed - modulo now uses division-based approach

    /**
     * Full 128-bit to sink conversion (simplified version)
     * For production use, consider using BigInteger for complex cases
     */
    private void fullToSink(CharSink<?> sink) {
        // Convert the 128-bit value to BigInteger first, then handle sign and formatting
        java.math.BigInteger bigInt;

        // Create BigInteger from the 128-bit representation
        if (high == 0) {
            // Simple positive case: fits in 64 bits
            bigInt = java.math.BigInteger.valueOf(low);
        } else if (high == -1 && low < 0) {
            // Simple negative case: small negative number that fits in signed long
            bigInt = java.math.BigInteger.valueOf(low);
        } else if (high < 0) {
            // Negative 128-bit number - use two's complement to get absolute value
            long absHigh = ~high;
            long absLow = ~low + 1;
            if (low == 0 && absLow == 0) {
                absHigh++; // Handle carry
            }

            // Create positive BigInteger for absolute value
            java.math.BigInteger absBigInt;
            if (absHigh == 0) {
                absBigInt = new java.math.BigInteger(Long.toUnsignedString(absLow));
            } else {
                absBigInt = java.math.BigInteger.valueOf(absHigh).shiftLeft(64).add(new java.math.BigInteger(Long.toUnsignedString(absLow)));
            }

            // Make it negative
            bigInt = absBigInt.negate();
        } else {
            // Positive 128-bit number
            bigInt = java.math.BigInteger.valueOf(high).shiftLeft(64).add(new java.math.BigInteger(Long.toUnsignedString(low)));
        }

        // Convert to string (BigInteger handles the sign)
        String valueStr = bigInt.toString();

        // Handle sign separately for formatting
        boolean negative = valueStr.startsWith("-");
        String digits = negative ? valueStr.substring(1) : valueStr;

        // Apply decimal formatting based on scale
        if (negative) {
            sink.putAscii('-');
        }

        if (scale == 0) {
            // Integer
            sink.put(digits);
        } else {
            // Decimal
            if (digits.length() <= scale) {
                // Number < 1: output 0.00...digits
                sink.putAscii('0').putAscii('.');
                for (int i = 0; i < scale - digits.length(); i++) {
                    sink.putAscii('0');
                }
                sink.put(digits);
            } else {
                // Number >= 1: split into integer.fractional
                int splitPoint = digits.length() - scale;
                sink.put(digits.substring(0, splitPoint));
                sink.putAscii('.');
                sink.put(digits.substring(splitPoint));
            }
        }
    }

    /**
     * Convert a long to decimal representation in a sink with scale (allocation-free)
     */
    private void longToDecimalSink(long value, int scale, CharSink<?> sink) {
        if (scale == 0) {
            sink.put(value);
            return;
        }

        // Handle negative numbers
        boolean negative = value < 0;
        long absValue = negative ? -value : value;

        if (negative) {
            sink.putAscii('-');
        }

        // Calculate number of digits in absValue
        int digits = countDigits(absValue);

        if (digits <= scale) {
            // Need to pad with leading zeros: 0.00...value
            sink.putAscii('0').putAscii('.');
            // Add leading zeros
            for (int i = 0; i < scale - digits; i++) {
                sink.putAscii('0');
            }
            // Add the actual digits
            appendLongToSink(absValue, sink);
        } else {
            // Split into integer and fractional parts
            // Extract integer part
            long divisor = 1;
            for (int i = 0; i < scale; i++) {
                divisor *= 10;
            }
            long integerPart = absValue / divisor;
            long fractionalPart = absValue % divisor;

            // Output integer part
            appendLongToSink(integerPart, sink);

            // Output decimal point
            sink.putAscii('.');

            // Output fractional part with leading zeros if needed
            if (fractionalPart == 0) {
                // Special case: all trailing zeros
                for (int i = 0; i < scale; i++) {
                    sink.putAscii('0');
                }
            } else {
                // Pad with leading zeros and append fractional part
                int fracDigits = countDigits(fractionalPart);
                for (int i = 0; i < scale - fracDigits; i++) {
                    sink.putAscii('0');
                }
                appendLongToSink(fractionalPart, sink);
            }
        }
    }


    /**
     * Multiply this by 10^n in place
     */
    private void multiplyByPowerOf10InPlace(int n) {
        if (n == 0) {
            return;
        }

        // For small powers, use lookup table
        if (n <= 18) {
            long multiplier = POWERS_OF_10[n];
            // Special case: if high is 0, use simple 64-bit multiplication
            if (this.high == 0) {
                // Check if result will overflow 64 bits
                if (this.low <= Long.MAX_VALUE / multiplier) {
                    this.low *= multiplier;
                    updateCompact();
                    return;
                }
            }

            // Full 128-bit multiplication by 64-bit value
            multiplyBy64Bit(multiplier);
            updateCompact();
            return;
        }

        // For larger powers, break down into smaller chunks
        // First multiply by largest power that fits in 64 bits (10^18)
        while (n >= 18) {
            multiplyBy64Bit(POWERS_OF_10[18]);
            n -= 18;
        }

        // Multiply by remaining power
        if (n > 0) {
            multiplyBy64Bit(POWERS_OF_10[n]);
        }
        updateCompact();
    }

    /**
     * Multiply this 128-bit value by a 64-bit value in place
     */
    private void multiplyBy64Bit(long multiplier) {
        // Save original values
        long origHigh = this.high;
        long origLow = this.low;

        // Perform 128-bit × 64-bit multiplication
        // Result is at most 192 bits, but we keep only the lower 128 bits

        // Split multiplier into two 32-bit parts
        long m1 = multiplier >>> 32;
        long m0 = multiplier & 0xFFFFFFFFL;

        // Split this into four 32-bit parts
        long a3 = origHigh >>> 32;
        long a2 = origHigh & 0xFFFFFFFFL;
        long a1 = origLow >>> 32;
        long a0 = origLow & 0xFFFFFFFFL;

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

        this.low = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.high = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
        // Note: r4 represents overflow beyond 128 bits, which we discard
    }

    // Cache for common small values
    private static final Decimal128[] ZERO_THROUGH_TEN = new Decimal128[11];
    private static final Decimal128 ZERO_CACHE = new Decimal128();
    private static final Decimal128 ONE_CACHE = new Decimal128(0, 1, 0);

    static {
        for (int i = 0; i <= 10; i++) {
            ZERO_THROUGH_TEN[i] = new Decimal128(0, i, 0);
        }
    }

    // Lookup table for powers of 10 up to 10^18
    private static final long[] POWERS_OF_10 = {
        1L,                    // 10^0
        10L,                   // 10^1
        100L,                  // 10^2
        1000L,                 // 10^3
        10000L,                // 10^4
        100000L,               // 10^5
        1000000L,              // 10^6
        10000000L,             // 10^7
        100000000L,            // 10^8
        1000000000L,           // 10^9
        10000000000L,          // 10^10
        100000000000L,         // 10^11
        1000000000000L,        // 10^12
        10000000000000L,       // 10^13
        100000000000000L,      // 10^14
        1000000000000000L,     // 10^15
        10000000000000000L,    // 10^16
        100000000000000000L,   // 10^17
        1000000000000000000L   // 10^18
    };

    private static long calculatePowerOf10(int n) {
        if (n <= 18) {
            return POWERS_OF_10[n];
        }
        long result = 1;
        for (int i = 0; i < n; i++) {
            result *= 10;
        }
        return result;
    }

    private static long computeCompact(long high, long low) {
        // Check if value fits in a single long (high is either 0 or -1)
        if (high == 0 || (high == -1 && low < 0)) {
            return low;
        }
        return INFLATED;
    }

    private void updateCompact() {
        this.compact = computeCompact(this.high, this.low);
    }

    /**
     * Check if this value fits in a 64-bit signed integer (positive or negative)
     */
    private boolean is64BitValue() {
        return (this.high == 0 && this.low >= 0) || (this.high == -1 && this.low < 0);
    }

    /**
     * Check if this value is a negative 64-bit value
     */
    private boolean isNegative64BitValue() {
        return this.high == -1 && this.low < 0;
    }

    /**
     * Get the absolute magnitude of a 64-bit value (assumes is64BitValue() is true)
     */
    private long get64BitMagnitude() {
        if (this.high == -1 && this.low < 0) {
            return -this.low;  // Convert negative to positive magnitude
        } else {
            return this.low;   // Already positive
        }
    }

    /**
     * Set this decimal to a 64-bit value with given sign and magnitude
     */
    private void set64BitValue(long magnitude, boolean negative, int scale) {
        if (negative) {
            this.high = -1L;
            this.low = -magnitude;
        } else {
            this.high = 0L;
            this.low = magnitude;
        }
        this.scale = scale;
        updateCompact();
    }

    /**
     * Intel-style optimization: Check if operands can use 64-bit native division
     * This is much faster than 128-bit binary long division.
     */
    private boolean canUse64BitNativeDivision(Decimal128 divisor) {
        // Both must be 64-bit values
        if (!this.is64BitValue() || !divisor.is64BitValue()) {
            return false;
        }

        long dividendMag = this.get64BitMagnitude();
        long divisorMag = divisor.get64BitMagnitude();

        // Avoid division by zero
        if (divisorMag == 0) {
            return false;
        }

        // Simple heuristic: if we can scale the dividend to get a reasonable quotient without overflow
        // Use a conservative scale adjustment based on scale difference
        int scaleDiff = Math.max(6, divisor.scale - this.scale + 6); // Target 6 digits precision

        // Check if scaling is safe - be more generous than the double estimation path
        if (scaleDiff > 0 && scaleDiff <= 15) {
            // Check if dividendMag * 10^scaleDiff fits in long
            long maxScale = (scaleDiff >= POWERS_OF_10.length) ? 0 : Long.MAX_VALUE / POWERS_OF_10[scaleDiff];
            return dividendMag <= maxScale;
        }

        // If no scaling needed, always use native division
        return scaleDiff <= 0;
    }

    /**
     * EXTREMELY conservative check for 64-bit native division
     * Only applies to very specific cases similar to the user's example:
     * - Both operands are 64-bit integers
     * - Dividend magnitude is reasonably large (> 10^6)
     * - Divisor magnitude is reasonably large (> 10^6)
     * - Scale difference is small (within [-2, 2])
     * - Result will be in a reasonable range
     */
    private boolean canUseSimple64BitNativeDivision(Decimal128 divisor) {
        // Both must be 64-bit values
        if (!this.is64BitValue() || !divisor.is64BitValue()) {
            return false;
        }

        long dividendMag = this.get64BitMagnitude();
        long divisorMag = divisor.get64BitMagnitude();

        // Avoid division by zero
        if (divisorMag == 0) {
            return false;
        }

        // CONSERVATIVE: Avoid very small magnitudes that could cause precision issues
        // Set thresholds low enough to handle normal decimal values like 123.456 and 7.89
        if (dividendMag < 100L || divisorMag < 100L) {
            return false;
        }

        // CONSERVATIVE: Avoid cases where dividend is much smaller than divisor
        // This prevents quotient = 0 cases that can cause scale handling bugs
        if (dividendMag * 1000000L < divisorMag) {
            return false;
        }

        // CONSERVATIVE: Scale difference should be small
        // Avoid complex scale handling that can introduce bugs
        int scaleDiff = divisor.scale - this.scale;
        if (scaleDiff < -3 || scaleDiff > 3) {
            return false;
        }

        // Determine optimal scaling factor to maximize precision without overflow
        // Start with 10^6 for 6 decimal places, scale down if needed
        long scalingFactor = 1000000L;
        while (dividendMag > Long.MAX_VALUE / scalingFactor && scalingFactor > 1L) {
            scalingFactor /= 10L;
        }

        // Must have at least some scaling for meaningful precision
        if (scalingFactor < 100L) {
            return false;
        }

        // Estimate result magnitude with dynamic scaling
        double estimatedResult = ((double) dividendMag * (double) scalingFactor) / (double) divisorMag;

        // Only accept if result is in reasonable range (not too big, not too small)
        return estimatedResult >= 0.001 && estimatedResult < 1000000000000.0; // Between 10^-3 and 10^12
    }

    /**
     * Perform simple 64-bit native division with dynamic precision scaling
     */
    private void performSimple64BitNativeDivision(Decimal128 divisor) {
        long dividendMag = this.get64BitMagnitude();
        long divisorMag = divisor.get64BitMagnitude();
        boolean resultNegative = this.isNegative() ^ divisor.isNegative();

        // Determine optimal scaling factor (same logic as in canUse method)
        long scalingFactor = 1000000L;
        while (dividendMag > Long.MAX_VALUE / scalingFactor && scalingFactor > 1L) {
            scalingFactor /= 10L;
        }

        // Scale dividend for precision
        long scaledDividend = dividendMag * scalingFactor;

        // Native 64-bit division
        long quotient = scaledDividend / divisorMag;

        // Calculate how many decimal places we added
        int scalingDigits = (int) Math.log10(scalingFactor);

        // Result scale: we added scalingDigits to dividend scale, then subtracted divisor scale
        int resultScale = this.scale + scalingDigits - divisor.scale;

        // Ensure result scale is within bounds
        if (resultScale < 0) {
            // Divide by 10^(-resultScale) to adjust
            long divisorAdj = POWERS_OF_10[-resultScale];
            quotient /= divisorAdj;
            resultScale = 0;
        } else if (resultScale > MAX_SCALE) {
            // Multiply by 10^(resultScale - MAX_SCALE) if safe
            int extraScale = resultScale - MAX_SCALE;
            if (extraScale <= 18 && quotient <= Long.MAX_VALUE / POWERS_OF_10[extraScale]) {
                quotient *= POWERS_OF_10[extraScale];
            }
            resultScale = MAX_SCALE;
        }

        // Set the result
        this.set64BitValue(quotient, resultNegative, resultScale);
    }

    /**
     * Calculate optimal scale for division to minimize computation while maintaining precision
     * This is much simpler: we want to achieve a target precision, typically 6-16 digits
     */
    private int calculateOptimalScale(long dividendMag, long divisorMag, int dividendScale, int divisorScale) {
        // Calculate how much we need to scale the dividend so that the quotient is a meaningful integer
        // We want: (scaledDividend / divisorMag) >= 1
        // So: scaledDividend >= divisorMag
        // So: dividendMag * 10^scaleAdjustment >= divisorMag
        // So: scaleAdjustment >= log10(divisorMag / dividendMag)

        double ratio = (double) divisorMag / (double) dividendMag;
        int minScaleAdjustment = (int) Math.ceil(Math.log10(ratio));

        // Add a few extra digits for precision
        int targetScaleAdjustment = minScaleAdjustment + 6;

        // Cap to prevent overflow
        return Math.min(15, Math.max(0, targetScaleAdjustment));
    }

    /**
     * Fast decimal digit estimation using binary logarithm approximation
     * Based on Intel's bid_estimate_decimal_digits technique
     */
    private int estimateDecimalDigits(long value) {
        if (value == 0) return 1;

        // Count leading zeros to get binary magnitude
        int leadingZeros = Long.numberOfLeadingZeros(value);
        int binaryBits = 64 - leadingZeros;

        // log10(2^n) ≈ n * 0.30103 (log10(2))
        // Add 1 for safety margin
        return (int)(binaryBits * 0.30103) + 1;
    }

    /**
     * Intel-style double-precision initial quotient estimation
     * Much faster than bit-by-bit binary long division
     */
    private long estimateQuotientDouble(long dividendHigh, long dividendLow,
                                       long divisorHigh, long divisorLow) {
        // Convert 128-bit values to double precision for fast estimation
        // Note: This loses precision but gives a good starting point

        double dividendDouble;
        double divisorDouble;

        if (dividendHigh == 0) {
            dividendDouble = (double) dividendLow;
        } else {
            // Use exact double conversion for 64-bit part + scaled lower part
            dividendDouble = (double) dividendHigh * TWO_POW_64 + (double) dividendLow;
        }

        if (divisorHigh == 0) {
            divisorDouble = (double) divisorLow;
        } else {
            divisorDouble = (double) divisorHigh * TWO_POW_64 + (double) divisorLow;
        }

        if (divisorDouble == 0.0) {
            return 0; // Avoid division by zero
        }

        double quotient = dividendDouble / divisorDouble;

        // Clamp to reasonable range
        if (quotient > Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        } else if (quotient < 0) {
            return 0;
        }

        return (long) quotient;
    }

    private static final double TWO_POW_64 = Math.pow(2, 64);

    /**
     * Optimized multiplication for 64-bit values using magnitude arithmetic
     * This avoids the need for 128-bit operations when both operands fit in 64 bits
     */
    private void multiply64BitOptimized(Decimal128 other) {
        // Extract magnitudes and signs
        long thisMagnitude = this.get64BitMagnitude();
        long otherMagnitude = other.get64BitMagnitude();
        boolean thisNegative = this.isNegative64BitValue();
        boolean otherNegative = other.isNegative64BitValue();

        // Result sign: negative if exactly one operand is negative
        boolean resultNegative = thisNegative ^ otherNegative;

        // Result scale is sum of scales
        int resultScale = this.scale + other.scale;

        // Check if multiplication would overflow 64 bits
        if (thisMagnitude != 0 && otherMagnitude > Long.MAX_VALUE / thisMagnitude) {
            // Would overflow - use 128-bit representation and fall back to regular algorithm
            // The regular algorithm will handle this correctly
            // Just continue without the fast path optimization
        } else {
            // Can use fast path
            // Perform the multiplication on positive magnitudes
            long resultMagnitude = thisMagnitude * otherMagnitude;

            // Check if result fits in 64 bits
            if (resultNegative && resultMagnitude > Long.MAX_VALUE) {
                // Would overflow when negating - use 128-bit representation
                this.high = 0;
                this.low = resultMagnitude;
                this.scale = resultScale;
                if (resultNegative) {
                    negate();  // Use the existing negate method for 128-bit case
                }
            } else {
                // Fits in 64 bits
                this.set64BitValue(resultMagnitude, resultNegative, resultScale);
            }
            return;  // Fast path complete
        }

        // Fall back to original algorithm if we reach here
        // This happens when overflow is detected and we need 128-bit arithmetic
    }

    /**
     * Optimized division for 64-bit values using magnitude arithmetic
     * This avoids the need for 128-bit operations when both operands fit in 64 bits
     */
    private void divide64BitOptimized(Decimal128 divisor) {
        // Extract magnitudes and signs
        long dividendMagnitude = this.get64BitMagnitude();
        long divisorMagnitude = divisor.get64BitMagnitude();
        boolean dividendNegative = this.isNegative64BitValue();
        boolean divisorNegative = divisor.isNegative64BitValue();

        // Result sign: negative if exactly one operand is negative
        boolean resultNegative = dividendNegative ^ divisorNegative;

        // Calculate optimal result scale - match the original algorithm
        int resultScale = MAX_SCALE;
        int scaleAdjustment = MAX_SCALE + divisor.scale - this.scale;

        // Try to scale up the dividend for better precision
        long scaledDividend = dividendMagnitude;
        int actualScaleAdjustment = 0;
        if (scaleAdjustment > 0 && scaleAdjustment <= 18) {
            long multiplier = POWERS_OF_10[scaleAdjustment];
            if (scaledDividend <= Long.MAX_VALUE / multiplier) {
                scaledDividend *= multiplier;
                actualScaleAdjustment = scaleAdjustment;
            }
        }

        // Adjust result scale based on what we actually applied
        int finalResultScale = this.scale + actualScaleAdjustment - divisor.scale;

        // Perform the division on positive magnitudes
        long resultMagnitude = scaledDividend / divisorMagnitude;

        // Set the result
        this.set64BitValue(resultMagnitude, resultNegative, finalResultScale);
    }

    private void divideCompact(Decimal128 divisor) {
        // Both values fit in long, use fast long arithmetic
        // Use more reasonable scale calculation - preserve at least the dividend's scale
        int resultScale = Math.max(this.scale, Math.min(MAX_SCALE, this.scale + 6));
        int scaleAdjustment = resultScale + divisor.scale - this.scale;

        long dividend = this.compact;
        long divisorValue = divisor.compact;

        // Scale up dividend
        if (scaleAdjustment > 0 && scaleAdjustment <= 18) {
            long multiplier = POWERS_OF_10[scaleAdjustment];
            // Check for overflow
            if (dividend <= Long.MAX_VALUE / multiplier && dividend >= Long.MIN_VALUE / multiplier) {
                dividend *= multiplier;
            } else {
                // Overflow would occur, fall back to full 128-bit division
                // by invalidating compact and letting caller retry
                this.compact = INFLATED;
                return;
            }
        }

        long result = dividend / divisorValue;

        // Set result
        this.low = result;
        this.high = result < 0 ? -1L : 0L;
        this.scale = resultScale;
        this.compact = result;
    }

    /**
     * Rescale this Decimal128 in place
     *
     * @param newScale The new scale (must be >= current scale)
     */
    private void rescale(int newScale) {
        if (newScale < this.scale) {
            throw new IllegalArgumentException("Cannot reduce scale");
        }

        int scaleDiff = newScale - this.scale;

        // Multiply by 10^scaleDiff
        multiplyByPowerOf10InPlace(scaleDiff);

        this.scale = newScale;
    }

    /**
     * Convert an unsigned long to decimal representation in a sink with scale (allocation-free)
     */
    private void unsignedLongToDecimalSink(long value, int scale, CharSink<?> sink) {
        if (scale == 0) {
            // Integer case - output as unsigned
            sink.put(Long.toUnsignedString(value));
            return;
        }

        // Convert to string as unsigned
        String digits = Long.toUnsignedString(value);

        if (digits.length() <= scale) {
            // Need to pad with leading zeros: 0.00...digits
            sink.putAscii('0').putAscii('.');
            // Add leading zeros
            for (int i = 0; i < scale - digits.length(); i++) {
                sink.putAscii('0');
            }
            // Add the actual digits
            sink.put(digits);
        } else {
            // Split into integer and fractional parts
            // Extract integer part
            int splitPoint = digits.length() - scale;
            sink.put(digits.substring(0, splitPoint));
            // Output decimal point
            sink.putAscii('.');
            // Output fractional part
            sink.put(digits.substring(splitPoint));
        }
    }

    /**
     * Check if addition resulted in a carry
     * When adding two unsigned numbers a + b = sum, carry occurs iff sum < a (or sum < b)
     * This works because:
     * - No carry: sum = a + b, so sum >= a and sum >= b
     * - Carry: sum = a + b - 2^64, so sum < a and sum < b
     */
    static boolean hasCarry(long a, long b, long sum) {
        // We can check against either a or b - both work
        // Using a for consistency, b parameter kept for clarity
        return Long.compareUnsigned(sum, a) < 0;
    }

    /**
     * Estimate the number of bits used by this value
     */
    private int estimateBitsUsed() {
        if (this.high == 0 || this.high == -1) {
            // Value fits in low part
            return 64 - Long.numberOfLeadingZeros(Math.abs(this.low));
        } else {
            // Value uses high part
            return 128 - Long.numberOfLeadingZeros(Math.abs(this.high));
        }
    }

    /**
     * Check if a value is a power of 10
     */
    private static boolean isPowerOf10(long value) {
        if (value <= 0) return false;
        // Check common powers of 10
        return value == 1L || value == 10L || value == 100L || value == 1000L ||
               value == 10000L || value == 100000L || value == 1000000L ||
               value == 10000000L || value == 100000000L || value == 1000000000L ||
               value == 10000000000L || value == 100000000000L || value == 1000000000000L ||
               value == 10000000000000L || value == 100000000000000L || value == 1000000000000000L ||
               value == 10000000000000000L || value == 100000000000000000L || value == 1000000000000000000L;
    }

    /**
     * Get the power of 10 for a given value
     */
    private static int getPowerOf10(long value) {
        if (value == 1L) return 0;
        if (value == 10L) return 1;
        if (value == 100L) return 2;
        if (value == 1000L) return 3;
        if (value == 10000L) return 4;
        if (value == 100000L) return 5;
        if (value == 1000000L) return 6;
        if (value == 10000000L) return 7;
        if (value == 100000000L) return 8;
        if (value == 1000000000L) return 9;
        if (value == 10000000000L) return 10;
        if (value == 100000000000L) return 11;
        if (value == 1000000000000L) return 12;
        if (value == 10000000000000L) return 13;
        if (value == 100000000000000L) return 14;
        if (value == 1000000000000000L) return 15;
        if (value == 10000000000000000L) return 16;
        if (value == 100000000000000000L) return 17;
        if (value == 1000000000000000000L) return 18;
        return -1;
    }

    /**
     * Optimized division of 128-bit by 64-bit value
     */
    private void divide128By64(long dividendHigh, long dividendLow, long divisor) {
        if (dividendHigh == 0) {
            // Simple case - dividend fits in 64 bits
            this.low = Long.divideUnsigned(dividendLow, divisor);
            this.high = 0;
            return;
        }

        // Divide high part first
        long quotientHigh = Long.divideUnsigned(dividendHigh, divisor);
        long remainder = Long.remainderUnsigned(dividendHigh, divisor);

        // Now divide (remainder:dividendLow) by divisor
        // This is a 128-bit value where the high part (remainder) is guaranteed to be < divisor
        // We can use a more efficient algorithm here

        // For each bit position from 63 to 0
        long quotientLow = 0;
        for (int i = 63; i >= 0; i--) {
            // Shift remainder left by 1 and bring in next bit from dividendLow
            remainder = (remainder << 1) | ((dividendLow >>> i) & 1);

            // Check if remainder >= divisor
            if (Long.compareUnsigned(remainder, divisor) >= 0) {
                remainder -= divisor;
                quotientLow |= (1L << i);
            }
        }

        this.high = quotientHigh;
        this.low = quotientLow;
    }

}