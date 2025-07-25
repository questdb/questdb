package io.questdb.std;

/**
 * MutableDecimal128 - A mutable 128-bit decimal number implementation
 * <p>
 * This class is designed to be used as a sink to avoid object allocation
 * in performance-critical code. It provides the same functionality as
 * Decimal128 but allows modification of its internal state.
 */
public class MutableDecimal128 {
    private long high;  // High 64 bits
    private long low;   // Low 64 bits
    private int scale;  // Number of decimal places

    /**
     * Constructor
     */
    public MutableDecimal128() {
        this.high = 0;
        this.low = 0;
        this.scale = 0;
    }

    /**
     * Constructor with initial values
     */
    public MutableDecimal128(long high, long low, int scale) {
        this.high = high;
        this.low = low;
        this.scale = scale;
    }

    /**
     * Add two Decimal128 numbers and store result in this sink
     *
     * @param a First operand
     * @param b Second operand
     */
    public void add(Decimal128 a, Decimal128 b) {
        // Copy a to this sink
        this.copyFrom(a);
        // Add b to this sink
        this.addDecimal128(b);
    }

    /**
     * Add a Decimal128 to this MutableDecimal128 (in-place)
     *
     * @param other The Decimal128 to add
     */
    public void addDecimal128(Decimal128 other) {
        // If scales match, use direct addition
        if (this.scale == other.getScale()) {
            // Perform 128-bit addition
            long sumLow = this.low + other.getLow();

            // Check for carry
            long carry = Decimal128.hasCarry(this.low, other.getLow(), sumLow) ? 1 : 0;

            // Update values in place
            this.low = sumLow;
            this.high = this.high + other.getHigh() + carry;
            return;
        }

        // Handle different scales
        if (this.scale < other.getScale()) {
            // Rescale this to match other's scale
            rescaleInPlace(other.getScale());
            // Now add with same scale
            long sumLow = this.low + other.getLow();
            long carry = Decimal128.hasCarry(this.low, other.getLow(), sumLow) ? 1 : 0;
            this.low = sumLow;
            this.high = this.high + other.getHigh() + carry;
        } else {
            // Need to rescale other - we'll do it mathematically
            // Scale difference
            int scaleDiff = this.scale - other.getScale();
            long otherHigh = other.getHigh();
            long otherLow = other.getLow();

            // Multiply other by 10^scaleDiff
            for (int i = 0; i < scaleDiff; i++) {
                // Multiply by 10: (8x + 2x)
                long high8 = (otherHigh << 3) | (otherLow >>> 61);
                long low8 = otherLow << 3;
                long high2 = (otherHigh << 1) | (otherLow >>> 63);
                long low2 = otherLow << 1;

                otherLow = low8 + low2;
                long carry = Decimal128.hasCarry(low8, low2, otherLow) ? 1 : 0;
                otherHigh = high8 + high2 + carry;
            }

            // Now add the scaled value
            long sumLow = this.low + otherLow;
            long carry = Decimal128.hasCarry(this.low, otherLow, sumLow) ? 1 : 0;
            this.low = sumLow;
            this.high = this.high + otherHigh + carry;
        }
    }

    /**
     * Add another MutableDecimal128 to this one (in-place)
     *
     * @param other The number to add
     */
    public void addMutable(MutableDecimal128 other) {
        if (this.scale == other.scale) {
            // Direct addition
            long sumLow = this.low + other.low;
            long carry = Decimal128.hasCarry(this.low, other.low, sumLow) ? 1 : 0;
            this.low = sumLow;
            this.high = this.high + other.high + carry;
        } else if (this.scale < other.scale) {
            // Rescale this
            rescaleInPlace(other.scale);
            long sumLow = this.low + other.low;
            long carry = Decimal128.hasCarry(this.low, other.low, sumLow) ? 1 : 0;
            this.low = sumLow;
            this.high = this.high + other.high + carry;
        } else {
            // Need to rescale other - but we can't modify it
            // For now, throw exception
            throw new UnsupportedOperationException("Cannot add with smaller scale without allocation");
        }
    }

    /**
     * Add a Decimal128 and store result in another sink
     *
     * @param other The number to add
     * @param sink  The sink to store result in
     */
    public void addTo(Decimal128 other, MutableDecimal128 sink) {
        // Copy this to sink
        sink.copyFrom(this);
        // Add other to sink
        sink.addDecimal128(other);
    }

    /**
     * Add another MutableDecimal128 to this one (in-place)
     * Convenience method - same as addMutable
     *
     * @param other The number to add
     */
    public void addTo(MutableDecimal128 other) {
        if (this.scale != other.scale) {
            throw new IllegalArgumentException("Cannot add numbers with different scales");
        }

        // Direct addition
        long sumLow = this.low + other.low;
        long carry = Decimal128.hasCarry(this.low, other.low, sumLow) ? 1 : 0;
        this.low = sumLow;
        this.high = this.high + other.high + carry;
    }

    /**
     * Copy values from another Decimal128
     */
    public void copyFrom(Decimal128 source) {
        this.high = source.getHigh();
        this.low = source.getLow();
        this.scale = source.getScale();
    }

    /**
     * Copy values from another MutableDecimal128
     */
    public void copyFrom(MutableDecimal128 source) {
        this.high = source.high;
        this.low = source.low;
        this.scale = source.scale;
    }

    /**
     * Divide this MutableDecimal128 by a Decimal128 (in-place)
     *
     * @param divisor     The Decimal128 to divide by
     * @param resultScale The desired scale of the result
     */
    public void divideDecimal128(Decimal128 divisor, int resultScale) {
        if (divisor.isZero()) {
            throw new ArithmeticException("Division by zero");
        }

        // Calculate how much to scale up the dividend
        int totalScaleUp = divisor.getScale() + resultScale - this.scale;

        // Scale up this (dividend) if needed
        if (totalScaleUp > 0) {
            for (int i = 0; i < totalScaleUp; i++) {
                multiplyBy10InPlace();
            }
        }

        // Track sign
        boolean thisNeg = this.isNegative();
        boolean divNeg = divisor.isNegative();
        boolean negative = (thisNeg != divNeg);

        // Make both positive for unsigned division
        if (thisNeg) {
            negateInPlace();
        }

        // We need divisor as positive - create temp vars (no allocation)
        long divHigh = divisor.getHigh();
        long divLow = divisor.getLow();
        if (divNeg) {
            // Negate divisor values
            long newLow = ~divLow + 1;
            long newHigh = ~divHigh;
            if (newLow == 0 && divLow != 0) {
                newHigh += 1;
            }
            divHigh = newHigh;
            divLow = newLow;
        }

        // Perform unsigned division in-place
        divideUnsignedInPlace(divHigh, divLow, totalScaleUp < 0 ? -totalScaleUp : 0);

        // Set result scale
        this.scale = resultScale;

        // Apply sign if needed
        if (negative) {
            negateInPlace();
        }
    }

    /**
     * Divide by a Decimal128 and store result in another sink
     *
     * @param divisor     The divisor
     * @param resultScale The desired scale of result
     * @param sink        The sink to store result in
     */
    public void divideTo(Decimal128 divisor, int resultScale, MutableDecimal128 sink) {
        // Copy this to sink
        sink.copyFrom(this);
        // Divide sink by divisor
        sink.divideDecimal128(divisor, resultScale);
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
    public void moduloDecimal128(Decimal128 divisor) {
        if (divisor.isZero()) {
            throw new ArithmeticException("Division by zero");
        }

        // Result scale should be the larger of the two scales
        int resultScale = Math.max(this.scale, divisor.getScale());

        // Align scales if different
        if (this.scale < resultScale) {
            rescaleInPlace(resultScale);
        }

        // Track sign - modulo result has same sign as dividend
        boolean negative = this.isNegative();

        // Make dividend positive for unsigned modulo
        if (negative) {
            negateInPlace();
        }

        // Get divisor values (handle sign)
        long divHigh = divisor.getHigh();
        long divLow = divisor.getLow();
        int divScale = divisor.getScale();

        if (divisor.isNegative()) {
            // Negate divisor values
            long newLow = ~divLow + 1;
            long newHigh = ~divHigh;
            if (newLow == 0 && divLow != 0) {
                newHigh += 1;
            }
            divHigh = newHigh;
            divLow = newLow;
        }

        // Scale divisor if needed
        if (divScale < resultScale) {
            int scaleDiff = resultScale - divScale;
            for (int i = 0; i < scaleDiff; i++) {
                // Multiply by 10: (8x + 2x)
                long high8 = (divHigh << 3) | (divLow >>> 61);
                long low8 = divLow << 3;
                long high2 = (divHigh << 1) | (divLow >>> 63);
                long low2 = divLow << 1;

                divLow = low8 + low2;
                long carry = Decimal128.hasCarry(low8, low2, divLow) ? 1 : 0;
                divHigh = high8 + high2 + carry;
            }
        }

        // Perform modulo operation in-place
        moduloUnsignedInPlace(divHigh, divLow);

        // Set result scale
        this.scale = resultScale;

        // Apply sign if needed (only if result is non-zero)
        if (negative && !this.isZero()) {
            negateInPlace();
        }
    }

    /**
     * Calculate modulo and store result in another sink
     *
     * @param divisor The divisor
     * @param sink    The sink to store result in
     */
    public void moduloTo(Decimal128 divisor, MutableDecimal128 sink) {
        // Copy this to sink
        sink.copyFrom(this);
        // Calculate modulo
        sink.moduloDecimal128(divisor);
    }

    /**
     * Multiply this MutableDecimal128 by a Decimal128 (in-place)
     *
     * @param other The Decimal128 to multiply by
     */
    public void multiplyDecimal128(Decimal128 other) {
        // Result scale is sum of scales
        int resultScale = this.scale + other.getScale();
        
        // Save the original signs before we modify anything
        boolean thisNegative = this.isNegative();
        boolean otherNegative = other.isNegative();
        
        // Convert to positive values for multiplication algorithm
        if (thisNegative) {
            negateInPlace();
        }
        
        // Get absolute value of other
        long otherHighAbs = other.getHigh();
        long otherLowAbs = other.getLow();
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
     * Multiply by a Decimal128 and store result in another sink
     *
     * @param other The number to multiply by
     * @param sink  The sink to store result in
     */
    public void multiplyTo(Decimal128 other, MutableDecimal128 sink) {
        // Copy this to sink
        sink.copyFrom(this);
        // Multiply sink by other
        sink.multiplyDecimal128(other);
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
        this.high = value < 0 ? -1L : 0L;
        this.low = value;
        this.scale = scale;
    }

    // Setters for individual fields
    public void setHigh(long high) {
        this.high = high;
    }

    public void setLow(long low) {
        this.low = low;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    /**
     * Subtract a Decimal128 from this MutableDecimal128 (in-place)
     *
     * @param other The Decimal128 to subtract
     */
    public void subtractDecimal128(Decimal128 other) {
        // Handle scale differences first
        if (this.scale < other.getScale()) {
            // Rescale this to match other's scale
            rescaleInPlace(other.getScale());
        }

        // Now perform subtraction
        if (this.scale == other.getScale()) {
            // Special case: subtracting zero
            if (other.isZero()) {
                // Nothing to do - subtracting zero doesn't change the value
                return;
            }
            
            // Direct subtraction via two's complement addition
            // Negate other: ~other + 1
            long otherLow = ~other.getLow() + 1;
            long otherHigh = ~other.getHigh();
            if (otherLow == 0 && other.getLow() != 0) {
                otherHigh += 1;
            }

            long sumLow = this.low + otherLow;
            long carry = Decimal128.hasCarry(this.low, otherLow, sumLow) ? 1 : 0;
            this.low = sumLow;
            this.high = this.high + otherHigh + carry;
        } else {
            // this.scale > other.getScale()
            
            // Special case: subtracting zero
            if (other.isZero()) {
                // Nothing to do - subtracting zero doesn't change the value
                return;
            }
            
            // Need to scale other up by (this.scale - other.getScale())
            int scaleDiff = this.scale - other.getScale();
            long otherHigh = other.getHigh();
            long otherLow = other.getLow();

            // Multiply other by 10^scaleDiff
            for (int i = 0; i < scaleDiff; i++) {
                // Multiply by 10: (8x + 2x)
                long high8 = (otherHigh << 3) | (otherLow >>> 61);
                long low8 = otherLow << 3;
                long high2 = (otherHigh << 1) | (otherLow >>> 63);
                long low2 = otherLow << 1;

                otherLow = low8 + low2;
                long carry = Decimal128.hasCarry(low8, low2, otherLow) ? 1 : 0;
                otherHigh = high8 + high2 + carry;
            }

            // Now negate the scaled value and add
            long negLow = ~otherLow + 1;
            long negHigh = ~otherHigh;
            if (negLow == 0 && otherLow != 0) {
                negHigh += 1;
            }

            long sumLow = this.low + negLow;
            long carry = Decimal128.hasCarry(this.low, negLow, sumLow) ? 1 : 0;
            this.low = sumLow;
            this.high = this.high + negHigh + carry;
        }
    }

    /**
     * Subtract a Decimal128 and store result in another sink
     *
     * @param other The number to subtract
     * @param sink  The sink to store result in
     */
    public void subtractTo(Decimal128 other, MutableDecimal128 sink) {
        // Copy this to sink
        sink.copyFrom(this);
        // Subtract other from sink
        sink.subtractDecimal128(other);
    }

    /**
     * Create an immutable Decimal128 from this mutable instance
     */
    public Decimal128 toDecimal128() {
        return new Decimal128(high, low, scale);
    }

    @Override
    public String toString() {
        return String.format("MutableDecimal128[high=%d, low=%d, scale=%d]", high, low, scale);
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
     * Perform unsigned division in-place
     *
     * @param divHigh        High 64 bits of divisor
     * @param divLow         Low 64 bits of divisor
     * @param divisorScaleUp How much to scale up divisor (if original totalScaleUp was negative)
     */
    private void divideUnsignedInPlace(long divHigh, long divLow, int divisorScaleUp) {
        // Scale divisor if needed
        for (int i = 0; i < divisorScaleUp; i++) {
            // Multiply by 10: (8x + 2x)
            long high8 = (divHigh << 3) | (divLow >>> 61);
            long low8 = divLow << 3;
            long high2 = (divHigh << 1) | (divLow >>> 63);
            long low2 = divLow << 1;

            divLow = low8 + low2;
            long carry = Decimal128.hasCarry(low8, low2, divLow) ? 1 : 0;
            divHigh = high8 + high2 + carry;
        }

        // Save dividend values
        long dividendHigh = this.high;
        long dividendLow = this.low;

        // Initialize result (quotient) to zero
        this.high = 0;
        this.low = 0;

        // Binary long division algorithm
        int shift = 0;
        long shiftedDivHigh = divHigh;
        long shiftedDivLow = divLow;

        // Find highest bit position where divisor fits into dividend
        while (compareUnsigned(dividendHigh, dividendLow, shiftedDivHigh, shiftedDivLow) >= 0 && shift < 128) {
            // Shift divisor left by 1
            long newHigh = (shiftedDivHigh << 1) | (shiftedDivLow >>> 63);
            long newLow = shiftedDivLow << 1;

            // Check if we would overflow
            if (newHigh < shiftedDivHigh) break;

            shiftedDivHigh = newHigh;
            shiftedDivLow = newLow;
            shift++;
        }

        // Back off one if we went too far
        if (shift > 0 && compareUnsigned(dividendHigh, dividendLow, shiftedDivHigh, shiftedDivLow) < 0) {
            shiftedDivHigh = (shiftedDivHigh >>> 1) | (shiftedDivLow << 63);
            shiftedDivLow = shiftedDivLow >>> 1;
            shift--;
        }

        // Perform division
        while (shift >= 0) {
            if (compareUnsigned(dividendHigh, dividendLow, shiftedDivHigh, shiftedDivLow) >= 0) {
                // Subtract divisor from dividend
                long diffLow = dividendLow - shiftedDivLow;
                long borrow = (Long.compareUnsigned(dividendLow, shiftedDivLow) < 0) ? 1 : 0;
                dividendHigh = dividendHigh - shiftedDivHigh - borrow;
                dividendLow = diffLow;

                // Set bit in quotient
                if (shift < 64) {
                    this.low |= (1L << shift);
                } else {
                    this.high |= (1L << (shift - 64));
                }
            }

            // Shift divisor right by 1
            shiftedDivLow = (shiftedDivLow >>> 1) | (shiftedDivHigh << 63);
            shiftedDivHigh = shiftedDivHigh >>> 1;
            shift--;
        }
    }

    /**
     * Perform unsigned modulo in-place
     *
     * @param divHigh High 64 bits of divisor
     * @param divLow  Low 64 bits of divisor
     */
    private void moduloUnsignedInPlace(long divHigh, long divLow) {
        // If dividend is less than divisor, remainder is dividend (no change needed)
        if (compareUnsigned(this.high, this.low, divHigh, divLow) < 0) {
            return;
        }

        // Binary long division to find remainder
        int shift = 0;
        long shiftedDivHigh = divHigh;
        long shiftedDivLow = divLow;

        // Find highest bit position where divisor fits
        while (compareUnsigned(this.high, this.low, shiftedDivHigh, shiftedDivLow) >= 0 && shift < 128) {
            // Shift divisor left by 1
            long newHigh = (shiftedDivHigh << 1) | (shiftedDivLow >>> 63);
            long newLow = shiftedDivLow << 1;

            // Check if we would overflow
            if (newHigh < shiftedDivHigh) break;

            shiftedDivHigh = newHigh;
            shiftedDivLow = newLow;
            shift++;
        }

        // Back off one if we went too far
        if (shift > 0 && compareUnsigned(this.high, this.low, shiftedDivHigh, shiftedDivLow) < 0) {
            shiftedDivHigh = (shiftedDivHigh >>> 1) | (shiftedDivLow << 63);
            shiftedDivLow = shiftedDivLow >>> 1;
            shift--;
        }

        // Perform division to get remainder
        while (shift >= 0) {
            if (compareUnsigned(this.high, this.low, shiftedDivHigh, shiftedDivLow) >= 0) {
                // Subtract divisor from this (remainder)
                long diffLow = this.low - shiftedDivLow;
                long borrow = (Long.compareUnsigned(this.low, shiftedDivLow) < 0) ? 1 : 0;
                this.high = this.high - shiftedDivHigh - borrow;
                this.low = diffLow;
            }

            // Shift divisor right by 1
            shiftedDivLow = (shiftedDivLow >>> 1) | (shiftedDivHigh << 63);
            shiftedDivHigh = shiftedDivHigh >>> 1;
            shift--;
        }
    }

    /**
     * Multiply this by 10 in place
     */
    private void multiplyBy10InPlace() {
        // Multiply by 10: (8x + 2x)
        long high8 = (this.high << 3) | (this.low >>> 61);
        long low8 = this.low << 3;
        long high2 = (this.high << 1) | (this.low >>> 63);
        long low2 = this.low << 1;

        this.low = low8 + low2;
        long carry = Decimal128.hasCarry(low8, low2, this.low) ? 1 : 0;
        this.high = high8 + high2 + carry;
    }

    /**
     * Negate this number in-place
     */
    private void negateInPlace() {
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
     * Rescale this MutableDecimal128 in place
     *
     * @param newScale The new scale (must be >= current scale)
     */
    private void rescaleInPlace(int newScale) {
        if (newScale < this.scale) {
            throw new IllegalArgumentException("Cannot reduce scale");
        }

        int scaleDiff = newScale - this.scale;

        // Multiply by 10^scaleDiff
        for (int i = 0; i < scaleDiff; i++) {
            multiplyBy10InPlace();
        }

        this.scale = newScale;
    }
}