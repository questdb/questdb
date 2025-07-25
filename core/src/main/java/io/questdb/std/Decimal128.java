package io.questdb.std;

/**
 * Decimal128 - A 128-bit decimal number implementation using two long values
 * <p>
 * This class represents decimal numbers with a fixed scale (number of decimal places)
 * using 128-bit integer arithmetic for precise calculations.
 */
public class Decimal128 {
    private final long high;  // High 64 bits
    private final long low;   // Low 64 bits
    private final int scale;  // Number of decimal places

    /**
     * Constructor
     *
     * @param high  High 64 bits of the 128-bit value
     * @param low   Low 64 bits of the 128-bit value
     * @param scale Number of decimal places
     */
    public Decimal128(long high, long low, int scale) {
        this.high = high;
        this.low = low;
        this.scale = scale;
    }

    /**
     * Create a Decimal128 from a double value
     *
     * @param value The double value
     * @param scale Number of decimal places
     */
    public static Decimal128 fromDouble(double value, int scale) {
        long scaleFactor = 1;
        for (int i = 0; i < scale; i++) {
            scaleFactor *= 10;
        }
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
        long h = value < 0 ? -1L : 0L;
        return new Decimal128(h, value, scale);
    }

    /**
     * Example usage
     */
    public static void main(String[] args) {
        // Create decimal numbers with 2 decimal places
        Decimal128 a = Decimal128.fromDouble(123.45, 2);  // 12345 (scaled by 100)
        Decimal128 b = Decimal128.fromDouble(67.89, 2);   // 6789 (scaled by 100)

        // Add them
        Decimal128 sum = a.add(b);

        System.out.println("Same scale addition:");
        System.out.println("a = " + a);
        System.out.println("b = " + b);
        System.out.println("a + b = " + sum.toString());
        System.out.println("As double: " + sum.toDouble());

        // Example with different scales
        System.out.println("\nDifferent scale addition:");
        Decimal128 price = Decimal128.fromDouble(19.99, 2);    // 2 decimal places
        Decimal128 tax = Decimal128.fromDouble(1.495, 3);      // 3 decimal places
        Decimal128 total = price.add(tax);                     // Result will have scale 3

        System.out.println("price (scale 2) = " + price);
        System.out.println("tax (scale 3) = " + tax);
        System.out.println("total (scale 3) = " + total.toString());
        System.out.println("As double: " + total.toDouble());

        // Multiplication example
        System.out.println("\nMultiplication:");
        Decimal128 quantity = Decimal128.fromDouble(3.5, 1);    // 3.5 items
        Decimal128 unitPrice = Decimal128.fromDouble(12.99, 2); // $12.99 per item
        Decimal128 totalPrice = quantity.multiply(unitPrice);   // Result has scale 3

        System.out.println("quantity = " + quantity);
        System.out.println("unit price = " + unitPrice);
        System.out.println("total price = " + totalPrice.toString());
        System.out.println("As double: " + totalPrice.toDouble());

        // Division example - same scales
        System.out.println("\nDivision (same scales):");
        Decimal128 totalAmount = Decimal128.fromDouble(100.00, 2);  // $100.00
        Decimal128 numPeople = Decimal128.fromDouble(3, 0);         // 3 people
        Decimal128 perPerson = totalAmount.divide(numPeople, 2);    // Result with 2 decimal places

        System.out.println("total amount = " + totalAmount);
        System.out.println("number of people = " + numPeople);
        System.out.println("per person = " + perPerson.toString());
        System.out.println("As double: " + perPerson.toDouble());

        // Division with different scales
        System.out.println("\nDivision (different scales):");
        Decimal128 distance = Decimal128.fromDouble(250.5, 1);      // 250.5 km (scale 1)
        Decimal128 time = Decimal128.fromDouble(3.25, 2);           // 3.25 hours (scale 2)
        Decimal128 speed = distance.divide(time, 2);                // km/h with 2 decimal places

        System.out.println("distance = " + distance + " km");
        System.out.println("time = " + time + " hours");
        System.out.println("speed = " + speed.toString() + " km/h");
        System.out.println("As double: " + speed.toDouble());

        // More complex division with different scales
        System.out.println("\nComplex division example:");
        Decimal128 amount = Decimal128.fromDouble(1000, 0);         // $1000 (no decimals)
        Decimal128 rate = Decimal128.fromDouble(0.045, 3);          // 4.5% rate (0.045)
        Decimal128 result = amount.multiply(rate);                  // $45.000

        System.out.println("amount = $" + amount.toDouble());
        System.out.println("rate = " + rate.toDouble());
        System.out.println("interest = $" + result.toDouble());

        // Division: interest / months
        Decimal128 months = Decimal128.fromDouble(12, 0);
        Decimal128 monthlyInterest = result.divide(months, 2);      // Monthly interest

        System.out.println("monthly interest = $" + monthlyInterest.toDouble());

        // Negative number operations
        System.out.println("\nNegative numbers:");
        Decimal128 positive = Decimal128.fromDouble(50.25, 2);
        Decimal128 negative = Decimal128.fromDouble(-30.75, 2);

        System.out.println("positive + negative = " + positive.add(negative).toDouble());
        System.out.println("positive - negative = " + positive.subtract(negative).toDouble());
        System.out.println("positive * negative = " + positive.multiply(negative).toDouble());
        System.out.println("negative / positive = " + negative.divide(positive, 4).toDouble());
    }

    /**
     * Negate a MutableDecimal128 in place
     *
     * @param sink The MutableDecimal128 to negate (modified in-place)
     */
    public static void negateInPlace(MutableDecimal128 sink) {
        long oldLow = sink.getLow();
        long oldHigh = sink.getHigh();

        // Two's complement: invert all bits and add 1
        long newLow = ~oldLow + 1;
        long newHigh = ~oldHigh;

        // Check for carry from low
        if (newLow == 0 && oldLow != 0) {
            newHigh += 1;
        }

        sink.setHigh(newHigh);
        sink.setLow(newLow);
    }

    /**
     * Rescale a MutableDecimal128 to a new scale in-place
     *
     * @param sink     The MutableDecimal128 to rescale (modified in-place)
     * @param newScale The new scale (must be >= current scale)
     */
    public static void rescaleToSink(MutableDecimal128 sink, int newScale) {
        if (newScale < sink.getScale()) {
            throw new IllegalArgumentException("Cannot reduce scale (would lose precision)");
        }

        if (newScale == sink.getScale()) {
            return;
        }

        // Calculate scale difference
        int scaleDiff = newScale - sink.getScale();

        // Multiply by 10^scaleDiff in-place
        for (int i = 0; i < scaleDiff; i++) {
            multiplyBy10InPlace(sink);
        }

        // Update scale
        sink.setScale(newScale);
    }

    /**
     * Add two Decimal128 numbers
     *
     * @param other The number to add
     * @return A new Decimal128 representing the sum with the larger scale
     */
    public Decimal128 add(Decimal128 other) {
        // If scales match, use direct addition
        if (this.scale == other.scale) {
            return addSameScale(other);
        }

        // Determine which number has larger scale
        if (this.scale > other.scale) {
            // Scale up the other number
            Decimal128 scaledOther = other.rescale(this.scale);
            return this.addSameScale(scaledOther);
        } else {
            // Scale up this number
            Decimal128 scaledThis = this.rescale(other.scale);
            return scaledThis.addSameScale(other);
        }
    }

    /**
     * Add another Decimal128 to this one and store result in sink
     *
     * @param other The number to add
     * @param sink  The MutableDecimal128 to store the result in
     */
    public void addTo(Decimal128 other, MutableDecimal128 sink) {
        // Copy this to sink and add other
        sink.copyFrom(this);
        sink.addDecimal128(other);
    }

    /**
     * Compare this to another Decimal128 (must have same scale)
     */
    public int compareTo(Decimal128 other) {
        if (this.scale != other.scale) {
            throw new IllegalArgumentException("Cannot compare numbers with different scales");
        }

        // Compare high parts first
        if (this.high != other.high) {
            return Long.compare(this.high, other.high);
        }

        // If high parts equal, compare low parts as unsigned
        return Long.compareUnsigned(this.low, other.low);
    }

    /**
     * Divide this Decimal128 by another
     *
     * @param divisor     The divisor
     * @param resultScale The desired scale of the result
     * @return A new Decimal128 representing the quotient
     * @throws ArithmeticException if divisor is zero
     */
    public Decimal128 divide(Decimal128 divisor, int resultScale) {
        if (divisor.isZero()) {
            throw new ArithmeticException("Division by zero");
        }

        // To perform division with proper precision:
        // (value1 / 10^scale1) / (value2 / 10^scale2) = (value1 * 10^scale2) / (value2 * 10^scale1)
        // To get result with resultScale decimal places:
        // = (value1 * 10^(scale2 + resultScale)) / (value2 * 10^scale1)

        // Calculate how much to scale up the dividend
        int totalScaleUp = divisor.scale + resultScale - this.scale;

        Decimal128 scaledDividend = this;
        Decimal128 scaledDivisor = divisor;

        // We need to ensure we don't lose precision
        // If totalScaleUp is negative, we scale up the divisor instead
        if (totalScaleUp < 0) {
            // Scale up the divisor instead
            for (int i = 0; i < -totalScaleUp; i++) {
                scaledDivisor = scaledDivisor.multiplyBy10();
            }
        } else if (totalScaleUp > 0) {
            // Scale up the dividend as normal
            for (int i = 0; i < totalScaleUp; i++) {
                scaledDividend = scaledDividend.multiplyBy10();
            }
        }

        // Track sign
        boolean negative = (this.isNegative() != divisor.isNegative());

        // Convert to positive for division
        Decimal128 absDividend = scaledDividend.isNegative() ? scaledDividend.negate() : scaledDividend;
        Decimal128 absDivisor = scaledDivisor.isNegative() ? scaledDivisor.negate() : scaledDivisor;

        // Perform unsigned division
        Decimal128 quotient = divideUnsigned(absDividend, absDivisor);

        // Apply sign and scale
        Decimal128 result = new Decimal128(quotient.high, quotient.low, resultScale);
        return negative ? result.negate() : result;
    }

    /**
     * Divide this Decimal128 by another and store result in sink
     *
     * @param divisor     The divisor
     * @param resultScale The desired scale of the result
     * @param sink        The MutableDecimal128 to store the result in
     * @throws ArithmeticException if divisor is zero
     */
    public void divideTo(Decimal128 divisor, int resultScale, MutableDecimal128 sink) {
        // Copy this to sink and divide by divisor (no allocations)
        sink.copyFrom(this);
        sink.divideDecimal128(divisor, resultScale);
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
     * Calculate the modulo (remainder) of this Decimal128 divided by another
     *
     * @param divisor The divisor
     * @return A new Decimal128 representing the remainder with the larger scale of the two operands
     * @throws ArithmeticException if divisor is zero
     */
    public Decimal128 modulo(Decimal128 divisor) {
        if (divisor.isZero()) {
            throw new ArithmeticException("Division by zero");
        }

        // Result scale should be the larger of the two scales
        int resultScale = Math.max(this.scale, divisor.scale);

        // Align scales if different
        Decimal128 dividend = this;
        Decimal128 div = divisor;

        if (this.scale < resultScale) {
            dividend = this.rescale(resultScale);
        }
        if (divisor.scale < resultScale) {
            div = divisor.rescale(resultScale);
        }

        // Track sign - modulo result has the same sign as dividend
        boolean negative = dividend.isNegative();

        // Convert to positive for calculation
        Decimal128 absDividend = dividend.isNegative() ? dividend.negate() : dividend;
        Decimal128 absDivisor = div.isNegative() ? div.negate() : div;

        // Perform division and get remainder
        Decimal128 remainder = moduloUnsigned(absDividend, absDivisor);

        // Apply sign and scale
        Decimal128 result = new Decimal128(remainder.high, remainder.low, resultScale);
        return negative && !result.isZero() ? result.negate() : result;
    }

    /**
     * Calculate the modulo (remainder) of this Decimal128 divided by another and store in sink
     *
     * @param divisor The divisor
     * @param sink    The MutableDecimal128 to store the result in
     * @throws ArithmeticException if divisor is zero
     */
    public void moduloTo(Decimal128 divisor, MutableDecimal128 sink) {
        // Copy this to sink and calculate modulo (no allocations)
        sink.copyFrom(this);
        sink.moduloDecimal128(divisor);
    }

    /**
     * Multiply two Decimal128 numbers
     *
     * @param other The number to multiply by
     * @return A new Decimal128 representing the product
     */
    public Decimal128 multiply(Decimal128 other) {
        // Result scale is sum of scales
        int resultScale = this.scale + other.scale;

        // For simplicity, convert to positive numbers and track sign
        boolean negative = (this.isNegative() != other.isNegative());

        Decimal128 absThis = this.isNegative() ? this.negate() : this;
        Decimal128 absOther = other.isNegative() ? other.negate() : other;

        // Perform unsigned multiplication
        Decimal128 product = multiplyUnsigned(absThis, absOther);

        // Apply sign and scale
        Decimal128 result = new Decimal128(product.high, product.low, resultScale);
        return negative ? result.negate() : result;
    }

    /**
     * Multiply this by another Decimal128 and store result in sink
     *
     * @param other The number to multiply by
     * @param sink  The MutableDecimal128 to store the result in
     */
    public void multiplyTo(Decimal128 other, MutableDecimal128 sink) {
        // Copy this to sink and multiply by other
        sink.copyFrom(this);
        sink.multiplyDecimal128(other);
    }

    /**
     * Negate this number
     */
    public Decimal128 negate() {
        // Two's complement: invert all bits and add 1
        long newLow = ~low + 1;
        long newHigh = ~high;

        // Check for carry from low
        if (newLow == 0 && low != 0) {
            newHigh += 1;
        }

        return new Decimal128(newHigh, newLow, scale);
    }

    /**
     * Rescale this decimal to a new scale (must be larger than current scale)
     *
     * @param newScale The new scale (must be >= current scale)
     * @return A new Decimal128 with the new scale
     */
    public Decimal128 rescale(int newScale) {
        if (newScale < this.scale) {
            throw new IllegalArgumentException("Cannot reduce scale (would lose precision)");
        }

        if (newScale == this.scale) {
            return this;
        }

        // Calculate scale difference
        int scaleDiff = newScale - this.scale;

        // Multiply by 10^scaleDiff
        Decimal128 result = this;
        for (int i = 0; i < scaleDiff; i++) {
            result = result.multiplyBy10();
        }

        return new Decimal128(result.high, result.low, newScale);
    }

    /**
     * Subtract another Decimal128 from this one
     */
    public Decimal128 subtract(Decimal128 other) {
        return this.add(other.negate());
    }

    /**
     * Subtract another Decimal128 from this one and store result in sink
     *
     * @param other The number to subtract
     * @param sink  The MutableDecimal128 to store the result in
     */
    public void subtractTo(Decimal128 other, MutableDecimal128 sink) {
        // Copy this to sink and subtract other (no allocations)
        sink.copyFrom(this);
        sink.subtractDecimal128(other);
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

    /**
     * Convert to string representation
     */
    @Override
    public String toString() {
        if (high == 0 && low >= 0) {
            // Simple case: fits in positive long
            return longToDecimalString(low, scale);
        } else if (high == -1 && low < 0) {
            // Simple negative case
            return longToDecimalString(low, scale);
        } else {
            // Complex case: full 128-bit conversion
            return fullToString();
        }
    }

    /**
     * Add first operand (which can be a MutableDecimal128) to second operand and store in sink
     * All three can be the same object for in-place operations
     *
     * @param first  First operand (as MutableDecimal128)
     * @param second Second operand
     * @param sink   The MutableDecimal128 to store the result in
     */
    private static void addSameScaleToSink(MutableDecimal128 first, Decimal128 second, MutableDecimal128 sink) {
        // Perform 128-bit addition
        long sumLow = first.getLow() + second.getLow();

        // Check for carry from low addition
        long carry = hasCarry(first.getLow(), second.getLow(), sumLow) ? 1 : 0;

        // Add high 64 bits with carry
        long sumHigh = first.getHigh() + second.getHigh() + carry;

        // Store result in sink (handles case where sink is same as first)
        sink.set(sumHigh, sumLow, first.getScale());
    }

    /**
     * Add first operand to second operand (which can be a MutableDecimal128) and store in sink
     *
     * @param first  First operand
     * @param second Second operand (as MutableDecimal128)
     * @param sink   The MutableDecimal128 to store the result in
     */
    private static void addSameScaleToSink(Decimal128 first, MutableDecimal128 second, MutableDecimal128 sink) {
        // Perform 128-bit addition
        long sumLow = first.getLow() + second.getLow();

        // Check for carry from low addition
        long carry = hasCarry(first.getLow(), second.getLow(), sumLow) ? 1 : 0;

        // Add high 64 bits with carry
        long sumHigh = first.getHigh() + second.getHigh() + carry;

        // Store result in sink
        sink.set(sumHigh, sumLow, first.getScale());
    }

    /**
     * Compare two Decimal128 values as unsigned 128-bit integers
     * Ignores scale - just compares the raw 128-bit values
     */
    private static int compareUnsigned(Decimal128 a, Decimal128 b) {
        // Compare high parts first as unsigned
        int highCmp = Long.compareUnsigned(a.high, b.high);
        if (highCmp != 0) {
            return highCmp;
        }

        // If high parts equal, compare low parts as unsigned
        return Long.compareUnsigned(a.low, b.low);
    }

    /**
     * Unsigned 128-bit division
     * Returns quotient only (no remainder)
     */
    private static Decimal128 divideUnsigned(Decimal128 dividend, Decimal128 divisor) {
        // Simple but slow division algorithm
        // For production, consider implementing a more efficient algorithm

        // Work with raw values, ignoring scale during division
        if (compareUnsigned(dividend, divisor) < 0) {
            return new Decimal128(0, 0, 0);
        }

        // Binary long division
        Decimal128 quotient = new Decimal128(0, 0, 0);
        Decimal128 remainder = new Decimal128(dividend.high, dividend.low, 0);
        Decimal128 workingDivisor = new Decimal128(divisor.high, divisor.low, 0);

        // Find the highest bit position where divisor fits
        int shift = 0;
        Decimal128 shiftedDivisor = workingDivisor;

        while (compareUnsigned(remainder, shiftedDivisor) >= 0 && shift < 128) {
            shiftedDivisor = shiftedDivisor.shiftLeft(1);
            shift++;
        }

        // Back off one if we went too far
        if (shift > 0 && compareUnsigned(remainder, shiftedDivisor) < 0) {
            shiftedDivisor = shiftedDivisor.shiftRight(1);
            shift--;
        }

        // Perform division
        while (shift >= 0) {
            if (compareUnsigned(remainder, shiftedDivisor) >= 0) {
                remainder = subtractUnsigned(remainder, shiftedDivisor);
                quotient = quotient.setBit(shift);
            }
            shiftedDivisor = shiftedDivisor.shiftRight(1);
            shift--;
        }

        return quotient;
    }

    /**
     * Unsigned 128-bit modulo operation
     * Returns remainder only
     */
    private static Decimal128 moduloUnsigned(Decimal128 dividend, Decimal128 divisor) {
        // If dividend is less than divisor, remainder is dividend
        if (compareUnsigned(dividend, divisor) < 0) {
            return dividend;
        }

        // Binary long division to find remainder
        Decimal128 remainder = new Decimal128(dividend.high, dividend.low, 0);
        Decimal128 workingDivisor = new Decimal128(divisor.high, divisor.low, 0);

        // Find the highest bit position where divisor fits
        int shift = 0;
        Decimal128 shiftedDivisor = workingDivisor;

        while (compareUnsigned(remainder, shiftedDivisor) >= 0 && shift < 128) {
            shiftedDivisor = shiftedDivisor.shiftLeft(1);
            shift++;
        }

        // Back off one if we went too far
        if (shift > 0 && compareUnsigned(remainder, shiftedDivisor) < 0) {
            shiftedDivisor = shiftedDivisor.shiftRight(1);
            shift--;
        }

        // Perform division to get remainder
        while (shift >= 0) {
            if (compareUnsigned(remainder, shiftedDivisor) >= 0) {
                remainder = subtractUnsigned(remainder, shiftedDivisor);
            }
            shiftedDivisor = shiftedDivisor.shiftRight(1);
            shift--;
        }

        return remainder;
    }

    /**
     * Multiply a MutableDecimal128 by 10 in-place
     *
     * @param sink The MutableDecimal128 to multiply (modified in-place)
     */
    private static void multiplyBy10InPlace(MutableDecimal128 sink) {
        long oldHigh = sink.getHigh();
        long oldLow = sink.getLow();

        // First multiply by 8 (shift left 3)
        long high8 = (oldHigh << 3) | (oldLow >>> 61);
        long low8 = oldLow << 3;

        // Then multiply by 2 (shift left 1)
        long high2 = (oldHigh << 1) | (oldLow >>> 63);
        long low2 = oldLow << 1;

        // Add them together (8x + 2x = 10x)
        long sumLow = low8 + low2;
        long carry = hasCarry(low8, low2, sumLow) ? 1 : 0;
        long sumHigh = high8 + high2 + carry;

        // Store result back in sink
        sink.setHigh(sumHigh);
        sink.setLow(sumLow);
    }

    /**
     * Multiply two unsigned 128-bit numbers (returns lower 128 bits of result)
     * Uses the standard long multiplication algorithm
     */
    private static Decimal128 multiplyUnsigned(Decimal128 a, Decimal128 b) {
        // Split each 128-bit number into four 32-bit chunks
        long a3 = a.high >>> 32;
        long a2 = a.high & 0xFFFFFFFFL;
        long a1 = a.low >>> 32;
        long a0 = a.low & 0xFFFFFFFFL;

        long b3 = b.high >>> 32;
        long b2 = b.high & 0xFFFFFFFFL;
        long b1 = b.low >>> 32;
        long b0 = b.low & 0xFFFFFFFFL;

        // Multiply all combinations (only keep lower 128 bits)
        // Result = a0*b0 + (a0*b1 + a1*b0)<<32 + (a0*b2 + a1*b1 + a2*b0)<<64 + ...

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

        long low = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        long high = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);

        return new Decimal128(high, low, 0);
    }

    /**
     * Multiply two numbers and store result in sink (handles signs properly)
     *
     * @param a    First operand
     * @param b    Second operand
     * @param sink The MutableDecimal128 to store the result in
     */
    private static void multiplyUnsignedToSink(Decimal128 a, Decimal128 b, MutableDecimal128 sink) {
        // Get absolute values for multiplication
        long aHigh = a.high;
        long aLow = a.low;
        long bHigh = b.high;
        long bLow = b.low;

        // Handle negative values by converting to positive
        boolean aNeg = a.isNegative();
        boolean bNeg = b.isNegative();

        if (aNeg) {
            // Two's complement negation
            aLow = ~aLow + 1;
            aHigh = ~aHigh;
            if (aLow == 0 && a.low != 0) {
                aHigh += 1;
            }
        }

        if (bNeg) {
            // Two's complement negation
            bLow = ~bLow + 1;
            bHigh = ~bHigh;
            if (bLow == 0 && b.low != 0) {
                bHigh += 1;
            }
        }

        // Split each 128-bit number into four 32-bit chunks
        long a3 = aHigh >>> 32;
        long a2 = aHigh & 0xFFFFFFFFL;
        long a1 = aLow >>> 32;
        long a0 = aLow & 0xFFFFFFFFL;

        long b3 = bHigh >>> 32;
        long b2 = bHigh & 0xFFFFFFFFL;
        long b1 = bLow >>> 32;
        long b0 = bLow & 0xFFFFFFFFL;

        // Multiply all combinations (only keep lower 128 bits)
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

        long low = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        long high = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);

        // Store result in sink
        sink.set(high, low, 0);
    }

    /**
     * Subtract two unsigned 128-bit numbers
     * a must be >= b (unsigned comparison)
     */
    private static Decimal128 subtractUnsigned(Decimal128 a, Decimal128 b) {
        // Perform subtraction
        long diffLow = a.low - b.low;
        long borrow = 0;

        // Check if we need to borrow
        if (Long.compareUnsigned(a.low, b.low) < 0) {
            borrow = 1;
        }

        long diffHigh = a.high - b.high - borrow;

        return new Decimal128(diffHigh, diffLow, 0);
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
     * Add two Decimal128 numbers with the same scale
     *
     * @param other The number to add (must have same scale)
     * @return A new Decimal128 representing the sum
     */
    private Decimal128 addSameScale(Decimal128 other) {
        // Perform 128-bit addition
        // First add the low 64 bits
        long sumLow = this.low + other.low;

        // Check for carry from low addition
        long carry = 0;
        if (hasCarry(this.low, other.low, sumLow)) {
            carry = 1;
        }

        // Add high 64 bits with carry
        long sumHigh = this.high + other.high + carry;

        return new Decimal128(sumHigh, sumLow, this.scale);
    }

    /**
     * Add two Decimal128 numbers with the same scale and store in sink
     *
     * @param other The number to add (must have same scale)
     * @param sink  The MutableDecimal128 to store the result in
     */
    private void addSameScaleTo(Decimal128 other, MutableDecimal128 sink) {
        // Perform 128-bit addition
        long sumLow = this.low + other.low;

        // Check for carry from low addition
        long carry = hasCarry(this.low, other.low, sumLow) ? 1 : 0;

        // Add high 64 bits with carry
        long sumHigh = this.high + other.high + carry;

        // Store result in sink
        sink.set(sumHigh, sumLow, this.scale);
    }

    /**
     * Full 128-bit to string conversion (simplified version)
     * For production use, consider using BigInteger for complex cases
     */
    private String fullToString() {
        // This is a simplified implementation
        // For full production use, you'd want a complete 128-bit division algorithm
        return String.format("Decimal128[high=%d, low=%d, scale=%d]", high, low, scale);
    }

    /**
     * Convert a long to decimal string with scale
     */
    private String longToDecimalString(long value, int scale) {
        if (scale == 0) {
            return Long.toString(value);
        }

        String str = Long.toString(Math.abs(value));

        // Pad with zeros if necessary
        while (str.length() <= scale) {
            str = "0" + str;
        }

        // Insert decimal point
        int pointPos = str.length() - scale;
        String result = str.substring(0, pointPos) + "." + str.substring(pointPos);

        // Add negative sign if needed
        if (value < 0) {
            result = "-" + result;
        }

        return result;
    }

    /**
     * Multiply this number by 10 (used for rescaling)
     *
     * @return A new Decimal128 that is 10 times this value
     */
    private Decimal128 multiplyBy10() {
        // First multiply by 8 (shift left 3)
        long high8 = (this.high << 3) | (this.low >>> 61);
        long low8 = this.low << 3;

        // Then multiply by 2 (shift left 1)
        long high2 = (this.high << 1) | (this.low >>> 63);
        long low2 = this.low << 1;

        // Add them together (8x + 2x = 10x)
        long sumLow = low8 + low2;
        long carry = hasCarry(low8, low2, sumLow) ? 1 : 0;
        long sumHigh = high8 + high2 + carry;

        return new Decimal128(sumHigh, sumLow, this.scale);
    }

    /**
     * Set a specific bit
     */
    private Decimal128 setBit(int bit) {
        if (bit < 64) {
            return new Decimal128(high, low | (1L << bit), scale);
        } else {
            return new Decimal128(high | (1L << (bit - 64)), low, scale);
        }
    }

    /**
     * Shift left by one bit
     */
    private Decimal128 shiftLeft(int bits) {
        if (bits == 0) return this;
        if (bits >= 128) return new Decimal128(0, 0, scale);

        if (bits < 64) {
            long newHigh = (high << bits) | (low >>> (64 - bits));
            long newLow = low << bits;
            return new Decimal128(newHigh, newLow, scale);
        } else {
            long newHigh = low << (bits - 64);
            return new Decimal128(newHigh, 0, scale);
        }
    }

    /**
     * Shift right by one bit
     */
    private Decimal128 shiftRight(int bits) {
        if (bits == 0) return this;
        if (bits >= 128) return new Decimal128(0, 0, scale);

        if (bits < 64) {
            long newLow = (low >>> bits) | (high << (64 - bits));
            long newHigh = high >> bits;
            return new Decimal128(newHigh, newLow, scale);
        } else {
            long newLow = high >> (bits - 64);
            return new Decimal128(high < 0 ? -1L : 0L, newLow, scale);
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
}