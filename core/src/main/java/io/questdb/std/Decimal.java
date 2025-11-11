/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.std;

/**
 * Common interface for all decimal types (Decimal64, Decimal128, Decimal256).
 * <p>
 * This interface defines the core operations required for decimal parsing and manipulation.
 * All decimal types in QuestDB implement this interface to provide consistent behavior
 * across different precision levels.
 * </p>
 * <p>
 * The interface is primarily used by {@link DecimalParser} to parse decimal literals
 * from strings into their binary representation. Each implementing class provides
 * precision-specific implementations optimized for their storage size.
 * </p>
 *
 * @see Decimal64
 * @see Decimal128
 * @see Decimal256
 * @see DecimalParser
 */
public interface Decimal {
    /**
     * Adds a specific digit multiplied by a power of ten to the current decimal value.
     * <p>
     * This method is used during decimal parsing to build up the value digit by digit.
     * It modifies the decimal in-place by adding (multiplier × 10^pow) to the current value.
     * </p>
     * <p>
     * Example: To add the digit 5 at position 10^2 (hundreds place):
     * <pre>
     * decimal.addPowerOfTenMultiple(2, 5);  // Adds 500 to the value
     * </pre>
     *
     * @param pow        the power of ten position (0 for ones, 1 for tens, 2 for hundreds, etc.)
     * @param multiplier the digit to add (0-9); 0 is a no-op for efficiency
     * @throws NumericException if the operation would cause overflow
     */
    void addPowerOfTenMultiple(int pow, int multiplier);

    /**
     * Returns the maximum precision (total number of significant digits) supported by this decimal type.
     * <p>
     * Precision limits vary by implementation:
     * <ul>
     * <li>Decimal64: 18 digits</li>
     * <li>Decimal128: 38 digits</li>
     * <li>Decimal256: 76 digits</li>
     * </ul>
     *
     * @return the maximum number of significant digits this type can represent
     */
    int getMaxPrecision();

    /**
     * Returns the maximum scale (number of digits after the decimal point) supported by this decimal type.
     * <p>
     * Scale limits vary by implementation:
     * <ul>
     * <li>Decimal64: 18</li>
     * <li>Decimal128: 38</li>
     * <li>Decimal256: 76</li>
     * </ul>
     *
     * @return the maximum number of decimal places this type can represent
     */
    int getMaxScale();

    /**
     * Returns true if this decimal represents the null value.
     */
    boolean isNull();

    /**
     * Negates this decimal value in-place, changing its sign.
     * <p>
     * If the decimal is positive, it becomes negative; if negative, it becomes positive.
     * Zero remains zero. Null values remain null.
     * <p>
     * Example:
     * <pre>
     * decimal.of(123, 2);  // 1.23
     * decimal.negate();    // -1.23
     * </pre>
     */
    void negate();

    /**
     * Sets this decimal to the null value.
     * <p>
     * After calling this method, the decimal represents a NULL, which is distinct
     * from zero. NULL decimals cannot participate in arithmetic operations.
     *
     * @see Decimals#DECIMAL64_NULL
     * @see Decimals#DECIMAL128_HI_NULL
     * @see Decimals#DECIMAL256_HH_NULL
     */
    void ofNull();

    /**
     * Resets this decimal to zero with scale 0.
     * <p>
     * This method initializes the decimal to represent the exact value 0, clearing
     * any previous value and resetting the scale to 0.
     */
    void ofZero();

    /**
     * Sets the scale directly without performing any rescaling operations on the underlying value.
     * <p>
     * <b>Warning:</b> This is a low-level operation that changes only the scale metadata
     * without adjusting the stored value. This effectively multiplies or divides the
     * represented number by a power of 10. Use with caution.
     * <p>
     * Example:
     * <pre>
     * decimal.of(12345, 2);  // Value: 123.45
     * decimal.setScale(3);   // Now represents: 12.345 (same raw value, different interpretation)
     * </pre>
     * <p>
     * For safe rescaling that adjusts the underlying value, use the {@code rescale()} method
     * available in concrete implementations.
     *
     * @param scale the new scale (number of decimal places)
     */
    void setScale(int scale);

    /**
     * Converts this decimal to a Decimal256 representation.
     * <p>
     * This method copies the value of this decimal into the provided Decimal256 instance,
     * performing any necessary conversions. The scale is preserved during conversion.
     * This is useful for promoting smaller decimal types (Decimal64, Decimal128) to the
     * highest precision type.
     * <p>
     * The conversion is always safe as Decimal256 has the highest precision and can
     * represent any value from smaller decimal types without loss of information.
     *
     * @param decimal the Decimal256 instance to write the converted value into
     */
    void toDecimal256(Decimal256 decimal);
}
