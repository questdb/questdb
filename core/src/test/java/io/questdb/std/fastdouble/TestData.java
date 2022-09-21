/*
 * @(#)TestData.java
 * Copyright © 2022. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

public final class TestData {
    private final String title;
    private final String input;
    private final int charOffset;
    private final int charLength;
    private final int byteOffset;
    private final int byteLength;
    private final double expectedDoubleValue;
    private final float expectedFloatValue;
    private final boolean valid;

    public TestData(String title,
                    String input,
                    int charOffset, int charLength,
                    int byteOffset, int byteLength,
                    double expectedDoubleValue,
                    float expectedFloatValue,
                    boolean valid) {
        this.title = title;
        this.input = input;
        this.charOffset = charOffset;
        this.charLength = charLength;
        this.byteOffset = byteOffset;
        this.byteLength = byteLength;
        this.expectedDoubleValue = expectedDoubleValue;
        this.expectedFloatValue = expectedFloatValue;
        this.valid = valid;
    }

    public TestData(String input, double expectedDoubleValue, float expectedFloatValue) {
        this(input, input, 0, input.length(), 0, input.length(),
                expectedDoubleValue,
                expectedFloatValue, true);
    }

    public TestData(String input, double expectedDoubleValue, float expectedFloatValue, int offset, int length) {
        this(
                input,
                input,
                offset,
                length,
                offset,
                length,
                expectedDoubleValue,
                expectedFloatValue, true);
    }

    public TestData(String title, String input, double expectedDoubleValue, float expectedFloatValue) {
        this(title.contains(input) ? title : title + " " + input,
                input,
                0,
                input.length(),
                0,
                input.length(),
                expectedDoubleValue,
                expectedFloatValue, true);
    }

    public TestData(String input) {
        this(input, input);
    }

    public TestData(String title, String input) {
        this(title.contains(input) ? title : title + " " + input, input, 0, input.length(), 0, input.length(),
                Double.NaN,
                Float.NaN, false);
    }

    public String title() {
        return title;
    }

    public String input() {
        return input;
    }

    public int charOffset() {
        return charOffset;
    }

    public int charLength() {
        return charLength;
    }

    public int byteOffset() {
        return byteOffset;
    }

    public int byteLength() {
        return byteLength;
    }

    public double expectedDoubleValue() {
        return expectedDoubleValue;
    }

    public float expectedFloatValue() {
        return expectedFloatValue;
    }

    public boolean valid() {
        return valid;
    }
}
