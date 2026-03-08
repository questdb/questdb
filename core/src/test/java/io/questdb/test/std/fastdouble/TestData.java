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

package io.questdb.test.std.fastdouble;

public final class TestData {
    private final int byteLength;
    private final int byteOffset;
    private final int charLength;
    private final int charOffset;
    private final double expectedDoubleValue;
    private final float expectedFloatValue;
    private final String input;
    private final String title;
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

    public int byteLength() {
        return byteLength;
    }

    public int byteOffset() {
        return byteOffset;
    }

    public int charLength() {
        return charLength;
    }

    public int charOffset() {
        return charOffset;
    }

    public double expectedDoubleValue() {
        return expectedDoubleValue;
    }

    public float expectedFloatValue() {
        return expectedFloatValue;
    }

    public String input() {
        return input;
    }

    public String title() {
        return title;
    }

    public boolean valid() {
        return valid;
    }
}
