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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Produces strings by applying the syntax rules for a double literal
 * value. The generation uses a random number generator.
 * <p>
 * <p>
 * References:
 * <dl>
 *     <dt>Java® Platform, Standard Edition & Java Development Kit.
 *          Version 16 API Specification.
 *     Javadoc of class java.lang.Double, method parseDouble(String).</dt>
 *     <dd><a href="https://docs.oracle.com/en/java/javase/16/docs/api/java.base/java/lang/Double.html#valueOf(java.lang.String)">
 *         docs.oracle.com</a></dd>
 * </dl>
 * <dl>
 *     <dt>The Java Language Specification. Java SE 16 Edition.
 *     Chapter 3.10.2 Floating-Point Literals</dt>
 *     <dd><a href="https://docs.oracle.com/javase/specs/jls/se16/html/jls-3.html#jls-3.10.2">
 *         docs.oracle.com</a></dd>
 * </dl>
 */
public class LexicalGenerator {

    private final static char[] DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    };
    private final static char[] DIGITS_OR_UNDERSCORE = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '_',
    };
    private final static char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'a', 'b', 'c', 'd', 'e', 'f',
            'A', 'B', 'C', 'D', 'E', 'F',
    };
    private final static char[] HEX_DIGITS_OR_UNDERSCORE = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'a', 'b', 'c', 'd', 'e', 'f',
            'A', 'B', 'C', 'D', 'E', 'F', '_'
    };
    private final boolean produceFloatTypeSuffix;
    private final boolean produceUnderscore;

    public LexicalGenerator(boolean produceUnderscore, boolean produceFloatTypeSuffix) {
        this.produceUnderscore = produceUnderscore;
        this.produceFloatTypeSuffix = produceFloatTypeSuffix;
    }

    public static void main(String... args) throws IOException {
        Path outPath = FileSystems.getDefault().getPath("data/canada_hex.txt");
        Path inPath = FileSystems.getDefault().getPath("data/canada.txt");
        try (BufferedReader r = Files.newBufferedReader(inPath, StandardCharsets.UTF_8);
             BufferedWriter w = Files.newBufferedWriter(outPath, StandardCharsets.UTF_8)) {
            for (String line = r.readLine(); line != null; line = r.readLine()) {
                w.write(Double.toHexString(Double.parseDouble(line)));
                w.write('\n');
            }
        }
    }

    public static void main1(String... args) throws IOException {
        Path path;
        if (args.length == 0) {
            System.out.println("Please provide the output file.");
            path = null;
            System.exit(10);
        } else {
            path = FileSystems.getDefault().getPath(args[0]);
        }
        Random rng = new Random(0);
        LexicalGenerator gen = new LexicalGenerator(false, true);
        Set<String> produced = new HashSet<>();
        try (BufferedWriter w = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            while (produced.size() < 100_000) {
                String str = gen.produceRandomInputStringFromLexicalRuleWithoutWhitespace(40, rng);
                if (produced.add(str)) {
                    w.write(str);
                    w.write('\n');
                }
            }
        }
    }

    /**
     * Produces a random input string from lexical rules.
     *
     * @param remaining the remaining number of rules to perform
     * @param rng       a random number generator
     * @return a string
     */
    public String produceRandomInputStringFromLexicalRuleWithWhitespace(int remaining, Random rng) {
        StringBuilder buf = new StringBuilder();
        remaining = produceRandomWhitespaces(remaining, rng, buf);
        if (remaining > 0) {
            remaining = produceRandomFloatValue(remaining, rng, buf);
        }
        if (remaining > 0) {
            produceRandomWhitespaces(remaining, rng, buf);
        }
        return buf.toString();
    }

    /**
     * Produces a random input string from lexical rules.
     * <dl>
     * <dt><i>FloatValue:</i>
     * <dd><i>[Sign]</i> {@code NaN}
     * <dd><i>[Sign]</i> {@code Infinity}
     * <dd><i>[Sign] FloatingPointLiteral</i>
     * <dd><i>[Sign] HexadecimalFloatingPointLiteral</i>
     * <dd><i>SignedInteger</i>
     * </dl>
     *
     * @param remaining the remaining number of rules to perform
     * @param rng       a random number generator
     * @return a string
     */
    public String produceRandomInputStringFromLexicalRuleWithoutWhitespace(int remaining, Random rng) {
        StringBuilder buf = new StringBuilder();
        produceRandomFloatValue(remaining, rng, buf);
        return buf.toString();
    }

    /**
     * <dl>
     * <dt><i>BinaryExponent:</i>
     * <dd><i>BinaryExponentIndicator SignedInteger</i>
     * </dl>
     *
     * <dl>
     * <dt><i>BinaryExponentIndicator:</i>
     * <dd>{@code p}
     * <dd>{@code P}
     * </dl>
     */
    private int produceRandomBinaryExponent(int remaining, Random rng, StringBuilder buf) {
        buf.append(rng.nextBoolean() ? 'p' : 'P');
        remaining--;
        remaining = produceRandomSignedInteger(remaining, rng, buf);
        return remaining;
    }

    /**
     * <dl>
     * <dt><i>DecimalFloatingPointLiteral:</i>
     * <dd><i>Digits {@code .} [Digits] [ExponentPart] [FloatTypeSuffix]</i>
     * <dd><i>{@code .} Digits [ExponentPart] [FloatTypeSuffix]</i>
     * <dd><i>Digits ExponentPart [FloatTypeSuffix]</i>
     * <dd><i>Digits [ExponentPart] FloatTypeSuffix</i>
     * </dl>
     */
    private int produceRandomDecimalFloatingPointLiteral(int remaining, Random rng, StringBuilder buf) {
        switch (rng.nextInt(4)) {
            case 0:
                remaining = produceRandomDigits(remaining, rng, buf);
                buf.append('.');
                remaining--;
                if (remaining > 0 && rng.nextBoolean()) {
                    remaining = produceRandomDigits(remaining, rng, buf);
                }
                if (remaining > 0 && rng.nextBoolean()) {
                    remaining = produceRandomExponentPart(remaining, rng, buf);
                }
                if (remaining > 0 && rng.nextBoolean()) {
                    remaining = produceRandomFloatTypeSuffix(remaining, rng, buf);
                }
                break;
            case 1:
                buf.append('.');
                remaining--;
                if (remaining > 0) {
                    remaining = produceRandomDigits(remaining, rng, buf);
                }
                if (remaining > 0 && rng.nextBoolean()) {
                    remaining = produceRandomExponentPart(remaining, rng, buf);
                }
                if (remaining > 0 && rng.nextBoolean()) {
                    remaining = produceRandomFloatTypeSuffix(remaining, rng, buf);
                }
                break;
            case 2:
                remaining = produceRandomDigits(remaining, rng, buf);
                if (remaining > 0) {
                    remaining = produceRandomExponentPart(remaining, rng, buf);
                }
                if (remaining > 0 && rng.nextBoolean()) {
                    remaining = produceRandomFloatTypeSuffix(remaining, rng, buf);
                }
                break;
            case 3:
                remaining = produceRandomDigits(remaining, rng, buf);
                if (remaining > 0 && rng.nextBoolean()) {
                    remaining = produceRandomExponentPart(remaining, rng, buf);
                }
                if (remaining > 0) {
                    remaining = produceRandomFloatTypeSuffix(remaining, rng, buf);
                }
                break;
        }
        return remaining;
    }

    private int produceRandomDigit(int remaining, Random rng, StringBuilder buf) {
        buf.append(rng.nextInt(10));
        remaining--;
        return remaining;
    }

    /**
     * Underscore is omitted here, because {@link Double#valueOf(String)} does
     * not permit them.
     */
    private int produceRandomDigitOrUnderscore(int remaining, Random rng, StringBuilder buf) {
        if (produceUnderscore) {
            buf.append(DIGITS_OR_UNDERSCORE[rng.nextInt(DIGITS_OR_UNDERSCORE.length)]);
        } else {
            buf.append(DIGITS[rng.nextInt(DIGITS.length)]);
        }
        remaining--;
        return remaining;
    }

    /**
     * <dl>
     * <dt><i>Digits:</i>
     * <dd><i>Digit</i>
     * <dd><i>Digit [DigitsAndUnderscores] Digit</i>
     * </dl>
     */
    private int produceRandomDigits(int remaining, Random rng, StringBuilder buf) {
        switch (rng.nextInt(2)) {
            case 0:
                remaining = produceRandomDigit(remaining, rng, buf);
                break;
            case 1:
                remaining = produceRandomDigit(remaining, rng, buf);
                if (remaining > 0 && rng.nextBoolean()) {
                    remaining = produceRandomDigitsAndUnderscores(remaining, rng, buf);
                }
                if (remaining > 0) {
                    remaining = produceRandomDigit(remaining, rng, buf);
                }
                break;
        }

        return remaining;
    }

    /**
     * <dl>
     * <dt><i>DigitsAndUnderscores:</i>
     * <dd><i>DigitOrUnderscore {DigitOrUnderscore}</i>
     * <dd><i>Digit [DigitsAndUnderscores] Digit</i>
     * </dl>
     */
    private int produceRandomDigitsAndUnderscores(int remaining, Random rng, StringBuilder buf) {
        switch (rng.nextInt(2)) {
            case 0:
                remaining = produceRandomDigitOrUnderscore(remaining, rng, buf);
                if (remaining > 0) {
                    int todo = rng.nextInt(remaining);
                    for (int i = 0; i < todo; i++) {
                        remaining = produceRandomDigitOrUnderscore(remaining, rng, buf);
                    }
                }
                break;
            case 1:
                remaining = produceRandomDigit(remaining, rng, buf);
                if (remaining > 0 && rng.nextBoolean()) {
                    remaining = produceRandomDigitsAndUnderscores(remaining, rng, buf);
                }
                if (remaining > 0) {
                    remaining = produceRandomDigit(remaining, rng, buf);
                }
                break;
        }
        return remaining;
    }

    /**
     * <dl>
     * <dt><i>ExponentPart:</i>
     * <dd><i>ExponentIndicator SignedInteger</i>
     * </dl>
     */
    private int produceRandomExponentPart(int remaining, Random rng, StringBuilder buf) {
        buf.append(rng.nextBoolean() ? 'e' : 'E');
        remaining--;
        remaining = produceRandomSignedInteger(remaining, rng, buf);
        return remaining;
    }

    /**
     * This method only produces something {@code produceFloatTypeSuffix}
     * is set to true.
     *
     * <dl>
     * <dt><i>FloatTypeSuffix:</i>
     * <dd><i>(one of)</i>
     * <dd><i>f F d D</i>
     * </dl>
     */
    private int produceRandomFloatTypeSuffix(int remaining, Random rng, StringBuilder buf) {
        if (produceFloatTypeSuffix) {
            switch (rng.nextInt(4)) {
                case 0:
                    buf.append('d');
                    break;
                case 1:
                    buf.append('D');
                    break;
            }
            remaining--;
        }
        return remaining;
    }

    private int produceRandomFloatValue(int remaining, Random rng, StringBuilder buf) {
        switch (rng.nextInt(4)) {
            case 0:
                if (remaining > 0 && rng.nextBoolean()) {
                    remaining = produceRandomSign(remaining, rng, buf);
                }
                if (remaining > 0) {
                    remaining = produceRandomNaNOrInfinity(remaining, rng, buf);
                }
                break;
            case 1:
                if (rng.nextBoolean()) {
                    remaining = produceRandomSign(remaining, rng, buf);
                }
                if (remaining > 0) {
                    remaining = produceRandomDecimalFloatingPointLiteral(remaining, rng, buf);
                }
                break;
            case 2:
                if (rng.nextBoolean()) {
                    remaining = produceRandomSign(remaining, rng, buf);
                }
                if (remaining > 0) {
                    remaining = produceRandomHexFloatingPointLiteral(remaining, rng, buf);
                }
                break;
            case 3:
                remaining = produceRandomSignedInteger(remaining, rng, buf);
                break;
        }
        return remaining;
    }

    private int produceRandomHexDigit(int remaining, Random rng, StringBuilder buf) {
        buf.append(HEX_DIGITS[rng.nextInt(HEX_DIGITS.length)]);
        remaining--;
        return remaining;
    }

    private int produceRandomHexDigitOrUnderscore(int remaining, Random rng, StringBuilder buf) {
        if (produceUnderscore) {
            buf.append(HEX_DIGITS_OR_UNDERSCORE[rng.nextInt(HEX_DIGITS_OR_UNDERSCORE.length)]);
        } else {
            buf.append(HEX_DIGITS[rng.nextInt(HEX_DIGITS.length)]);
        }
        remaining--;
        return remaining;
    }

    private int produceRandomHexDigits(int remaining, Random rng, StringBuilder buf) {
        remaining = produceRandomHexDigit(remaining, rng, buf);
        if (rng.nextBoolean()) {
            remaining = produceRandomHexDigitsAndUnderscores(remaining, rng, buf);
            remaining = produceRandomHexDigit(remaining, rng, buf);
        }
        return remaining;
    }

    private int produceRandomHexDigitsAndUnderscores(int remaining, Random rng, StringBuilder buf) {
        int todo = rng.nextInt(Math.max(remaining, 1));
        for (int i = 0; i < todo; i++) {
            remaining = produceRandomHexDigitOrUnderscore(remaining, rng, buf);
        }
        return remaining;
    }

    private int produceRandomHexFloatingPointLiteral(int remaining, Random rng, StringBuilder buf) {
        remaining = produceRandomHexSignificand(remaining, rng, buf);
        if (remaining > 0) {
            remaining = produceRandomBinaryExponent(remaining, rng, buf);
        }
        if (remaining > 0) {
            remaining = produceRandomFloatTypeSuffix(remaining, rng, buf);
        }
        return remaining;
    }

    private int produceRandomHexNumeral(int remaining, Random rng, StringBuilder buf) {
        buf.append(rng.nextBoolean() ? "0x" : "0X");
        remaining--;
        if (remaining > 0) {
            remaining = produceRandomHexDigits(remaining, rng, buf);
        }
        return remaining;
    }

    /**
     * <dl>
     * <dt><i>HexSignificand:</i>
     * <dd><i>HexNumeral</i>
     * <dd><i>HexNumeral</i> {@code .}
     * <dd>{@code 0x} <i>[HexDigits]</i> {@code .} <i>HexDigits</i>
     * <dd>{@code 0X} <i>[HexDigits]</i> {@code .} <i>HexDigits</i>
     * </dl>
     */
    private int produceRandomHexSignificand(int remaining, Random rng, StringBuilder buf) {
        switch (rng.nextInt(3)) {
            case 0:
                remaining = produceRandomHexNumeral(remaining, rng, buf);
                break;
            case 1:
                remaining = produceRandomHexNumeral(remaining, rng, buf);
                buf.append('.');
                remaining--;
                break;
            case 2:
                buf.append(rng.nextBoolean() ? "0x" : "0X");
                remaining--;
                if (remaining > 0 && rng.nextBoolean()) {
                    remaining = produceRandomHexDigits(remaining, rng, buf);
                }
                if (remaining > 0 && rng.nextBoolean()) {
                    buf.append('.');
                    remaining--;
                    remaining = produceRandomHexDigits(remaining, rng, buf);
                }
                break;
        }
        return remaining;
    }

    private int produceRandomNaNOrInfinity(int remaining, Random rng, StringBuilder buf) {
        buf.append(rng.nextBoolean() ? "NaN" : "Infinity");
        remaining--;
        return remaining;

    }

    private int produceRandomSign(int remaining, Random rng, StringBuilder buf) {
        buf.append(rng.nextBoolean() ? '+' : '-');
        remaining--;
        return --remaining;
    }

    /**
     * <dl>
     * <dt><i>SignedInteger:</i>
     * <dd><i>[Sign] Digits</i>
     * </dl>
     */
    private int produceRandomSignedInteger(int remaining, Random rng, StringBuilder buf) {
        if (rng.nextBoolean()) {
            remaining = produceRandomSign(remaining, rng, buf);
        }
        if (remaining > 0) {
            remaining = produceRandomDigits(remaining, rng, buf);
        }
        return remaining;
    }

    private int produceRandomWhitespaces(int remaining, Random rng, StringBuilder buf) {
        if (remaining > 0) {
            int todo = rng.nextInt(remaining + 1);
            for (int i = 0; i < todo; i++) {
                buf.append((char) rng.nextInt(0x21));
            }
            remaining -= todo;
        }
        return remaining;
    }


}
