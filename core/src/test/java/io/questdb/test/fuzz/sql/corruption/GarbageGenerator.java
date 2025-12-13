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

package io.questdb.test.fuzz.sql.corruption;

import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.fuzz.sql.SqlToken;
import io.questdb.test.fuzz.sql.TokenizedQuery;

/**
 * Generates random garbage SQL that doesn't follow any grammar.
 * This is for testing parser robustness against completely malformed input.
 */
public final class GarbageGenerator {

    private static final String[] RANDOM_WORDS = {
            "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS",
            "NULL", "TRUE", "FALSE", "AS", "ON", "BY", "TO", "OF",
            "foo", "bar", "baz", "qux", "x", "y", "z", "a1", "b2",
            "table", "column", "value", "name", "id", "type", "data",
            "123", "456", "0", "-1", "3.14", "0.0", "1e10", "NaN",
            "'hello'", "'world'", "''", "'test'", "'abc''def'",
            "+", "-", "*", "/", "=", "<", ">", "<=", ">=", "<>", "!=",
            "(", ")", ",", ";", ".", ":", "@", "#", "$", "%"
    };

    private static final String CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    private static final String SPECIAL_CHARS = "!@#$%^&*()_+-=[]{}|;':\",./<>?`~\\";

    private GarbageGenerator() {
    }

    /**
     * Generates a TokenizedQuery with random tokens.
     *
     * @param rnd random source
     * @return garbage TokenizedQuery
     */
    public static TokenizedQuery generate(Rnd rnd) {
        int tokenCount = 1 + rnd.nextInt(20);
        return generateWithCount(rnd, tokenCount);
    }

    /**
     * Generates a TokenizedQuery with specified number of random tokens.
     */
    public static TokenizedQuery generateWithCount(Rnd rnd, int tokenCount) {
        TokenizedQuery query = new TokenizedQuery();

        for (int i = 0; i < tokenCount; i++) {
            int tokenType = rnd.nextInt(5);
            switch (tokenType) {
                case 0:
                    // Random word from list
                    query.add(randomToken(rnd));
                    break;
                case 1:
                    // Random identifier
                    query.addIdentifier(randomIdentifier(rnd));
                    break;
                case 2:
                    // Random number
                    query.addLiteral(randomNumber(rnd));
                    break;
                case 3:
                    // Random string literal
                    query.addLiteral(randomString(rnd));
                    break;
                case 4:
                    // Random special characters
                    query.addPunctuation(randomSpecial(rnd));
                    break;
            }
        }

        return query;
    }

    /**
     * Generates a raw garbage string (not tokenized).
     */
    public static String generateRaw(Rnd rnd) {
        int mode = rnd.nextInt(5);
        switch (mode) {
            case 0:
                return generateEmptyOrWhitespace(rnd);
            case 1:
                return generateRandomChars(rnd);
            case 2:
                return generateEdgeCase(rnd);
            case 3:
                return generateTokenized(rnd);
            default:
                return generateMixed(rnd);
        }
    }

    /**
     * Generates empty or whitespace-only string.
     */
    public static String generateEmptyOrWhitespace(Rnd rnd) {
        int type = rnd.nextInt(4);
        switch (type) {
            case 0:
                return "";
            case 1:
                return " ";
            case 2:
                // Multiple spaces
                int spaceCount = 1 + rnd.nextInt(10);
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < spaceCount; i++) {
                    sb.append(' ');
                }
                return sb.toString();
            default:
                // Mixed whitespace
                int count = 1 + rnd.nextInt(5);
                sb = new StringBuilder();
                for (int i = 0; i < count; i++) {
                    int wsType = rnd.nextInt(3);
                    switch (wsType) {
                        case 0:
                            sb.append(' ');
                            break;
                        case 1:
                            sb.append('\t');
                            break;
                        case 2:
                            sb.append('\n');
                            break;
                    }
                }
                return sb.toString();
        }
    }

    /**
     * Generates random character string.
     */
    public static String generateRandomChars(Rnd rnd) {
        int len = 1 + rnd.nextInt(50);
        StringSink sink = new StringSink();
        for (int i = 0; i < len; i++) {
            char c = (char) rnd.nextInt(128);
            sink.put(c);
        }
        return sink.toString();
    }

    /**
     * Generates edge case strings.
     */
    public static String generateEdgeCase(Rnd rnd) {
        int type = rnd.nextInt(10);
        switch (type) {
            case 0:
                return "\0"; // Null byte
            case 1:
                return "\0\0\0"; // Multiple null bytes
            case 2:
                return "\u0001\u0002\u0003"; // Control characters
            case 3:
                return "\r\n\r\n"; // Line endings
            case 4:
                return repeatChar('(', 100); // Many open parens
            case 5:
                return repeatChar(')', 100); // Many close parens
            case 6:
                return repeatChar('\'', 100); // Many quotes
            case 7:
                return repeatString("SELECT ", 50); // Repeated keyword
            case 8:
                return generateLongIdentifier(rnd); // Very long identifier
            case 9:
                return generateLongString(rnd); // Very long string literal
            default:
                return "";
        }
    }

    private static SqlToken randomToken(Rnd rnd) {
        String word = RANDOM_WORDS[rnd.nextInt(RANDOM_WORDS.length)];

        // Determine token type based on content
        if (word.startsWith("'")) {
            return SqlToken.literal(word);
        } else if (word.length() == 1 && "+-*/=<>(),.;:@#$%".indexOf(word.charAt(0)) >= 0) {
            return SqlToken.punctuation(word);
        } else if (Character.isDigit(word.charAt(0)) || word.charAt(0) == '-' && word.length() > 1) {
            return SqlToken.literal(word);
        } else if (word.toUpperCase().equals(word) && Character.isLetter(word.charAt(0))) {
            return SqlToken.keyword(word);
        } else {
            return SqlToken.identifier(word);
        }
    }

    private static String randomIdentifier(Rnd rnd) {
        int len = 1 + rnd.nextInt(10);
        StringBuilder sb = new StringBuilder();
        sb.append(CHARS.charAt(rnd.nextInt(52))); // Start with letter
        for (int i = 1; i < len; i++) {
            sb.append(CHARS.charAt(rnd.nextInt(CHARS.length())));
        }
        return sb.toString();
    }

    private static String randomNumber(Rnd rnd) {
        int type = rnd.nextInt(4);
        switch (type) {
            case 0:
                return String.valueOf(rnd.nextInt());
            case 1:
                return String.valueOf(rnd.nextLong());
            case 2:
                return String.valueOf(rnd.nextDouble());
            default:
                return String.valueOf(rnd.nextFloat());
        }
    }

    private static String randomString(Rnd rnd) {
        int len = rnd.nextInt(20);
        StringBuilder sb = new StringBuilder();
        sb.append('\'');
        for (int i = 0; i < len; i++) {
            char c = CHARS.charAt(rnd.nextInt(CHARS.length()));
            if (c == '\'') {
                sb.append("''"); // Escape single quote
            } else {
                sb.append(c);
            }
        }
        sb.append('\'');
        return sb.toString();
    }

    private static String randomSpecial(Rnd rnd) {
        int len = 1 + rnd.nextInt(3);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append(SPECIAL_CHARS.charAt(rnd.nextInt(SPECIAL_CHARS.length())));
        }
        return sb.toString();
    }

    private static String generateTokenized(Rnd rnd) {
        return generate(rnd).serialize();
    }

    private static String generateMixed(Rnd rnd) {
        StringSink sink = new StringSink();
        int parts = 2 + rnd.nextInt(5);
        for (int i = 0; i < parts; i++) {
            int type = rnd.nextInt(4);
            switch (type) {
                case 0:
                    sink.put(RANDOM_WORDS[rnd.nextInt(RANDOM_WORDS.length)]);
                    break;
                case 1:
                    sink.put(randomSpecial(rnd));
                    break;
                case 2:
                    sink.put((char) rnd.nextInt(128));
                    break;
                case 3:
                    sink.put(' ');
                    break;
            }
        }
        return sink.toString();
    }

    private static String repeatChar(char c, int count) {
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append(c);
        }
        return sb.toString();
    }

    private static String repeatString(String s, int count) {
        StringBuilder sb = new StringBuilder(s.length() * count);
        for (int i = 0; i < count; i++) {
            sb.append(s);
        }
        return sb.toString();
    }

    private static String generateLongIdentifier(Rnd rnd) {
        int len = 500 + rnd.nextInt(500);
        StringBuilder sb = new StringBuilder(len);
        sb.append('x');
        for (int i = 1; i < len; i++) {
            sb.append(CHARS.charAt(rnd.nextInt(CHARS.length())));
        }
        return sb.toString();
    }

    private static String generateLongString(Rnd rnd) {
        int len = 500 + rnd.nextInt(500);
        StringBuilder sb = new StringBuilder(len + 2);
        sb.append('\'');
        for (int i = 0; i < len; i++) {
            char c = CHARS.charAt(rnd.nextInt(CHARS.length()));
            if (c == '\'') {
                sb.append("''");
            } else {
                sb.append(c);
            }
        }
        sb.append('\'');
        return sb.toString();
    }
}
