/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.utils;

public class NumberFormat {

    private static final long[] pow10;

    static {
        pow10 = new long[500];
        pow10[0] = 1;
        for (int i = 1; i < pow10.length; i++) {
            pow10[i] = pow10[i - 1] * 10;
        }
    }

    public static void append(StringBuilder builder, double d, int scale) {
        if (d == Double.POSITIVE_INFINITY) {
            builder.append("Infinity");
            return;
        }

        if (d == Double.NEGATIVE_INFINITY) {
            builder.append("-Infinity");
            return;
        }

        if (d != d) {
            builder.append("NaN");
            return;
        }

        if (d < 0) {
            builder.append('-');
            d = -d;
        }
        long factor = pow10[scale];
        long scaled = (long) (d * factor + 0.5);
        long targetScale = scale + 1;
        long z;
        while (targetScale < 20 && (z = factor * 10) <= scaled) {
            factor = z;
            targetScale++;
        }

        // factor overflow, fallback to slow method rather than thrown an exception
        if (targetScale == 20) {
            builder.append(Double.toString(d));
            return;
        }

        while (targetScale > 0) {
            if (targetScale-- == scale) {
                builder.append('.');
            }
            builder.append((char) ('0' + scaled / factor % 10));
            factor /= 10;
        }
    }

    public static void append(StringBuilder builder, float f, int scale) {
        if (f == Float.POSITIVE_INFINITY) {
            builder.append("Infinity");
            return;
        }

        if (f == Float.NEGATIVE_INFINITY) {
            builder.append("-Infinity");
            return;
        }

        if (f != f) {
            builder.append("NaN");
            return;
        }

        if (f < 0) {
            builder.append('-');
            f = -f;
        }
        int factor = (int) pow10[scale];
        int scaled = (int) (f * factor + 0.5);
        int targetScale = scale + 1;
        int z;
        while (targetScale < 11 && (z = factor * 10) <= scaled) {
            factor = z;
            targetScale++;
        }

        if (targetScale == 11) {
            builder.append(Float.toString(f));
            return;
        }

        while (targetScale > 0) {
            if (targetScale-- == scale) {
                builder.append('.');
            }
            builder.append((char) ('0' + scaled / factor % 10));
            factor /= 10;
        }
    }

    public static void append(StringBuilder b, int i) {
        if (i < 0) {
            if (i == Integer.MAX_VALUE) {
                b.append("-2147483648");
                return;
            }
            b.append('-');
            i = -i;
        }
        int c;
        if (i < 10) {
            b.append((char) ('0' + i));
        } else if (i < 100) {  // two
            b.append((char) ('0' + i / 10));
            b.append((char) ('0' + i % 10));
        } else if (i < 1000) { // three
            b.append((char) ('0' + i / 100));
            b.append((char) ('0' + (c = i % 100) / 10));
            b.append((char) ('0' + (c % 10)));
        } else if (i < 10000) { // four
            b.append((char) ('0' + i / 1000));
            b.append((char) ('0' + (c = i % 1000) / 100));
            b.append((char) ('0' + (c %= 100) / 10));
            b.append((char) ('0' + (c % 10)));
        } else if (i < 100000) { // five
            b.append((char) ('0' + i / 10000));
            b.append((char) ('0' + (c = i % 10000) / 1000));
            b.append((char) ('0' + (c %= 1000) / 100));
            b.append((char) ('0' + (c %= 100) / 10));
            b.append((char) ('0' + (c % 10)));
        } else if (i < 1000000) { // six
            b.append((char) ('0' + i / 100000));
            b.append((char) ('0' + (c = i % 100000) / 10000));
            b.append((char) ('0' + (c %= 10000) / 1000));
            b.append((char) ('0' + (c %= 1000) / 100));
            b.append((char) ('0' + (c %= 100) / 10));
            b.append((char) ('0' + (c % 10)));
        } else if (i < 10000000) { // seven
            b.append((char) ('0' + i / 1000000));
            b.append((char) ('0' + (c = i % 1000000) / 100000));
            b.append((char) ('0' + (c %= 100000) / 10000));
            b.append((char) ('0' + (c %= 10000) / 1000));
            b.append((char) ('0' + (c %= 1000) / 100));
            b.append((char) ('0' + (c %= 100) / 10));
            b.append((char) ('0' + (c % 10)));
        } else if (i < 100000000) { // eight
            b.append((char) ('0' + i / 10000000));
            b.append((char) ('0' + (c = i % 10000000) / 1000000));
            b.append((char) ('0' + (c %= 1000000) / 100000));
            b.append((char) ('0' + (c %= 100000) / 10000));
            b.append((char) ('0' + (c %= 10000) / 1000));
            b.append((char) ('0' + (c %= 1000) / 100));
            b.append((char) ('0' + (c %= 100) / 10));
            b.append((char) ('0' + (c % 10)));
        } else if (i < 1000000000) { // nine
            b.append((char) ('0' + i / 100000000));
            b.append((char) ('0' + (c = i % 100000000) / 10000000));
            b.append((char) ('0' + (c %= 10000000) / 1000000));
            b.append((char) ('0' + (c %= 1000000) / 100000));
            b.append((char) ('0' + (c %= 100000) / 10000));
            b.append((char) ('0' + (c %= 10000) / 1000));
            b.append((char) ('0' + (c %= 1000) / 100));
            b.append((char) ('0' + (c %= 100) / 10));
            b.append((char) ('0' + (c % 10)));
        } else {
            // ten
            b.append((char) ('0' + i / 1000000000));
            b.append((char) ('0' + (c = i % 1000000000) / 100000000));
            b.append((char) ('0' + (c %= 100000000) / 10000000));
            b.append((char) ('0' + (c %= 10000000) / 1000000));
            b.append((char) ('0' + (c %= 1000000) / 100000));
            b.append((char) ('0' + (c %= 100000) / 10000));
            b.append((char) ('0' + (c %= 10000) / 1000));
            b.append((char) ('0' + (c %= 1000) / 100));
            b.append((char) ('0' + (c %= 100) / 10));
            b.append((char) ('0' + (c % 10)));
        }
    }

    public static void appendLong11(StringBuilder b, long i) {
        long c;
        b.append((char) ('0' + i / 10000000000L));
        b.append((char) ('0' + (c = i % 10000000000L) / 1000000000));
        b.append((char) ('0' + (c %= 1000000000) / 100000000));
        b.append((char) ('0' + (c %= 100000000) / 10000000));
        b.append((char) ('0' + (c %= 10000000) / 1000000));
        b.append((char) ('0' + (c %= 1000000) / 100000));
        b.append((char) ('0' + (c %= 100000) / 10000));
        b.append((char) ('0' + (c %= 10000) / 1000));
        b.append((char) ('0' + (c %= 1000) / 100));
        b.append((char) ('0' + (c %= 100) / 10));
        b.append((char) ('0' + (c % 10)));
    }

    public static void appendLong12(StringBuilder b, long i) {
        long c;
        b.append((char) ('0' + i / 100000000000L));
        b.append((char) ('0' + (c = i % 100000000000L) / 10000000000L));
        b.append((char) ('0' + (c %= 10000000000L) / 1000000000));
        b.append((char) ('0' + (c %= 1000000000) / 100000000));
        b.append((char) ('0' + (c %= 100000000) / 10000000));
        b.append((char) ('0' + (c %= 10000000) / 1000000));
        b.append((char) ('0' + (c %= 1000000) / 100000));
        b.append((char) ('0' + (c %= 100000) / 10000));
        b.append((char) ('0' + (c %= 10000) / 1000));
        b.append((char) ('0' + (c %= 1000) / 100));
        b.append((char) ('0' + (c %= 100) / 10));
        b.append((char) ('0' + (c % 10)));
    }

    public static void append(StringBuilder b, long i) {
        if (i <= Integer.MAX_VALUE && i >= Integer.MIN_VALUE) {
            append(b, (int) i);
        } else {
            if (i < 0) {
                if (i == Long.MIN_VALUE) {
                    b.append("-9223372036854775808");
                    return;
                }
                b.append('-');
                i = -i;
            }
            if (i < 100000000000L) { //  eleven
                appendLong11(b, i);
            } else if (i < 1000000000000L) { //  twelve
                appendLong12(b, i);
            } else if (i < 10000000000000L) { //  thirteen
                appendLong13(b, i);
            } else if (i < 100000000000000L) { //  fourteen
                appendLong14(b, i);
            } else if (i < 1000000000000000L) { //  fifteen
                appendLong15(b, i);
            } else if (i < 10000000000000000L) { //  sixteen
                appendLong16(b, i);
            } else if (i < 100000000000000000L) { //  seventeen
                appendLong17(b, i);
            } else if (i < 1000000000000000000L) { //  eighteen
                appendLong18(b, i);
            } else { //  nineteen
                appendLong19(b, i);
            }
        }
    }

    private static void appendLong19(StringBuilder b, long i) {
        long c;
        b.append((char) ('0' + i / 1000000000000000000L));
        b.append((char) ('0' + (c = i % 1000000000000000000L) / 100000000000000000L));
        b.append((char) ('0' + (c %= 100000000000000000L) / 10000000000000000L));
        b.append((char) ('0' + (c %= 10000000000000000L) / 1000000000000000L));
        b.append((char) ('0' + (c %= 1000000000000000L) / 100000000000000L));
        b.append((char) ('0' + (c %= 100000000000000L) / 10000000000000L));
        b.append((char) ('0' + (c %= 10000000000000L) / 1000000000000L));
        b.append((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        b.append((char) ('0' + (c %= 100000000000L) / 10000000000L));
        b.append((char) ('0' + (c %= 10000000000L) / 1000000000));
        b.append((char) ('0' + (c %= 1000000000) / 100000000));
        b.append((char) ('0' + (c %= 100000000) / 10000000));
        b.append((char) ('0' + (c %= 10000000) / 1000000));
        b.append((char) ('0' + (c %= 1000000) / 100000));
        b.append((char) ('0' + (c %= 100000) / 10000));
        b.append((char) ('0' + (c %= 10000) / 1000));
        b.append((char) ('0' + (c %= 1000) / 100));
        b.append((char) ('0' + (c %= 100) / 10));
        b.append((char) ('0' + (c % 10)));
    }

    private static void appendLong18(StringBuilder b, long i) {
        long c;
        b.append((char) ('0' + i / 100000000000000000L));
        b.append((char) ('0' + (c = i % 100000000000000000L) / 10000000000000000L));
        b.append((char) ('0' + (c %= 10000000000000000L) / 1000000000000000L));
        b.append((char) ('0' + (c %= 1000000000000000L) / 100000000000000L));
        b.append((char) ('0' + (c %= 100000000000000L) / 10000000000000L));
        b.append((char) ('0' + (c %= 10000000000000L) / 1000000000000L));
        b.append((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        b.append((char) ('0' + (c %= 100000000000L) / 10000000000L));
        b.append((char) ('0' + (c %= 10000000000L) / 1000000000));
        b.append((char) ('0' + (c %= 1000000000) / 100000000));
        b.append((char) ('0' + (c %= 100000000) / 10000000));
        b.append((char) ('0' + (c %= 10000000) / 1000000));
        b.append((char) ('0' + (c %= 1000000) / 100000));
        b.append((char) ('0' + (c %= 100000) / 10000));
        b.append((char) ('0' + (c %= 10000) / 1000));
        b.append((char) ('0' + (c %= 1000) / 100));
        b.append((char) ('0' + (c %= 100) / 10));
        b.append((char) ('0' + (c % 10)));
    }

    private static void appendLong17(StringBuilder b, long i) {
        long c;
        b.append((char) ('0' + i / 10000000000000000L));
        b.append((char) ('0' + (c = i % 10000000000000000L) / 1000000000000000L));
        b.append((char) ('0' + (c %= 1000000000000000L) / 100000000000000L));
        b.append((char) ('0' + (c %= 100000000000000L) / 10000000000000L));
        b.append((char) ('0' + (c %= 10000000000000L) / 1000000000000L));
        b.append((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        b.append((char) ('0' + (c %= 100000000000L) / 10000000000L));
        b.append((char) ('0' + (c %= 10000000000L) / 1000000000));
        b.append((char) ('0' + (c %= 1000000000) / 100000000));
        b.append((char) ('0' + (c %= 100000000) / 10000000));
        b.append((char) ('0' + (c %= 10000000) / 1000000));
        b.append((char) ('0' + (c %= 1000000) / 100000));
        b.append((char) ('0' + (c %= 100000) / 10000));
        b.append((char) ('0' + (c %= 10000) / 1000));
        b.append((char) ('0' + (c %= 1000) / 100));
        b.append((char) ('0' + (c %= 100) / 10));
        b.append((char) ('0' + (c % 10)));
    }

    private static void appendLong16(StringBuilder b, long i) {
        long c;
        b.append((char) ('0' + i / 1000000000000000L));
        b.append((char) ('0' + (c = i % 1000000000000000L) / 100000000000000L));
        b.append((char) ('0' + (c %= 100000000000000L) / 10000000000000L));
        b.append((char) ('0' + (c %= 10000000000000L) / 1000000000000L));
        b.append((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        b.append((char) ('0' + (c %= 100000000000L) / 10000000000L));
        b.append((char) ('0' + (c %= 10000000000L) / 1000000000));
        b.append((char) ('0' + (c %= 1000000000) / 100000000));
        b.append((char) ('0' + (c %= 100000000) / 10000000));
        b.append((char) ('0' + (c %= 10000000) / 1000000));
        b.append((char) ('0' + (c %= 1000000) / 100000));
        b.append((char) ('0' + (c %= 100000) / 10000));
        b.append((char) ('0' + (c %= 10000) / 1000));
        b.append((char) ('0' + (c %= 1000) / 100));
        b.append((char) ('0' + (c %= 100) / 10));
        b.append((char) ('0' + (c % 10)));
    }

    private static void appendLong15(StringBuilder b, long i) {
        long c;
        b.append((char) ('0' + i / 100000000000000L));
        b.append((char) ('0' + (c = i % 100000000000000L) / 10000000000000L));
        b.append((char) ('0' + (c %= 10000000000000L) / 1000000000000L));
        b.append((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        b.append((char) ('0' + (c %= 100000000000L) / 10000000000L));
        b.append((char) ('0' + (c %= 10000000000L) / 1000000000));
        b.append((char) ('0' + (c %= 1000000000) / 100000000));
        b.append((char) ('0' + (c %= 100000000) / 10000000));
        b.append((char) ('0' + (c %= 10000000) / 1000000));
        b.append((char) ('0' + (c %= 1000000) / 100000));
        b.append((char) ('0' + (c %= 100000) / 10000));
        b.append((char) ('0' + (c %= 10000) / 1000));
        b.append((char) ('0' + (c %= 1000) / 100));
        b.append((char) ('0' + (c %= 100) / 10));
        b.append((char) ('0' + (c % 10)));
    }

    private static void appendLong14(StringBuilder b, long i) {
        long c;
        b.append((char) ('0' + i / 10000000000000L));
        b.append((char) ('0' + (c = i % 10000000000000L) / 1000000000000L));
        b.append((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        b.append((char) ('0' + (c %= 100000000000L) / 10000000000L));
        b.append((char) ('0' + (c %= 10000000000L) / 1000000000));
        b.append((char) ('0' + (c %= 1000000000) / 100000000));
        b.append((char) ('0' + (c %= 100000000) / 10000000));
        b.append((char) ('0' + (c %= 10000000) / 1000000));
        b.append((char) ('0' + (c %= 1000000) / 100000));
        b.append((char) ('0' + (c %= 100000) / 10000));
        b.append((char) ('0' + (c %= 10000) / 1000));
        b.append((char) ('0' + (c %= 1000) / 100));
        b.append((char) ('0' + (c %= 100) / 10));
        b.append((char) ('0' + (c % 10)));
    }

    private static void appendLong13(StringBuilder b, long i) {
        long c;
        b.append((char) ('0' + i / 1000000000000L));
        b.append((char) ('0' + (c = i % 1000000000000L) / 100000000000L));
        b.append((char) ('0' + (c %= 100000000000L) / 10000000000L));
        b.append((char) ('0' + (c %= 10000000000L) / 1000000000));
        b.append((char) ('0' + (c %= 1000000000) / 100000000));
        b.append((char) ('0' + (c %= 100000000) / 10000000));
        b.append((char) ('0' + (c %= 10000000) / 1000000));
        b.append((char) ('0' + (c %= 1000000) / 100000));
        b.append((char) ('0' + (c %= 100000) / 10000));
        b.append((char) ('0' + (c %= 10000) / 1000));
        b.append((char) ('0' + (c %= 1000) / 100));
        b.append((char) ('0' + (c %= 100) / 10));
        b.append((char) ('0' + (c % 10)));
    }
}