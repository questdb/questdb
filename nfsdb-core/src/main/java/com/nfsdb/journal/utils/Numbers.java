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

import com.nfsdb.journal.export.CharSink;

public class Numbers {

    private static final long[] pow10;

    static {
        pow10 = new long[500];
        pow10[0] = 1;
        for (int i = 1; i < pow10.length; i++) {
            pow10[i] = pow10[i - 1] * 10;
        }
    }

    public static void append(CharSink sink, double d, int scale) {
        if (d == Double.POSITIVE_INFINITY) {
            sink.put("Infinity");
            return;
        }

        if (d == Double.NEGATIVE_INFINITY) {
            sink.put("-Infinity");
            return;
        }

        if (d != d) {
            sink.put("NaN");
            return;
        }

        if (d < 0) {
            sink.put('-');
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
            sink.put(Double.toString(d));
            return;
        }

        while (targetScale > 0) {
            if (targetScale-- == scale) {
                sink.put('.');
            }
            sink.put((char) ('0' + scaled / factor % 10));
            factor /= 10;
        }
    }

    public static void append(CharSink sink, float f, int scale) {
        if (f == Float.POSITIVE_INFINITY) {
            sink.put("Infinity");
            return;
        }

        if (f == Float.NEGATIVE_INFINITY) {
            sink.put("-Infinity");
            return;
        }

        if (f != f) {
            sink.put("NaN");
            return;
        }

        if (f < 0) {
            sink.put('-');
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
            sink.put(Float.toString(f));
            return;
        }

        while (targetScale > 0) {
            if (targetScale-- == scale) {
                sink.put('.');
            }
            sink.put((char) ('0' + scaled / factor % 10));
            factor /= 10;
        }
    }

    public static void append(CharSink sink, int i) {
        if (i < 0) {
            if (i == Integer.MAX_VALUE) {
                sink.put("-2147483648");
                return;
            }
            sink.put('-');
            i = -i;
        }
        int c;
        if (i < 10) {
            sink.put((char) ('0' + i));
        } else if (i < 100) {  // two
            sink.put((char) ('0' + i / 10));
            sink.put((char) ('0' + i % 10));
        } else if (i < 1000) { // three
            sink.put((char) ('0' + i / 100));
            sink.put((char) ('0' + (c = i % 100) / 10));
            sink.put((char) ('0' + (c % 10)));
        } else if (i < 10000) { // four
            sink.put((char) ('0' + i / 1000));
            sink.put((char) ('0' + (c = i % 1000) / 100));
            sink.put((char) ('0' + (c %= 100) / 10));
            sink.put((char) ('0' + (c % 10)));
        } else if (i < 100000) { // five
            sink.put((char) ('0' + i / 10000));
            sink.put((char) ('0' + (c = i % 10000) / 1000));
            sink.put((char) ('0' + (c %= 1000) / 100));
            sink.put((char) ('0' + (c %= 100) / 10));
            sink.put((char) ('0' + (c % 10)));
        } else if (i < 1000000) { // six
            sink.put((char) ('0' + i / 100000));
            sink.put((char) ('0' + (c = i % 100000) / 10000));
            sink.put((char) ('0' + (c %= 10000) / 1000));
            sink.put((char) ('0' + (c %= 1000) / 100));
            sink.put((char) ('0' + (c %= 100) / 10));
            sink.put((char) ('0' + (c % 10)));
        } else if (i < 10000000) { // seven
            sink.put((char) ('0' + i / 1000000));
            sink.put((char) ('0' + (c = i % 1000000) / 100000));
            sink.put((char) ('0' + (c %= 100000) / 10000));
            sink.put((char) ('0' + (c %= 10000) / 1000));
            sink.put((char) ('0' + (c %= 1000) / 100));
            sink.put((char) ('0' + (c %= 100) / 10));
            sink.put((char) ('0' + (c % 10)));
        } else if (i < 100000000) { // eight
            sink.put((char) ('0' + i / 10000000));
            sink.put((char) ('0' + (c = i % 10000000) / 1000000));
            sink.put((char) ('0' + (c %= 1000000) / 100000));
            sink.put((char) ('0' + (c %= 100000) / 10000));
            sink.put((char) ('0' + (c %= 10000) / 1000));
            sink.put((char) ('0' + (c %= 1000) / 100));
            sink.put((char) ('0' + (c %= 100) / 10));
            sink.put((char) ('0' + (c % 10)));
        } else if (i < 1000000000) { // nine
            sink.put((char) ('0' + i / 100000000));
            sink.put((char) ('0' + (c = i % 100000000) / 10000000));
            sink.put((char) ('0' + (c %= 10000000) / 1000000));
            sink.put((char) ('0' + (c %= 1000000) / 100000));
            sink.put((char) ('0' + (c %= 100000) / 10000));
            sink.put((char) ('0' + (c %= 10000) / 1000));
            sink.put((char) ('0' + (c %= 1000) / 100));
            sink.put((char) ('0' + (c %= 100) / 10));
            sink.put((char) ('0' + (c % 10)));
        } else {
            // ten
            sink.put((char) ('0' + i / 1000000000));
            sink.put((char) ('0' + (c = i % 1000000000) / 100000000));
            sink.put((char) ('0' + (c %= 100000000) / 10000000));
            sink.put((char) ('0' + (c %= 10000000) / 1000000));
            sink.put((char) ('0' + (c %= 1000000) / 100000));
            sink.put((char) ('0' + (c %= 100000) / 10000));
            sink.put((char) ('0' + (c %= 10000) / 1000));
            sink.put((char) ('0' + (c %= 1000) / 100));
            sink.put((char) ('0' + (c %= 100) / 10));
            sink.put((char) ('0' + (c % 10)));
        }
    }

    public static void appendLong11(CharSink sink, long i) {
        long c;
        sink.put((char) ('0' + i / 10000000000L));
        sink.put((char) ('0' + (c = i % 10000000000L) / 1000000000));
        sink.put((char) ('0' + (c %= 1000000000) / 100000000));
        sink.put((char) ('0' + (c %= 100000000) / 10000000));
        sink.put((char) ('0' + (c %= 10000000) / 1000000));
        sink.put((char) ('0' + (c %= 1000000) / 100000));
        sink.put((char) ('0' + (c %= 100000) / 10000));
        sink.put((char) ('0' + (c %= 10000) / 1000));
        sink.put((char) ('0' + (c %= 1000) / 100));
        sink.put((char) ('0' + (c %= 100) / 10));
        sink.put((char) ('0' + (c % 10)));
    }

    public static void appendLong12(CharSink sink, long i) {
        long c;
        sink.put((char) ('0' + i / 100000000000L));
        sink.put((char) ('0' + (c = i % 100000000000L) / 10000000000L));
        sink.put((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.put((char) ('0' + (c %= 1000000000) / 100000000));
        sink.put((char) ('0' + (c %= 100000000) / 10000000));
        sink.put((char) ('0' + (c %= 10000000) / 1000000));
        sink.put((char) ('0' + (c %= 1000000) / 100000));
        sink.put((char) ('0' + (c %= 100000) / 10000));
        sink.put((char) ('0' + (c %= 10000) / 1000));
        sink.put((char) ('0' + (c %= 1000) / 100));
        sink.put((char) ('0' + (c %= 100) / 10));
        sink.put((char) ('0' + (c % 10)));
    }

    public static void append(CharSink sink, long i) {
        if (i <= Integer.MAX_VALUE && i >= Integer.MIN_VALUE) {
            append(sink, (int) i);
        } else {
            if (i < 0) {
                if (i == Long.MIN_VALUE) {
                    sink.put("-9223372036854775808");
                    return;
                }
                sink.put('-');
                i = -i;
            }
            if (i < 100000000000L) { //  eleven
                appendLong11(sink, i);
            } else if (i < 1000000000000L) { //  twelve
                appendLong12(sink, i);
            } else if (i < 10000000000000L) { //  thirteen
                appendLong13(sink, i);
            } else if (i < 100000000000000L) { //  fourteen
                appendLong14(sink, i);
            } else if (i < 1000000000000000L) { //  fifteen
                appendLong15(sink, i);
            } else if (i < 10000000000000000L) { //  sixteen
                appendLong16(sink, i);
            } else if (i < 100000000000000000L) { //  seventeen
                appendLong17(sink, i);
            } else if (i < 1000000000000000000L) { //  eighteen
                appendLong18(sink, i);
            } else { //  nineteen
                appendLong19(sink, i);
            }
        }
    }

    private static void appendLong19(CharSink sink, long i) {
        long c;
        sink.put((char) ('0' + i / 1000000000000000000L));
        sink.put((char) ('0' + (c = i % 1000000000000000000L) / 100000000000000000L));
        sink.put((char) ('0' + (c %= 100000000000000000L) / 10000000000000000L));
        sink.put((char) ('0' + (c %= 10000000000000000L) / 1000000000000000L));
        sink.put((char) ('0' + (c %= 1000000000000000L) / 100000000000000L));
        sink.put((char) ('0' + (c %= 100000000000000L) / 10000000000000L));
        sink.put((char) ('0' + (c %= 10000000000000L) / 1000000000000L));
        sink.put((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        sink.put((char) ('0' + (c %= 100000000000L) / 10000000000L));
        sink.put((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.put((char) ('0' + (c %= 1000000000) / 100000000));
        sink.put((char) ('0' + (c %= 100000000) / 10000000));
        sink.put((char) ('0' + (c %= 10000000) / 1000000));
        sink.put((char) ('0' + (c %= 1000000) / 100000));
        sink.put((char) ('0' + (c %= 100000) / 10000));
        sink.put((char) ('0' + (c %= 10000) / 1000));
        sink.put((char) ('0' + (c %= 1000) / 100));
        sink.put((char) ('0' + (c %= 100) / 10));
        sink.put((char) ('0' + (c % 10)));
    }

    private static void appendLong18(CharSink sink, long i) {
        long c;
        sink.put((char) ('0' + i / 100000000000000000L));
        sink.put((char) ('0' + (c = i % 100000000000000000L) / 10000000000000000L));
        sink.put((char) ('0' + (c %= 10000000000000000L) / 1000000000000000L));
        sink.put((char) ('0' + (c %= 1000000000000000L) / 100000000000000L));
        sink.put((char) ('0' + (c %= 100000000000000L) / 10000000000000L));
        sink.put((char) ('0' + (c %= 10000000000000L) / 1000000000000L));
        sink.put((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        sink.put((char) ('0' + (c %= 100000000000L) / 10000000000L));
        sink.put((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.put((char) ('0' + (c %= 1000000000) / 100000000));
        sink.put((char) ('0' + (c %= 100000000) / 10000000));
        sink.put((char) ('0' + (c %= 10000000) / 1000000));
        sink.put((char) ('0' + (c %= 1000000) / 100000));
        sink.put((char) ('0' + (c %= 100000) / 10000));
        sink.put((char) ('0' + (c %= 10000) / 1000));
        sink.put((char) ('0' + (c %= 1000) / 100));
        sink.put((char) ('0' + (c %= 100) / 10));
        sink.put((char) ('0' + (c % 10)));
    }

    private static void appendLong17(CharSink sink, long i) {
        long c;
        sink.put((char) ('0' + i / 10000000000000000L));
        sink.put((char) ('0' + (c = i % 10000000000000000L) / 1000000000000000L));
        sink.put((char) ('0' + (c %= 1000000000000000L) / 100000000000000L));
        sink.put((char) ('0' + (c %= 100000000000000L) / 10000000000000L));
        sink.put((char) ('0' + (c %= 10000000000000L) / 1000000000000L));
        sink.put((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        sink.put((char) ('0' + (c %= 100000000000L) / 10000000000L));
        sink.put((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.put((char) ('0' + (c %= 1000000000) / 100000000));
        sink.put((char) ('0' + (c %= 100000000) / 10000000));
        sink.put((char) ('0' + (c %= 10000000) / 1000000));
        sink.put((char) ('0' + (c %= 1000000) / 100000));
        sink.put((char) ('0' + (c %= 100000) / 10000));
        sink.put((char) ('0' + (c %= 10000) / 1000));
        sink.put((char) ('0' + (c %= 1000) / 100));
        sink.put((char) ('0' + (c %= 100) / 10));
        sink.put((char) ('0' + (c % 10)));
    }

    private static void appendLong16(CharSink sink, long i) {
        long c;
        sink.put((char) ('0' + i / 1000000000000000L));
        sink.put((char) ('0' + (c = i % 1000000000000000L) / 100000000000000L));
        sink.put((char) ('0' + (c %= 100000000000000L) / 10000000000000L));
        sink.put((char) ('0' + (c %= 10000000000000L) / 1000000000000L));
        sink.put((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        sink.put((char) ('0' + (c %= 100000000000L) / 10000000000L));
        sink.put((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.put((char) ('0' + (c %= 1000000000) / 100000000));
        sink.put((char) ('0' + (c %= 100000000) / 10000000));
        sink.put((char) ('0' + (c %= 10000000) / 1000000));
        sink.put((char) ('0' + (c %= 1000000) / 100000));
        sink.put((char) ('0' + (c %= 100000) / 10000));
        sink.put((char) ('0' + (c %= 10000) / 1000));
        sink.put((char) ('0' + (c %= 1000) / 100));
        sink.put((char) ('0' + (c %= 100) / 10));
        sink.put((char) ('0' + (c % 10)));
    }

    private static void appendLong15(CharSink sink, long i) {
        long c;
        sink.put((char) ('0' + i / 100000000000000L));
        sink.put((char) ('0' + (c = i % 100000000000000L) / 10000000000000L));
        sink.put((char) ('0' + (c %= 10000000000000L) / 1000000000000L));
        sink.put((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        sink.put((char) ('0' + (c %= 100000000000L) / 10000000000L));
        sink.put((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.put((char) ('0' + (c %= 1000000000) / 100000000));
        sink.put((char) ('0' + (c %= 100000000) / 10000000));
        sink.put((char) ('0' + (c %= 10000000) / 1000000));
        sink.put((char) ('0' + (c %= 1000000) / 100000));
        sink.put((char) ('0' + (c %= 100000) / 10000));
        sink.put((char) ('0' + (c %= 10000) / 1000));
        sink.put((char) ('0' + (c %= 1000) / 100));
        sink.put((char) ('0' + (c %= 100) / 10));
        sink.put((char) ('0' + (c % 10)));
    }

    private static void appendLong14(CharSink sink, long i) {
        long c;
        sink.put((char) ('0' + i / 10000000000000L));
        sink.put((char) ('0' + (c = i % 10000000000000L) / 1000000000000L));
        sink.put((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        sink.put((char) ('0' + (c %= 100000000000L) / 10000000000L));
        sink.put((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.put((char) ('0' + (c %= 1000000000) / 100000000));
        sink.put((char) ('0' + (c %= 100000000) / 10000000));
        sink.put((char) ('0' + (c %= 10000000) / 1000000));
        sink.put((char) ('0' + (c %= 1000000) / 100000));
        sink.put((char) ('0' + (c %= 100000) / 10000));
        sink.put((char) ('0' + (c %= 10000) / 1000));
        sink.put((char) ('0' + (c %= 1000) / 100));
        sink.put((char) ('0' + (c %= 100) / 10));
        sink.put((char) ('0' + (c % 10)));
    }

    private static void appendLong13(CharSink sink, long i) {
        long c;
        sink.put((char) ('0' + i / 1000000000000L));
        sink.put((char) ('0' + (c = i % 1000000000000L) / 100000000000L));
        sink.put((char) ('0' + (c %= 100000000000L) / 10000000000L));
        sink.put((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.put((char) ('0' + (c %= 1000000000) / 100000000));
        sink.put((char) ('0' + (c %= 100000000) / 10000000));
        sink.put((char) ('0' + (c %= 10000000) / 1000000));
        sink.put((char) ('0' + (c %= 1000000) / 100000));
        sink.put((char) ('0' + (c %= 100000) / 10000));
        sink.put((char) ('0' + (c %= 10000) / 1000));
        sink.put((char) ('0' + (c %= 1000) / 100));
        sink.put((char) ('0' + (c %= 100) / 10));
        sink.put((char) ('0' + (c % 10)));
    }

    public static int parseInt(CharSequence sequence) {
        return parseInt(sequence, 0, sequence.length());
    }

    public static int parseInt(CharSequence sequence, int p, int lim) {

        if (lim == p) {
            throw new NumberFormatException("empty string");
        }

        boolean negative = sequence.charAt(p) == '-';
        if (negative) {
            p++;
        }

        if (p >= lim) {
            throw new NumberFormatException("Expect some numbers after sign");
        }

        int val = 0;
        for (; p < lim; p++) {
            int c = sequence.charAt(p);
            if (c < '0' || c > '9') {
                throw new NumberFormatException("Illegal character at " + p);
            }
            // val * 10 + (c - '0')
            int r = (val << 3) + (val << 1) + (c - '0');
            if (r < val) {
                throw new NumberFormatException("Number overflow");
            }
            val = r;
        }

        return negative ? -val : val;
    }
}