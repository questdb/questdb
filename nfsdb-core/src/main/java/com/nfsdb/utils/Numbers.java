/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.utils;

import com.nfsdb.exceptions.NumericException;
import com.nfsdb.io.sink.CharSink;

public final class Numbers {

    public static final int INT_NaN = Integer.MIN_VALUE;
    public static final long LONG_NaN = Long.MIN_VALUE;
    private static final long[] pow10;
    private static final long LONG_OVERFLOW_MAX = Long.MAX_VALUE / 10;
    private static final long INT_OVERFLOW_MAX = Integer.MAX_VALUE / 10;
    private final static String NaN = "NaN";
    private static final String INFINITY = "Infinity";
    private static final double[] pow10d = new double[]{1, 1E1, 1E2, 1E3, 1E4, 1E5, 1E6, 1E7, 1E8, 1E9, 1E10, 1E11, 1E12, 1E13, 1E14, 1E15, 1E16, 1E17, 1E18, 1E19, 1E20, 1E21, 1E22, 1E23, 1E24, 1E25, 1E26, 1E27, 1E28, 1E29, 1E30, 1E31, 1E32, 1E33, 1E34, 1E35, 1E36, 1E37, 1E38, 1E39, 1E40, 1E41, 1E42, 1E43, 1E44, 1E45, 1E46, 1E47, 1E48, 1E49, 1E50, 1E51, 1E52, 1E53, 1E54, 1E55, 1E56, 1E57, 1E58, 1E59, 1E60, 1E61, 1E62, 1E63, 1E64, 1E65, 1E66, 1E67, 1E68, 1E69, 1E70, 1E71, 1E72, 1E73, 1E74, 1E75, 1E76, 1E77, 1E78, 1E79, 1E80, 1E81, 1E82, 1E83, 1E84, 1E85, 1E86, 1E87, 1E88, 1E89, 1E90, 1E91, 1E92, 1E93, 1E94, 1E95, 1E96, 1E97, 1E98, 1E99, 1E100, 1E101, 1E102, 1E103, 1E104, 1E105, 1E106, 1E107, 1E108, 1E109, 1E110, 1E111, 1E112, 1E113, 1E114, 1E115, 1E116, 1E117, 1E118, 1E119, 1E120, 1E121, 1E122, 1E123, 1E124, 1E125, 1E126, 1E127, 1E128, 1E129, 1E130, 1E131, 1E132, 1E133, 1E134, 1E135, 1E136, 1E137, 1E138, 1E139, 1E140, 1E141, 1E142, 1E143, 1E144, 1E145, 1E146, 1E147, 1E148, 1E149, 1E150, 1E151, 1E152, 1E153, 1E154, 1E155, 1E156, 1E157, 1E158, 1E159, 1E160, 1E161, 1E162, 1E163, 1E164, 1E165, 1E166, 1E167, 1E168, 1E169, 1E170, 1E171, 1E172, 1E173, 1E174, 1E175, 1E176, 1E177, 1E178, 1E179, 1E180, 1E181, 1E182, 1E183, 1E184, 1E185, 1E186, 1E187, 1E188, 1E189, 1E190, 1E191, 1E192, 1E193, 1E194, 1E195, 1E196, 1E197, 1E198, 1E199, 1E200, 1E201, 1E202, 1E203, 1E204, 1E205, 1E206, 1E207, 1E208, 1E209, 1E210, 1E211, 1E212, 1E213, 1E214, 1E215, 1E216, 1E217, 1E218, 1E219, 1E220, 1E221, 1E222, 1E223, 1E224, 1E225, 1E226, 1E227, 1E228, 1E229, 1E230, 1E231, 1E232, 1E233, 1E234, 1E235, 1E236, 1E237, 1E238, 1E239, 1E240, 1E241, 1E242, 1E243, 1E244, 1E245, 1E246, 1E247, 1E248, 1E249, 1E250, 1E251, 1E252, 1E253, 1E254, 1E255, 1E256, 1E257, 1E258, 1E259, 1E260, 1E261, 1E262, 1E263, 1E264, 1E265, 1E266, 1E267, 1E268, 1E269, 1E270, 1E271, 1E272, 1E273, 1E274, 1E275, 1E276, 1E277, 1E278, 1E279, 1E280, 1E281, 1E282, 1E283, 1E284, 1E285, 1E286, 1E287, 1E288, 1E289, 1E290, 1E291, 1E292, 1E293, 1E294, 1E295, 1E296, 1E297, 1E298, 1E299, 1E300, 1E301, 1E302, 1E303, 1E304, 1E305, 1E306, 1E307, 1E308};
    private static final float[] pow10f = new float[]{1, 1E1f, 1E2f, 1E3f, 1E4f, 1E5f, 1E6f, 1E7f, 1E8f, 1E9f, 1E10f, 1E11f, 1E12f, 1E13f, 1E14f, 1E15f, 1E16f, 1E17f, 1E18f, 1E19f, 1E20f, 1E21f, 1E22f, 1E23f, 1E24f, 1E25f, 1E26f, 1E27f, 1E28f, 1E29f, 1E30f, 1E31f, 1E32f, 1E33f, 1E34f, 1E35f, 1E36f, 1E37f, 1E38f};

    private Numbers() {
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
        int targetScale = scale + 1;
        while (targetScale < 20 && pow10[targetScale] <= scaled) {
            factor = pow10[targetScale++];
        }

        // factor overflow, fallback to slow method rather than throwing exception
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
            if (i == Integer.MIN_VALUE) {
                sink.put("NaN");
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

    public static void append(CharSink sink, long i) {
        if (i > Integer.MAX_VALUE || i < Integer.MIN_VALUE) {
            if (i < 0) {
                if (i == Long.MIN_VALUE) {
                    sink.put("NaN");
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
        } else {
            append(sink, (int) i);
        }
    }

    public static void appendTrim(CharSink sink, double d, int scale) {
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

        if (d == 0d) {
            sink.put('0');
            return;
        }

        if (d < 0) {
            sink.put('-');
            d = -d;
        }

        long scaled = (long) (d * pow10[scale] + 0.5);

        // adjust scale to remove trailing zeroes
        int k = 1;
        while (scaled % pow10[k] == 0 && ((double) scaled) / pow10[k] > d) {
            k++;
        }
        scale = scale - k + 1;

        long factor = pow10[scale];
        scaled = (long) (d * factor + 0.5);

        int targetScale = scale + 1;
        while (targetScale < 20 && pow10[targetScale] <= scaled) {
            factor = pow10[targetScale++];
        }


        // factor overflow, fallback to slow method rather than throwing exception
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

    public static int ceilPow2(int value) {
        if ((value != 0) && (value & (value - 1)) > 0) {
            value |= (value >>> 1);
            value |= (value >>> 2);
            value |= (value >>> 4);
            value |= (value >>> 8);
            value |= (value >>> 16);
            value++;

            if (value < 0) {
                value >>>= 1;
            }
        }

        return value;
    }

    public static int msb(int value) {
        return 31 - Integer.numberOfLeadingZeros(value);
    }

    public static double parseDouble(CharSequence sequence) throws NumericException {
        return parseDouble(sequence, 0, sequence.length());
    }

    @SuppressWarnings("Duplicates")
    public static double parseDouble(CharSequence sequence, int p, int lim) throws NumericException {

        if (lim == p) {
            throw NumericException.INSTANCE;
        }

        boolean negative = sequence.charAt(p) == '-';
        if (negative) {
            p++;
        }

        if (p >= lim) {
            throw NumericException.INSTANCE;
        }


        switch (sequence.charAt(p)) {
            case 'N':
                return parseConst(sequence, p, lim, NaN, Double.NaN);
            case 'I':
                return parseConst(sequence, p, lim, INFINITY, negative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY);
        }

        long val = 0;
        int dp = -1;
        int dpe = lim;
        int exp = 0;
        out:
        for (int i = p; i < lim; i++) {
            int c = sequence.charAt(i);
            switch (c) {
                case '.':
                    dp = i;
                    continue;
                case 'E':
                case 'e':
                    exp = parseInt(sequence, i + 1, lim);
                    if (dpe == lim) {
                        dpe = i;
                    }
                    break out;
                default:
                    if (c < '0' || c > '9') {
                        throw NumericException.INSTANCE;
                    }

                    if (val <= LONG_OVERFLOW_MAX) {
                        // val * 10 + (c - '0')
                        val = (val << 3) + (val << 1) + (c - '0');
                    } else if (dpe == lim) {
                        dpe = i;
                    }
            }
        }

        exp = dp == -1 ? exp : exp - (dpe - dp - 1);

        if (exp > 308) {
            exp = 308;
        } else if (exp < -308) {
            exp = -308;
        }

        if (exp > 0) {
            return (negative ? -val : val) * pow10d[exp];
        } else {
            return (negative ? -val : val) / pow10d[-exp];
        }
    }

    public static double parseDoubleQuiet(CharSequence sequence) {
        try {
            return parseDouble(sequence, 0, sequence.length());
        } catch (NumericException e) {
            return Double.NaN;
        }
    }

    public static float parseFloat(CharSequence sequence) throws NumericException {
        return parseFloat(sequence, 0, sequence.length());
    }

    @SuppressWarnings("Duplicates")
    public static float parseFloat(CharSequence sequence, int p, int lim) throws NumericException {

        if (lim == p) {
            throw NumericException.INSTANCE;
        }

        boolean negative = sequence.charAt(p) == '-';
        if (negative) {
            p++;
        }

        if (p >= lim) {
            throw NumericException.INSTANCE;
        }


        switch (sequence.charAt(p)) {
            case 'N':
                return parseFloatConst(sequence, p, lim, NaN, Float.NaN);
            case 'I':
                return parseFloatConst(sequence, p, lim, INFINITY, negative ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY);
        }

        int val = 0;
        int dp = -1;
        int dpe = lim;
        int exp = 0;
        out:
        for (int i = p; i < lim; i++) {
            int c = sequence.charAt(i);
            switch (c) {
                case '.':
                    dp = i;
                    continue;
                case 'E':
                case 'e':
                    exp = parseInt(sequence, i + 1, lim);
                    if (dpe == lim) {
                        dpe = i;
                    }
                    break out;
                default:
                    if (c < '0' || c > '9') {
                        throw NumericException.INSTANCE;
                    }

                    if (val <= INT_OVERFLOW_MAX) {
                        // val * 10 + (c - '0')
                        val = (val << 3) + (val << 1) + (c - '0');
                    } else if (dpe == lim) {
                        dpe = i;
                    }
            }
        }

        exp = dp == -1 ? exp : exp - (dpe - dp - 1);

        if (exp > 38) {
            exp = 38;
        } else if (exp < -38) {
            exp = -38;
        }

        if (exp > 0) {
            return (negative ? -val : val) * pow10f[exp];
        } else {
            return (negative ? -val : val) / pow10f[-exp];
        }
    }

    public static float parseFloatQuiet(CharSequence sequence) {
        try {
            return parseFloat(sequence, 0, sequence.length());
        } catch (Exception e) {
            return Float.NaN;
        }
    }

    public static int parseInt(CharSequence sequence) throws NumericException {
        if (sequence == null) {
            throw NumericException.INSTANCE;
        }

        return parseInt0(sequence, 0, sequence.length());
    }

    public static int parseInt(CharSequence sequence, int p, int lim) throws NumericException {
        if (sequence == null) {
            throw NumericException.INSTANCE;
        }
        return parseInt0(sequence, p, lim);
    }

    public static int parseIntQuiet(CharSequence sequence) {
        try {
            if (sequence == null || Chars.equals("NaN", sequence)) {
                return Integer.MIN_VALUE;
            }
            return parseInt0(sequence, 0, sequence.length());
        } catch (NumericException e) {
            return Integer.MIN_VALUE;
        }

    }

    public static long parseLong(CharSequence sequence) throws NumericException {
        if (sequence == null) {
            throw NumericException.INSTANCE;
        }
        return parseLong0(sequence, 0, sequence.length());
    }

    public static long parseLong(CharSequence sequence, int p, int lim) throws NumericException {
        if (sequence == null) {
            throw NumericException.INSTANCE;
        }
        return parseLong0(sequence, p, lim);
    }

    public static long parseLongQuiet(CharSequence sequence) {
        if (sequence == null) {
            return Long.MIN_VALUE;
        }
        try {
            return parseLong0(sequence, 0, sequence.length());
        } catch (NumericException e) {
            return Long.MIN_VALUE;
        }
    }

    private static void appendLong11(CharSink sink, long i) {
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

    private static void appendLong12(CharSink sink, long i) {
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


    //////////////////////

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

    private static double parseConst(CharSequence sequence, int p, int lim, String target, double value) throws NumericException {

        if (lim - p > target.length()) {
            throw NumericException.INSTANCE;
        }

        for (int i = 0; i < target.length(); i++) {
            if (p + i >= lim) {
                throw NumericException.INSTANCE;
            }

            if (sequence.charAt(p + i) != target.charAt(i)) {
                throw NumericException.INSTANCE;
            }
        }
        return value;
    }

    private static float parseFloatConst(CharSequence sequence, int p, int lim, String target, float value) throws NumericException {

        if (lim - p > target.length()) {
            throw NumericException.INSTANCE;
        }

        for (int i = 0; i < target.length(); i++) {
            if (p + i >= lim) {
                throw NumericException.INSTANCE;
            }

            if (sequence.charAt(p + i) != target.charAt(i)) {
                throw NumericException.INSTANCE;
            }
        }
        return value;
    }

    private static int parseInt0(CharSequence sequence, int p, int lim) throws NumericException {

        if (lim == p) {
            throw NumericException.INSTANCE;
        }

        boolean negative = sequence.charAt(p) == '-';
        if (negative) {
            p++;
        }

        if (p >= lim) {
            throw NumericException.INSTANCE;
        }

        int val = 0;
        for (; p < lim; p++) {
            int c = sequence.charAt(p);
            if (c < '0' || c > '9') {
                throw NumericException.INSTANCE;
            }
            // val * 10 + (c - '0')
            int r = (val << 3) + (val << 1) - (c - '0');
            if (r > val) {
                throw NumericException.INSTANCE;
            }
            val = r;
        }

        if (val == Integer.MIN_VALUE && !negative) {
            throw NumericException.INSTANCE;
        }
        return negative ? val : -val;
    }

    private static long parseLong0(CharSequence sequence, int p, int lim) throws NumericException {

        if (lim == p) {
            throw NumericException.INSTANCE;
        }

        boolean negative = sequence.charAt(p) == '-';
        if (negative) {
            p++;
        }

        if (p >= lim) {
            throw NumericException.INSTANCE;
        }

        long val = 0;
        for (; p < lim; p++) {
            int c = sequence.charAt(p);
            if (c < '0' || c > '9') {
                throw NumericException.INSTANCE;
            }
            // val * 10 + (c - '0')
            long r = (val << 3) + (val << 1) - (c - '0');
            if (r > val) {
                throw NumericException.INSTANCE;
            }
            val = r;
        }

        if (val == Long.MIN_VALUE && !negative) {
            throw NumericException.INSTANCE;
        }
        return negative ? val : -val;
    }

    static {
        pow10 = new long[20];
        pow10[0] = 1;
        for (int i = 1; i < pow10.length; i++) {
            pow10[i] = pow10[i - 1] * 10;
        }
    }
}