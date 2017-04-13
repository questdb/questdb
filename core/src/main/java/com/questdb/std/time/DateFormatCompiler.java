/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std.time;


import com.questdb.ex.NumericException;
import com.questdb.misc.BytecodeAssembler;
import com.questdb.misc.Numbers;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.IntList;
import com.questdb.std.Lexer;
import com.questdb.std.ObjList;

public class DateFormatCompiler {
    public static final int OP_ERA = 1;
    public static final int OP_YEAR_ONE_DIGIT = 2;
    public static final int OP_YEAR_TWO_DIGITS = 3;
    public static final int OP_YEAR_FOUR_DIGITS = 4;
    public static final int OP_MONTH_ONE_DIGIT = 5;
    public static final int OP_MONTH_TWO_DIGITS = 6;
    public static final int OP_MONTH_SHORT_NAME = 7;
    public static final int OP_MONTH_LONG_NAME = 8;
    public static final int OP_DAY_ONE_DIGIT = 9;
    public static final int OP_DAY_TWO_DIGITS = 10;
    public static final int OP_DAY_NAME_SHORT = 11;
    public static final int OP_DAY_NAME_LONG = 12;
    public static final int OP_DAY_OF_WEEK = 13;
    public static final int OP_AM_PM = 14;
    public static final int OP_HOUR_24_ONE_DIGIT = 15;
    public static final int OP_HOUR_24_TWO_DIGITS = 32;
    public static final int OP_HOUR_24_ONE_DIGIT_ONE_BASED = 16;
    public static final int OP_HOUR_24_TWO_DIGITS_ONE_BASED = 33;
    public static final int OP_HOUR_12_ONE_DIGIT = 17;
    public static final int OP_HOUR_12_TWO_DIGITS = 34;
    public static final int OP_HOUR_12_ONE_DIGIT_ONE_BASED = 18;
    public static final int OP_HOUR_12_TWO_DIGITS_ONE_BASED = 35;
    public static final int OP_MINUTE_ONE_DIGIT = 19;
    public static final int OP_MINUTE_TWO_DIGITS = 29;
    public static final int OP_SECOND_ONE_DIGIT = 20;
    public static final int OP_SECOND_TWO_DIGITS = 30;
    public static final int OP_MILLIS_ONE_DIGIT = 21;
    public static final int OP_MILLIS_THREE_DIGITS = 31;
    public static final int OP_TIME_ZONE_GMT_BASED = 22;
    public static final int OP_TIME_ZONE_SHORT = 23;
    public static final int OP_TIME_ZONE_LONG = 24;
    public static final int OP_TIME_ZONE_RFC_822 = 25;
    public static final int OP_TIME_ZONE_ISO_8601_1 = 26;
    public static final int OP_TIME_ZONE_ISO_8601_2 = 27;
    public static final int OP_TIME_ZONE_ISO_8601_3 = 28;
    public static final int OP_YEAR_GREEDY = 132;
    public static final int OP_MONTH_GREEDY = 135;
    public static final int OP_DAY_GREEDY = 139;
    public static final int OP_HOUR_24_GREEDY = 140;
    public static final int OP_HOUR_24_GREEDY_ONE_BASED = 141;
    public static final int OP_HOUR_12_GREEDY = 142;
    public static final int OP_HOUR_12_GREEDY_ONE_BASED = 143;
    public static final int OP_MINUTE_GREEDY = 144;
    public static final int OP_SECOND_GREEDY = 145;
    public static final int OP_MILLIS_GREEDY = 146;

    static final CharSequenceIntHashMap opMap;
    static final ObjList<String> opList;
    private final Lexer lexer;
    private final BytecodeAssembler asm = new BytecodeAssembler();

    public DateFormatCompiler() {
        this.lexer = new Lexer();
        for (int i = 0, n = opList.size(); i < n; i++) {
            lexer.defineSymbol(opList.getQuick(i));
        }
    }

    public DateFormat create(CharSequence sequence, boolean generic) {
        return create(sequence, 0, sequence.length(), generic);
    }

    public DateFormat create(CharSequence sequence, int lo, int hi, boolean generic) {
        this.lexer.setContent(sequence, lo, hi);
        IntList compiled = new IntList();
        ObjList<String> delimiters = new ObjList<>();

        while (this.lexer.hasNext()) {
            CharSequence cs = lexer.next();
            int op = opMap.get(cs);
            if (op == -1) {
                makeLastOpGreedy(compiled);
                delimiters.add(cs.toString());
                compiled.add(-(delimiters.size()));
            } else {
                switch (op) {
                    case OP_AM_PM:
                        makeLastOpGreedy(compiled);
                        break;
                    default:
                        break;
                }
                compiled.add(op);
            }
        }

        // make last operation "greedy"
        makeLastOpGreedy(compiled);
        return generic ? new GenericDateFormat(compiled, delimiters) : compile(compiled, delimiters);
    }

    private static void addOp(String op, int opDayTwoDigits) {
        opMap.put(op, opDayTwoDigits);
        opList.add(op);
    }

    private DateFormat compile(IntList ops, ObjList<String> delims) {
        asm.clear();
        asm.setupPool();
        int thisClassIndex = asm.poolClass(asm.poolUtf8("com/questdb/std/time/DateFormatAsm"));
        int superclassIndex = asm.poolClass(AbstractDateFormat.class);
        int dateLocaleClassIndex = asm.poolClass(DateLocale.class);
        int dateFormatUtilsClassIndex = asm.poolClass(DateFormatUtils.class);
        int numbersClassIndex = asm.poolClass(Numbers.class);
        int datesClassIndex = asm.poolClass(Dates.class);
        int numericExceptionClassIndex = asm.poolClass(NumericException.class);
        int minLongIndex = asm.poolLongConst(Long.MIN_VALUE);
        int minMillisIndex = asm.poolLongConst(Dates.MINUTE_MILLIS);

        int superIndex = asm.poolMethod(superclassIndex, "<init>", "()V");
        int matchWeekdayIndex = asm.poolMethod(dateLocaleClassIndex, "matchWeekday", "(Ljava/lang/CharSequence;II)J");
        int matchMonthIndex = asm.poolMethod(dateLocaleClassIndex, "matchMonth", "(Ljava/lang/CharSequence;II)J");
        int decodeLenIndex = asm.poolMethod(numbersClassIndex, "decodeLen", "(J)I");
        int decodeIntIndex = asm.poolMethod(numbersClassIndex, "decodeInt", "(J)I");
        int assertRemainingIndex = asm.poolMethod(dateFormatUtilsClassIndex, "assertRemaining", "(II)V");
        int parseIntIndex = asm.poolMethod(numbersClassIndex, "parseInt", "(Ljava/lang/CharSequence;II)I");
        int assertStringIndex = asm.poolMethod(dateFormatUtilsClassIndex, "assertString", "(Ljava/lang/CharSequence;ILjava/lang/CharSequence;II)I");
        int assertCharIndex = asm.poolMethod(dateFormatUtilsClassIndex, "assertChar", "(CLjava/lang/CharSequence;II)V");
        int computeMillisIndex = asm.poolMethod(dateFormatUtilsClassIndex, "computeMillis", "(IIIIIIIIJLcom/questdb/std/time/DateLocale;II)J");
        int parseOffsetIndex = asm.poolMethod(datesClassIndex, "parseOffset", "(Ljava/lang/CharSequence;II)J");

        int parseNameIndex = asm.poolUtf8("parse");
        int parseSigIndex = asm.poolUtf8("(Ljava/lang/CharSequence;IILcom/questdb/std/time/DateLocale;)J");

        // pool only delimiters over 1 char in length
        // when delimiter is 1 char we would use shorter code path
        // that doesn't require constant
        IntList delimIndices = new IntList(delims.size());
        for (int i = 0, n = delims.size(); i < n; i++) {
            String delim = delims.getQuick(i);
            if (delim.length() > 1) {
                delimIndices.add(asm.poolStringConst(asm.poolUtf8(delim)));
            } else {
                // keep indexes in both lists the same
                delimIndices.add(-1);
            }
        }

        asm.finishPool();

        asm.defineClass(1, thisClassIndex, superclassIndex);
        // interface count
        asm.putShort(0);
        // field count
        asm.putShort(0);
        // method count
        asm.putShort(2);
        asm.defineDefaultConstructor(superIndex);

        // public long parse(CharSequence in, int lo, int hi, DateLocale locale) throws NumericException
        asm.startMethod(0x01, parseNameIndex, parseSigIndex, 13, 18);

        // define stack variables
        // when pattern is not used a default value must be assigned

        // int day = 1
        asm.iconst(1);
        asm.istore(5);

        // int month = 1
        asm.iconst(1);
        asm.istore(6);

        // int year = 1970
        asm.iconst(1970);
        asm.istore(7);

        // int hour = 0
        asm.iconst(0);
        asm.istore(8);

        // int minute = 0;
        asm.iconst(0);
        asm.istore(9);

        // int second = 0
        asm.iconst(0);
        asm.istore(10);

        // int millis = 0
        asm.iconst(0);
        asm.istore(11);

        // int pos = lo
        asm.iload(2);
        asm.istore(12);

        // int timezone = -1
        asm.iconst(-1);
        asm.istore(15);

        // long offset = Long.MIN_VALUE
        asm.ldc2_w(minLongIndex);
        asm.lstore(16);

        for (int i = 0, n = ops.size(); i < n; i++) {
            int op = ops.getQuick(i);
            switch (op) {
                case OP_DAY_NAME_SHORT:
                    // l = locale.matchWeekday(in, pos, hi);
                    // pos += Numbers.decodeLen(l);

                    asm.aload(4);
                    asm.aload(1);
                    asm.iload(12);
                    asm.iload(3);
                    asm.invokeVirtual(matchWeekdayIndex);
                    asm.lstore(13);
                    asm.iload(12);
                    asm.lload(13);
                    asm.invokeStatic(decodeLenIndex);
                    asm.iadd();
                    asm.istore(12);
                    break;
                case OP_DAY_TWO_DIGITS:
                    // DateFormatUtils.assertRemaining(pos + 1, hi);
                    // day = Numbers.parseInt(in, pos, pos += 2);

                    asm.iload(12);
                    asm.iconst(1);
                    asm.iadd();
                    asm.iload(3);
                    asm.invokeStatic(assertRemainingIndex);
                    asm.aload(1);
                    asm.iload(12);
                    asm.iinc(12, 2);
                    asm.iload(12);
                    asm.invokeStatic(parseIntIndex);
                    asm.istore(5);
                    break;
                case OP_MONTH_SHORT_NAME:
                    // l = locale.matchMonth(in, pos, hi);
                    // month = Numbers.decodeInt(l) + 1;
                    // pos += Numbers.decodeLen(l);

                    asm.aload(4);
                    asm.aload(1);
                    asm.iload(12);
                    asm.iload(3);
                    asm.invokeVirtual(matchMonthIndex);
                    asm.lstore(13);
                    asm.lload(13);
                    asm.invokeStatic(decodeIntIndex);
                    asm.iconst(1);
                    asm.iadd();
                    asm.istore(6);

                    asm.iload(12);
                    asm.lload(13);
                    asm.invokeStatic(decodeLenIndex);
                    asm.iadd();
                    asm.istore(12);
                    break;
                case OP_YEAR_FOUR_DIGITS:
                    asm.iload(12);
                    asm.iconst(3);
                    asm.iadd();

                    // load "hi"
                    asm.iload(3);
                    asm.invokeStatic(assertRemainingIndex);

                    // load "in"
                    asm.aload(1);
                    asm.iload(12);
                    asm.iinc(12, 4);
                    asm.iload(12);
                    asm.invokeStatic(parseIntIndex);
                    asm.istore(7);
                    break;
                case OP_HOUR_24_TWO_DIGITS:
                    asm.iload(12);
                    asm.iconst(1);
                    asm.iadd();
                    asm.iload(3);
                    asm.invokeStatic(assertRemainingIndex);

                    asm.aload(1);
                    asm.iload(12);
                    asm.iinc(12, 2);
                    asm.iload(12);
                    asm.invokeStatic(parseIntIndex);
                    asm.istore(8);
                    break;
                case OP_MINUTE_TWO_DIGITS:
                    asm.iload(12);
                    asm.iconst(1);
                    asm.iadd();
                    asm.iload(3);
                    asm.invokeStatic(assertRemainingIndex);

                    asm.aload(1);
                    asm.iload(12);
                    asm.iinc(12, 2);
                    asm.iload(12);
                    asm.invokeStatic(parseIntIndex);
                    asm.istore(9);
                    break;

                case OP_SECOND_TWO_DIGITS:
                    asm.iload(12);
                    asm.iconst(1);
                    asm.iadd();
                    asm.iload(3);
                    asm.invokeStatic(assertRemainingIndex);

                    asm.aload(1);
                    asm.iload(12);
                    asm.iinc(12, 2);
                    asm.iload(12);
                    asm.invokeStatic(parseIntIndex);
                    asm.istore(10);
                    break;

                case DateFormatCompiler.OP_TIME_ZONE_SHORT:
                case DateFormatCompiler.OP_TIME_ZONE_GMT_BASED:
                case DateFormatCompiler.OP_TIME_ZONE_ISO_8601_1:
                case DateFormatCompiler.OP_TIME_ZONE_ISO_8601_2:
                case DateFormatCompiler.OP_TIME_ZONE_ISO_8601_3:
                case DateFormatCompiler.OP_TIME_ZONE_LONG:
                case DateFormatCompiler.OP_TIME_ZONE_RFC_822:
//                    l = Dates.parseOffset(in, pos, hi);
//                    if (l == Long.MIN_VALUE) {
//                        l = locale.matchZone(in, pos, hi);
//                        timezone = Numbers.decodeInt(l);
//                        pos += Numbers.decodeLen(l);
//                    } else {
//                        offset = Numbers.decodeInt(l) * Dates.MINUTE_MILLIS;
//                        pos += Numbers.decodeLen(l);
//                    }

                    asm.aload(1);
                    asm.iload(12);
                    asm.iload(3);
                    asm.invokeStatic(parseOffsetIndex);
                    asm.istore(13);

                    asm.lload(13);
                    asm.ldc2_w(minLongIndex);
                    asm.lcmp();
                    int branch1 = asm.ifne();

                    //
                    asm.aload(4);
                    asm.aload(1);
                    asm.iload(12);
                    asm.iload(3);
                    asm.invokeVirtual(0); // matchzone
                    asm.lstore(13);

                    //
                    asm.lload(13);
                    asm.invokeStatic(decodeIntIndex);
                    asm.istore(15);
                    int branch2 = asm.goto_();

                    //                    FRAME FULL [com/questdb/std/time/DateFormatImpl2 java/lang/CharSequence I I com/questdb/std/time/DateLocale I I I I I I I I J I J] []
                    asm.setJmp(branch1, asm.position());

                    asm.lload(13);
                    asm.invokeStatic(decodeIntIndex);
                    asm.i2l();
                    asm.ldc(minMillisIndex);
                    asm.lmul();
                    asm.lstore(16);
//                    FRAME SAME
                    asm.setJmp(branch2, asm.position());

                    asm.iload(12);
                    asm.iload(13);
                    asm.invokeStatic(decodeLenIndex);
                    asm.iadd();
                    asm.istore(12);
                    break;
                default:
                    if (op > 0) {
                        throw new IllegalArgumentException("Not yet supported: " + op);
                    }
                    String delimiter = delims.getQuick(-op - 1);
                    int len = delimiter.length();
                    if (len == 1) {
                        // DateFormatUtils.assertChar(' ', in, pos++, hi);
                        asm.iconst(delimiter.charAt(0));
                        asm.aload(1);
                        asm.iload(12);
                        asm.iinc(12, 1);
                        asm.iload(3);
                        asm.invokeStatic(assertCharIndex);
                    } else {
                        // pos = DateFormatUtils.assertString(", ", 2, in, pos, hi);
                        asm.ldc(delimIndices.getQuick(-op - 1));
                        asm.iconst(len);
                        asm.aload(1);
                        asm.iload(12);
                        asm.iload(3);
                        asm.invokeStatic(assertStringIndex);
                        asm.istore(12);
                    }
            }
        }
//        return DateFormatUtils.computeMillis(
//                year,
//                month,
//                day,
//                hour,
//                minute,
//                second,
//                millis,
//                timezone,
//                offset,
//                locale,
//                pos,
//                hi
//        );

        asm.iload(7);
        asm.iload(6);
        asm.iload(5);
        asm.iload(8);
        asm.iload(9);
        asm.iload(10);
        asm.iload(11);
        asm.iload(15);
        asm.lload(16);
        asm.aload(4);
        asm.iload(12);
        asm.iload(3);
        asm.invokeStatic(computeMillisIndex);
        asm.lreturn();
        asm.endMethodCode();

        // exceptions
        asm.putShort(0);
        // attributes
        asm.putShort(0);
        asm.endMethod();

        // class attribute count
        asm.putShort(0);


        try {
            return asm.newInstance(DateFormat.class);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    private int makeGreedy(int oldOp) {
        switch (oldOp) {
            case OP_YEAR_ONE_DIGIT:
                return OP_YEAR_GREEDY;
            case OP_MONTH_ONE_DIGIT:
                return OP_MONTH_GREEDY;
            case OP_DAY_ONE_DIGIT:
                return OP_DAY_GREEDY;
            case OP_HOUR_24_ONE_DIGIT:
                return OP_HOUR_24_GREEDY;
            case OP_HOUR_24_ONE_DIGIT_ONE_BASED:
                return OP_HOUR_24_GREEDY_ONE_BASED;
            case OP_HOUR_12_ONE_DIGIT:
                return OP_HOUR_12_GREEDY;
            case OP_HOUR_12_ONE_DIGIT_ONE_BASED:
                return OP_HOUR_12_GREEDY_ONE_BASED;
            case OP_MINUTE_ONE_DIGIT:
                return OP_MINUTE_GREEDY;
            case OP_SECOND_ONE_DIGIT:
                return OP_SECOND_GREEDY;
            case OP_MILLIS_ONE_DIGIT:
                return OP_MILLIS_GREEDY;
            default:
                return oldOp;
        }
    }

    private void makeLastOpGreedy(IntList compiled) {
        int lastOpIndex = compiled.size() - 1;
        if (lastOpIndex > -1) {
            int oldOp = compiled.getQuick(lastOpIndex);
            if (oldOp > 0) {
                int newOp = makeGreedy(oldOp);
                if (newOp != oldOp) {
                    compiled.setQuick(lastOpIndex, newOp);
                }
            }
        }
    }

    static {
        opMap = new CharSequenceIntHashMap();
        opList = new ObjList<>();

        addOp("G", OP_ERA);
        addOp("y", OP_YEAR_ONE_DIGIT);
        addOp("yy", OP_YEAR_TWO_DIGITS);
        addOp("yyyy", OP_YEAR_FOUR_DIGITS);
        addOp("M", OP_MONTH_ONE_DIGIT);
        addOp("MM", OP_MONTH_TWO_DIGITS);
        addOp("MMM", OP_MONTH_SHORT_NAME);
        addOp("MMMM", OP_MONTH_LONG_NAME);
        addOp("d", OP_DAY_ONE_DIGIT);
        addOp("dd", OP_DAY_TWO_DIGITS);
        addOp("E", OP_DAY_NAME_SHORT);
        addOp("EE", OP_DAY_NAME_LONG);
        addOp("u", OP_DAY_OF_WEEK);
        addOp("a", OP_AM_PM);
        addOp("H", OP_HOUR_24_ONE_DIGIT);
        addOp("HH", OP_HOUR_24_TWO_DIGITS);
        addOp("k", OP_HOUR_24_ONE_DIGIT_ONE_BASED);
        addOp("kk", OP_HOUR_24_TWO_DIGITS_ONE_BASED);
        addOp("K", OP_HOUR_12_ONE_DIGIT);
        addOp("KK", OP_HOUR_12_TWO_DIGITS);
        addOp("h", OP_HOUR_12_ONE_DIGIT_ONE_BASED);
        addOp("hh", OP_HOUR_12_TWO_DIGITS_ONE_BASED);
        addOp("m", OP_MINUTE_ONE_DIGIT);
        addOp("mm", OP_MINUTE_TWO_DIGITS);
        addOp("s", OP_SECOND_ONE_DIGIT);
        addOp("ss", OP_SECOND_TWO_DIGITS);
        addOp("S", OP_MILLIS_ONE_DIGIT);
        addOp("SSS", OP_MILLIS_THREE_DIGITS);
        addOp("z", OP_TIME_ZONE_SHORT);
        addOp("zz", OP_TIME_ZONE_GMT_BASED);
        addOp("zzz", OP_TIME_ZONE_LONG);
        addOp("Z", OP_TIME_ZONE_RFC_822);
        addOp("x", OP_TIME_ZONE_ISO_8601_1);
        addOp("xx", OP_TIME_ZONE_ISO_8601_2);
        addOp("xxx", OP_TIME_ZONE_ISO_8601_3);
    }
}
