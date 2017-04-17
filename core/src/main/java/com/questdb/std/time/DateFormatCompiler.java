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


import com.questdb.misc.BytecodeAssembler;
import com.questdb.misc.Numbers;
import com.questdb.std.*;

import static com.questdb.std.time.DateFormatUtils.HOUR_24;
import static com.questdb.std.time.DateFormatUtils.HOUR_AM;

public class DateFormatCompiler {
    static final int OP_ERA = 1;
    static final int OP_YEAR_ONE_DIGIT = 2;
    static final int OP_YEAR_TWO_DIGITS = 3;
    static final int OP_YEAR_FOUR_DIGITS = 4;
    static final int OP_MONTH_ONE_DIGIT = 5;
    static final int OP_MONTH_TWO_DIGITS = 6;
    static final int OP_MONTH_SHORT_NAME = 7;
    static final int OP_MONTH_LONG_NAME = 8;
    static final int OP_DAY_ONE_DIGIT = 9;
    static final int OP_DAY_TWO_DIGITS = 10;
    static final int OP_DAY_NAME_SHORT = 11;
    static final int OP_DAY_NAME_LONG = 12;
    static final int OP_DAY_OF_WEEK = 13;
    static final int OP_AM_PM = 14;
    static final int OP_HOUR_24_ONE_DIGIT = 15;
    static final int OP_HOUR_24_TWO_DIGITS = 32;
    static final int OP_HOUR_24_ONE_DIGIT_ONE_BASED = 16;
    static final int OP_HOUR_24_TWO_DIGITS_ONE_BASED = 33;
    static final int OP_HOUR_12_ONE_DIGIT = 17;
    static final int OP_HOUR_12_TWO_DIGITS = 34;
    static final int OP_HOUR_12_ONE_DIGIT_ONE_BASED = 18;
    static final int OP_HOUR_12_TWO_DIGITS_ONE_BASED = 35;
    static final int OP_MINUTE_ONE_DIGIT = 19;
    static final int OP_MINUTE_TWO_DIGITS = 29;
    static final int OP_SECOND_ONE_DIGIT = 20;
    static final int OP_SECOND_TWO_DIGITS = 30;
    static final int OP_MILLIS_ONE_DIGIT = 21;
    static final int OP_MILLIS_THREE_DIGITS = 31;
    static final int OP_TIME_ZONE_GMT_BASED = 22;
    static final int OP_TIME_ZONE_SHORT = 23;
    static final int OP_TIME_ZONE_LONG = 24;
    static final int OP_TIME_ZONE_RFC_822 = 25;
    static final int OP_TIME_ZONE_ISO_8601_1 = 26;
    static final int OP_TIME_ZONE_ISO_8601_2 = 27;
    static final int OP_TIME_ZONE_ISO_8601_3 = 28;
    static final int OP_YEAR_GREEDY = 132;
    static final int OP_MONTH_GREEDY = 135;
    static final int OP_DAY_GREEDY = 139;
    static final int OP_HOUR_24_GREEDY = 140;
    static final int OP_HOUR_24_GREEDY_ONE_BASED = 141;
    static final int OP_HOUR_12_GREEDY = 142;
    static final int OP_HOUR_12_GREEDY_ONE_BASED = 143;
    static final int OP_MINUTE_GREEDY = 144;
    static final int OP_SECOND_GREEDY = 145;
    static final int OP_MILLIS_GREEDY = 146;
    static final CharSequenceIntHashMap opMap;
    static final ObjList<String> opList;
    private static final int P_INPUT_STR = 1;
    private static final int P_LO = 2;
    private static final int P_HI = 3;
    private static final int P_LOCALE = 4;

    private static final int LOCAL_DAY = 5;
    private static final int LOCAL_MONTH = 6;
    private static final int LOCAL_YEAR = 7;
    private static final int LOCAL_HOUR = 8;
    private static final int LOCAL_MINUTE = 9;
    private static final int LOCAL_SECOND = 10;
    private static final int LOCAL_MILLIS = 11;
    private static final int LOCAL_POS = 12;
    private static final int LOCAL_TEMP_LONG = 13;
    private static final int LOCAL_TIMEZONE = 15;
    private static final int LOCAL_OFFSET = 16;
    private static final int LOCAL_HOUR_TYPE = 18;
    private static final int LOCAL_ERA = 19;
    private final Lexer lexer;
    private final BytecodeAssembler asm = new BytecodeAssembler();
    private final IntList ops = new IntList();
    private final ObjList<String> delimiters = new ObjList<>();

    public DateFormatCompiler() {
        this.lexer = new Lexer();
        for (int i = 0, n = opList.size(); i < n; i++) {
            lexer.defineSymbol(opList.getQuick(i));
        }
    }

    public DateFormat compile(CharSequence sequence, boolean generic) {
        return compile(sequence, 0, sequence.length(), generic);
    }

    public DateFormat compile(CharSequence sequence, int lo, int hi, boolean generic) {
        this.lexer.setContent(sequence, lo, hi);

        IntList ops;
        ObjList<String> delimiters;

        if (generic) {
            ops = new IntList();
            delimiters = new ObjList<>();
        } else {
            ops = this.ops;
            delimiters = this.delimiters;
            ops.clear();
            delimiters.clear();
        }

        while (this.lexer.hasNext()) {
            CharSequence cs = lexer.next();
            int op = opMap.get(cs);
            if (op == -1) {
                makeLastOpGreedy(ops);
                delimiters.add(cs.toString());
                ops.add(-(delimiters.size()));
            } else {
                switch (op) {
                    case OP_AM_PM:
                        makeLastOpGreedy(ops);
                        break;
                    default:
                        break;
                }
                ops.add(op);
            }
        }

        // make last operation "greedy"
        makeLastOpGreedy(ops);
        return generic ? new GenericDateFormat(ops, delimiters) : compile(ops, delimiters);
    }

    private static void addOp(String op, int opDayTwoDigits) {
        opMap.put(op, opDayTwoDigits);
        opList.add(op);
    }

    private void addTempToPos(int decodeLenIndex) {
        asm.iload(LOCAL_POS);
        decodeInt(decodeLenIndex);
        asm.iadd();
        asm.istore(LOCAL_POS);
    }

    private DateFormat compile(IntList ops, ObjList<String> delimiters) {
        asm.init(DateFormat.class);
        asm.setupPool();
        int thisClassIndex = asm.poolClass(asm.poolUtf8("com/questdb/std/time/DateFormatAsm"));
        int stackMapTableIndex = asm.poolUtf8("StackMapTable");
        int superclassIndex = asm.poolClass(AbstractDateFormat.class);
        int dateLocaleClassIndex = asm.poolClass(DateLocale.class);
        int dateFormatUtilsClassIndex = asm.poolClass(DateFormatUtils.class);
        int numbersClassIndex = asm.poolClass(Numbers.class);
        int datesClassIndex = asm.poolClass(Dates.class);
        int charSequenceClassIndex = asm.poolClass(CharSequence.class);
        int minLongIndex = asm.poolLongConst(Long.MIN_VALUE);
        int minMillisIndex = asm.poolLongConst(Dates.MINUTE_MILLIS);

        int superIndex = asm.poolMethod(superclassIndex, "<init>", "()V");
        int matchWeekdayIndex = asm.poolMethod(dateLocaleClassIndex, "matchWeekday", "(Ljava/lang/CharSequence;II)J");
        int matchMonthIndex = asm.poolMethod(dateLocaleClassIndex, "matchMonth", "(Ljava/lang/CharSequence;II)J");
        int matchZoneIndex = asm.poolMethod(dateLocaleClassIndex, "matchZone", "(Ljava/lang/CharSequence;II)J");
        int matchAMPMIndex = asm.poolMethod(dateLocaleClassIndex, "matchAMPM", "(Ljava/lang/CharSequence;II)J");
        int matchEraIndex = asm.poolMethod(dateLocaleClassIndex, "matchEra", "(Ljava/lang/CharSequence;II)J");

        int parseIntSafelyIndex = asm.poolMethod(numbersClassIndex, "parseIntSafely", "(Ljava/lang/CharSequence;II)J");

        int decodeLenIndex = asm.poolMethod(numbersClassIndex, "decodeLen", "(J)I");
        int decodeIntIndex = asm.poolMethod(numbersClassIndex, "decodeInt", "(J)I");
        int assertRemainingIndex = asm.poolMethod(dateFormatUtilsClassIndex, "assertRemaining", "(II)V");
        int assertNoTailIndex = asm.poolMethod(dateFormatUtilsClassIndex, "assertNoTail", "(II)V");
        int parseIntIndex = asm.poolMethod(numbersClassIndex, "parseInt", "(Ljava/lang/CharSequence;II)I");
        int assertStringIndex = asm.poolMethod(dateFormatUtilsClassIndex, "assertString", "(Ljava/lang/CharSequence;ILjava/lang/CharSequence;II)I");
        int assertCharIndex = asm.poolMethod(dateFormatUtilsClassIndex, "assertChar", "(CLjava/lang/CharSequence;II)V");
        int computeMillisIndex = asm.poolMethod(dateFormatUtilsClassIndex, "compute", "(Lcom/questdb/std/time/DateLocale;IIIIIIIIIJI)J");
        int adjustYearIndex = asm.poolMethod(dateFormatUtilsClassIndex, "adjustYear", "(I)I");
        int parseYearGreedyIndex = asm.poolMethod(dateFormatUtilsClassIndex, "parseYearGreedy", "(Ljava/lang/CharSequence;II)J");
        int parseOffsetIndex = asm.poolMethod(datesClassIndex, "parseOffset", "(Ljava/lang/CharSequence;II)J");

        int parseNameIndex = asm.poolUtf8("parse");
        int parseSigIndex = asm.poolUtf8("(Ljava/lang/CharSequence;IILcom/questdb/std/time/DateLocale;)J");

        // pool only delimiters over 1 char in length
        // when delimiter is 1 char we would use shorter code path
        // that doesn't require constant
        IntList delimIndices = new IntList(delimiters.size());
        for (int i = 0, n = delimiters.size(); i < n; i++) {
            String delim = delimiters.getQuick(i);
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
        compileParseMethod(
                ops,
                delimiters,
                thisClassIndex,
                stackMapTableIndex,
                dateLocaleClassIndex,
                charSequenceClassIndex,
                minLongIndex,
                minMillisIndex,
                matchWeekdayIndex,
                matchMonthIndex,
                matchZoneIndex,
                matchAMPMIndex,
                matchEraIndex,
                parseIntSafelyIndex,
                decodeLenIndex,
                decodeIntIndex,
                assertRemainingIndex,
                assertNoTailIndex,
                parseIntIndex,
                assertStringIndex,
                assertCharIndex,
                computeMillisIndex,
                adjustYearIndex,
                parseYearGreedyIndex,
                parseOffsetIndex,
                parseNameIndex,
                parseSigIndex,
                delimIndices
        );


        // class attribute count
        asm.putShort(0);

        return asm.newInstance();
    }

    private void compileParseMethod(IntList ops, ObjList<String> delimiters, int thisClassIndex, int stackMapTableIndex, int dateLocaleClassIndex, int charSequenceClassIndex, int minLongIndex, int minMillisIndex, int matchWeekdayIndex, int matchMonthIndex, int matchZoneIndex, int matchAMPMIndex, int matchEraIndex, int parseIntSafelyIndex, int decodeLenIndex, int decodeIntIndex, int assertRemainingIndex, int assertNoTailIndex, int parseIntIndex, int assertStringIndex, int assertCharIndex, int computeMillisIndex, int adjustYearIndex, int parseYearGreedyIndex, int parseOffsetIndex, int parseNameIndex, int parseSigIndex, IntList delimIndices) {

        int stackState = computeParseMethodStack(ops);

        // public long parse(CharSequence in, int lo, int hi, DateLocale locale) throws NumericException
        asm.startMethod(0x01, parseNameIndex, parseSigIndex, 13, 20);

        // define stack variables
        // when pattern is not used a default value must be assigned

        if ((stackState & (1 << LOCAL_DAY)) == 0) {
            // int day = 1
            asm.iconst(1);
            asm.istore(LOCAL_DAY);
        }

        if ((stackState & (1 << LOCAL_MONTH)) == 0) {
            // int month = 1
            asm.iconst(1);
            asm.istore(LOCAL_MONTH);
        }

        if ((stackState & (1 << LOCAL_YEAR)) == 0) {
            // int year = 1970
            asm.iconst(1970);
            asm.istore(LOCAL_YEAR);
        }

        if ((stackState & (1 << LOCAL_HOUR)) == 0) {
            // int hour = 0
            asm.iconst(0);
            asm.istore(LOCAL_HOUR);
        }

        if ((stackState & (1 << LOCAL_MINUTE)) == 0) {
            // int minute = 0;
            asm.iconst(0);
            asm.istore(LOCAL_MINUTE);
        }

        if ((stackState & (1 << LOCAL_SECOND)) == 0) {
            // int second = 0
            asm.iconst(0);
            asm.istore(LOCAL_SECOND);
        }

        if ((stackState & (1 << LOCAL_MILLIS)) == 0) {
            // int millis = 0
            asm.iconst(0);
            asm.istore(LOCAL_MILLIS);
        }

        // int pos = lo
        asm.iload(P_LO);
        asm.istore(LOCAL_POS);

        // int timezone = -1
        asm.iconst(-1);
        asm.istore(LOCAL_TIMEZONE);

        // long offset = Long.MIN_VALUE
        asm.ldc2_w(minLongIndex);
        asm.lstore(LOCAL_OFFSET);

        asm.iconst(HOUR_24);
        asm.istore(LOCAL_HOUR_TYPE);

        if ((stackState & (1 << LOCAL_ERA)) == 0) {
            asm.iconst(1);
            asm.istore(LOCAL_ERA);
        }

        if ((stackState & (1 << LOCAL_TEMP_LONG)) == 0) {
            asm.lconst_0();
            asm.lstore(LOCAL_TEMP_LONG);
        }

        LongList frameOffsets = new LongList();

        for (int i = 0, n = ops.size(); i < n; i++) {
            int op = ops.getQuick(i);
            switch (op) {
                // AM/PM
                case OP_AM_PM:
                    // l = locale.matchAMPM(in, pos, hi);
                    // hourType = Numbers.decodeInt(l);
                    // pos += Numbers.decodeLen(l);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeMatch(matchAMPMIndex);
                    decodeInt(decodeIntIndex);
                    asm.istore(LOCAL_HOUR_TYPE);
                    addTempToPos(decodeLenIndex);
                    break;
                case OP_MILLIS_ONE_DIGIT:
                    // assertRemaining(pos, hi);
                    // millis = Numbers.parseInt(in, pos, ++pos);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_MILLIS);
                    break;
                case OP_MILLIS_THREE_DIGITS:
                    // assertRemaining(pos + 2, hi);
                    // millis = Numbers.parseInt(in, pos, pos += 3);
                    stackState &= ~(1 << LOCAL_MILLIS);
                    parseDigits(assertRemainingIndex, parseIntIndex, 3, LOCAL_MILLIS);
                    break;
                case OP_MILLIS_GREEDY:
                    // l = Numbers.parseIntSafely(in, pos, hi);
                    // millis = Numbers.decodeInt(l);
                    // pos += Numbers.decodeLen(l);
                    stackState &= ~(1 << LOCAL_MILLIS);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseIntSafelyIndex, decodeLenIndex, decodeIntIndex, LOCAL_MILLIS);
                    break;
                case OP_SECOND_ONE_DIGIT:
                    // assertRemaining(pos, hi);
                    // second = Numbers.parseInt(in, pos, ++pos);
                    stackState &= ~(1 << LOCAL_SECOND);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_SECOND);
                    break;
                case OP_SECOND_TWO_DIGITS:
                    stackState &= ~(1 << LOCAL_SECOND);
                    parseTwoDigits(assertRemainingIndex, parseIntIndex, LOCAL_SECOND);
                    break;
                case OP_SECOND_GREEDY:
                    // l = Numbers.parseIntSafely(in, pos, hi);
                    // second = Numbers.decodeInt(l);
                    // pos += Numbers.decodeLen(l);
                    stackState &= ~(1 << LOCAL_SECOND);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseIntSafelyIndex, decodeLenIndex, decodeIntIndex, LOCAL_SECOND);
                    break;
                case OP_MINUTE_ONE_DIGIT:
                    // assertRemaining(pos, hi);
                    // minute = Numbers.parseInt(in, pos, ++pos);
                    stackState &= ~(1 << LOCAL_MINUTE);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_MINUTE);
                    break;
                case OP_MINUTE_TWO_DIGITS:
                    // assertRemaining(pos + 1, hi);
                    // minute = Numbers.parseInt(in, pos, pos += 2);
                    stackState &= ~(1 << LOCAL_MINUTE);
                    parseTwoDigits(assertRemainingIndex, parseIntIndex, LOCAL_MINUTE);
                    break;

                case OP_MINUTE_GREEDY:
                    // l = Numbers.parseIntSafely(in, pos, hi);
                    // minute = Numbers.decodeInt(l);
                    // pos += Numbers.decodeLen(l);
                    stackState &= ~(1 << LOCAL_MINUTE);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseIntSafelyIndex, decodeLenIndex, decodeIntIndex, LOCAL_MINUTE);
                    break;
                // HOUR (0-11)
                case OP_HOUR_12_ONE_DIGIT:
                    // assertRemaining(pos, hi);
                    // hour = Numbers.parseInt(in, pos, ++pos);
                    // if (hourType == HOUR_24) {
                    //     hourType = HOUR_AM;
                    // }
                    stackState &= ~(1 << LOCAL_HOUR);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_HOUR);
                    setHourType(frameOffsets, HOUR_AM, stackState);
                    break;
                case OP_HOUR_12_TWO_DIGITS:
                    // assertRemaining(pos + 1, hi);
                    // hour = Numbers.parseInt(in, pos, pos += 2);
                    // if (hourType == HOUR_24) {
                    //     hourType = HOUR_AM;
                    // }
                    stackState &= ~(1 << LOCAL_HOUR);
                    parseTwoDigits(assertRemainingIndex, parseIntIndex, LOCAL_HOUR);
                    setHourType(frameOffsets, HOUR_AM, stackState);
                    break;

                case OP_HOUR_12_GREEDY:
                    // l = Numbers.parseIntSafely(in, pos, hi);
                    // hour = Numbers.decodeInt(l);
                    // pos += Numbers.decodeLen(l);
                    // if (hourType == HOUR_24) {
                    //     hourType = HOUR_AM;
                    // }
                    stackState &= ~(1 << LOCAL_HOUR);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseIntSafelyIndex, decodeLenIndex, decodeIntIndex, LOCAL_HOUR);
                    setHourType(frameOffsets, HOUR_AM, stackState);
                    break;
                // HOUR (1-12)
                case OP_HOUR_12_ONE_DIGIT_ONE_BASED:
                    // assertRemaining(pos, hi);
                    // hour = Numbers.parseInt(in, pos, ++pos) - 1;
                    // if (hourType == HOUR_24) {
                    //    hourType = HOUR_AM;
                    // }
                    stackState &= ~(1 << LOCAL_HOUR);
                    parseDigitsSub1(assertRemainingIndex, parseIntIndex, 1, LOCAL_HOUR);
                    setHourType(frameOffsets, HOUR_AM, stackState);
                    break;

                case OP_HOUR_12_TWO_DIGITS_ONE_BASED:
                    // assertRemaining(pos + 1, hi);
                    // hour = Numbers.parseInt(in, pos, pos += 2) - 1;
                    // if (hourType == HOUR_24) {
                    //    hourType = HOUR_AM;
                    //}
                    stackState &= ~(1 << LOCAL_HOUR);
                    parseDigitsSub1(assertRemainingIndex, parseIntIndex, 2, LOCAL_HOUR);
                    setHourType(frameOffsets, HOUR_AM, stackState);
                    break;

                case OP_HOUR_12_GREEDY_ONE_BASED:
                    // l = Numbers.parseIntSafely(in, pos, hi);
                    // hour = Numbers.decodeInt(l) - 1;
                    // pos += Numbers.decodeLen(l);
                    // if (hourType == HOUR_24) {
                    //    hourType = HOUR_AM;
                    //}
                    stackState &= ~(1 << LOCAL_HOUR);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);

                    asm.aload(P_INPUT_STR);
                    asm.iload(LOCAL_POS);
                    asm.iload(P_HI);
                    asm.invokeStatic(parseIntSafelyIndex);
                    asm.lstore(LOCAL_TEMP_LONG);
                    decodeInt(decodeIntIndex);
                    asm.iconst(1);
                    asm.isub();
                    asm.istore(LOCAL_HOUR);
                    addTempToPos(decodeLenIndex);
                    setHourType(frameOffsets, HOUR_AM, stackState);
                    break;
                // HOUR (0-23)
                case OP_HOUR_24_ONE_DIGIT:
                    // assertRemaining(pos, hi);
                    // hour = Numbers.parseInt(in, pos, ++pos);
                    stackState &= ~(1 << LOCAL_HOUR);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_HOUR);
                    break;

                case OP_HOUR_24_TWO_DIGITS:
                    // assertRemaining(pos + 1, hi);
                    // hour = Numbers.parseInt(in, pos, pos += 2);
                    stackState &= ~(1 << LOCAL_HOUR);
                    parseTwoDigits(assertRemainingIndex, parseIntIndex, LOCAL_HOUR);
                    break;

                case OP_HOUR_24_GREEDY:
                    // l = Numbers.parseIntSafely(in, pos, hi);
                    // hour = Numbers.decodeInt(l);
                    // pos += Numbers.decodeLen(l);
                    stackState &= ~(1 << LOCAL_HOUR);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseIntSafelyIndex, decodeLenIndex, decodeIntIndex, LOCAL_HOUR);
                    break;
                // HOUR (1 - 24)
                case OP_HOUR_24_ONE_DIGIT_ONE_BASED:
                    // assertRemaining(pos, hi);
                    // hour = Numbers.parseInt(in, pos, ++pos) - 1;
                    stackState &= ~(1 << LOCAL_HOUR);
                    parseDigitsSub1(assertRemainingIndex, parseIntIndex, 1, LOCAL_HOUR);
                    break;

                case OP_HOUR_24_TWO_DIGITS_ONE_BASED:
                    // assertRemaining(pos + 1, hi);
                    // hour = Numbers.parseInt(in, pos, pos += 2) - 1;
                    stackState &= ~(1 << LOCAL_HOUR);
                    parseDigitsSub1(assertRemainingIndex, parseIntIndex, 2, LOCAL_HOUR);
                    break;

                case OP_HOUR_24_GREEDY_ONE_BASED:
                    // l = Numbers.parseIntSafely(in, pos, hi);
                    // hour = Numbers.decodeInt(l) - 1;
                    // pos += Numbers.decodeLen(l);
                    stackState &= ~(1 << LOCAL_HOUR);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);

                    asm.aload(P_INPUT_STR);
                    asm.iload(LOCAL_POS);
                    asm.iload(P_HI);
                    asm.invokeStatic(parseIntSafelyIndex);
                    asm.lstore(LOCAL_TEMP_LONG);
                    decodeInt(decodeIntIndex);
                    asm.iconst(1);
                    asm.isub();
                    asm.istore(LOCAL_HOUR);
                    addTempToPos(decodeLenIndex);
                    break;
                // DAY
                case OP_DAY_ONE_DIGIT:
                    // assertRemaining(pos, hi);
                    //day = Numbers.parseInt(in, pos, ++pos);
                    stackState &= ~(1 << LOCAL_DAY);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_DAY);
                    break;
                case OP_DAY_TWO_DIGITS:
                    // assertRemaining(pos + 1, hi);
                    // day = Numbers.parseInt(in, pos, pos += 2);
                    stackState &= ~(1 << LOCAL_DAY);
                    parseTwoDigits(assertRemainingIndex, parseIntIndex, LOCAL_DAY);
                    break;
                case OP_DAY_GREEDY:
                    // l = Numbers.parseIntSafely(in, pos, hi);
                    // day = Numbers.decodeInt(l);
                    // pos += Numbers.decodeLen(l);
                    stackState &= ~(1 << LOCAL_DAY);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseIntSafelyIndex, decodeLenIndex, decodeIntIndex, LOCAL_DAY);
                    break;
                case OP_DAY_NAME_LONG:
                case OP_DAY_NAME_SHORT:
                    // l = locale.matchWeekday(in, pos, hi);
                    // pos += Numbers.decodeLen(l);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeMatch(matchWeekdayIndex);
                    addTempToPos(decodeLenIndex);
                    break;
                case OP_DAY_OF_WEEK:
                    // assertRemaining(pos, hi);
                    // // ignore weekday
                    // Numbers.parseInt(in, pos, ++pos);
                    asm.iload(LOCAL_POS);
                    asm.iload(P_HI);
                    asm.invokeStatic(assertRemainingIndex);

                    asm.aload(P_INPUT_STR);
                    asm.iload(LOCAL_POS);
                    asm.iinc(LOCAL_POS, 1);
                    asm.iload(LOCAL_POS);
                    asm.invokeStatic(parseIntIndex);
                    asm.pop();
                    break;

                case OP_MONTH_ONE_DIGIT:
                    // assertRemaining(pos, hi);
                    // month = Numbers.parseInt(in, pos, ++pos);
                    stackState &= ~(1 << LOCAL_MONTH);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_MONTH);
                    break;
                case OP_MONTH_TWO_DIGITS:
                    // assertRemaining(pos + 1, hi);
                    // month = Numbers.parseInt(in, pos, pos += 2);
                    stackState &= ~(1 << LOCAL_MONTH);
                    parseTwoDigits(assertRemainingIndex, parseIntIndex, LOCAL_MONTH);
                    break;
                case OP_MONTH_GREEDY:
                    // l = Numbers.parseIntSafely(in, pos, hi);
                    // month = Numbers.decodeInt(l);
                    // pos += Numbers.decodeLen(l);
                    stackState &= ~(1 << LOCAL_MONTH);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseIntSafelyIndex, decodeLenIndex, decodeIntIndex, LOCAL_MONTH);
                    break;

                case OP_MONTH_SHORT_NAME:
                case OP_MONTH_LONG_NAME:
                    // l = locale.matchMonth(in, pos, hi);
                    // month = Numbers.decodeInt(l) + 1;
                    // pos += Numbers.decodeLen(l);
                    stackState &= ~(1 << LOCAL_MONTH);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);

                    invokeMatch(matchMonthIndex);
                    decodeInt(decodeIntIndex);
                    asm.iconst(1);
                    asm.iadd();
                    asm.istore(LOCAL_MONTH);
                    addTempToPos(decodeLenIndex);
                    break;

                case OP_YEAR_ONE_DIGIT:
                    // assertRemaining(pos, hi);
                    // year = Numbers.parseInt(in, pos, ++pos);
                    stackState &= ~(1 << LOCAL_YEAR);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_YEAR);
                    break;
                case OP_YEAR_TWO_DIGITS:
                    // assertRemaining(pos + 1, hi);
                    // year = adjustYear(Numbers.parseInt(in, pos, pos += 2));
                    stackState &= ~(1 << LOCAL_YEAR);

                    asm.iload(LOCAL_POS);
                    asm.iconst(1);
                    asm.iadd();
                    asm.iload(P_HI);
                    asm.invokeStatic(assertRemainingIndex);

                    asm.aload(P_INPUT_STR);
                    asm.iload(LOCAL_POS);
                    asm.iinc(LOCAL_POS, 2);
                    asm.iload(LOCAL_POS);
                    asm.invokeStatic(parseIntIndex);
                    asm.invokeStatic(adjustYearIndex);
                    asm.istore(LOCAL_YEAR);
                    break;
                case OP_YEAR_FOUR_DIGITS:
                    // assertRemaining(pos + 3, hi);
                    // year = Numbers.parseInt(in, pos, pos += 4);
                    stackState &= ~(1 << LOCAL_YEAR);
                    parseDigits(assertRemainingIndex, parseIntIndex, 4, LOCAL_YEAR);
                    break;
                case OP_YEAR_GREEDY:
                    // l = Numbers.parseIntSafely(in, pos, hi);
                    // len = Numbers.decodeLen(l);
                    // if (len == 2) {
                    //     year = adjustYear(Numbers.decodeInt(l));
                    // } else {
                    //     year = Numbers.decodeInt(l);
                    // }
                    // pos += len;

                    stackState &= ~(1 << LOCAL_YEAR);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);

                    asm.aload(P_INPUT_STR);
                    asm.iload(LOCAL_POS);
                    asm.iload(P_HI);
                    asm.invokeStatic(parseYearGreedyIndex);
                    asm.lstore(LOCAL_TEMP_LONG);
                    decodeInt(decodeIntIndex);
                    asm.istore(LOCAL_YEAR);
                    addTempToPos(decodeLenIndex);
                    break;
                case OP_ERA:
                    // l = locale.matchEra(in, pos, hi);
                    // era = Numbers.decodeInt(l);
                    // pos += Numbers.decodeLen(l);
                    stackState &= ~(1 << LOCAL_ERA);

                    invokeMatch(matchEraIndex);
                    decodeInt(decodeIntIndex);
                    asm.istore(LOCAL_ERA);
                    addTempToPos(decodeLenIndex);
                    break;
                case OP_TIME_ZONE_SHORT:
                case OP_TIME_ZONE_GMT_BASED:
                case OP_TIME_ZONE_ISO_8601_1:
                case OP_TIME_ZONE_ISO_8601_2:
                case OP_TIME_ZONE_ISO_8601_3:
                case OP_TIME_ZONE_LONG:
                case OP_TIME_ZONE_RFC_822:

                    // l = Dates.parseOffset(in, pos, hi);
                    // if (l == Long.MIN_VALUE) {
                    //     l = locale.matchZone(in, pos, hi);
                    //     timezone = Numbers.decodeInt(l);
                    //     pos += Numbers.decodeLen(l);
                    // } else {
                    //     offset = Numbers.decodeInt(l) * Dates.MINUTE_MILLIS;
                    //     pos += Numbers.decodeLen(l);
                    // }

                    stackState &= ~(1 << LOCAL_TEMP_LONG);

                    asm.aload(P_INPUT_STR);
                    asm.iload(LOCAL_POS);
                    asm.iload(P_HI);
                    asm.invokeStatic(parseOffsetIndex);
                    asm.lstore(LOCAL_TEMP_LONG);

                    asm.lload(LOCAL_TEMP_LONG);
                    asm.ldc2_w(minLongIndex);
                    asm.lcmp();
                    int branch1 = asm.ifne();

                    //
                    invokeMatch(matchZoneIndex);
                    //
                    decodeInt(decodeIntIndex);
                    asm.istore(LOCAL_TIMEZONE);
                    int branch2 = asm.goto_();

                    int p = asm.position();
                    frameOffsets.add((((long) p) << 32) | stackState);
                    asm.setJmp(branch1, p);

                    decodeInt(decodeIntIndex);
                    asm.i2l();
                    asm.ldc2_w(minMillisIndex);
                    asm.lmul();
                    asm.lstore(LOCAL_OFFSET);
                    p = asm.position();
                    frameOffsets.add((((long) p) << 32) | stackState);
                    asm.setJmp(branch2, p);

                    addTempToPos(decodeLenIndex);

                    break;
                default:
                    if (op > 0) {
                        throw new IllegalArgumentException("Not yet supported: " + op);
                    }
                    String delimiter = delimiters.getQuick(-op - 1);
                    int len = delimiter.length();
                    if (len == 1) {
                        // DateFormatUtils.assertChar(' ', in, pos++, hi);
                        asm.iconst(delimiter.charAt(0));
                        asm.aload(P_INPUT_STR);
                        asm.iload(LOCAL_POS);
                        asm.iinc(LOCAL_POS, 1);
                        asm.iload(P_HI);
                        asm.invokeStatic(assertCharIndex);
                    } else {
                        // pos = DateFormatUtils.assertString(", ", 2, in, pos, hi);
                        asm.ldc(delimIndices.getQuick(-op - 1));
                        asm.iconst(len);
                        asm.aload(P_INPUT_STR);
                        asm.iload(LOCAL_POS);
                        asm.iload(P_HI);
                        asm.invokeStatic(assertStringIndex);
                        asm.istore(LOCAL_POS);
                    }
            }
        }

        // check that there is no tail
        asm.iload(LOCAL_POS);
        asm.iload(P_HI);
        asm.invokeStatic(assertNoTailIndex);

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

        asm.aload(P_LOCALE);
        asm.iload(LOCAL_ERA);
        asm.iload(LOCAL_YEAR);
        asm.iload(LOCAL_MONTH);
        asm.iload(LOCAL_DAY);
        asm.iload(LOCAL_HOUR);
        asm.iload(LOCAL_MINUTE);
        asm.iload(LOCAL_SECOND);
        asm.iload(LOCAL_MILLIS);
        asm.iload(LOCAL_TIMEZONE);
        asm.lload(LOCAL_OFFSET);
        asm.iload(LOCAL_HOUR_TYPE);
        asm.invokeStatic(computeMillisIndex);
        asm.lreturn();
        asm.endMethodCode();

        // exceptions
        asm.putShort(0);
        // attributes
        int n = frameOffsets.size();
        asm.putShort(n > 0 ? 1 : 0);
        if (n > 0) {
            asm.startStackMapTables(stackMapTableIndex, n);

            int prevStackState = 0;
            int start = asm.getCodeStart();

            for (int i = 0; i < n; i++) {
                long l = frameOffsets.getQuick(i);
                int offset = (int) (l >> 32);
                int ss = (int) (l & 0xffffffffL);

                if (i == 0 || prevStackState != ss) {
                    asm.full_frame(offset - start);
                    // 18 local variables
                    int countPos = asm.position();
                    int count = 18;
                    asm.putShort(0);

                    asm.putITEM_Object(thisClassIndex);
                    asm.putITEM_Object(charSequenceClassIndex);
                    asm.putITEM_Integer();
                    asm.putITEM_Integer();
                    asm.putITEM_Object(dateLocaleClassIndex);


                    if ((ss & (1 << LOCAL_DAY)) == 0) {
                        asm.putITEM_Integer();
                    } else {
                        asm.putITEM_Top();
                    }

                    if ((ss & (1 << LOCAL_MONTH)) == 0) {
                        asm.putITEM_Integer();
                    } else {
                        asm.putITEM_Top();
                    }

                    if ((ss & (1 << LOCAL_YEAR)) == 0) {
                        asm.putITEM_Integer();
                    } else {
                        asm.putITEM_Top();
                    }

                    if ((ss & (1 << LOCAL_HOUR)) == 0) {
                        asm.putITEM_Integer();
                    } else {
                        asm.putITEM_Top();
                    }

                    if ((ss & (1 << LOCAL_MINUTE)) == 0) {
                        asm.putITEM_Integer();
                    } else {
                        asm.putITEM_Top();
                    }

                    if ((ss & (1 << LOCAL_SECOND)) == 0) {
                        asm.putITEM_Integer();
                    } else {
                        asm.putITEM_Top();
                    }

                    if ((ss & (1 << LOCAL_MILLIS)) == 0) {
                        asm.putITEM_Integer();
                    } else {
                        asm.putITEM_Top();
                    }

                    // LOCAL_POS is always set
                    asm.putITEM_Integer();


                    if ((ss & (1 << LOCAL_TEMP_LONG)) == 0) {
                        asm.putITEM_Long();
                    } else {
                        asm.putITEM_Top();
                        asm.putITEM_Top();
                        count++;
                    }

                    // LOCAL_TIMEZONE is always set
                    asm.putITEM_Integer();

                    // LOCAL_OFFSET
                    asm.putITEM_Long();

                    // LOCAL_HOUR_TYPE
                    asm.putITEM_Integer();

                    if ((ss & (1 << LOCAL_ERA)) == 0) {
                        asm.putITEM_Integer();
                    } else {
                        asm.putITEM_Top();
                    }

                    asm.putShort(countPos, count);

                    // 0 stack
                    asm.putShort(0);
                    prevStackState = ss;
                } else {
                    asm.same_frame(offset - start);
                }
                start = offset + 1;
            }
            asm.endStackMapTables();
        }

        asm.endMethod();
    }

    /*
     * For each operation code in given operation list determine which stack slots can be left uninitialized. Set bits
     * in the result integer will represent these stack slots. Bits positions are values of LOCAL_ constants.
     */
    private int computeParseMethodStack(IntList ops) {
        int result = 0;
        for (int i = 0, n = ops.size(); i < n; i++) {
            switch (ops.getQuick(i)) {
                case OP_AM_PM:
                    result |= (1 << LOCAL_TEMP_LONG);
                    break;
                case OP_MILLIS_GREEDY:
                    result |= (1 << LOCAL_TEMP_LONG);
                    // fall through
                case OP_MILLIS_ONE_DIGIT:
                case OP_MILLIS_THREE_DIGITS:
                    result |= (1 << LOCAL_MILLIS);
                    break;
                case OP_SECOND_GREEDY:
                    result |= (1 << LOCAL_TEMP_LONG);
                    // fall through
                case OP_SECOND_ONE_DIGIT:
                case OP_SECOND_TWO_DIGITS:
                    result |= (1 << LOCAL_SECOND);
                    break;
                case OP_MINUTE_GREEDY:
                    result |= (1 << LOCAL_TEMP_LONG);
                case OP_MINUTE_ONE_DIGIT:
                case OP_MINUTE_TWO_DIGITS:
                    result |= (1 << LOCAL_MINUTE);
                    break;
                // HOUR (0-11)
                case OP_HOUR_12_GREEDY:
                case OP_HOUR_12_GREEDY_ONE_BASED:
                case OP_HOUR_24_GREEDY:
                case OP_HOUR_24_GREEDY_ONE_BASED:
                    result |= (1 << LOCAL_TEMP_LONG);
                case OP_HOUR_12_ONE_DIGIT:
                case OP_HOUR_12_TWO_DIGITS:
                case OP_HOUR_12_ONE_DIGIT_ONE_BASED:
                case OP_HOUR_12_TWO_DIGITS_ONE_BASED:
                case OP_HOUR_24_ONE_DIGIT:
                case OP_HOUR_24_TWO_DIGITS:
                case OP_HOUR_24_ONE_DIGIT_ONE_BASED:
                case OP_HOUR_24_TWO_DIGITS_ONE_BASED:
                    result |= (1 << LOCAL_HOUR);
                    break;
                // DAY
                case OP_DAY_GREEDY:
                    result |= (1 << LOCAL_TEMP_LONG);
                case OP_DAY_ONE_DIGIT:
                case OP_DAY_TWO_DIGITS:
                    result |= (1 << LOCAL_DAY);
                    break;
                case OP_DAY_NAME_LONG:
                case OP_DAY_NAME_SHORT:
                    result |= (1 << LOCAL_TEMP_LONG);
                    break;
                case OP_MONTH_GREEDY:
                case OP_MONTH_SHORT_NAME:
                case OP_MONTH_LONG_NAME:
                    result |= (1 << LOCAL_TEMP_LONG);
                case OP_MONTH_ONE_DIGIT:
                case OP_MONTH_TWO_DIGITS:
                    result |= (1 << LOCAL_MONTH);
                    break;
                case OP_YEAR_GREEDY:
                    result |= (1 << LOCAL_TEMP_LONG);
                case OP_YEAR_ONE_DIGIT:
                case OP_YEAR_TWO_DIGITS:
                case OP_YEAR_FOUR_DIGITS:
                    result |= (1 << LOCAL_YEAR);
                    break;
                case OP_ERA:
                    result |= (1 << LOCAL_ERA);
                    break;
                case OP_TIME_ZONE_SHORT:
                case OP_TIME_ZONE_GMT_BASED:
                case OP_TIME_ZONE_ISO_8601_1:
                case OP_TIME_ZONE_ISO_8601_2:
                case OP_TIME_ZONE_ISO_8601_3:
                case OP_TIME_ZONE_LONG:
                case OP_TIME_ZONE_RFC_822:
                    result |= (1 << LOCAL_TEMP_LONG);
                    break;
                default:
                    break;

            }
        }
        return result;
    }

    private void decodeInt(int decodeIntIndex) {
        asm.lload(LOCAL_TEMP_LONG);
        asm.invokeStatic(decodeIntIndex);
    }

    private void invokeMatch(int matchIndex) {
        asm.aload(P_LOCALE);
        asm.aload(P_INPUT_STR);
        asm.iload(LOCAL_POS);
        asm.iload(P_HI);
        asm.invokeVirtual(matchIndex);
        asm.lstore(LOCAL_TEMP_LONG);
    }

    private void invokeParseIntSafelyAndStore(int parseIntSafelyIndex, int decodeLenIndex, int decodeIntIndex, int target) {
        asm.aload(P_INPUT_STR);
        asm.iload(LOCAL_POS);
        asm.iload(P_HI);
        asm.invokeStatic(parseIntSafelyIndex);
        asm.lstore(LOCAL_TEMP_LONG);
        decodeInt(decodeIntIndex);
        asm.istore(target);
        addTempToPos(decodeLenIndex);
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

    private void parseDigits(int assertRemainingIndex, int parseIntIndex, int digitCount, int target) {
        asm.iload(LOCAL_POS);
        if (digitCount > 1) {
            asm.iconst(digitCount - 1);
            asm.iadd();
        }
        asm.iload(P_HI);
        asm.invokeStatic(assertRemainingIndex);

        asm.aload(P_INPUT_STR);
        asm.iload(LOCAL_POS);
        asm.iinc(LOCAL_POS, digitCount);
        asm.iload(LOCAL_POS);
        asm.invokeStatic(parseIntIndex);
        asm.istore(target);
    }

    private void parseDigitsSub1(int assertRemainingIndex, int parseIntIndex, int digitCount, int target) {
        asm.iload(LOCAL_POS);
        if (digitCount > 1) {
            asm.iconst(digitCount - 1);
            asm.iadd();
        }
        asm.iload(P_HI);
        asm.invokeStatic(assertRemainingIndex);

        asm.aload(P_INPUT_STR);
        asm.iload(LOCAL_POS);
        asm.iinc(LOCAL_POS, digitCount);
        asm.iload(LOCAL_POS);
        asm.invokeStatic(parseIntIndex);
        asm.iconst(1);
        asm.isub();
        asm.istore(target);
    }

    private void parseTwoDigits(int assertRemainingIndex, int parseIntIndex, int target) {
        parseDigits(assertRemainingIndex, parseIntIndex, 2, target);
    }

    private void setHourType(LongList frameOffsets, int hourType, int stackState) {
        asm.iload(LOCAL_HOUR_TYPE);
        asm.iconst(HOUR_24);
        int branch = asm.if_icmpne();
        asm.iconst(hourType);
        asm.istore(LOCAL_HOUR_TYPE);
        int p = asm.position();
        frameOffsets.add((((long) p) << 32) | stackState);
        asm.setJmp(branch, p);
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
