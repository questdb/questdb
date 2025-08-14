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

package io.questdb.std.datetime.microtime;


import io.questdb.cairo.ColumnType;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.AbstractDateFormat;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

public class MicrosFormatCompiler {
    static final int OP_AM_PM = 14;
    static final int OP_DAY_GREEDY = 139;
    static final int OP_DAY_NAME_LONG = 12;
    static final int OP_DAY_NAME_SHORT = 11;
    static final int OP_DAY_OF_WEEK = 13;
    static final int OP_DAY_OF_YEAR = 36;
    static final int OP_DAY_ONE_DIGIT = 9;
    static final int OP_DAY_TWO_DIGITS = 10;
    static final int OP_ERA = 1;
    static final int OP_HOUR_12_GREEDY = 142;
    static final int OP_HOUR_12_GREEDY_ONE_BASED = 143;
    static final int OP_HOUR_12_ONE_DIGIT = 17;
    static final int OP_HOUR_12_ONE_DIGIT_ONE_BASED = 18;
    static final int OP_HOUR_12_TWO_DIGITS = 34;
    static final int OP_HOUR_12_TWO_DIGITS_ONE_BASED = 35;
    static final int OP_HOUR_24_GREEDY = 140;
    static final int OP_HOUR_24_GREEDY_ONE_BASED = 141;
    static final int OP_HOUR_24_ONE_DIGIT = 15;
    static final int OP_HOUR_24_ONE_DIGIT_ONE_BASED = 16;
    static final int OP_HOUR_24_TWO_DIGITS = 32;
    static final int OP_HOUR_24_TWO_DIGITS_ONE_BASED = 33;
    static final int OP_ISO_WEEK_OF_YEAR = 38;
    static final int OP_MICROS_GREEDY3 = 149;
    static final int OP_MICROS_GREEDY6 = 51;
    static final int OP_MICROS_ONE_DIGIT = 39;
    static final int OP_MICROS_THREE_DIGITS = 49;
    static final int OP_MILLIS_GREEDY = 146;
    static final int OP_MILLIS_ONE_DIGIT = 21;
    static final int OP_MILLIS_THREE_DIGITS = 31;
    static final int OP_MINUTE_GREEDY = 144;
    static final int OP_MINUTE_ONE_DIGIT = 19;
    static final int OP_MINUTE_TWO_DIGITS = 29;
    static final int OP_MONTH_GREEDY = 135;
    static final int OP_MONTH_LONG_NAME = 8;
    static final int OP_MONTH_ONE_DIGIT = 5;
    static final int OP_MONTH_SHORT_NAME = 7;
    static final int OP_MONTH_TWO_DIGITS = 6;
    static final int OP_NANOS_GREEDY = 147;
    static final int OP_NANOS_GREEDY9 = 52;
    static final int OP_NANOS_ONE_DIGIT = 40;
    static final int OP_NANOS_THREE_DIGITS = 50;
    static final int OP_SECOND_GREEDY = 145;
    static final int OP_SECOND_ONE_DIGIT = 20;
    static final int OP_SECOND_TWO_DIGITS = 30;
    static final int OP_TIME_ZONE_GMT_BASED = 22;
    static final int OP_TIME_ZONE_ISO_8601_1 = 26;
    static final int OP_TIME_ZONE_ISO_8601_2 = 27;
    static final int OP_TIME_ZONE_ISO_8601_3 = 28;
    static final int OP_TIME_ZONE_LONG = 24;
    static final int OP_TIME_ZONE_RFC_822 = 25;
    static final int OP_TIME_ZONE_SHORT = 23;
    static final int OP_WEEK_OF_YEAR = 37;
    static final int OP_YEAR_FOUR_DIGITS = 4;
    static final int OP_YEAR_GREEDY = 132;
    static final int OP_YEAR_ISO_FOUR_DIGITS = 41;
    static final int OP_YEAR_ONE_DIGIT = 2;
    static final int OP_YEAR_THREE_DIGITS = 148;
    static final int OP_YEAR_TWO_DIGITS = 3;
    static final ObjList<String> opList;
    static final CharSequenceIntHashMap opMap;
    private static final int FA_DAY = 5;
    private static final int FA_DAY_OF_WEEK = 10;
    private static final int FA_DAY_OF_YEAR = 14;
    private static final int FA_HOUR = 4;
    private static final int FA_ISO_WEEK_OF_YEAR = 13;
    private static final int FA_ISO_YEAR = 9;
    private static final int FA_LEAP = 8;
    private static final int FA_LOCAL_DATETIME = 1;
    private static final int FA_LOCAL_LOCALE = 3;
    private static final int FA_LOCAL_SINK = 5;
    private static final int FA_LOCAL_TIMEZONE = 4;
    private static final int FA_MILLIS_MICROS = 11;
    private static final int FA_MINUTE = 3;
    private static final int FA_MONTH = 6;
    private static final int FA_SECOND = 2;
    private static final int FA_SECOND_MICROS = 15;
    private static final int FA_SECOND_MILLIS = 1;
    private static final int FA_WEEK_OF_YEAR = 12;
    private static final int FA_YEAR = 7;
    private static final int FORMAT_METHOD_STACK_START = 6;
    private static final int LOCAL_DAY = 5;
    private static final int LOCAL_ERA = 19;
    private static final int LOCAL_HOUR = 8;
    private static final int LOCAL_HOUR_TYPE = 18;
    private static final int LOCAL_MICROS = 20;
    private static final int LOCAL_MILLIS = 11;
    private static final int LOCAL_MINUTE = 9;
    private static final int LOCAL_MONTH = 6;
    private static final int LOCAL_OFFSET = 16;
    private static final int LOCAL_POS = 12;
    private static final int LOCAL_SECOND = 10;
    private static final int LOCAL_TEMP_LONG = 13;
    private static final int LOCAL_TIMEZONE = 15;
    private static final int LOCAL_WEEK = 21;
    private static final int LOCAL_YEAR = 7;
    private static final int P_HI = 3;
    private static final int P_INPUT_STR = 1;
    private static final int P_LO = 2;
    private static final int P_LOCALE = 4;
    private final static ThreadLocal<StringSink> tlSink = new ThreadLocal<>(StringSink::new);
    private final BytecodeAssembler asm = new BytecodeAssembler();
    private final IntList delimiterIndexes = new IntList();
    private final ObjList<String> delimiters = new ObjList<>();
    private final int[] fmtAttributeIndex = new int[32];
    private final LongList frameOffsets = new LongList();
    private final GenericLexer lexer = new GenericLexer(2048);
    private final IntList ops = new IntList();

    public MicrosFormatCompiler() {
        for (int i = 0, n = opList.size(); i < n; i++) {
            lexer.defineSymbol(opList.getQuick(i));
        }
    }

    public static int getOpCode(String opName) {
        return opMap.get(opName);
    }

    public static int getOpCount() {
        return opList.size();
    }

    public static String getOpName(int index) {
        return opList.getQuick(index);
    }

    public DateFormat compile(CharSequence pattern) {
        return compile(pattern, false);
    }

    public DateFormat compile(CharSequence pattern, boolean generic) {
        return compile(pattern, 0, pattern.length(), generic);
    }

    public DateFormat compile(CharSequence pattern, int lo, int hi, boolean generic) {
        this.lexer.of(pattern, lo, hi);

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
            final CharSequence cs = lexer.next();
            final int op = opMap.get(cs);
            switch (op) {
                case -1:
                    makeLastOpGreedy(ops);
                    // Don't use Chars.toString here to avoid simultaneous Misc.getThreadLocalSink() mutations.
                    delimiters.add(toString(cs));
                    ops.add(-(delimiters.size()));
                    break;
                case OP_AM_PM:
                case OP_TIME_ZONE_SHORT:
                    makeLastOpGreedy(ops);
                    // fall thru
                default:
                    ops.add(op);
                    break;
            }
        }

        // make last operation "greedy"
        makeLastOpGreedy(ops);
        return generic ? new GenericMicrosFormat(ops, delimiters) : compile(ops, delimiters);
    }

    private static void addOp(String op, int opDayTwoDigits) {
        opMap.put(op, opDayTwoDigits);
        opList.add(op);
    }

    private static String toString(CharSequence cs) {
        final StringSink sink = tlSink.get();
        sink.clear();
        sink.put(cs);
        return sink.toString();
    }

    private void addTempToPos(int decodeLenIndex) {
        asm.iload(LOCAL_POS);
        decodeInt(decodeLenIndex);
        asm.iadd();
        asm.istore(LOCAL_POS);
    }

    private void assembleFormatMethod(
            IntList ops,
            ObjList<String> delimiters,
            int getWeekdayIndex,
            int getShortWeekdayIndex,
            int getMonthIndex,
            int getShortMonthIndex,
            int appendEraIndex,
            int appendAmPmIndex,
            int appendHour12Index,
            int appendHour12PaddedIndex,
            int appendHour121Index,
            int appendHour121PaddedIndex,
            int appendHour241Index,
            int appendHour241PaddedIndex,
            int getYearIndex,
            int getIsoYearIndex,
            int isLeapYearIndex,
            int getMonthOfYearIndex,
            int getDayOfMonthIndex,
            int getHourOfDayIndex,
            int getMinuteOfHourIndex,
            int getSecondOfMinuteIndex,
            int getMillisOfSecondIndex,
            int getMicrosOfMilliIndex,
            int getMicrosOfSecondIndex,
            int getDayOfWeekIndex,
            int append00000Index,
            int append00Index,
            int append0Index,
            int appendYear000Index,
            int appendYear00Index,
            int appendYear0Index,
            int appendYearIndex,
            int sinkPutIntIndex,
            int sinkPutStrIndex,
            int sinkPutChrIndex,
            int formatNameIndex,
            int formatSigIndex,
            int getDayOfYearIndex,
            int getWeekOfYearIndex,
            int getWeekIndex
    ) {
        int formatAttributes = computeFormatAttributes(ops);
        asm.startMethod(formatNameIndex, formatSigIndex, 6, FORMAT_METHOD_STACK_START + Integer.bitCount(formatAttributes));

        assembleFormatMethodStack(
                formatAttributes,
                getYearIndex,
                getIsoYearIndex,
                isLeapYearIndex,
                getMonthOfYearIndex,
                getDayOfMonthIndex,
                getHourOfDayIndex,
                getMinuteOfHourIndex,
                getSecondOfMinuteIndex,
                getMillisOfSecondIndex,
                getMicrosOfMilliIndex,
                getMicrosOfSecondIndex,
                getDayOfWeekIndex,
                getDayOfYearIndex,
                getWeekOfYearIndex,
                getWeekIndex
        );

        for (int i = 0, n = ops.size(); i < n; i++) {
            int op = ops.getQuick(i);
            switch (op) {
                // AM/PM
                case MicrosFormatCompiler.OP_AM_PM:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_HOUR]);
                    asm.aload(FA_LOCAL_LOCALE);
                    asm.invokeStatic(appendAmPmIndex);
                    break;
                // MICROS3
                case MicrosFormatCompiler.OP_MICROS_ONE_DIGIT:
                case MicrosFormatCompiler.OP_MICROS_GREEDY3:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_MILLIS_MICROS]);
                    asm.invokeInterface(sinkPutIntIndex, 1);
                    asm.pop();
                    break;
                // MICROS6 & NANOS9 are formatted the same
                case MicrosFormatCompiler.OP_MICROS_GREEDY6:
                case MicrosFormatCompiler.OP_NANOS_GREEDY9:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_SECOND_MICROS]);
                    asm.invokeStatic(append00000Index);
                    break;
                case MicrosFormatCompiler.OP_MICROS_THREE_DIGITS:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_MILLIS_MICROS]);
                    asm.invokeStatic(append00Index);
                    break;
                // NANOS
                case MicrosFormatCompiler.OP_NANOS_ONE_DIGIT:
                case MicrosFormatCompiler.OP_NANOS_GREEDY:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iconst(0);
                    asm.invokeInterface(sinkPutIntIndex, 1);
                    asm.pop();
                    break;
                case MicrosFormatCompiler.OP_NANOS_THREE_DIGITS:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iconst(0);
                    asm.invokeStatic(append00Index);
                    break;
                // MILLIS
                case MicrosFormatCompiler.OP_MILLIS_ONE_DIGIT:
                case MicrosFormatCompiler.OP_MILLIS_GREEDY:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_SECOND_MILLIS]);
                    asm.invokeInterface(sinkPutIntIndex, 1);
                    asm.pop();
                    break;
                case MicrosFormatCompiler.OP_MILLIS_THREE_DIGITS:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_SECOND_MILLIS]);
                    asm.invokeStatic(append00Index);
                    break;
                // SECOND
                case MicrosFormatCompiler.OP_SECOND_ONE_DIGIT:
                case MicrosFormatCompiler.OP_SECOND_GREEDY:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_SECOND]);
                    asm.invokeInterface(sinkPutIntIndex, 1);
                    asm.pop();
                    break;
                case MicrosFormatCompiler.OP_SECOND_TWO_DIGITS:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_SECOND]);
                    asm.invokeStatic(append0Index);
                    break;
                // MINUTE
                case MicrosFormatCompiler.OP_MINUTE_ONE_DIGIT:
                case MicrosFormatCompiler.OP_MINUTE_GREEDY:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_MINUTE]);
                    asm.invokeInterface(sinkPutIntIndex, 1);
                    asm.pop();
                    break;
                case MicrosFormatCompiler.OP_MINUTE_TWO_DIGITS:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_MINUTE]);
                    asm.invokeStatic(append0Index);
                    break;
                // HOUR (0-11)
                case MicrosFormatCompiler.OP_HOUR_12_ONE_DIGIT:
                case MicrosFormatCompiler.OP_HOUR_12_GREEDY:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_HOUR]);
                    asm.invokeStatic(appendHour12Index);
                    break;

                case MicrosFormatCompiler.OP_HOUR_12_TWO_DIGITS:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_HOUR]);
                    asm.invokeStatic(appendHour12PaddedIndex);
                    break;

                // HOUR (1-12)
                case MicrosFormatCompiler.OP_HOUR_12_ONE_DIGIT_ONE_BASED:
                case MicrosFormatCompiler.OP_HOUR_12_GREEDY_ONE_BASED:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_HOUR]);
                    asm.invokeStatic(appendHour121Index);
                    break;

                case MicrosFormatCompiler.OP_HOUR_12_TWO_DIGITS_ONE_BASED:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_HOUR]);
                    asm.invokeStatic(appendHour121PaddedIndex);
                    break;
                // HOUR (0-23)
                case MicrosFormatCompiler.OP_HOUR_24_ONE_DIGIT:
                case MicrosFormatCompiler.OP_HOUR_24_GREEDY:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_HOUR]);
                    asm.invokeInterface(sinkPutIntIndex, 1);
                    asm.pop();
                    break;
                case MicrosFormatCompiler.OP_HOUR_24_TWO_DIGITS:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_HOUR]);
                    asm.invokeStatic(append0Index);
                    break;

                // HOUR (1 - 24)
                case MicrosFormatCompiler.OP_HOUR_24_ONE_DIGIT_ONE_BASED:
                case MicrosFormatCompiler.OP_HOUR_24_GREEDY_ONE_BASED:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_HOUR]);
                    asm.invokeStatic(appendHour241Index);
                    break;

                case MicrosFormatCompiler.OP_HOUR_24_TWO_DIGITS_ONE_BASED:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_HOUR]);
                    asm.invokeStatic(appendHour241PaddedIndex);
                    break;
                // DAY
                case MicrosFormatCompiler.OP_DAY_ONE_DIGIT:
                case MicrosFormatCompiler.OP_DAY_GREEDY:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_DAY]);
                    asm.invokeInterface(sinkPutIntIndex, 1);
                    asm.pop();
                    break;
                case MicrosFormatCompiler.OP_DAY_TWO_DIGITS:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_DAY]);
                    asm.invokeStatic(append0Index);
                    break;
                case MicrosFormatCompiler.OP_DAY_NAME_LONG:
                    asm.aload(FA_LOCAL_SINK);
                    asm.aload(FA_LOCAL_LOCALE);
                    asm.iload(fmtAttributeIndex[FA_DAY_OF_WEEK]);
                    asm.invokeVirtual(getWeekdayIndex);
                    asm.invokeInterface(sinkPutStrIndex, 1);
                    asm.pop();
                    break;
                case MicrosFormatCompiler.OP_DAY_NAME_SHORT:
                    asm.aload(FA_LOCAL_SINK);
                    asm.aload(FA_LOCAL_LOCALE);
                    asm.iload(fmtAttributeIndex[FA_DAY_OF_WEEK]);
                    asm.invokeVirtual(getShortWeekdayIndex);
                    asm.invokeInterface(sinkPutStrIndex, 1);
                    asm.pop();
                    break;
                case MicrosFormatCompiler.OP_DAY_OF_WEEK:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_DAY_OF_WEEK]);
                    asm.invokeInterface(sinkPutIntIndex, 1);
                    asm.pop();
                    break;
                case MicrosFormatCompiler.OP_DAY_OF_YEAR:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_DAY_OF_YEAR]);
                    asm.invokeInterface(sinkPutIntIndex, 1);
                    asm.pop();
                    break;
                case MicrosFormatCompiler.OP_ISO_WEEK_OF_YEAR:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_ISO_WEEK_OF_YEAR]);
                    asm.invokeStatic(append0Index);
                    break;
                case MicrosFormatCompiler.OP_WEEK_OF_YEAR:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_WEEK_OF_YEAR]);
                    asm.invokeInterface(sinkPutIntIndex, 1);
                    asm.pop();
                    break;
                // MONTH
                case MicrosFormatCompiler.OP_MONTH_ONE_DIGIT:
                case MicrosFormatCompiler.OP_MONTH_GREEDY:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_MONTH]);
                    asm.invokeInterface(sinkPutIntIndex, 1);
                    asm.pop();
                    break;
                case MicrosFormatCompiler.OP_MONTH_TWO_DIGITS:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_MONTH]);
                    asm.invokeStatic(append0Index);
                    break;
                case MicrosFormatCompiler.OP_MONTH_SHORT_NAME:
                    asm.aload(FA_LOCAL_SINK);
                    asm.aload(FA_LOCAL_LOCALE);
                    asm.iload(fmtAttributeIndex[FA_MONTH]);
                    asm.iconst(1);
                    asm.isub();
                    asm.invokeVirtual(getShortMonthIndex);
                    asm.invokeInterface(sinkPutStrIndex, 1);
                    asm.pop();
                    break;
                case MicrosFormatCompiler.OP_MONTH_LONG_NAME:
                    asm.aload(FA_LOCAL_SINK);
                    asm.aload(FA_LOCAL_LOCALE);
                    asm.iload(fmtAttributeIndex[FA_MONTH]);
                    asm.iconst(1);
                    asm.isub();
                    asm.invokeVirtual(getMonthIndex);
                    asm.invokeInterface(sinkPutStrIndex, 1);
                    asm.pop();
                    break;
                // YEAR
                case MicrosFormatCompiler.OP_YEAR_ONE_DIGIT:
                case MicrosFormatCompiler.OP_YEAR_GREEDY:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_YEAR]);
                    asm.invokeStatic(appendYearIndex);
                    break;
                case MicrosFormatCompiler.OP_YEAR_TWO_DIGITS:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_YEAR]);
                    asm.iconst(100);
                    asm.irem();
                    asm.invokeStatic(appendYear0Index);
                    break;
                case MicrosFormatCompiler.OP_YEAR_THREE_DIGITS:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_YEAR]);
                    asm.iconst(1000);
                    asm.irem();
                    asm.invokeStatic(appendYear00Index);
                    break;
                case MicrosFormatCompiler.OP_YEAR_FOUR_DIGITS:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_YEAR]);
                    asm.invokeStatic(appendYear000Index);
                    break;
                case MicrosFormatCompiler.OP_YEAR_ISO_FOUR_DIGITS:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_ISO_YEAR]);
                    asm.invokeStatic(appendYear000Index);
                    break;
                // ERA
                case MicrosFormatCompiler.OP_ERA:
                    asm.aload(FA_LOCAL_SINK);
                    asm.iload(fmtAttributeIndex[FA_YEAR]);
                    asm.aload(FA_LOCAL_LOCALE);
                    asm.invokeStatic(appendEraIndex);
                    break;

                // TIMEZONE
                case MicrosFormatCompiler.OP_TIME_ZONE_SHORT:
                case MicrosFormatCompiler.OP_TIME_ZONE_GMT_BASED:
                case MicrosFormatCompiler.OP_TIME_ZONE_ISO_8601_1:
                case MicrosFormatCompiler.OP_TIME_ZONE_ISO_8601_2:
                case MicrosFormatCompiler.OP_TIME_ZONE_ISO_8601_3:
                case MicrosFormatCompiler.OP_TIME_ZONE_LONG:
                case MicrosFormatCompiler.OP_TIME_ZONE_RFC_822:
                    asm.aload(FA_LOCAL_SINK);
                    asm.aload(FA_LOCAL_TIMEZONE);
                    asm.invokeInterface(sinkPutStrIndex, 1);
                    break;
                // SEPARATORS
                default:
                    if (op < 0) {
                        String delimiter = delimiters.getQuick(-op - 1);
                        if (delimiter.length() > 1) {
                            asm.aload(FA_LOCAL_SINK);
                            asm.ldc(delimiterIndexes.getQuick(-op - 1));
                            asm.invokeInterface(sinkPutStrIndex, 1);
                        } else {
                            asm.aload(FA_LOCAL_SINK);
                            asm.iconst(delimiter.charAt(0));
                            asm.invokeInterface(sinkPutChrIndex, 1);
                        }
                        asm.pop();
                    }
                    break;
            }
        }

        asm.return_();
        asm.endMethodCode();
        asm.putShort(0);
        asm.putShort(0);
        asm.endMethod();
    }

    private void assembleFormatMethodStack(
            int formatAttributes,
            int getYearIndex,
            int getIsoYearIndex,
            int isLeapYearIndex,
            int getMonthOfYearIndex,
            int getDayOfMonthIndex,
            int getHourOfDayIndex,
            int getMinuteOfHourIndex,
            int getSecondOfMinuteIndex,
            int getMillisOfSecondIndex,
            int getMicrosOfMilliIndex,
            int getMicrosOfSecondIndex,
            int getDayOfWeekIndex,
            int getDayOfYearIndex,
            int getWeekOfYearIndex,
            int getWeekIndex) {
        int index = FORMAT_METHOD_STACK_START;
        if (invokeConvertMillis(formatAttributes, FA_YEAR, getYearIndex, index)) {
            fmtAttributeIndex[FA_YEAR] = index++;
        }

        if (invokeConvertMillis(formatAttributes, FA_ISO_YEAR, getIsoYearIndex, index)) {
            fmtAttributeIndex[FA_ISO_YEAR] = index++;
        }

        if ((formatAttributes & (1 << FA_LEAP)) != 0) {
            asm.iload(fmtAttributeIndex[FA_YEAR]);
            asm.invokeStatic(isLeapYearIndex);
            asm.istore(index);
            fmtAttributeIndex[FA_LEAP] = index++;
        }

        if ((formatAttributes & (1 << FA_MONTH)) != 0) {
            asm.lload(FA_LOCAL_DATETIME);
            asm.iload(fmtAttributeIndex[FA_YEAR]);
            asm.iload(fmtAttributeIndex[FA_LEAP]);
            asm.invokeStatic(getMonthOfYearIndex);
            asm.istore(index);
            fmtAttributeIndex[FA_MONTH] = index++;
        }

        if ((formatAttributes & (1 << FA_DAY)) != 0) {
            asm.lload(FA_LOCAL_DATETIME);
            asm.iload(fmtAttributeIndex[FA_YEAR]);
            asm.iload(fmtAttributeIndex[FA_MONTH]);
            asm.iload(fmtAttributeIndex[FA_LEAP]);
            asm.invokeStatic(getDayOfMonthIndex);
            asm.istore(index);
            fmtAttributeIndex[FA_DAY] = index++;
        }

        if (invokeConvertMillis(formatAttributes, FA_HOUR, getHourOfDayIndex, index)) {
            fmtAttributeIndex[FA_HOUR] = index++;
        }

        if (invokeConvertMillis(formatAttributes, FA_MINUTE, getMinuteOfHourIndex, index)) {
            fmtAttributeIndex[FA_MINUTE] = index++;
        }

        if (invokeConvertMillis(formatAttributes, FA_SECOND, getSecondOfMinuteIndex, index)) {
            fmtAttributeIndex[FA_SECOND] = index++;
        }

        if (invokeConvertMillis(formatAttributes, FA_SECOND_MILLIS, getMillisOfSecondIndex, index)) {
            fmtAttributeIndex[FA_SECOND_MILLIS] = index++;
        }

        if (invokeConvertMillis(formatAttributes, FA_MILLIS_MICROS, getMicrosOfMilliIndex, index)) {
            fmtAttributeIndex[FA_MILLIS_MICROS] = index++;
        }

        if (invokeConvertMillis(formatAttributes, FA_SECOND_MICROS, getMicrosOfSecondIndex, index)) {
            fmtAttributeIndex[FA_SECOND_MICROS] = index++;
        }

        if (invokeConvertMillis(formatAttributes, FA_DAY_OF_WEEK, getDayOfWeekIndex, index)) {
            fmtAttributeIndex[FA_DAY_OF_WEEK] = index;
        }

        if (invokeConvertMillis(formatAttributes, FA_DAY_OF_YEAR, getDayOfYearIndex, index)) {
            fmtAttributeIndex[FA_DAY_OF_YEAR] = index;
        }

        if (invokeConvertMillis(formatAttributes, FA_ISO_WEEK_OF_YEAR, getWeekIndex, index)) {
            fmtAttributeIndex[FA_ISO_WEEK_OF_YEAR] = index;
        }

        if (invokeConvertMillis(formatAttributes, FA_WEEK_OF_YEAR, getWeekOfYearIndex, index)) {
            fmtAttributeIndex[FA_WEEK_OF_YEAR] = index;
        }

    }

    private void assembleGetColumnTypeMethod(int getColumnTypeNameIndex, int getColumnTypeSigIndex, int columnTypeIndex) {
        asm.startMethod(getColumnTypeNameIndex, getColumnTypeSigIndex, 1, 1);
        asm.ldc(columnTypeIndex);
        asm.ireturn();
        asm.endMethodCode();
        asm.putShort(0);
        asm.putShort(0);
        asm.endMethod();
    }

    private void assembleParseMethod(
            IntList ops,
            ObjList<String> delimiters,
            int thisClassIndex,
            int stackMapTableIndex,
            int dateLocaleClassIndex,
            int charSequenceClassIndex,
            int minLongIndex,
            int matchWeekdayIndex,
            int matchMonthIndex,
            int matchZoneIndex,
            int matchAMPMIndex,
            int matchEraIndex,
            int parseIntSafelyIndex,
            int parseInt000GreedyIndex,
            int parseLong000000GreedyIndex,
            int parseNanosAsMicrosGreedyIndex,
            int decodeLenIndex,
            int decodeIntIndex,
            int assertRemainingIndex,
            int assertNoTailIndex,
            int parseIntIndex,
            int assertStringIndex,
            int assertCharIndex,
            int computeIndex,
            int adjustYearIndex,
            int parseYearGreedyIndex,
            int parseOffsetIndex,
            int parseNameIndex,
            int parseSigIndex,
            IntList delimIndices,
            int charAtIndex
    ) {

        int stackState = computeParseMethodStack(ops);

        // public long parse(CharSequence in, int lo, int hi, DateLocale locale) throws NumericException
        asm.startMethod(parseNameIndex, parseSigIndex, 15, 22);

        // define stack variables
        // when pattern is not used a default value must be assigned

        if ((stackState & (1 << LOCAL_DAY)) == 0) {
            // int day = 1
            asm.iconst(1);
            asm.istore(LOCAL_DAY);
        }

        if ((stackState & (1 << LOCAL_WEEK)) == 0) {
            // int week = -1
            asm.iconst(-1);
            asm.istore(LOCAL_WEEK);
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

        asm.iconst(CommonUtils.HOUR_24);
        asm.istore(LOCAL_HOUR_TYPE);

        if ((stackState & (1 << LOCAL_ERA)) == 0) {
            asm.iconst(1);
            asm.istore(LOCAL_ERA);
        }

        if ((stackState & (1 << LOCAL_TEMP_LONG)) == 0) {
            asm.lconst_0();
            asm.lstore(LOCAL_TEMP_LONG);
        }

        if ((stackState & (1 << LOCAL_MICROS)) == 0) {
            asm.iconst(0);
            asm.istore(LOCAL_MICROS);
        }

        frameOffsets.clear();
        for (int i = 0, n = ops.size(); i < n; i++) {
            int op = ops.getQuick(i);
            switch (op) {
                // AM/PM
                case OP_AM_PM:
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeMatch(matchAMPMIndex);
                    decodeInt(decodeIntIndex);
                    asm.istore(LOCAL_HOUR_TYPE);
                    addTempToPos(decodeLenIndex);
                    break;
                case OP_MICROS_ONE_DIGIT:
                    stackState &= ~(1 << LOCAL_MICROS);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_MICROS);
                    break;
                case OP_MICROS_THREE_DIGITS:
                    stackState &= ~(1 << LOCAL_MICROS);
                    parseDigits(assertRemainingIndex, parseIntIndex, 3, LOCAL_MICROS);
                    break;
                case OP_MICROS_GREEDY3:
                    stackState &= ~(1 << LOCAL_MICROS);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseInt000GreedyIndex, decodeLenIndex, decodeIntIndex, LOCAL_MICROS);
                    break;
                case OP_MICROS_GREEDY6:
                    stackState &= ~(1 << LOCAL_MICROS);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseLong000000GreedyIndex, decodeLenIndex, decodeIntIndex, LOCAL_MICROS);
                    break;
                case OP_NANOS_GREEDY9:
                    stackState &= ~(1 << LOCAL_MICROS);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseNanosAsMicrosGreedyIndex, decodeLenIndex, decodeIntIndex, LOCAL_MICROS);
                    break;
                case OP_NANOS_ONE_DIGIT:
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, -1);
                    break;
                case OP_NANOS_THREE_DIGITS:
                    parseDigits(assertRemainingIndex, parseIntIndex, 3, -1);
                    break;
                case OP_NANOS_GREEDY:
                    invokeParseIntSafelyAndStore(parseIntSafelyIndex, decodeLenIndex, decodeIntIndex, -1);
                    break;
                case OP_MILLIS_ONE_DIGIT:
                    stackState &= ~(1 << LOCAL_MILLIS);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_MILLIS);
                    break;
                case OP_MILLIS_THREE_DIGITS:
                    stackState &= ~(1 << LOCAL_MILLIS);
                    parseDigits(assertRemainingIndex, parseIntIndex, 3, LOCAL_MILLIS);
                    break;
                case OP_MILLIS_GREEDY:
                    stackState &= ~(1 << LOCAL_MILLIS);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseInt000GreedyIndex, decodeLenIndex, decodeIntIndex, LOCAL_MILLIS);
                    break;
                case OP_SECOND_ONE_DIGIT:
                    stackState &= ~(1 << LOCAL_SECOND);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_SECOND);
                    break;
                case OP_SECOND_TWO_DIGITS:
                    stackState &= ~(1 << LOCAL_SECOND);
                    parseTwoDigits(assertRemainingIndex, parseIntIndex, LOCAL_SECOND);
                    break;
                case OP_SECOND_GREEDY:
                    stackState &= ~(1 << LOCAL_SECOND);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseIntSafelyIndex, decodeLenIndex, decodeIntIndex, LOCAL_SECOND);
                    break;
                case OP_MINUTE_ONE_DIGIT:
                    stackState &= ~(1 << LOCAL_MINUTE);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_MINUTE);
                    break;
                case OP_MINUTE_TWO_DIGITS:
                    stackState &= ~(1 << LOCAL_MINUTE);
                    parseTwoDigits(assertRemainingIndex, parseIntIndex, LOCAL_MINUTE);
                    break;

                case OP_MINUTE_GREEDY:
                    stackState &= ~(1 << LOCAL_MINUTE);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseIntSafelyIndex, decodeLenIndex, decodeIntIndex, LOCAL_MINUTE);
                    break;

                // HOUR - 12-hour clock convention
                // Note: both the 0-11 system and the 1-12 system are parsed exactly the same way.
                // In the 1-12 system, hour '12' is the same as hour '0' in the 0-11 system.
                // All other values are the same.
                // In fact, the 1-12 system should be called the 12-11 system, since it maps 12 to 0.
                // Comparison table:
                // 0-11 | 12-11
                // 0    | 12
                // 1    | 1
                // 2    | 2
                // [...]
                //
                // 11   | 11
                // This means that in both the 0-11 and 1-12 systems, we can use the same parsing logic and later treat 12 as if it were 0.
                case OP_HOUR_12_ONE_DIGIT:
                case OP_HOUR_12_ONE_DIGIT_ONE_BASED:
                    stackState &= ~(1 << LOCAL_HOUR);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_HOUR);
                    setHourType(stackState);
                    break;
                case OP_HOUR_12_TWO_DIGITS:
                case OP_HOUR_12_TWO_DIGITS_ONE_BASED:
                    stackState &= ~(1 << LOCAL_HOUR);
                    parseTwoDigits(assertRemainingIndex, parseIntIndex, LOCAL_HOUR);
                    setHourType(stackState);
                    break;
                case OP_HOUR_12_GREEDY:
                case OP_HOUR_12_GREEDY_ONE_BASED:
                    stackState &= ~(1 << LOCAL_HOUR);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseIntSafelyIndex, decodeLenIndex, decodeIntIndex, LOCAL_HOUR);
                    setHourType(stackState);
                    break;

                // HOUR (0-23)
                case OP_HOUR_24_ONE_DIGIT:
                case OP_HOUR_24_ONE_DIGIT_ONE_BASED:
                    stackState &= ~(1 << LOCAL_HOUR);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_HOUR);
                    break;
                case OP_HOUR_24_TWO_DIGITS:
                case OP_HOUR_24_TWO_DIGITS_ONE_BASED:
                    stackState &= ~(1 << LOCAL_HOUR);
                    parseTwoDigits(assertRemainingIndex, parseIntIndex, LOCAL_HOUR);
                    break;
                case OP_HOUR_24_GREEDY:
                case OP_HOUR_24_GREEDY_ONE_BASED:
                    stackState &= ~(1 << LOCAL_HOUR);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseIntSafelyIndex, decodeLenIndex, decodeIntIndex, LOCAL_HOUR);
                    break;

                // DAY
                case OP_DAY_ONE_DIGIT:
                    stackState &= ~(1 << LOCAL_DAY);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_DAY);
                    break;
                case OP_DAY_TWO_DIGITS:
                    stackState &= ~(1 << LOCAL_DAY);
                    parseTwoDigits(assertRemainingIndex, parseIntIndex, LOCAL_DAY);
                    break;
                case OP_DAY_GREEDY:
                    stackState &= ~(1 << LOCAL_DAY);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseIntSafelyIndex, decodeLenIndex, decodeIntIndex, LOCAL_DAY);
                    break;
                case OP_DAY_NAME_LONG:
                case OP_DAY_NAME_SHORT:
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeMatch(matchWeekdayIndex);
                    addTempToPos(decodeLenIndex);
                    break;
                case OP_ISO_WEEK_OF_YEAR:
                    stackState &= ~(1 << LOCAL_WEEK);
                    parseTwoDigits(assertRemainingIndex, parseIntIndex, LOCAL_WEEK);
                    break;
                case OP_WEEK_OF_YEAR:
                case OP_DAY_OF_YEAR:
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    asm.aload(P_INPUT_STR);
                    asm.iload(LOCAL_POS);
                    asm.iload(P_HI);
                    asm.invokeStatic(parseIntSafelyIndex);
                    asm.lstore(LOCAL_TEMP_LONG);
                    addTempToPos(decodeLenIndex);
                    break;
                case OP_DAY_OF_WEEK:
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
                    stackState &= ~(1 << LOCAL_MONTH);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_MONTH);
                    break;
                case OP_MONTH_TWO_DIGITS:
                    stackState &= ~(1 << LOCAL_MONTH);
                    parseTwoDigits(assertRemainingIndex, parseIntIndex, LOCAL_MONTH);
                    break;
                case OP_MONTH_GREEDY:
                    stackState &= ~(1 << LOCAL_MONTH);
                    stackState &= ~(1 << LOCAL_TEMP_LONG);
                    invokeParseIntSafelyAndStore(parseIntSafelyIndex, decodeLenIndex, decodeIntIndex, LOCAL_MONTH);
                    break;

                case OP_MONTH_SHORT_NAME:
                case OP_MONTH_LONG_NAME:
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
                    stackState &= ~(1 << LOCAL_YEAR);
                    parseDigits(assertRemainingIndex, parseIntIndex, 1, LOCAL_YEAR);
                    break;
                case OP_YEAR_TWO_DIGITS:
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
                case OP_YEAR_THREE_DIGITS:
                    stackState &= ~(1 << LOCAL_YEAR);

                    asm.iload(LOCAL_POS);
                    asm.iconst(2);
                    asm.iadd();
                    asm.iload(P_HI);
                    asm.invokeStatic(assertRemainingIndex);

                    asm.aload(P_INPUT_STR);
                    asm.iload(LOCAL_POS);
                    asm.iinc(LOCAL_POS, 3);
                    asm.iload(LOCAL_POS);
                    asm.invokeStatic(parseIntIndex);
                    asm.istore(LOCAL_YEAR);
                    break;
                case OP_YEAR_ISO_FOUR_DIGITS:
                case OP_YEAR_FOUR_DIGITS: {
                    asm.iload(LOCAL_POS);
                    asm.iload(P_HI);
                    int b1 = asm.if_icmpge();
                    asm.aload(P_INPUT_STR);
                    asm.iload(LOCAL_POS);
                    asm.invokeInterface(charAtIndex, 1); //charAt
                    asm.iconst('-');
                    int b2 = asm.if_icmpne();
                    asm.iload(LOCAL_POS);
                    asm.iconst(4);
                    asm.iadd();
                    asm.iload(P_HI);
                    asm.invokeStatic(assertRemainingIndex);
                    asm.aload(P_INPUT_STR);
                    asm.iload(LOCAL_POS);
                    asm.iconst(1);
                    asm.iadd();
                    asm.iinc(LOCAL_POS, 5);
                    asm.iload(LOCAL_POS);
                    asm.invokeStatic(parseIntIndex);
                    asm.ineg();
                    asm.istore(LOCAL_YEAR);
                    int b3 = asm.goto_();

                    int p = asm.position();
                    frameOffsets.add(Numbers.encodeLowHighInts(stackState, p));
                    asm.setJmp(b1, p);
                    asm.setJmp(b2, p);

                    asm.iload(LOCAL_POS);
                    asm.iconst(3);
                    asm.iadd();
                    asm.iload(P_HI);
                    asm.invokeStatic(assertRemainingIndex);

                    asm.aload(P_INPUT_STR);
                    asm.iload(LOCAL_POS);
                    asm.iinc(LOCAL_POS, 4);
                    asm.iload(LOCAL_POS);
                    asm.invokeStatic(parseIntIndex);
                    asm.istore(LOCAL_YEAR);

                    stackState &= ~(1 << LOCAL_YEAR);

                    p = asm.position();
                    frameOffsets.add(Numbers.encodeLowHighInts(stackState, p));
                    asm.setJmp(b3, p);
                }
                break;
                case OP_YEAR_GREEDY:
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
                    frameOffsets.add(Numbers.encodeLowHighInts(stackState, p));
                    asm.setJmp(branch1, p);

                    decodeInt(decodeIntIndex);
                    asm.i2l();
                    asm.lstore(LOCAL_OFFSET);
                    p = asm.position();
                    frameOffsets.add(Numbers.encodeLowHighInts(stackState, p));
                    asm.setJmp(branch2, p);

                    addTempToPos(decodeLenIndex);

                    break;
                default:
                    String delimiter = delimiters.getQuick(-op - 1);
                    int len = delimiter.length();
                    if (len == 1) {
                        asm.iconst(delimiter.charAt(0));
                        asm.aload(P_INPUT_STR);
                        asm.iload(LOCAL_POS);
                        asm.iinc(LOCAL_POS, 1);
                        asm.iload(P_HI);
                        asm.invokeStatic(assertCharIndex);
                    } else {
                        asm.ldc(delimIndices.getQuick(-op - 1));
                        asm.iconst(len);
                        asm.aload(P_INPUT_STR);
                        asm.iload(LOCAL_POS);
                        asm.iload(P_HI);
                        asm.invokeStatic(assertStringIndex);
                        asm.istore(LOCAL_POS);
                    }
                    break;
            }
        }

        // check that there is no tail
        asm.iload(LOCAL_POS);
        asm.iload(P_HI);
        asm.invokeStatic(assertNoTailIndex);
        asm.aload(P_LOCALE);
        asm.iload(LOCAL_ERA);
        asm.iload(LOCAL_YEAR);
        asm.iload(LOCAL_MONTH);
        asm.iload(LOCAL_WEEK);
        asm.iload(LOCAL_DAY);
        asm.iload(LOCAL_HOUR);
        asm.iload(LOCAL_MINUTE);
        asm.iload(LOCAL_SECOND);
        asm.iload(LOCAL_MILLIS);
        asm.iload(LOCAL_MICROS);
        asm.iload(LOCAL_TIMEZONE);
        asm.lload(LOCAL_OFFSET);
        asm.iload(LOCAL_HOUR_TYPE);
        asm.invokeStatic(computeIndex);
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
                int offset = Numbers.decodeHighInt(l);
                int ss = Numbers.decodeLowInt(l);

                if (i == 0 || prevStackState != ss) {
                    asm.full_frame(offset - start);
                    // 19 local variables
                    int countPos = asm.position();
                    int count = 20; // TOTAL COUNT of local variables
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

                    // MICROS
                    if ((ss & (1 << LOCAL_MICROS)) == 0) {
                        asm.putITEM_Integer();
                    } else {
                        asm.putITEM_Top();
                    }

                    if ((ss & (1 << LOCAL_WEEK)) == 0) {
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

    /**
     * Bytecode is assembled from fragments of switch() statement in GenericDateFormat. Compilation removes loop/switch
     * and does not include code that wouldn't be relevant for the given pattern. Main performance benefit however comes
     * from removing redundant local variable initialization code. For example year has to be defaulted to 1970 when
     * it isn't present in the pattern and does not have to be defaulted at all when it is.
     */
    private DateFormat compile(IntList ops, ObjList<String> delimiters) {
        asm.init(DateFormat.class);
        asm.setupPool();
        int thisClassIndex = asm.poolClass(asm.poolUtf8("io/questdb/std/datetime/TimestampFormatAsm"));
        int stackMapTableIndex = asm.poolUtf8("StackMapTable");
        int superclassIndex = asm.poolClass(AbstractDateFormat.class);
        int dateLocaleClassIndex = asm.poolClass(DateLocale.class);
        int charSequenceClassIndex = asm.poolClass(CharSequence.class);
        int minLongIndex = asm.poolLongConst(Long.MIN_VALUE);

        int superIndex = asm.poolMethod(superclassIndex, "<init>", "()V");
        int matchWeekdayIndex = asm.poolMethod(DateLocale.class, "matchWeekday", "(Ljava/lang/CharSequence;II)J");
        int matchMonthIndex = asm.poolMethod(DateLocale.class, "matchMonth", "(Ljava/lang/CharSequence;II)J");
        int matchZoneIndex = asm.poolMethod(DateLocale.class, "matchZone", "(Ljava/lang/CharSequence;II)J");
        int matchAMPMIndex = asm.poolMethod(DateLocale.class, "matchAMPM", "(Ljava/lang/CharSequence;II)J");
        int matchEraIndex = asm.poolMethod(DateLocale.class, "matchEra", "(Ljava/lang/CharSequence;II)J");
        int getWeekdayIndex = asm.poolMethod(DateLocale.class, "getWeekday", "(I)Ljava/lang/String;");
        int getShortWeekdayIndex = asm.poolMethod(DateLocale.class, "getShortWeekday", "(I)Ljava/lang/String;");
        int getMonthIndex = asm.poolMethod(DateLocale.class, "getMonth", "(I)Ljava/lang/String;");
        int getShortMonthIndex = asm.poolMethod(DateLocale.class, "getShortMonth", "(I)Ljava/lang/String;");

        int parseIntSafelyIndex = asm.poolMethod(Numbers.class, "parseIntSafely", "(Ljava/lang/CharSequence;II)J");
        int parseInt000GreedyIndex = asm.poolMethod(Numbers.class, "parseInt000Greedy", "(Ljava/lang/CharSequence;II)J");
        int parseLong000000GreedyIndex = asm.poolMethod(Numbers.class, "parseLong000000Greedy", "(Ljava/lang/CharSequence;II)J");
        int parseNanosAsMicrosGreedyIndex = asm.poolMethod(Micros.class, "parseNanosAsMicrosGreedy", "(Ljava/lang/CharSequence;II)J");
        int decodeLenIndex = asm.poolMethod(Numbers.class, "decodeHighInt", "(J)I");
        int decodeIntIndex = asm.poolMethod(Numbers.class, "decodeLowInt", "(J)I");
        int parseIntIndex = asm.poolMethod(Numbers.class, "parseInt", "(Ljava/lang/CharSequence;II)I");

        int assertRemainingIndex = asm.poolMethod(MicrosFormatUtils.class, "assertRemaining", "(II)V");
        int assertNoTailIndex = asm.poolMethod(MicrosFormatUtils.class, "assertNoTail", "(II)V");
        int assertStringIndex = asm.poolMethod(MicrosFormatUtils.class, "assertString", "(Ljava/lang/CharSequence;ILjava/lang/CharSequence;II)I");
        int assertCharIndex = asm.poolMethod(MicrosFormatUtils.class, "assertChar", "(CLjava/lang/CharSequence;II)V");
        int computeIndex = asm.poolMethod(MicrosFormatUtils.class, "compute", "(Lio/questdb/std/datetime/DateLocale;IIIIIIIIIIIJI)J");
        int adjustYearIndex = asm.poolMethod(MicrosFormatUtils.class, "adjustYear", "(I)I");
        int parseYearGreedyIndex = asm.poolMethod(MicrosFormatUtils.class, "parseYearGreedy", "(Ljava/lang/CharSequence;II)J");
        int appendEraIndex = asm.poolMethod(MicrosFormatUtils.class, "appendEra", "(Lio/questdb/std/str/CharSink;ILio/questdb/std/datetime/DateLocale;)V");
        int appendAmPmIndex = asm.poolMethod(MicrosFormatUtils.class, "appendAmPm", "(Lio/questdb/std/str/CharSink;ILio/questdb/std/datetime/DateLocale;)V");
        int appendHour12Index = asm.poolMethod(MicrosFormatUtils.class, "appendHour12", "(Lio/questdb/std/str/CharSink;I)V");
        int appendHour12PaddedIndex = asm.poolMethod(MicrosFormatUtils.class, "appendHour12Padded", "(Lio/questdb/std/str/CharSink;I)V");
        int appendHour121Index = asm.poolMethod(MicrosFormatUtils.class, "appendHour121", "(Lio/questdb/std/str/CharSink;I)V");
        int appendHour121PaddedIndex = asm.poolMethod(MicrosFormatUtils.class, "appendHour121Padded", "(Lio/questdb/std/str/CharSink;I)V");
        int appendHour241Index = asm.poolMethod(MicrosFormatUtils.class, "appendHour241", "(Lio/questdb/std/str/CharSink;I)V");
        int appendHour241PaddedIndex = asm.poolMethod(MicrosFormatUtils.class, "appendHour241Padded", "(Lio/questdb/std/str/CharSink;I)V");
        int append00000Index = asm.poolMethod(MicrosFormatUtils.class, "append00000", "(Lio/questdb/std/str/CharSink;I)V");
        int append00Index = asm.poolMethod(MicrosFormatUtils.class, "append00", "(Lio/questdb/std/str/CharSink;I)V");
        int append0Index = asm.poolMethod(MicrosFormatUtils.class, "append0", "(Lio/questdb/std/str/CharSink;I)V");
        int appendYear000Index = asm.poolMethod(MicrosFormatUtils.class, "appendYear000", "(Lio/questdb/std/str/CharSink;I)V");
        int appendYear00Index = asm.poolMethod(MicrosFormatUtils.class, "appendYear00", "(Lio/questdb/std/str/CharSink;I)V");
        int appendYear0Index = asm.poolMethod(MicrosFormatUtils.class, "appendYear0", "(Lio/questdb/std/str/CharSink;I)V");
        int appendYearIndex = asm.poolMethod(MicrosFormatUtils.class, "appendYear", "(Lio/questdb/std/str/CharSink;I)V");

        int parseOffsetIndex = asm.poolMethod(Dates.class, "parseOffset", "(Ljava/lang/CharSequence;II)J");
        int getYearIndex = asm.poolMethod(Micros.class, "getYear", "(J)I");
        int getIsoYearIndex = asm.poolMethod(Micros.class, "getIsoYear", "(J)I");
        int isLeapYearIndex = asm.poolMethod(CommonUtils.class, "isLeapYear", "(I)Z");
        int getMonthOfYearIndex = asm.poolMethod(Micros.class, "getMonthOfYear", "(JIZ)I");
        int getDayOfMonthIndex = asm.poolMethod(Micros.class, "getDayOfMonth", "(JIIZ)I");
        int getHourOfDayIndex = asm.poolMethod(Micros.class, "getHourOfDay", "(J)I");
        int getMinuteOfHourIndex = asm.poolMethod(Micros.class, "getMinuteOfHour", "(J)I");
        int getSecondOfMinuteIndex = asm.poolMethod(Micros.class, "getSecondOfMinute", "(J)I");
        int getMillisOfSecondIndex = asm.poolMethod(Micros.class, "getMillisOfSecond", "(J)I");
        int getMicrosOfMilliIndex = asm.poolMethod(Micros.class, "getMicrosOfMilli", "(J)I");
        int getMicrosOfSecondIndex = asm.poolMethod(Micros.class, "getMicrosOfSecond", "(J)I");
        int getDayOfWeekIndex = asm.poolMethod(Micros.class, "getDayOfWeekSundayFirst", "(J)I");
        int getDayOfYearIndex = asm.poolMethod(Micros.class, "getDayOfYear", "(J)I");
        int getWeekIndex = asm.poolMethod(Micros.class, "getWeek", "(J)I");
        int getWeekOfYearIndex = asm.poolMethod(Micros.class, "getWeekOfYear", "(J)I");

        int sinkPutIntIndex = asm.poolInterfaceMethod(CharSink.class, "put", "(I)Lio/questdb/std/str/CharSink;");
        int sinkPutStrIndex = asm.poolInterfaceMethod(CharSink.class, "put", "(Ljava/lang/CharSequence;)Lio/questdb/std/str/CharSink;");
        int sinkPutChrIndex = asm.poolInterfaceMethod(CharSink.class, "put", "(C)Lio/questdb/std/str/CharSink;");

        int charAtIndex = asm.poolInterfaceMethod(charSequenceClassIndex, "charAt", "(I)C");

        int parseNameIndex = asm.poolUtf8("parse");
        int parseSigIndex = asm.poolUtf8("(Ljava/lang/CharSequence;IILio/questdb/std/datetime/DateLocale;)J");
        int formatNameIndex = asm.poolUtf8("format");
        int formatSigIndex = asm.poolUtf8("(JLio/questdb/std/datetime/DateLocale;Ljava/lang/CharSequence;Lio/questdb/std/str/CharSink;)V");

        int getColumnTypeNameIndex = asm.poolUtf8("getColumnType");
        int getColumnTypeSigIndex = asm.poolUtf8("()I");
        int columnTypeIndex = asm.getPoolCount();
        asm.poolIntConst(ColumnType.TIMESTAMP_MICRO);

        // pool only delimiters over 1 char in length
        // when delimiter is 1 char we would use shorter code path
        // that doesn't require constant
        delimiterIndexes.clear();
        for (int i = 0, n = delimiters.size(); i < n; i++) {
            String delimiter = delimiters.getQuick(i);
            if (delimiter.length() > 1) {
                delimiterIndexes.add(asm.poolStringConst(asm.poolUtf8(delimiter)));
            } else {
                // keep indexes in both lists the same
                delimiterIndexes.add(-1);
            }
        }

        asm.finishPool();

        asm.defineClass(thisClassIndex, superclassIndex);
        asm.interfaceCount(0);
        asm.fieldCount(0);
        asm.methodCount(4);
        asm.defineDefaultConstructor(superIndex);

        assembleParseMethod(
                ops,
                delimiters,
                thisClassIndex,
                stackMapTableIndex,
                dateLocaleClassIndex,
                charSequenceClassIndex,
                minLongIndex,
                matchWeekdayIndex,
                matchMonthIndex,
                matchZoneIndex,
                matchAMPMIndex,
                matchEraIndex,
                parseIntSafelyIndex,
                parseInt000GreedyIndex,
                parseLong000000GreedyIndex,
                parseNanosAsMicrosGreedyIndex,
                decodeLenIndex,
                decodeIntIndex,
                assertRemainingIndex,
                assertNoTailIndex,
                parseIntIndex,
                assertStringIndex,
                assertCharIndex,
                computeIndex,
                adjustYearIndex,
                parseYearGreedyIndex,
                parseOffsetIndex,
                parseNameIndex,
                parseSigIndex,
                delimiterIndexes,
                charAtIndex
        );

        assembleFormatMethod(
                ops,
                delimiters,
                getWeekdayIndex,
                getShortWeekdayIndex,
                getMonthIndex,
                getShortMonthIndex,
                appendEraIndex,
                appendAmPmIndex,
                appendHour12Index,
                appendHour12PaddedIndex,
                appendHour121Index,
                appendHour121PaddedIndex,
                appendHour241Index,
                appendHour241PaddedIndex,
                getYearIndex,
                getIsoYearIndex,
                isLeapYearIndex,
                getMonthOfYearIndex,
                getDayOfMonthIndex,
                getHourOfDayIndex,
                getMinuteOfHourIndex,
                getSecondOfMinuteIndex,
                getMillisOfSecondIndex,
                getMicrosOfMilliIndex,
                getMicrosOfSecondIndex,
                getDayOfWeekIndex,
                append00000Index,
                append00Index,
                append0Index,
                appendYear000Index,
                appendYear00Index,
                appendYear0Index,
                appendYearIndex,
                sinkPutIntIndex,
                sinkPutStrIndex,
                sinkPutChrIndex,
                formatNameIndex,
                formatSigIndex,
                getDayOfYearIndex,
                getWeekOfYearIndex,
                getWeekIndex
        );

        assembleGetColumnTypeMethod(getColumnTypeNameIndex, getColumnTypeSigIndex, columnTypeIndex);

        // class attribute count
        asm.putShort(0);

        return asm.newInstance();
    }

    private int computeFormatAttributes(IntList ops) {
        int attributes = 0;
        for (int i = 0, n = ops.size(); i < n; i++) {
            switch (ops.getQuick(i)) {
                // AM/PM
                case MicrosFormatCompiler.OP_AM_PM:
                    attributes |= (1 << FA_HOUR);
                    break;
                // MICROS
                case MicrosFormatCompiler.OP_MICROS_ONE_DIGIT:
                case MicrosFormatCompiler.OP_MICROS_GREEDY3:
                case MicrosFormatCompiler.OP_MICROS_THREE_DIGITS:
                    attributes |= (1 << FA_MILLIS_MICROS);
                    break;
                // formatting method for MICROS6 and NANOS9 is the same
                case MicrosFormatCompiler.OP_MICROS_GREEDY6:
                case MicrosFormatCompiler.OP_NANOS_GREEDY9:
                    attributes |= (1 << FA_SECOND_MICROS);
                    break;

                // NANOS we are parsing microsecond resolution time, it does not
                // have nanos, therefore we do not need to store value on stack, it is constant 0

                // MILLIS
                case MicrosFormatCompiler.OP_MILLIS_ONE_DIGIT:
                case MicrosFormatCompiler.OP_MILLIS_GREEDY:
                case MicrosFormatCompiler.OP_MILLIS_THREE_DIGITS:
                    attributes |= (1 << FA_SECOND_MILLIS);
                    break;
                // SECOND
                case MicrosFormatCompiler.OP_SECOND_ONE_DIGIT:
                case MicrosFormatCompiler.OP_SECOND_GREEDY:
                case MicrosFormatCompiler.OP_SECOND_TWO_DIGITS:
                    attributes |= (1 << FA_SECOND);
                    break;
                // MINUTE
                case MicrosFormatCompiler.OP_MINUTE_ONE_DIGIT:
                case MicrosFormatCompiler.OP_MINUTE_GREEDY:
                case MicrosFormatCompiler.OP_MINUTE_TWO_DIGITS:
                    attributes |= (1 << FA_MINUTE);
                    break;
                // HOUR
                case MicrosFormatCompiler.OP_HOUR_12_ONE_DIGIT:
                case MicrosFormatCompiler.OP_HOUR_12_GREEDY:
                case MicrosFormatCompiler.OP_HOUR_12_TWO_DIGITS:
                case MicrosFormatCompiler.OP_HOUR_12_ONE_DIGIT_ONE_BASED:
                case MicrosFormatCompiler.OP_HOUR_12_GREEDY_ONE_BASED:
                case MicrosFormatCompiler.OP_HOUR_12_TWO_DIGITS_ONE_BASED:
                case MicrosFormatCompiler.OP_HOUR_24_ONE_DIGIT:
                case MicrosFormatCompiler.OP_HOUR_24_GREEDY:
                case MicrosFormatCompiler.OP_HOUR_24_TWO_DIGITS:
                case MicrosFormatCompiler.OP_HOUR_24_ONE_DIGIT_ONE_BASED:
                case MicrosFormatCompiler.OP_HOUR_24_GREEDY_ONE_BASED:
                case MicrosFormatCompiler.OP_HOUR_24_TWO_DIGITS_ONE_BASED:
                    attributes |= (1 << FA_HOUR);
                    break;

                // DAY OF MONTH
                case MicrosFormatCompiler.OP_DAY_ONE_DIGIT:
                case MicrosFormatCompiler.OP_DAY_GREEDY:
                case MicrosFormatCompiler.OP_DAY_TWO_DIGITS:
                    attributes |= (1 << FA_DAY);
                    attributes |= (1 << FA_MONTH);
                    attributes |= (1 << FA_YEAR);
                    attributes |= (1 << FA_LEAP);
                    break;
                // DAY OF WEEK
                case MicrosFormatCompiler.OP_DAY_NAME_LONG:
                case MicrosFormatCompiler.OP_DAY_NAME_SHORT:
                case MicrosFormatCompiler.OP_DAY_OF_WEEK:
                    attributes |= (1 << FA_DAY_OF_WEEK);
                    break;
                case MicrosFormatCompiler.OP_DAY_OF_YEAR:
                    attributes |= (1 << FA_DAY_OF_YEAR);
                    break;
                case MicrosFormatCompiler.OP_WEEK_OF_YEAR:
                    attributes |= (1 << FA_WEEK_OF_YEAR);
                    break;
                case MicrosFormatCompiler.OP_ISO_WEEK_OF_YEAR:
                    attributes |= (1 << FA_ISO_WEEK_OF_YEAR);
                    break;
                // MONTH
                case MicrosFormatCompiler.OP_MONTH_ONE_DIGIT:
                case MicrosFormatCompiler.OP_MONTH_GREEDY:
                case MicrosFormatCompiler.OP_MONTH_TWO_DIGITS:
                case MicrosFormatCompiler.OP_MONTH_SHORT_NAME:
                case MicrosFormatCompiler.OP_MONTH_LONG_NAME:
                    attributes |= (1 << FA_MONTH);
                    attributes |= (1 << FA_YEAR);
                    attributes |= (1 << FA_LEAP);
                    break;
                // YEAR
                case MicrosFormatCompiler.OP_YEAR_ONE_DIGIT:
                case MicrosFormatCompiler.OP_YEAR_GREEDY:
                case MicrosFormatCompiler.OP_YEAR_TWO_DIGITS:
                case MicrosFormatCompiler.OP_YEAR_THREE_DIGITS:
                case MicrosFormatCompiler.OP_YEAR_FOUR_DIGITS:
                    attributes |= (1 << FA_YEAR);
                    break;
                case MicrosFormatCompiler.OP_YEAR_ISO_FOUR_DIGITS:
                    attributes |= (1 << FA_ISO_YEAR);
                    break;
                // ERA
                case MicrosFormatCompiler.OP_ERA:
                    attributes |= (1 << FA_YEAR);
                    break;
                default:
                    break;
            }
        }
        return attributes;
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
                case OP_MICROS_GREEDY3:
                    result |= (1 << LOCAL_TEMP_LONG);
                    // fall through
                case OP_MICROS_ONE_DIGIT:
                case OP_MICROS_THREE_DIGITS:
                case OP_MICROS_GREEDY6:
                    result |= (1 << LOCAL_MICROS);
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
                case OP_YEAR_THREE_DIGITS:
                case OP_YEAR_FOUR_DIGITS:
                case OP_YEAR_ISO_FOUR_DIGITS:
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

    private boolean invokeConvertMillis(int formatAttributes, int bit, int funcIndex, int stackIndex) {
        if ((formatAttributes & (1 << bit)) != 0) {
            asm.lload(FA_LOCAL_DATETIME);
            asm.invokeStatic(funcIndex);
            asm.istore(stackIndex);
            return true;
        }
        return false;
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
        if (target > -1) {
            asm.istore(target);
        } else {
            asm.pop();
        }
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
            case OP_MICROS_ONE_DIGIT:
                return OP_MICROS_GREEDY3;
            case OP_NANOS_ONE_DIGIT:
                return OP_NANOS_GREEDY;
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
        if (target > -1) {
            asm.istore(target);
        } else {
            asm.pop();
        }
    }

    private void parseTwoDigits(int assertRemainingIndex, int parseIntIndex, int target) {
        parseDigits(assertRemainingIndex, parseIntIndex, 2, target);
    }

    private void setHourType(int stackState) {
        asm.iload(LOCAL_HOUR_TYPE);
        asm.iconst(CommonUtils.HOUR_24);
        int branch = asm.if_icmpne();
        asm.iconst(CommonUtils.HOUR_AM);
        asm.istore(LOCAL_HOUR_TYPE);
        int p = asm.position();
        frameOffsets.add(Numbers.encodeLowHighInts(stackState, p));
        asm.setJmp(branch, p);
    }

    static {
        opMap = new CharSequenceIntHashMap();
        opList = new ObjList<>();

        addOp("G", OP_ERA);
        addOp("y", OP_YEAR_ONE_DIGIT);
        addOp("yy", OP_YEAR_TWO_DIGITS);
        addOp("yyy", OP_YEAR_THREE_DIGITS);
        addOp("yyyy", OP_YEAR_FOUR_DIGITS);
        addOp("YYYY", OP_YEAR_ISO_FOUR_DIGITS);
        addOp("M", OP_MONTH_ONE_DIGIT);
        addOp("MM", OP_MONTH_TWO_DIGITS);
        addOp("MMM", OP_MONTH_SHORT_NAME);
        addOp("MMMM", OP_MONTH_LONG_NAME);
        addOp("d", OP_DAY_ONE_DIGIT);
        addOp("dd", OP_DAY_TWO_DIGITS);
        addOp("E", OP_DAY_NAME_SHORT);
        addOp("EE", OP_DAY_NAME_LONG);
        addOp("u", OP_DAY_OF_WEEK);
        addOp("D", OP_DAY_OF_YEAR);
        addOp("w", OP_WEEK_OF_YEAR);
        addOp("ww", OP_ISO_WEEK_OF_YEAR);
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
        addOp("N", OP_NANOS_ONE_DIGIT);
        addOp("NNN", OP_NANOS_THREE_DIGITS);
        addOp("z", OP_TIME_ZONE_SHORT);
        addOp("zz", OP_TIME_ZONE_GMT_BASED);
        addOp("zzz", OP_TIME_ZONE_LONG);
        addOp("Z", OP_TIME_ZONE_RFC_822);
        addOp("x", OP_TIME_ZONE_ISO_8601_1);
        addOp("xx", OP_TIME_ZONE_ISO_8601_2);
        addOp("xxx", OP_TIME_ZONE_ISO_8601_3);
        addOp("U", OP_MICROS_ONE_DIGIT);
        addOp("UUU", OP_MICROS_THREE_DIGITS);
        addOp("U+", OP_MICROS_GREEDY6);
        addOp("N+", OP_NANOS_GREEDY9);
    }
}
