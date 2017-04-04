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

package com.questdb.std;


import com.questdb.misc.Dates;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;

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

    public DateFormatCompiler() {
        this.lexer = new Lexer();
        for (int i = 0, n = opList.size(); i < n; i++) {
            lexer.defineSymbol(opList.getQuick(i));
        }
    }

    public static void main(String[] args) throws ParseException {
        String input = "Jul 4, '1024 BC";
        SimpleDateFormat parser = new SimpleDateFormat("MMM d, ''y G");
        Date date = parser.parse(input);
        System.out.println(Dates.toString(date.getTime()));

        Set<String> allZones = ZoneId.getAvailableZoneIds();
        LocalDateTime dt = LocalDateTime.of(1919, 9, 1, 0, 0);
        long millis = Dates.toMillis(1919, 9, 1, 0, 0);

// Create a List using the set of zones and sort it.
        List<String> zoneList = new ArrayList<>(allZones);
        Collections.sort(zoneList);

        for (String s : zoneList) {
            ZoneId zone = ZoneId.of(s);
            TZ tz = new TZ(zone.getRules());
            ZonedDateTime zdt = dt.atZone(zone);
            ZoneOffset offset = zdt.getOffset();
            if (offset.getTotalSeconds() != (tz.adjust(millis) - millis) / 1000) {
                System.out.println(zone);
            }
            int secondsOfHour = offset.getTotalSeconds() % (60 * 60);
            String out = String.format("%35s %10s %10s%n", zone, offset, Dates.toString(tz.adjust(millis)));

            // Write only time zones that do not have a whole hour offset
            // to standard out.
            if (secondsOfHour != 0) {
//                System.out.println(zdt.getHour());
                System.out.printf(out);
            }
        }

    }

    public DateFormat create(CharSequence sequence) {
        return create(sequence, 0, sequence.length());
    }

    public DateFormat create(CharSequence sequence, int lo, int hi) {
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
        return new DateFormatImpl(compiled, delimiters);
    }

    private static void addOp(String op, int opDayTwoDigits) {
        opMap.put(op, opDayTwoDigits);
        opList.add(op);
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
