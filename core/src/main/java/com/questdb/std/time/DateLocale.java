/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.std.*;

import java.text.DateFormatSymbols;

public class DateLocale {
    private final IntObjHashMap<ObjList<CharSequence>> months = new IntObjHashMap<>();
    private final IntObjHashMap<ObjList<CharSequence>> weekdays = new IntObjHashMap<>();
    private final IntObjHashMap<ObjList<CharSequence>> amspms = new IntObjHashMap<>();
    private final IntObjHashMap<ObjList<CharSequence>> eras = new IntObjHashMap<>();
    private final IntObjHashMap<ObjList<CharSequence>> zones = new IntObjHashMap<>();
    private final String[] monthArray;
    private final String[] shortMonthArray;
    private final String[] weekdayArray;
    private final String[] shortWeekdayArray;
    private final String[] ampmArray;
    private final String[] eraArray;
    private final TimeZoneRuleFactory factory;
    private final String id;

    DateLocale(String id, DateFormatSymbols symbols, TimeZoneRuleFactory timeZoneRuleFactory, @Transient CharSequenceHashSet cache) {
        this.id = id;
        this.factory = timeZoneRuleFactory;
        index(monthArray = symbols.getMonths(), months);
        index(shortMonthArray = symbols.getShortMonths(), months);
        index(weekdayArray = symbols.getWeekdays(), weekdays);
        index(shortWeekdayArray = symbols.getShortWeekdays(), weekdays);
        index(ampmArray = symbols.getAmPmStrings(), amspms);
        index(eraArray = symbols.getEras(), eras);
        indexZones(symbols.getZoneStrings(), timeZoneRuleFactory, cache);
    }

    public String getAMPM(int index) {
        return Unsafe.arrayGet(ampmArray, index);
    }

    public String getEra(int index) {
        return Unsafe.arrayGet(eraArray, index);
    }

    public String getId() {
        return id;
    }

    public String getMonth(int index) {
        return Unsafe.arrayGet(monthArray, index);
    }

    public TimeZoneRules getRules(CharSequence timeZoneName) throws NumericException {
        return getZoneRules(Numbers.decodeInt(matchZone(timeZoneName, 0, timeZoneName.length())));
    }

    public String getShortMonth(int index) {
        return Unsafe.arrayGet(shortMonthArray, index);
    }

    public String getShortWeekday(int index) {
        return Unsafe.arrayGet(shortWeekdayArray, index);
    }

    public String getWeekday(int index) {
        return Unsafe.arrayGet(weekdayArray, index);
    }

    public TimeZoneRules getZoneRules(int index) {
        return factory.getTimeZoneRulesQuick(index);
    }

    public long matchAMPM(CharSequence content, int lo, int hi) throws NumericException {
        return findToken(content, lo, hi, amspms);
    }

    public long matchEra(CharSequence content, int lo, int hi) throws NumericException {
        return findToken(content, lo, hi, eras);
    }

    public long matchMonth(CharSequence content, int lo, int hi) throws NumericException {
        return findToken(content, lo, hi, months);
    }

    public long matchWeekday(CharSequence content, int lo, int hi) throws NumericException {
        return findToken(content, lo, hi, weekdays);
    }

    public long matchZone(CharSequence content, int lo, int hi) throws NumericException {
        return findToken(content, lo, hi, zones);
    }

    private static void index(String[] tokens, IntObjHashMap<ObjList<CharSequence>> map) {
        for (int i = 0, n = tokens.length; i < n; i++) {
            defineToken(Unsafe.arrayGet(tokens, i), i, map);
        }
    }

    private static void defineToken(String token, int pos, IntObjHashMap<ObjList<CharSequence>> map) {
        if (token == null || token.length() == 0) {
            return;
        }

        char c0 = Character.toUpperCase(token.charAt(0));
        ObjList<CharSequence> l = map.get(c0);
        if (l == null) {
            l = new ObjList<>();
            map.put(c0, l);
        }
        l.add(((char) pos) + token.toUpperCase());
        l.sort(Lexer2.COMPARATOR);
    }

    private static long findToken(CharSequence content, int lo, int hi, IntObjHashMap<ObjList<CharSequence>> map) throws NumericException {

        if (lo >= hi) {
            throw NumericException.INSTANCE;
        }

        char c = Character.toUpperCase(content.charAt(lo));

        ObjList<CharSequence> l = map.get(c);
        if (l == null) {
            throw NumericException.INSTANCE;
        }

        for (int i = 0, sz = l.size(); i < sz; i++) {
            CharSequence txt = l.get(i);
            int n = txt.length() - 1;
            boolean match = n <= hi - lo;
            if (match) {
                for (int k = 1; k < n; k++) {
                    if (Character.toUpperCase(content.charAt(lo + k)) != txt.charAt(k + 1)) {
                        match = false;
                        break;
                    }
                }
            }

            if (match) {
                return (long) n << 32L | txt.charAt(0);
            }
        }

        throw NumericException.INSTANCE;
    }

    private void indexZones(String[][] zones, TimeZoneRuleFactory timeZoneRuleFactory, CharSequenceHashSet cache) {
        // this is a workaround a problem where UTC timezone comes nearly last
        // in this array, which gives way to Antarctica/Troll take its place

        if (cache.add("UTC")) {
            int index = timeZoneRuleFactory.getTimeZoneRulesIndex("UTC");
            if (index != -1) {
                defineToken("UTC", index, this.zones);
            }
        }

        // end of workaround

        for (int i = 0, n = zones.length; i < n; i++) {
            String[] zNames = zones[i];
            String key = zNames[0];

            int index = timeZoneRuleFactory.getTimeZoneRulesIndex(key);
            if (index == -1) {
                continue;
            }

            for (int k = 1, m = zNames.length; k < m; k++) {
                String name = zNames[k];
                // we already added this name, skip
                if (cache.add(name)) {
                    defineToken(name, index, this.zones);
                }
            }
        }
    }
}
