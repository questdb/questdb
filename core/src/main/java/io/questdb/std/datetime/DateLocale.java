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

package io.questdb.std.datetime;

import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

import java.text.DateFormatSymbols;

public class DateLocale {
    private final String[] ampmArray;
    private final IntObjHashMap<ObjList<CharSequence>> amspms = new IntObjHashMap<>();
    private final String[] eraArray;
    private final IntObjHashMap<ObjList<CharSequence>> eras = new IntObjHashMap<>();
    private final TimeZoneRuleFactory factory;
    private final String[] monthArray;
    private final IntObjHashMap<ObjList<CharSequence>> months = new IntObjHashMap<>();
    private final String name;
    private final String[] shortMonthArray;
    private final String[] shortWeekdayArray;
    private final String[] weekdayArray;
    private final IntObjHashMap<ObjList<CharSequence>> weekdays = new IntObjHashMap<>();
    private final IntObjHashMap<ObjList<CharSequence>> zones = new IntObjHashMap<>();

    public DateLocale(String name, DateFormatSymbols symbols, TimeZoneRuleFactory timeZoneRuleFactory) {
        this.name = name;
        this.factory = timeZoneRuleFactory;
        index(monthArray = symbols.getMonths(), months);
        index(shortMonthArray = symbols.getShortMonths(), months);
        index(weekdayArray = symbols.getWeekdays(), weekdays);
        index(shortWeekdayArray = symbols.getShortWeekdays(), weekdays);
        index(ampmArray = symbols.getAmPmStrings(), amspms);
        index(eraArray = symbols.getEras(), eras);
        indexZones(symbols.getZoneStrings(), timeZoneRuleFactory);
    }

    @SuppressWarnings("unchecked")
    public static void sort(IntObjHashMap<ObjList<CharSequence>> map) {
        Object[] values = map.getValues();
        for (int i = 0, n = values.length; i < n; i++) {
            if (values[i] != null) {
                ObjList<CharSequence> l = (ObjList<CharSequence>) values[i];
                if (l.size() > 1) {
                    l.sort(GenericLexer.COMPARATOR);
                }
            }
        }
    }

    public String getAMPM(int index) {
        return ampmArray[index];
    }

    public String getEra(int index) {
        return eraArray[index];
    }

    public String getMonth(int index) {
        return monthArray[index];
    }

    public String getName() {
        return name;
    }

    public TimeZoneRules getRules(CharSequence timeZoneName, int resolution) throws NumericException {
        return getZoneRules(Numbers.decodeLowInt(matchZone(timeZoneName, 0, timeZoneName.length())), resolution);
    }

    public String getShortMonth(int index) {
        return shortMonthArray[index];
    }

    public String getShortWeekday(int index) {
        return shortWeekdayArray[index];
    }

    public String getWeekday(int index) {
        return weekdayArray[index];
    }

    public TimeZoneRules getZoneRules(int index, int resolution) {
        return factory.getTimeZoneRulesQuick(index, resolution);
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

    private static void defineToken(String token, int pos, IntObjHashMap<ObjList<CharSequence>> map) {
        if (token == null || token.isEmpty()) {
            return;
        }

        char c0 = Character.toUpperCase(token.charAt(0));
        int index = map.keyIndex(c0);
        ObjList<CharSequence> l;
        if (index > -1) {
            l = new ObjList<>();
            map.putAt(index, c0, l);
        } else {
            l = map.valueAtQuick(index);
        }
        l.add(((char) pos) + token.toUpperCase());
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
                return Numbers.encodeLowHighInts(txt.charAt(0), n);
            }
        }

        throw NumericException.INSTANCE;
    }

    private static void index(String[] tokens, IntObjHashMap<ObjList<CharSequence>> map) {
        for (int i = 0, n = tokens.length; i < n; i++) {
            defineToken(tokens[i], i, map);
        }
        sort(map);
    }

    private void indexZones(String[][] zones, TimeZoneRuleFactory timeZoneRuleFactory) {
        CharSequenceHashSet cache = new CharSequenceHashSet();
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

            for (int k = 0, m = zNames.length; k < m; k++) {
                String name = zNames[k];
                // we already added this name, skip
                if (cache.add(name)) {
                    defineToken(name, index, this.zones);
                }
            }
        }
        sort(this.zones);
    }
}
