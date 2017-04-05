package com.questdb.std.time;

import com.questdb.ex.NumericException;
import com.questdb.misc.Unsafe;
import com.questdb.std.*;

import java.text.DateFormatSymbols;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class DateLocale extends DateFormatSymbols {
    public static final CharSequenceObjHashMap<DateLocale> LOCALES = new CharSequenceObjHashMap<>();
    private final IntObjHashMap<List<CharSequence>> months = new IntObjHashMap<>();
    private final IntObjHashMap<List<CharSequence>> weekdays = new IntObjHashMap<>();
    private final IntObjHashMap<List<CharSequence>> amspms = new IntObjHashMap<>();
    private final IntObjHashMap<List<CharSequence>> eras = new IntObjHashMap<>();
    private final IntObjHashMap<List<CharSequence>> zones = new IntObjHashMap<>();
    private final ObjList<TimeZoneRules> rules = new ObjList<>();

    public DateLocale(Locale locale) {
        super(locale);
        index(this.getMonths(), months);
        index(this.getShortMonths(), months);
        index(this.getWeekdays(), weekdays);
        index(this.getShortWeekdays(), weekdays);
        index(this.getAmPmStrings(), amspms);
        index(this.getEras(), eras);

        String[][] zones = this.getZoneStrings();
        int index = 0;
        for (int i = 0, n = zones.length; i < n; i++) {
            String[] zNames = zones[i];
            String key = zNames[0];
            TimeZoneRules rules = TimeZoneRuleFactory.INSTANCE.get(key);
            if (rules == null) {
                String alias = ZoneId.SHORT_IDS.get(key);

                if (alias == null) {
                    System.out.println("no match: " + key);
                    continue;
                }
                rules = TimeZoneRuleFactory.INSTANCE.get(alias);

                if (rules == null) {
                    System.out.println("no match for alias: " + alias + ", key: " + key);
                    continue;
                }
            }

            this.rules.add(rules);

            for (int k = 0, m = zNames.length; k < m; k++) {
                defineToken(zNames[k], index, this.zones);
            }

            index++;
        }
    }

    public TimeZoneRules getZoneRules(int index) {
        return rules.getQuick(index);
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

    private static void index(String[] tokens, IntObjHashMap<List<CharSequence>> map) {
        for (int i = 0, n = tokens.length; i < n; i++) {
            defineToken(Unsafe.arrayGet(tokens, i), i, map);
        }
    }

    private static void defineToken(String token, int pos, IntObjHashMap<List<CharSequence>> map) {
        if (token.length() == 0) {
            return;
        }

        char c0 = Character.toUpperCase(token.charAt(0));
        List<CharSequence> l = map.get(c0);
        if (l == null) {
            l = new ArrayList<>();
            map.put(c0, l);
        }
        l.add(((char) pos) + token.toUpperCase());
        l.sort(Lexer.COMPARATOR);
    }

    private static long findToken(CharSequence content, int lo, int hi, IntObjHashMap<List<CharSequence>> map) throws NumericException {

        if (lo >= hi) {
            throw NumericException.INSTANCE;
        }

        char c = Character.toUpperCase(content.charAt(lo));

        List<CharSequence> l = map.get(c);
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

    static {
        for (Locale l : Locale.getAvailableLocales()) {
            LOCALES.put(l.toLanguageTag(), new DateLocale(l));
        }
    }
}
