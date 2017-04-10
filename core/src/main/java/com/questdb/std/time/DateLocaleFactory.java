package com.questdb.std.time;

import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.CharSequenceObjHashMap;

import java.text.DateFormatSymbols;
import java.util.Locale;

public class DateLocaleFactory {
    public static final DateLocaleFactory INSTANCE = new DateLocaleFactory(TimeZoneRuleFactory.INSTANCE);

    private final CharSequenceObjHashMap<DateLocale> dateLocales = new CharSequenceObjHashMap<>();

    public DateLocaleFactory(TimeZoneRuleFactory timeZoneRuleFactory) {
        CharSequenceHashSet cache = new CharSequenceHashSet();
        for (Locale l : Locale.getAvailableLocales()) {
            dateLocales.put(l.toLanguageTag(), new DateLocale(new DateFormatSymbols(l), timeZoneRuleFactory, cache));
            cache.clear();
        }
    }

    public DateLocale getDateLocale(CharSequence id) {
        return dateLocales.get(id);
    }
}
