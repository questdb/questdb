package com.questdb.std.time;

import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.CharSequenceObjHashMap;

import java.text.DateFormatSymbols;
import java.util.Locale;

public class DateLocaleFactory {
    public static final DateLocaleFactory INSTANCE = new DateLocaleFactory(TimeZoneRuleFactory.INSTANCE);

    private final CharSequenceObjHashMap<DateLocale> dateLocales = new CharSequenceObjHashMap<>();
    private final DateLocale defaultDateLocale;

    public DateLocaleFactory(TimeZoneRuleFactory timeZoneRuleFactory) {
        CharSequenceHashSet cache = new CharSequenceHashSet();
        for (Locale l : Locale.getAvailableLocales()) {
            String tag = l.toLanguageTag();
            dateLocales.put(tag, new DateLocale(tag, new DateFormatSymbols(l), timeZoneRuleFactory, cache));
            cache.clear();
        }
        defaultDateLocale = dateLocales.get(Locale.getDefault().toLanguageTag());
    }

    public DateLocale getDateLocale(CharSequence id) {
        return dateLocales.get(id);
    }

    public DateLocale getDefaultDateLocale() {
        return defaultDateLocale;
    }
}
