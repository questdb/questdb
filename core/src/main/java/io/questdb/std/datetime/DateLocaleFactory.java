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

package io.questdb.std.datetime;

import io.questdb.std.ConcurrentHashMap;
import org.jetbrains.annotations.TestOnly;

import java.text.DateFormatSymbols;
import java.util.Locale;
import java.util.function.BiFunction;

public class DateLocaleFactory {

    public static final DateLocaleFactory INSTANCE = new DateLocaleFactory(TimeZoneRuleFactory.INSTANCE);
    public static final DateLocale EN_LOCALE = INSTANCE.getLocale("en");
    private final ConcurrentHashMap<DateLocale> dateLocales = new ConcurrentHashMap<>();
    private final DateLocale dummyLocale = new DateLocale("en-quest", new DateFormatSymbols(), TimeZoneRuleFactory.INSTANCE);
    private final TimeZoneRuleFactory timeZoneRuleFactory;
    private final BiFunction<CharSequence, DateLocale, DateLocale> computeDateLocaleBiFunc = this::computeDateLocale;

    public DateLocaleFactory(TimeZoneRuleFactory timeZoneRuleFactory) {
        this.timeZoneRuleFactory = timeZoneRuleFactory;
        for (java.util.Locale l : java.util.Locale.getAvailableLocales()) {
            String tag = l.toLanguageTag();
            if ("und".equals(tag)) {
                tag = "";
            }
            dateLocales.put(tag, dummyLocale);
        }
    }

    @TestOnly
    public static void load() {
    }

    public DateLocale getLocale(CharSequence id) {
        DateLocale dateLocale = dateLocales.get(id);
        if (dateLocale == null) {
            return null;
        }
        if (dateLocale != dummyLocale) {
            return dateLocale;
        }
        return dateLocales.compute(id, computeDateLocaleBiFunc);
    }

    private DateLocale computeDateLocale(CharSequence key, DateLocale val) {
        if (val != dummyLocale) {
            // Someone was faster than us.
            return val;
        }
        String keyStr = key.toString();
        Locale locale = Locale.forLanguageTag(keyStr);
        return new DateLocale(keyStr, new DateFormatSymbols(locale), timeZoneRuleFactory);
    }
}
