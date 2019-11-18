/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.std.microtime;

import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.CharSequenceObjHashMap;

import java.text.DateFormatSymbols;
import java.util.Locale;

public class TimestampLocaleFactory {
    public static final TimestampLocaleFactory INSTANCE = new TimestampLocaleFactory(TimeZoneRuleFactory.INSTANCE);

    private final CharSequenceObjHashMap<TimestampLocale> dateLocales = new CharSequenceObjHashMap<>();
    private final TimestampLocale defaultTimestampLocale;

    public TimestampLocaleFactory(TimeZoneRuleFactory timeZoneRuleFactory) {
        CharSequenceHashSet cache = new CharSequenceHashSet();
        for (Locale l : Locale.getAvailableLocales()) {
            String tag = l.toLanguageTag();
            if ("und".equals(tag)) {
                tag = "";
            }
            dateLocales.put(tag, new TimestampLocale(new DateFormatSymbols(l), timeZoneRuleFactory, cache));
            cache.clear();
        }
        defaultTimestampLocale = dateLocales.get(Locale.getDefault().toLanguageTag());
    }

    public CharSequenceObjHashMap<TimestampLocale> getAll() {
        return dateLocales;
    }

    public TimestampLocale getDateLocale(CharSequence id) {
        return dateLocales.get(id);
    }

    public TimestampLocale getDefaultTimestampLocale() {
        return defaultTimestampLocale;
    }
}
