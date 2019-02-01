/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.std.microtime;

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
            if ("und".equals(tag)) {
                tag = "";
            }
            dateLocales.put(tag, new DateLocale(tag, new DateFormatSymbols(l), timeZoneRuleFactory, cache));
            cache.clear();
        }
        defaultDateLocale = dateLocales.get(Locale.getDefault().toLanguageTag());
    }

    public CharSequenceObjHashMap<DateLocale> getAll() {
        return dateLocales;
    }

    public DateLocale getDateLocale(CharSequence id) {
        return dateLocales.get(id);
    }

    public DateLocale getDefaultDateLocale() {
        return defaultDateLocale;
    }
}
