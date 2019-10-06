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
            dateLocales.put(tag, new TimestampLocale(tag, new DateFormatSymbols(l), timeZoneRuleFactory, cache));
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
