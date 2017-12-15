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

package com.questdb.std.microtime;


import com.questdb.std.Chars;

import java.util.concurrent.ConcurrentHashMap;

public class DateFormatFactory {
    private final static ThreadLocal<DateFormatCompiler> tlCompiler = ThreadLocal.withInitial(DateFormatCompiler::new);
    private final ConcurrentHashMap<CharSequence, DateFormat> cache = new ConcurrentHashMap<>();

    /**
     * Retrieves cached data format, if already exists of creates and caches new one. Concurrent behaviour is
     * backed by ConcurrentHashMap, making method calls thread-safe and largely non-blocking.
     * <p>
     * Input pattern does not have to be a string, but it does have to implement hashCode/equals methods
     * correctly. No new objects created when pattern already exists.
     *
     * @param pattern can be mutable and is not stored if same pattern already in cache.
     * @return compiled implementation of DateFormat
     */
    public DateFormat get(CharSequence pattern) {
        return cache.computeIfAbsent(Chars.stringOf(pattern), p -> tlCompiler.get().compile(p));
    }
}
