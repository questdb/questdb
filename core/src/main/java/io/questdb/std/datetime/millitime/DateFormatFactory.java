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

package io.questdb.std.datetime.millitime;


import io.questdb.cairo.TimestampDateFormatFactory;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.datetime.DateFormat;

import java.util.function.Function;


public class DateFormatFactory implements TimestampDateFormatFactory {
    public static final DateFormatFactory INSTANCE = new DateFormatFactory();
    private final static ThreadLocal<DateFormatCompiler> tlCompiler = ThreadLocal.withInitial(DateFormatCompiler::new);
    private static final Function<CharSequence, DateFormat> mapper = DateFormatFactory::map;
    private final ConcurrentHashMap<DateFormat> cache = new ConcurrentHashMap<>();

    private DateFormatFactory() {
    }

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
    @Override
    public DateFormat get(CharSequence pattern) {
        return cache.computeIfAbsent(pattern, mapper);
    }

    private static DateFormat map(CharSequence value) {
        return tlCompiler.get().compile(value);
    }
}
