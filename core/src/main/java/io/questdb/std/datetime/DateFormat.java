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

import io.questdb.std.NumericException;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Instances of DateFormat do not have state. They are thread-safe. In that multiple threads can use
 * same DateFormat instance without risk of data corruption.
 */
public interface DateFormat {

    /**
     * Formats a datetime value to the sink.
     *
     * @param datetime     the datetime value
     * @param locale       the date locale
     * @param timeZoneName the timezone name, may be null
     * @param sink         the sink to write to
     */
    void format(long datetime, @NotNull DateLocale locale, @Nullable CharSequence timeZoneName, @NotNull CharSink<?> sink);

    /**
     * Returns the column type for this date format.
     *
     * @return the column type
     */
    int getColumnType();

    /**
     * Parses a date string to a timestamp.
     *
     * @param in     the input string
     * @param lo     the start index
     * @param hi     the end index
     * @param locale the date locale
     * @return the parsed timestamp
     * @throws NumericException if parsing fails
     */
    long parse(@NotNull CharSequence in, int lo, int hi, @NotNull DateLocale locale) throws NumericException;

    /**
     * Parses a date string to a timestamp.
     *
     * @param in     the input string
     * @param locale the date locale
     * @return the parsed timestamp
     * @throws NumericException if parsing fails
     */
    long parse(@NotNull CharSequence in, @NotNull DateLocale locale) throws NumericException;
}
