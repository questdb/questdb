/*+*****************************************************************************
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

package io.questdb.cairo;

import io.questdb.std.Hash;
import io.questdb.std.str.StringSink;

/**
 * Tiny stateless transforms shared by {@link TableWriter#internDimensionValue(int, CharSequence)}
 * (write side) and {@link TableReader#keyOfDimensionValue(int, CharSequence)} /
 * {@link TableReader#valueOfDimensionKey(int, int)} (read side), so the {@code HASH}/{@code TRUNCATE}
 * dimension math is single-sourced instead of duplicated (and risking drift) across both classes.
 */
public final class CompositeDimensionTransform {

    private CompositeDimensionTransform() {
    }

    /**
     * Maps {@code value} into a dense bucket in {@code [0, buckets)} for a {@code hash(col, N)}
     * dimension ({@code buckets} is the SQL-supplied {@code N}: a plain positive bucket count, not
     * necessarily a power of two -- {@code PartitionTransform.parseBucketCount} only requires
     * {@code N > 0}).
     * <p>
     * This deliberately does not pass {@code buckets} straight through as
     * {@link Hash#boundedHash(CharSequence, int)}'s {@code max}: that method ANDs the hash with
     * {@code max} as a bitmask (every existing caller -- {@code SymbolMapWriter}/
     * {@code SymbolMapReaderImpl} -- passes {@code ceilPow2(capacity) - 1}, i.e. an all-ones mask),
     * so a raw bucket count like {@code 8} ({@code 0b1000}) would restrict the result to
     * {@code {0, 8}} -- and {@code 8} itself is out of the intended {@code [0, 8)} range -- instead of
     * spreading it over all 8 buckets. Passing {@code Integer.MAX_VALUE} as the mask instead is a
     * no-op over {@code boundedHash}'s internal {@code 0xFFFFFFF} mask, so this just recovers a
     * null-safe, non-negative hash; {@link Math#floorMod(int, int)} then reduces that into range for
     * any positive {@code buckets}.
     */
    public static int hashBucket(CharSequence value, int buckets) {
        return Math.floorMod(Hash.boundedHash(value, Integer.MAX_VALUE), buckets);
    }

    /**
     * The first {@code n} characters of {@code value}, materialized into the reusable {@code sink}
     * -- or {@code value} itself, unchanged, when it is {@code null} or already no longer than
     * {@code n}. Used to derive the interned prefix for a {@code truncate(col, N)} dimension.
     */
    public static CharSequence truncatedPrefix(CharSequence value, int n, StringSink sink) {
        if (value == null || value.length() <= n) {
            return value;
        }
        sink.clear();
        for (int i = 0; i < n; i++) {
            sink.put(value.charAt(i));
        }
        return sink;
    }
}
