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

package org.questdb;

import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.str.StringSink;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.CharBuffer;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Case-insensitive hashing, comparison and map lookups of table and column names. A server passes many kinds of
 * CharSequence through {@link Chars}, so the setup does the same before the JIT compiles the code under test: with
 * a single kind of key, the JIT inlines calls that a server makes through a virtual call, and the numbers mislead.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CaseInsensitiveLookupBenchmark {
    private static final String ASCII = "sensor_temperature_x";
    private static final String CYRILLIC = "температурадатчикаок";
    private static final String LATIN1 = "àáâãäåæçèéêëìíîïðñòó";
    private static final String[] SHORT_NAMES = {"ts", "id", "price", "sym"};
    private static final String[] SHORT_NAMES_OTHER_CASE = {"TS", "Id", "PRICE", "Sym"};
    private static final String[] TABLE_NAMES = {"trades_eu_west_2024", "quotes_eu_west_2024", "orders_us_east_2024", "fills_us_east_20240"};
    private static final String[] TABLE_NAMES_OTHER_CASE = {"TRADES_EU_WEST_2024", "Quotes_EU_West_2024", "ORDERS_us_east_2024", "Fills_US_East_20240"};
    private static final int WARMUP_ROUNDS = 200_000;
    private String asciiLower;
    private String asciiLowerCopy;
    private String asciiUpper;
    private long checksum;
    private ConcurrentHashMap<Integer> concurrentMap;
    private StringSink cyrillicLower;
    private StringSink cyrillicLowerCopy;
    private StringSink cyrillicUpper;
    private int i;
    private StringSink latin1Prefixed;
    private LowerCaseCharSequenceObjHashMap<Integer> objMap;
    private String[] shortNames;
    private StringSink[] shortNamesOtherCase;
    private int[] tableNameHashCodes;
    private String[] tableNames;
    private String[] tableNamesOtherCase;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CaseInsensitiveLookupBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(2)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public Integer concurrentMapGetShortName() {
        return concurrentMap.get(shortNames[i++ & 3]);
    }

    @Benchmark
    public Integer concurrentMapGetShortNameOtherCase() {
        return concurrentMap.get(shortNamesOtherCase[i++ & 3]);
    }

    @Benchmark
    public Integer concurrentMapGetTableName() {
        return concurrentMap.get(tableNames[i++ & 3]);
    }

    @Benchmark
    public boolean equalsAsciiStrings() {
        return Chars.equalsIgnoreCase(asciiLower, asciiLowerCopy);
    }

    @Benchmark
    public boolean equalsAsciiStringsOtherCase() {
        return Chars.equalsIgnoreCase(asciiLower, asciiUpper);
    }

    @Benchmark
    public boolean equalsCyrillicSinks() {
        return Chars.equalsIgnoreCase(cyrillicLower, cyrillicLowerCopy);
    }

    @Benchmark
    public boolean equalsCyrillicSinksOtherCase() {
        return Chars.equalsIgnoreCase(cyrillicLower, cyrillicUpper);
    }

    @Benchmark
    public int hashAsciiString() {
        return Chars.lowerCaseHashCode(asciiUpper);
    }

    @Benchmark
    public int hashLatin1SinkRange() {
        return Chars.lowerCaseHashCode(latin1Prefixed, 1, latin1Prefixed.length());
    }

    @Benchmark
    public Integer objMapGetTableName() {
        return objMap.get(tableNames[i++ & 3]);
    }

    @Benchmark
    public Integer objMapGetTableNameOtherCase() {
        return objMap.get(tableNamesOtherCase[i++ & 3]);
    }

    @Benchmark
    public Integer objMapKeyIndexWithHashCode() {
        final int k = i++ & 3;
        return objMap.valueAt(objMap.keyIndex(tableNames[k], tableNameHashCodes[k]));
    }

    @Benchmark
    public Integer objMapKeyIndexWithHashCodeOtherCase() {
        final int k = i++ & 3;
        return objMap.valueAt(objMap.keyIndex(tableNamesOtherCase[k], tableNameHashCodes[k]));
    }

    @Setup(Level.Trial)
    public void setup() {
        asciiLower = copy(ASCII);
        asciiLowerCopy = copy(ASCII);
        asciiUpper = ASCII.toUpperCase(Locale.ROOT);
        cyrillicLower = sink(CYRILLIC);
        cyrillicLowerCopy = sink(CYRILLIC);
        cyrillicUpper = sink(CYRILLIC.toUpperCase(Locale.ROOT));
        latin1Prefixed = sink("_" + LATIN1.toUpperCase(Locale.ROOT));

        concurrentMap = new ConcurrentHashMap<>(false);
        objMap = new LowerCaseCharSequenceObjHashMap<>();
        for (int k = 0; k < 4; k++) {
            // map keys are other instances than the lookup keys, as they are in a server
            concurrentMap.put(copy(SHORT_NAMES[k]), k);
            concurrentMap.put(copy(TABLE_NAMES[k]), k);
            objMap.put(copy(TABLE_NAMES[k]), k);
        }
        shortNames = new String[4];
        shortNamesOtherCase = new StringSink[4];
        tableNameHashCodes = new int[4];
        tableNames = new String[4];
        tableNamesOtherCase = new String[4];
        for (int k = 0; k < 4; k++) {
            shortNames[k] = copy(SHORT_NAMES[k]);
            shortNamesOtherCase[k] = sink(SHORT_NAMES_OTHER_CASE[k]);
            tableNames[k] = copy(TABLE_NAMES[k]);
            tableNamesOtherCase[k] = copy(TABLE_NAMES_OTHER_CASE[k]);
            tableNameHashCodes[k] = Chars.lowerCaseHashCode(TABLE_NAMES[k]);
        }

        // keys of several CharSequence kinds, so that the JIT sees what it sees in a server
        final CharSequence[] keys = new CharSequence[12];
        for (int k = 0; k < 4; k++) {
            keys[k] = new StringBuilder(TABLE_NAMES_OTHER_CASE[k]);
            keys[4 + k] = CharBuffer.wrap(SHORT_NAMES_OTHER_CASE[k]);
            keys[8 + k] = sink(TABLE_NAMES[k]);
        }
        for (int r = 0; r < WARMUP_ROUNDS; r++) {
            final int k = r & 3;
            for (int e = 0; e < 3; e++) {
                final CharSequence key = keys[4 * e + k];
                checksum += Chars.lowerCaseHashCode(key);
                checksum += Chars.lowerCaseHashCode(key, 1, key.length());
                checksum += Chars.equalsIgnoreCase(key, tableNames[k]) ? 1 : 0;
                checksum += Chars.equalsIgnoreCase(tableNames[k], key) ? 1 : 0;
                checksum += Chars.equalsIgnoreCase(key, tableNames[k], 0, Math.min(key.length(), tableNames[k].length())) ? 1 : 0;
                checksum += value(concurrentMap.get(key)) + value(objMap.get(key));
            }
            checksum += Chars.lowerCaseHashCode(tableNames[k]) + Chars.lowerCaseHashCode(cyrillicUpper);
            checksum += Chars.equalsIgnoreCase(asciiLower, asciiUpper) ? 1 : 0;
            checksum += Chars.equalsIgnoreCase(cyrillicLower, cyrillicUpper) ? 1 : 0;
            checksum += value(concurrentMap.get(shortNames[k])) + value(concurrentMap.get(shortNamesOtherCase[k]));
            checksum += value(concurrentMap.get(tableNames[k])) + value(objMap.get(tableNames[k]));
        }
    }

    private static String copy(String value) {
        return new String(value.toCharArray());
    }

    private static StringSink sink(CharSequence value) {
        final StringSink sink = new StringSink();
        sink.put(value);
        return sink;
    }

    private static int value(Integer value) {
        return value != null ? value : -1;
    }
}
