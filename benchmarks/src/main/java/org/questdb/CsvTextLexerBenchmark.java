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

package org.questdb;

import io.questdb.cutlass.text.CsvTextLexer;
import io.questdb.cutlass.text.DefaultTextConfiguration;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.str.DirectUtf8Sink;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CsvTextLexerBenchmark {

    private static final long BUFFER_SIZE = 1024 * 1024;
    private static final int COLUMNS = 105;
    private DirectUtf8Sink input;
    private CsvTextLexer lexer;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CsvTextLexerBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(3)
                // Uncomment to collect a flame graph via async-profiler:
                //.addProfiler(AsyncProfiler.class, "output=flamegraph")
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup
    public void setup() {
        this.input = new DirectUtf8Sink(BUFFER_SIZE);
        this.lexer = new CsvTextLexer(new DefaultTextConfiguration());

        Rnd rnd = new Rnd();
        long lineLenEstimate = 0;
        long nowUs = System.nanoTime() / 1000;
        while (input.size() < (BUFFER_SIZE - lineLenEstimate)) {
            // The CSV structure is the same as in the ClickBench input file: https://datasets.clickhouse.com/hits_compatible/hits.csv.gz
            input.put(rnd.nextLong()) // WatchID
                    .put(",").put(rnd.nextInt(2)) // JavaEnable
                    .put(",").putQuoted(rnd.nextString(20 + rnd.nextInt(100))) // Title
                    .put(",").put(rnd.nextInt(2)) // GoodEvent
                    .put(",\"").putISODate(nowUs + rnd.nextLong(1_000)).put('"') // EventTime
                    .put(",\"").putISODate(nowUs + rnd.nextLong(1_000)).put('"') // Eventdate
                    .put(",").put(rnd.nextPositiveInt()) // CounterID
                    .put(",").put(rnd.nextPositiveInt()) // ClientIP
                    .put(",").put(rnd.nextPositiveInt()) // RegionID
                    .put(",").put(rnd.nextPositiveLong()) // UserID
                    .put(",").put(rnd.nextInt(2)) // CounterClass
                    .put(",").put(rnd.nextInt(32)) // OS
                    .put(",").put(rnd.nextInt(100)) // UserAgent
                    .put(",").putQuoted(rnd.nextString(40 + rnd.nextInt(40))) // URL
                    .put(",").putQuoted(rnd.nextString(20 + rnd.nextInt(10))) // Referer
                    .put(",").put(rnd.nextInt(2)) // IsRefresh
                    .put(",").put(rnd.nextInt(1000)) // RefererCategoryID
                    .put(",").put(rnd.nextPositiveInt()) // RefererRegionID
                    .put(",").put(rnd.nextInt(1000)) // URLCategoryID
                    .put(",").put(rnd.nextPositiveInt()) // URLRegionID
                    .put(",").put(rnd.nextInt(1000)) // ResolutionWidth
                    .put(",").put(rnd.nextInt(1000)) // ResolutionHeight
                    .put(",").put(rnd.nextInt(1000)) // ResolutionDepth
                    .put(",").put(rnd.nextInt(128)) // FlashMajor
                    .put(",").put(rnd.nextInt(128)) // FlashMinor
                    .put(",").put(rnd.nextInt(128)) // FlashMinor2
                    .put(",").put(rnd.nextInt(128)) // NetMajor
                    .put(",").put(rnd.nextInt(128)) // NetMinor
                    .put(",").put(rnd.nextInt(100)) // UserAgentMajor
                    .put(",").putQuoted(rnd.nextString(20 + rnd.nextInt(10))) // UserAgentMinor
                    .put(",").put(rnd.nextInt(2)) // CookieEnable
                    .put(",").put(rnd.nextInt(2)) // JavascriptEnable
                    .put(",").put(rnd.nextInt(2)) // IsMobile
                    .put(",").putQuoted(rnd.nextString(20 + rnd.nextInt(10))) // MobilePhoneModel
                    .put(",").putQuoted(rnd.nextString(20 + rnd.nextInt(10))) // Params
                    .put(",").put(rnd.nextInt()) // IPNetworkID
                    .put(",").put(rnd.nextPositiveInt()) // TraficSourceID
                    .put(",").put(rnd.nextInt(10)) // SearchEngineID
                    .put(",").putQuoted(rnd.nextString(30 + rnd.nextInt(30))) // SearchPhrase
                    .put(",").put(rnd.nextInt(10)) // AdvEngineID
                    .put(",").put(rnd.nextInt(2)) // IsArtifical
                    .put(",").put(rnd.nextInt(1000)) // WindowClientWidth
                    .put(",").put(rnd.nextInt(1000)) // WindowClientHeight
                    .put(",").put(rnd.nextInt(10)) // ClientTimeZone
                    .put(",\"").putISODate(nowUs + rnd.nextLong(1_000)).put('"') // ClientEventTime
                    .put(",").put(rnd.nextInt(100)) // SilverlightVersion1
                    .put(",").put(rnd.nextInt(100)) // SilverlightVersion2
                    .put(",").put(rnd.nextShort()) // SilverlightVersion3
                    .put(",").put(rnd.nextInt(100)) // SilverlightVersion4
                    .put(",").putQuoted(rnd.nextString(10)) // PageCharset
                    .put(",").put(rnd.nextShort()) // CodeVersion
                    .put(",").put(rnd.nextInt(2)) // IsLink
                    .put(",").put(rnd.nextInt(2)) // IsDownload
                    .put(",").put(rnd.nextInt(2)) // IsNotBounce
                    .put(",").put(rnd.nextPositiveLong()) // FUniqID
                    .put(",").putQuoted(rnd.nextString(40 + rnd.nextInt(40))) // OriginalURL
                    .put(",").put(rnd.nextPositiveInt()) // HID
                    .put(",").put(rnd.nextInt(2)) // IsOldCounter
                    .put(",").put(rnd.nextInt(2)) // IsEvent
                    .put(",").put(rnd.nextInt(2)) // IsNotBounce
                    .put(",").put(rnd.nextInt(2)) // DontCountHits
                    .put(",").put(rnd.nextInt(2)) // WithHash
                    .put(",").put(rnd.nextChar()) // HitColor
                    .put(",\"").putISODate(nowUs + rnd.nextLong(1_000)).put('"') // LocalEventTime
                    .put(",").put(rnd.nextInt(100)) // Age
                    .put(",").put(rnd.nextInt(10)) // Sex
                    .put(",").put(rnd.nextInt(100)) // Income
                    .put(",").put(rnd.nextInt(100)) // Interests
                    .put(",").put(rnd.nextInt(100)) // Robotness
                    .put(",").put(rnd.nextInt()) // RemoteIP
                    .put(",").put(rnd.nextInt()) // WindowName
                    .put(",").put(rnd.nextInt()) // OpenerName
                    .put(",").put(rnd.nextInt(1000)) // HistoryLength
                    .put(",").putQuoted(rnd.nextString(2)) // BrowserLanguage
                    .put(",").putQuoted(rnd.nextString(2)) // BrowserCountry
                    .put(",").putQuoted(rnd.nextString(10)) // SocialNetwork
                    .put(",").putQuoted(rnd.nextString(5)) // SocialAction
                    .put(",").put(rnd.nextInt(500)) // HTTPError
                    .put(",").put(rnd.nextInt(10000)) // SendTiming
                    .put(",").put(rnd.nextInt(10000)) // DNSTiming
                    .put(",").put(rnd.nextInt(10000)) // ConnectTiming
                    .put(",").put(rnd.nextInt(10000)) // ResponseStartTiming
                    .put(",").put(rnd.nextInt(10000)) // ResponseEndTiming
                    .put(",").put(rnd.nextInt(10000)) // FetchTiming
                    .put(",").put(rnd.nextInt(100)) // SocialSourceNetworkID
                    .put(",").putQuoted(rnd.nextString(32)) // SocialSourcePage
                    .put(",").put(rnd.nextLong(10000)) // ParamPrice
                    .put(",").put(rnd.nextLong(100)) // ParamOrderID
                    .put(",").putQuoted(rnd.nextString(16)) // ParamCurrency
                    .put(",").put(rnd.nextLong(100)) // ParamCurrencyID
                    .put(",").putQuoted(rnd.nextString(16)) // OpenstatServiceName
                    .put(",").put(rnd.nextLong(10000)) // OpenstatCampaignID
                    .put(",").putQuoted(rnd.nextString(32)) // OpenstatAdID
                    .put(",").put(rnd.nextLong(10000)) // OpenstatSourceID
                    .put(",").putQuoted(rnd.nextString(16)) // UTMSource
                    .put(",").putQuoted(rnd.nextString(16)) // UTMMedium
                    .put(",").putQuoted(rnd.nextString(16)) // UTMCampaign
                    .put(",").putQuoted(rnd.nextString(16)) // UTMContent
                    .put(",").putQuoted(rnd.nextString(16)) // UTMTerm
                    .put(",").putQuoted(rnd.nextString(16)) // FromTag
                    .put(",").put(rnd.nextInt(2)) // HasGCLID
                    .put(",").put(rnd.nextPositiveLong()) // RefererHash
                    .put(",").put(rnd.nextPositiveLong()) // URLHash
                    .put(",").put(rnd.nextInt(1000000)) // CLID
                    .put("\n");
            if (lineLenEstimate == 0) {
                lineLenEstimate = 3L * input.size();
            }
        }
    }

    @TearDown
    public void tearDown() {
        this.input = Misc.free(input);
    }

    @Benchmark
    public void testParse(Blackhole bh) {
        long bufLo = input.lo();
        long bufHi = input.hi();

        AtomicInteger counter = new AtomicInteger();

        CsvTextLexer.Listener listener = (line, fields, hi) -> {
            bh.consume(line);
            for (int i = 0, n = fields.size(); i < n; i++) {
                bh.consume(fields.getQuick(i));
            }
        };
        for (int i = 0; i < COLUMNS; i++) {
            counter.set(0);
            lexer.setupLimits(Integer.MAX_VALUE, listener);
            lexer.parse(bufLo, bufHi);
            lexer.parseLast();
        }
        bh.consume(counter.get());
    }
}
