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

package org.questdb;

import io.questdb.MessageBusImpl;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.DefaultDdlListener;
import io.questdb.cairo.DefaultLifecycleManager;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxnScoreboardPoolFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class TableReaderReloadBenchmark {

    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));
    private static final long ts;
    private static CairoEngine cairoEngine;
    private static TableReader reader;
    private static TableWriter writer;

    public static void main(String[] args) throws RunnerException {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            cairoEngine = engine;
            SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                    .with(
                            configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null,
                            null,
                            -1,
                            null
                    );
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compiler.compile("create table if not exists test(f timestamp) timestamp (f) PARTITION BY DAY", sqlExecutionContext);
            } catch (SqlException e) {
                throw new ExceptionInInitializerError();
            }
        }
        Options opt = new OptionsBuilder()
                .include(TableReaderReloadBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .forks(1)
                .build();

        new Runner(opt).run();

        LogFactory.haltInstance();
    }

    @Setup(Level.Iteration)
    public void setup() throws NumericException {
        TableToken tableToken = new TableToken("test", "test", null, 0, false, false, false);
        writer = new TableWriter(
                configuration,
                tableToken,
                null,
                new MessageBusImpl(configuration),
                true,
                DefaultLifecycleManager.INSTANCE,
                configuration.getDbRoot(),
                DefaultDdlListener.INSTANCE,
                () -> Numbers.LONG_NULL,
                cairoEngine
        );
        writer.truncate();
        // create 10 partitions
        appendRow(MicrosFormatUtils.parseTimestamp("2012-03-01T00:00:00.000000Z"));
        appendRow(MicrosFormatUtils.parseTimestamp("2012-03-02T00:00:00.000000Z"));
        appendRow(MicrosFormatUtils.parseTimestamp("2012-03-03T00:00:00.000000Z"));
        appendRow(MicrosFormatUtils.parseTimestamp("2012-03-04T00:00:00.000000Z"));
        appendRow(MicrosFormatUtils.parseTimestamp("2012-03-05T00:00:00.000000Z"));
        appendRow(MicrosFormatUtils.parseTimestamp("2012-03-06T00:00:00.000000Z"));
        appendRow(MicrosFormatUtils.parseTimestamp("2012-03-07T00:00:00.000000Z"));
        appendRow(MicrosFormatUtils.parseTimestamp("2012-03-08T00:00:00.000000Z"));
        appendRow(MicrosFormatUtils.parseTimestamp("2012-03-09T00:00:00.000000Z"));
        appendRow(MicrosFormatUtils.parseTimestamp("2012-03-10T00:00:00.000000Z"));
        writer.commit();
        reader = new TableReader(0, configuration, tableToken, TxnScoreboardPoolFactory.createPool(configuration));

        // ensure reader opens all partitions and maps all data
        for (int i = 0; i < reader.getPartitionCount(); i++) {
            reader.openPartition(i);
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        System.out.println("writer size = " + Math.max(writer.size(), writer.size()));
        writer.close();
        reader.close();
    }

    @Benchmark
    public void testBaseline() {
        appendRow(ts);
        writer.commit();
    }

    @Benchmark
    public void testReload() {
        appendRow(ts);
        writer.commit();
        reader.reload();
    }

    private static void appendRow(long timestamp) {
        TableWriter.Row r = writer.newRow(timestamp);
        r.append();
    }

    static {
        try {
            ts = MicrosFormatUtils.parseTimestamp("2012-03-10T00:00:00.000000Z");
        } catch (NumericException e) {
            throw new ExceptionInInitializerError();
        }
    }
}
