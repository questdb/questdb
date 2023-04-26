/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.Metrics;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import org.openjdk.jmh.annotations.*;
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
    private static TableReader reader;
    private static long sum = 0;
    private static TableWriter writer;

    public static void main(String[] args) throws RunnerException {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                    .with(
                            configuration.getSecurityContextFactory().getRootContext(),
                            null,
                            null,
                            -1,
                            null
                    );
            try (SqlCompiler compiler = new SqlCompiler(engine)) {
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
        TableToken tableToken = new TableToken("test", "test", 0, false);
        writer = new TableWriter(configuration, tableToken, Metrics.disabled());
        writer.truncate();
        // create 10 partitions
        appendRow(TimestampFormatUtils.parseTimestamp("2012-03-01T00:00:00.000000Z"));
        appendRow(TimestampFormatUtils.parseTimestamp("2012-03-02T00:00:00.000000Z"));
        appendRow(TimestampFormatUtils.parseTimestamp("2012-03-03T00:00:00.000000Z"));
        appendRow(TimestampFormatUtils.parseTimestamp("2012-03-04T00:00:00.000000Z"));
        appendRow(TimestampFormatUtils.parseTimestamp("2012-03-05T00:00:00.000000Z"));
        appendRow(TimestampFormatUtils.parseTimestamp("2012-03-06T00:00:00.000000Z"));
        appendRow(TimestampFormatUtils.parseTimestamp("2012-03-07T00:00:00.000000Z"));
        appendRow(TimestampFormatUtils.parseTimestamp("2012-03-08T00:00:00.000000Z"));
        appendRow(TimestampFormatUtils.parseTimestamp("2012-03-09T00:00:00.000000Z"));
        appendRow(TimestampFormatUtils.parseTimestamp("2012-03-10T00:00:00.000000Z"));
        writer.commit();
        reader = new TableReader(configuration, tableToken);

        // ensure reader opens all partitions and maps all data
        RecordCursor cursor = reader.getCursor();
        Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            sum += record.getTimestamp(0);
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() throws NumericException {
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
            ts = TimestampFormatUtils.parseTimestamp("2012-03-10T00:00:00.000000Z");
        } catch (NumericException e) {
            throw new ExceptionInInitializerError();
        }
    }
}
