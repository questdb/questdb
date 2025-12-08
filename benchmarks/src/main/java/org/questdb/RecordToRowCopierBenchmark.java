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

import io.questdb.cairo.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Benchmark comparing bytecode-generated vs loop-based RecordToRowCopier implementations.
 * Tests performance across different column counts and type profiles.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class RecordToRowCopierBenchmark {

    @Param({"10", "100", "500", "1000", "2000"})
    private int columnCount;

    @Param({"1000", "10000"})
    private int rowCount;

    @Param({"ALL_INT", "MIXED", "WITH_STRINGS"})
    private String typeProfile;

    private CairoConfiguration configuration;
    private CairoEngine engine;
    private SqlExecutionContext sqlExecutionContext;
    private String srcTable;
    private String dstTable;

    @Setup(Level.Trial)
    public void setup() throws SqlException {
        configuration = new DefaultCairoConfiguration("target/benchmark-data");
        engine = new CairoEngine(configuration);
        sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                .with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );

        // Create unique table names for this configuration
        srcTable = "src_" + columnCount + "_" + typeProfile;
        dstTable = "dst_" + columnCount + "_" + typeProfile;

        // Create source table
        StringBuilder createSql = new StringBuilder("create table ").append(srcTable).append(" (ts timestamp");
        for (int i = 0; i < columnCount; i++) {
            createSql.append(", col").append(i).append(" ");
            switch (typeProfile) {
                case "ALL_INT":
                    createSql.append("int");
                    break;
                case "MIXED":
                    // Mix of types
                    if (i % 5 == 0) createSql.append("long");
                    else if (i % 5 == 1) createSql.append("double");
                    else if (i % 5 == 2) createSql.append("string");
                    else if (i % 5 == 3) createSql.append("symbol");
                    else createSql.append("int");
                    break;
                case "WITH_STRINGS":
                    // Heavy on strings
                    if (i % 3 == 0) createSql.append("string");
                    else if (i % 3 == 1) createSql.append("symbol");
                    else createSql.append("int");
                    break;
            }
        }
        createSql.append(") timestamp(ts) partition by DAY");
        execute("drop table if exists " + srcTable);
        execute(createSql.toString());

        // Create destination table (same structure)
        createSql = new StringBuilder("create table ").append(dstTable).append(" (ts timestamp");
        for (int i = 0; i < columnCount; i++) {
            createSql.append(", col").append(i).append(" ");
            switch (typeProfile) {
                case "ALL_INT":
                    createSql.append("int");
                    break;
                case "MIXED":
                    if (i % 5 == 0) createSql.append("long");
                    else if (i % 5 == 1) createSql.append("double");
                    else if (i % 5 == 2) createSql.append("string");
                    else if (i % 5 == 3) createSql.append("symbol");
                    else createSql.append("int");
                    break;
                case "WITH_STRINGS":
                    if (i % 3 == 0) createSql.append("string");
                    else if (i % 3 == 1) createSql.append("symbol");
                    else createSql.append("int");
                    break;
            }
        }
        createSql.append(") timestamp(ts) partition by DAY");
        execute("drop table if exists " + dstTable);
        execute(createSql.toString());

        // Insert test data
        StringBuilder insertSql = new StringBuilder("insert into ").append(srcTable).append(" select ");
        insertSql.append("timestamp_sequence(0, 1000000) ts");
        for (int i = 0; i < columnCount; i++) {
            insertSql.append(", ");
            switch (typeProfile) {
                case "ALL_INT":
                    insertSql.append("cast(x as int) col").append(i);
                    break;
                case "MIXED":
                    if (i % 5 == 0) insertSql.append("x col").append(i);
                    else if (i % 5 == 1) insertSql.append("x * 1.5 col").append(i);
                    else if (i % 5 == 2) insertSql.append("'str' || x col").append(i);
                    else if (i % 5 == 3) insertSql.append("'sym' || cast(x % 100 as symbol) col").append(i);
                    else insertSql.append("cast(x as int) col").append(i);
                    break;
                case "WITH_STRINGS":
                    if (i % 3 == 0) insertSql.append("'string' || x col").append(i);
                    else if (i % 3 == 1) insertSql.append("'sym' || cast(x % 100 as symbol) col").append(i);
                    else insertSql.append("cast(x as int) col").append(i);
                    break;
            }
        }
        insertSql.append(" from long_sequence(").append(rowCount).append(")");
        execute(insertSql.toString());
    }

    @TearDown(Level.Trial)
    public void tearDown() throws SqlException {
        execute("drop table if exists " + srcTable);
        execute("drop table if exists " + dstTable);
        engine.close();
    }

    @Setup(Level.Invocation)
    public void clearDestination() throws SqlException {
        execute("truncate table " + dstTable);
    }

    @Benchmark
    public void testInsertAsSelect() throws SqlException {
        execute("insert into " + dstTable + " select * from " + srcTable);
    }

    private void execute(String sql) throws SqlException {
        engine.execute(sql, sqlExecutionContext);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RecordToRowCopierBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
