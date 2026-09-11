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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@Measurement(iterations = 5, time = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
public class JoinCastBenchmark {
    @Param({"none", "string", "symbol", "varchar"})
    public String cast;
    @Param({"1000", "10000"})
    public int rowCount;
    private CairoEngine engine;
    private long expectedChecksum;
    private long expectedRowCount;
    private RecordCursorFactory factory;
    private String root;
    private SqlExecutionContextImpl sqlExecutionContext;

    @Benchmark
    public long join() throws SqlException {
        long count = 0;
        long checksum = 0;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                count++;
                checksum += record.getLong(0) + record.getLong(1);
            }
        }
        if (count != expectedRowCount || checksum != expectedChecksum) {
            throw new IllegalStateException("Unexpected join result: rows=" + count + ", checksum=" + checksum);
        }
        return checksum;
    }

    @Setup
    public void setup() throws Exception {
        root = java.nio.file.Files.createTempDirectory("questdb-join-cast-").toString();
        engine = new CairoEngine(new DefaultCairoConfiguration(root));
        sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                .with(AllowAllSecurityContext.INSTANCE, null);
        engine.execute("CREATE TABLE trades AS (SELECT x id, (x % 1000)::string::symbol symbol FROM long_sequence(" + rowCount + "))", sqlExecutionContext);
        String query = "SELECT t1.id, t2.id FROM (trades LIMIT " + rowCount + ") t1 "
                + "INNER JOIN (trades LIMIT " + rowCount + ") t2 ON t1.symbol = t2.symbol"
                + ("none".equals(cast) ? "" : "::" + cast);
        factory = engine.select(query, sqlExecutionContext);
        expectedRowCount = (long) rowCount * rowCount / 1000;
        expectedChecksum = (long) rowCount * (rowCount + 1) * (rowCount / 1000);
        try (RecordCursorFactory plan = engine.select("EXPLAIN " + query, sqlExecutionContext);
             RecordCursor cursor = plan.getCursor(sqlExecutionContext)) {
            while (cursor.hasNext()) {
                System.out.println(cursor.getRecord().getStrA(0));
            }
        }
        join();
    }

    @TearDown
    public void tearDown() {
        factory = Misc.free(factory);
        sqlExecutionContext = Misc.free(sqlExecutionContext);
        engine = Misc.free(engine);
        if (root != null) {
            try (Path path = new Path().of(root)) {
                Files.rmdir(path, true);
            }
        }
    }
}
