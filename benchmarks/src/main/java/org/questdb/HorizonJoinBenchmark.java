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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;

/**
 * Arguments: warmup runs, runs per batch, batch count. Compilation and table creation
 * are outside the timed batches. Run against baseline and candidate classes in fresh JVMs.
 */
public class HorizonJoinBenchmark {
    public static void main(String[] args) throws Exception {
        final int warmup = args.length > 0 ? Integer.parseInt(args[0]) : 100;
        final int repetitions = args.length > 1 ? Integer.parseInt(args[1]) : 50;
        final int batches = args.length > 2 ? Integer.parseInt(args[2]) : 9;
        final Path root = Files.createTempDirectory("horizon-benchmark-");
        final DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root.toString()) {
            @Override
            public boolean isDevModeEnabled() {
                return true;
            }
        };
        try (CairoEngine engine = new CairoEngine(configuration);
             SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1)) {
            context.with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
            context.setParallelHorizonJoinEnabled(false);
            engine.execute("CREATE TABLE trades AS (SELECT (x % 32)::INT sym, "
                    + "timestamp_sequence(0, 10_000) ts FROM long_sequence(5_000)) TIMESTAMP(ts)", context);
            for (boolean isDense : new boolean[]{true, false}) {
                final int rows = isDense ? 50_000 : 500;
                final int step = isDense ? 1_000 : 100_000;
                engine.execute("CREATE TABLE quotes AS (SELECT (x % 32)::INT sym, x::DOUBLE price, "
                        + "timestamp_sequence(0, " + step + ") ts FROM long_sequence(" + rows + ")) TIMESTAMP(ts)", context);
                for (boolean hasJoinKeys : new boolean[]{false, true}) {
                    final String sql = "SELECT sum(q.price) FROM trades t HORIZON JOIN quotes q "
                            + (hasJoinKeys ? "ON (t.sym = q.sym) " : "") + "LIST (-1s, 0s, 1s) AS h";
                    try (RecordCursorFactory factory = engine.select(sql, context)) {
                        double checksum = 0;
                        for (int i = 0; i < warmup; i++) {
                            checksum += run(factory, context);
                        }
                        final double[] samples = new double[batches];
                        for (int batch = 0; batch < batches; batch++) {
                            final long start = System.nanoTime();
                            for (int i = 0; i < repetitions; i++) {
                                checksum += run(factory, context);
                            }
                            samples[batch] = (System.nanoTime() - start) / (repetitions * 1_000_000.0);
                        }
                        System.out.printf(Locale.ROOT, "HORIZON dense=%s keyed=%s samples_ms=%s checksum=%.0f%n",
                                isDense, hasJoinKeys, Arrays.toString(samples), checksum);
                        Arrays.sort(samples);
                        System.out.printf(Locale.ROOT, "HORIZON median_ms=%.6f%n", samples[batches / 2]);
                    }
                }
                engine.execute("DROP TABLE quotes", context);
            }
        } finally {
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(path);
                }
            }
        }
    }

    private static double run(RecordCursorFactory factory, SqlExecutionContextImpl context) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            double sum = 0;
            while (cursor.hasNext()) {
                sum += cursor.getRecord().getDouble(0);
            }
            return sum;
        }
    }
}
