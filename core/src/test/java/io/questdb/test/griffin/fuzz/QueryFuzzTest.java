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

package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Seeded random query fuzzer. Generates 1..3 WAL tables with all supported
 * scalar types plus DECIMAL and DOUBLE arrays, inserts rows that span
 * multiple DAY partitions, then runs a budget of randomly generated
 * SELECT / GROUP BY / SAMPLE BY / ASOF-LT-SPLICE JOIN queries and
 * materializes every result row.
 * <p>
 * Oracle is crash-only: {@link SqlException} is swallowed (legitimate
 * user-facing error); anything else, including {@link CairoException},
 * is recorded as a failure, because a generated SELECT should never leak
 * an internal exception. Failures are collected for the whole run and
 * reported together at the end, so a single invocation surfaces every
 * bug in one go. The driving seeds are printed so a failure can be
 * reproduced deterministically.
 * <p>
 * Budget knobs:
 * <ul>
 *     <li>{@code -Dquestdb.fuzz.queries=N} &mdash; number of queries per
 *         run (default 100). Crank up locally when hunting bugs.</li>
 *     <li>{@code -Dquestdb.fuzz.diff.jit=true|false} &mdash; differential
 *         JIT-on/off mode (default true). When enabled, every query is run
 *         twice and the materializations are compared; any divergence is
 *         reported as a failure.</li>
 * </ul>
 */
public class QueryFuzzTest extends AbstractCairoTest {

    @Test
    public void testQueryFuzz() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = TestUtils.generateRandom(LOG);
            FuzzConfig config = new FuzzConfig(rnd);

            LOG.info().$("fuzz config: tables=").$(config.getNumTables())
                    .$(", rows=").$(config.getRowsPerTable())
                    .$(", queries=").$(config.getNumQueries())
                    .$(", diffJit=").$(config.isDiffJitEnabled()).$();

            FuzzTableFactory factory = new FuzzTableFactory(config);
            ObjList<FuzzTable> tables = new ObjList<>();
            for (int i = 0; i < config.getNumTables(); i++) {
                tables.add(factory.create(rnd, "fuzz_t" + i, QueryFuzzTest::execute));
            }
            drainWalQueue();

            QueryRunner runner = new QueryRunner(engine, sqlExecutionContext, config.isDiffJitEnabled());
            int skipped = 0;
            List<QueryRunner.Result> failures = new ArrayList<>();
            try (BufferedWriter dump = openDump(config.getDumpPath())) {
                for (int q = 0; q < config.getNumQueries(); q++) {
                    GeneratedQuery query = QueryGenerator.generate(rnd, tables);
                    if (dump != null) {
                        dump.write(query.sql());
                        dump.newLine();
                    }
                    QueryRunner.Result result = runner.run(query);
                    if (result.isSkipped()) {
                        skipped++;
                        LOG.info().$("fuzz skip (").$safe(result.getSkipReason()).$("): ").$safe(query.sql()).$();
                    } else if (result.isFailed()) {
                        LOG.error().$("fuzz failure on query: ").$safe(query.sql())
                                .$(" -- ").$(result.getFailure().getClass().getName())
                                .$(": ").$safe(result.getFailure().getMessage())
                                .$();
                        failures.add(result);
                    }
                }
            }
            LOG.info().$("fuzz done: ").$(config.getNumQueries()).$(" queries, ")
                    .$(skipped).$(" skipped on expected errors, ")
                    .$(failures.size()).$(" failures")
                    .$();

            if (!failures.isEmpty()) {
                throw buildFailure(failures);
            }
        });
    }

    private static BufferedWriter openDump(String path) throws IOException {
        if (path == null || path.isEmpty()) {
            return null;
        }
        return new BufferedWriter(new FileWriter(Paths.get(path).toFile(), true));
    }

    private static AssertionError buildFailure(List<QueryRunner.Result> failures) {
        StringBuilder sb = new StringBuilder("query fuzz found ").append(failures.size())
                .append(" unexpected failure(s):\n");
        int i = 0;
        for (QueryRunner.Result r : failures) {
            sb.append("  [").append(++i).append("] ")
                    .append(r.getFailure().getClass().getSimpleName())
                    .append(": ").append(r.getFailure().getMessage()).append('\n')
                    .append("        sql: ").append(r.getSql()).append('\n');
        }
        // Chain the first cause so the stack trace still points at real source.
        return new AssertionError(sb.toString(), failures.get(0).getFailure());
    }
}
