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

package io.questdb.test.griffin;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the bound on the SqlCodeGenerator WhereClauseParser pool. A deeply nested scalar-subquery
 * query grows the pool to its recursion depth on demand; the generator must release the deep
 * parsers at the top-level compilation boundary (its clear()) so one deep query cannot pin
 * O(maxDepth) parser scratch state for the compiler lifetime.
 */
public class WhereClauseParserPoolBoundTest extends AbstractCairoTest {

    // Mirrors SqlCodeGenerator.MAX_RETAINED_WHERE_CLAUSE_PARSERS (private).
    private static final int MAX_RETAINED = 8;
    private static final int NESTING_LEVELS = 14;

    @Test
    public void testDeepNestingReleasesParsersAtCompilationBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01'), ('2024-01-02'), ('2024-01-03')");

            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                // A single deeply-nested scalar-subquery query drives the parser pool to its
                // recursion depth.
                compileAndFree(compiler, buildDeeplyNestedQuery());
                final int poolAfterDeep = compiler.getWhereClauseParserPoolSizeForTesting();
                Assert.assertTrue(
                        "deep compile must grow the pool past the retained head, was " + poolAfterDeep,
                        poolAfterDeep > MAX_RETAINED
                );

                // Compiling a shallow query on the same compiler crosses a top-level boundary
                // (codeGenerator.clear()) and must trim the pool back to the bound.
                compileAndFree(compiler, "SELECT ts FROM t");
                final int poolAfterShallow = compiler.getWhereClauseParserPoolSizeForTesting();
                Assert.assertTrue(
                        "shallow compile must release deep parsers, pool was " + poolAfterShallow,
                        poolAfterShallow <= MAX_RETAINED
                );
            }
        });
    }

    private static String buildDeeplyNestedQuery() {
        // Each level nests one scalar subquery inside the previous level's WHERE clause, so the
        // generator re-enters generate() once per level and the parser pool grows accordingly.
        String inner = "(SELECT min(ts) FROM t)";
        for (int i = 0; i < NESTING_LEVELS; i++) {
            inner = "(SELECT min(ts) FROM t WHERE ts >= " + inner + ")";
        }
        return "SELECT ts FROM t WHERE ts >= " + inner;
    }

    private void compileAndFree(SqlCompilerImpl compiler, CharSequence sql) throws Exception {
        final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
        RecordCursorFactory factory = cq.getRecordCursorFactory();
        Misc.free(factory);
    }
}
