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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

/**
 * Pins the join side {@link QueryGenerator} has to keep producing for SPLICE JOIN and HORIZON JOIN: a WINDOW JOIN
 * sub-query ordered by its master's designated timestamp (see {@link FuzzSource#rowIdJoinSide}). Its rows arrive in
 * timestamp order without random access, and those two joins read their sides by row id, so ORDER BY keeps its sort
 * for them. QueryFuzzTest skips a query that fails to compile on both storages, so it stays green if that sort goes
 * missing; {@link #testWindowJoinSideKeepsRandomAccess} compiles the WINDOW JOIN sides of the corpus to catch that.
 * The generator is random, so each assertion runs over a corpus drawn from a fixed seed.
 */
public class RowIdJoinShapeCoverageTest extends AbstractCairoTest {
    private static final int CORPUS_SIZE = 20_000;
    // Each side of a SPLICE JOIN and the left-hand side of a HORIZON JOIN read the WINDOW JOIN sub-query one time in
    // five, so the 20_000-query corpus holds over a hundred SPLICE JOINs and HORIZON JOINs over it. The floor leaves
    // a wide margin below that.
    private static final int MIN_QUERIES_PER_SHAPE = 40;
    private static final Pattern WINDOW_JOIN_SIDE = Pattern.compile("\\(SELECT t\\.\\*, count\\(\\) AS wc, sum\\(p\\.k\\) AS ws FROM fuzz_t\\d+ t WINDOW JOIN ");

    @Test
    public void testCorpusCoversWindowJoinSides() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> corpus = generateCorpus();
            assertCorpusCount(corpus, " SPLICE JOIN ");
            assertCorpusCount(corpus, " HORIZON JOIN ");
        });
    }

    @Test
    public void testWindowJoinSideKeepsRandomAccess() throws Exception {
        // A WINDOW JOIN offers no page frames, so nothing can read its rows by row id but the sort its ORDER BY keeps.
        // No SPLICE JOIN or HORIZON JOIN over such a side may fail for the lack of random access. SqlOptimiser drops
        // the ORDER BY of a sub-query whose order an outer ORDER BY or aggregation makes irrelevant to it, and then
        // there is no sort to keep, so the check reads the queries whose WINDOW JOIN keeps its ORDER BY.
        assertMemoryLeak(() -> {
            final ObjList<String> corpus = generateCorpus();
            int checked = 0;
            int compiled = 0;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (int i = 0, n = corpus.size(); i < n; i++) {
                    final String sql = corpus.getQuick(i);
                    if (!WINDOW_JOIN_SIDE.matcher(sql).find() || !isWindowJoinOrderByKept(compiler, sql)) {
                        continue;
                    }
                    checked++;
                    try (RecordCursorFactory ignore = select(sql)) {
                        compiled++;
                    } catch (SqlException e) {
                        final CharSequence message = e.getFlyweightMessage();
                        Assert.assertFalse(sql + "\n" + message, Chars.contains(message, "side of splice join doesn't support random access"));
                        Assert.assertFalse(sql + "\n" + message, Chars.contains(message, "of HORIZON JOIN can only be a table"));
                    }
                }
            }
            Assert.assertTrue(
                    "only " + compiled + " of " + checked + " SPLICE and HORIZON JOINs over a WINDOW JOIN side compiled",
                    compiled >= MIN_QUERIES_PER_SHAPE
            );
        });
    }

    private static void assertCorpusCount(ObjList<String> corpus, String join) {
        int count = 0;
        for (int i = 0, n = corpus.size(); i < n; i++) {
            final String sql = corpus.getQuick(i);
            if (sql.contains(join) && WINDOW_JOIN_SIDE.matcher(sql).find()) {
                count++;
            }
        }
        Assert.assertTrue(
                "the generator produced " + count + " queries with" + join + "over a WINDOW JOIN side in " + CORPUS_SIZE + " queries",
                count >= MIN_QUERIES_PER_SHAPE
        );
    }

    // Whether a WINDOW JOIN of the optimised query lost its ORDER BY.
    private static boolean hasWindowJoinWithoutOrderBy(IQueryModel model) {
        if (model.getSelectModelType() == IQueryModel.SELECT_MODEL_WINDOW_JOIN && model.getOrderHash().size() == 0) {
            return true;
        }
        final ObjList<IQueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            if (hasWindowJoinWithoutOrderBy(joinModels.getQuick(i))) {
                return true;
            }
        }
        return (model.getNestedModel() != null && hasWindowJoinWithoutOrderBy(model.getNestedModel()))
                || (model.getUnionModel() != null && hasWindowJoinWithoutOrderBy(model.getUnionModel()));
    }

    // Whether every WINDOW JOIN of the optimised query still carries its ORDER BY. A query the optimiser rejects never
    // reaches the joins.
    private static boolean isWindowJoinOrderByKept(SqlCompiler compiler, String sql) {
        final ExecutionModel executionModel;
        try {
            executionModel = compiler.generateExecutionModel(sql, sqlExecutionContext);
        } catch (SqlException e) {
            return false;
        }
        return executionModel instanceof IQueryModel model && !hasWindowJoinWithoutOrderBy(model);
    }

    // The corpus over the tables of a fixed-seed fuzz run.
    private ObjList<String> generateCorpus() throws SqlException {
        final Rnd rnd = new Rnd(0x5eed, 0x7041);
        final FuzzConfig config = new FuzzConfig(rnd);
        final FuzzTableFactory factory = new FuzzTableFactory(config);
        final ObjList<FuzzTable> tables = new ObjList<>();
        for (int i = 0; i < config.getNumTables(); i++) {
            tables.add(factory.create(rnd, "fuzz_t" + i, sql -> execute(sql), AbstractCairoTest::drainWalQueue));
        }
        final ObjList<String> corpus = new ObjList<>();
        for (int i = 0; i < CORPUS_SIZE; i++) {
            corpus.add(QueryGenerator.generate(rnd, tables, null, false, true, true, true, true, true, true, config).sql());
        }
        return corpus;
    }
}
