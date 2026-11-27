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

import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.griffin.fuzz.clauses.HashJoinGroupByClause;
import io.questdb.test.griffin.fuzz.types.DoubleType;
import io.questdb.test.griffin.fuzz.types.IntKeyType;
import io.questdb.test.griffin.fuzz.types.IntType;
import io.questdb.test.griffin.fuzz.types.LongKeyType;
import io.questdb.test.griffin.fuzz.types.StringType;
import io.questdb.test.griffin.fuzz.types.SymbolType;
import io.questdb.test.griffin.fuzz.types.TimestampType;
import io.questdb.test.griffin.fuzz.types.VarcharKeyType;
import io.questdb.test.griffin.fuzz.types.VarcharType;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

/**
 * Pins the query shapes {@link HashJoinGroupByClause} has to keep producing for QueryFuzzTest, whose
 * fused axis compares each of them with the fused hash join GROUP BY on and off. QueryFuzzTest only
 * proves the queries it happens to draw, so a generator that stops filtering a build input over a
 * VARCHAR or STRING column would leave unchecked the build that runs a filter the JIT does not
 * compile in rounds, on the workers, however few rows it holds.
 * <p>
 * The generator is random, so each assertion runs over a corpus; the seed is fixed so a failure is
 * reproducible.
 */
public class HashJoinGroupByShapeCoverageTest {
    private static final int CORPUS_SIZE = 4_000;

    @Test
    public void testCorpusCoversBuildFiltersOverVarSizeColumns() {
        final Rnd rnd = new Rnd(0x5eed, 0x4a6b);
        final FuzzTable left = table("t0");
        final FuzzTable right = table("t1");
        // The FILL shapes read the config's table sizes; its own Rnd leaves the generator's draws
        // as they are.
        final FuzzConfig config = new FuzzConfig(new Rnd());
        // The VARCHAR and STRING columns that are no join key, under their own names or a
        // projection's alias.
        final Pattern varSize = Pattern.compile("\\b(?:c2|c3)(?:_v)?\\b");
        int whereConjuncts = 0;
        int projections = 0;
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final String sql = HashJoinGroupByClause.generate(rnd, left, right, null, false, config).sql();
            // An INNER join's WHERE conjunct over its right input alone, which the optimiser hands
            // to the input's scan filter, or a filtered projection that a LEFT join builds.
            if (sql.contains(" r JOIN ") || sql.contains(" l JOIN ")) {
                final int where = sql.lastIndexOf(" WHERE (");
                if (where > 0) {
                    for (String conjunct : sql.substring(where + 7).split(" AND ")) {
                        if (conjunct.contains("r.") && !conjunct.contains("l.") && varSize.matcher(conjunct).find()) {
                            whereConjuncts++;
                            break;
                        }
                    }
                }
            }
            final int join = sql.indexOf(" l LEFT JOIN (SELECT ");
            if (join > 0) {
                final int open = join + " l LEFT JOIN ".length();
                final String input = sql.substring(open, sql.indexOf(") r ON ", open));
                final int where = input.indexOf(" WHERE ");
                if (where > 0 && varSize.matcher(input.substring(where)).find()) {
                    projections++;
                }
            }
        }
        final String counts = "INNER WHERE conjuncts " + whereConjuncts + ", LEFT build projections " + projections;
        Assert.assertTrue(counts, whereConjuncts > 0);
        Assert.assertTrue(counts, projections > 0);
    }

    private static FuzzTable table(String name) {
        final ObjList<FuzzColumn> columns = new ObjList<>();
        columns.add(new FuzzColumn("sym", SymbolType.INSTANCE));
        columns.add(new FuzzColumn("k", IntKeyType.INSTANCE));
        columns.add(new FuzzColumn("lk", LongKeyType.INSTANCE));
        columns.add(new FuzzColumn("vk", VarcharKeyType.INSTANCE));
        columns.add(new FuzzColumn("c0", IntType.INSTANCE));
        columns.add(new FuzzColumn("c1", DoubleType.INSTANCE));
        columns.add(new FuzzColumn("c2", VarcharType.INSTANCE));
        columns.add(new FuzzColumn("c3", StringType.INSTANCE));
        columns.add(new FuzzColumn("ts", TimestampType.INSTANCE));
        return new FuzzTable(name, columns, "ts");
    }
}
