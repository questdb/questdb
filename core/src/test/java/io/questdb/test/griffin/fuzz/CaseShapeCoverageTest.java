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

import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import io.questdb.test.griffin.fuzz.expr.ExpressionGenerator;
import io.questdb.test.griffin.fuzz.expr.FuzzExpr;
import io.questdb.test.griffin.fuzz.types.BooleanType;
import io.questdb.test.griffin.fuzz.types.DateType;
import io.questdb.test.griffin.fuzz.types.DoubleType;
import io.questdb.test.griffin.fuzz.types.IntType;
import io.questdb.test.griffin.fuzz.types.LongType;
import io.questdb.test.griffin.fuzz.types.TimestampType;
import io.questdb.test.griffin.fuzz.types.VarcharType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

/**
 * Pins the CASE, extract() and cast() shapes {@link ExpressionGenerator} spells, and checks
 * that the parser accepts their keyword spelling exactly when it accepts their plain spelling.
 * The spelling axis in {@link QueryRunner} checks the same on whole queries, and their rows.
 * The shapes are the ones the parser once rejected inside a CASE: FROM in
 * {@code extract(day FROM ts)} and the multi-word type of {@code cast(x AS DOUBLE PRECISION)}
 * read as CASE keywords, and the function-call operand of {@code CASE abs(x) WHEN ...} taken
 * for the searched form.
 */
public class CaseShapeCoverageTest extends AbstractCairoTest {
    private static final int CORPUS_SIZE = 5_000;
    private static final Pattern KEYWORD_FORM = Pattern.compile("CASE [^W]| FROM | AS ");
    private static final Pattern[] SHAPES = {
            Pattern.compile("CASE .*extract\\([a-z]+ FROM "),
            Pattern.compile("CASE (abs|length|extract|cast)\\("),
            Pattern.compile("CASE -[0-9]+ WHEN "),
            Pattern.compile("CASE null WHEN "),
            Pattern.compile("CASE .*cast\\(.* AS DOUBLE PRECISION\\)"),
            Pattern.compile("CASE .*cast\\(.* AS TIMESTAMP WITH TIME ZONE\\)"),
    };

    @Test
    public void testKeywordAndPlainSpellingsAgree() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        c_int INT, c_long LONG, c_double DOUBLE, c_varchar VARCHAR,
                        c_bool BOOLEAN, c_date DATE, ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final ObjList<FuzzColumn> columns = new ObjList<>();
            columns.add(new FuzzColumn("c_int", IntType.INSTANCE));
            columns.add(new FuzzColumn("c_long", LongType.INSTANCE));
            columns.add(new FuzzColumn("c_double", DoubleType.INSTANCE));
            columns.add(new FuzzColumn("c_varchar", VarcharType.INSTANCE));
            columns.add(new FuzzColumn("c_bool", BooleanType.INSTANCE));
            columns.add(new FuzzColumn("c_date", DateType.INSTANCE));
            columns.add(new FuzzColumn("ts", TimestampType.INSTANCE));

            final Rnd rnd = TestUtils.generateRandom(LOG);
            final ExpressionGenerator gen = new ExpressionGenerator(rnd, columns, null, 3);
            final StringSink keyword = new StringSink();
            final StringSink plain = new StringSink();
            final int[] shapeCounts = new int[SHAPES.length];
            int spelledTwice = 0;
            int bothCompiled = 0;
            for (int i = 0; i < CORPUS_SIZE; i++) {
                final FuzzExpr expr = gen.generateAnyKind();
                keyword.clear();
                expr.appendSql(keyword);
                plain.clear();
                expr.appendSql(plain, BindContext.plainSpelling());
                // the plain spelling has no simple CASE, no extract FROM and no cast AS
                Assert.assertFalse("plain spelling kept a keyword form: " + plain,
                        KEYWORD_FORM.matcher(plain).find());
                for (int s = 0; s < SHAPES.length; s++) {
                    if (SHAPES[s].matcher(keyword).find()) {
                        shapeCounts[s]++;
                    }
                }
                if (keyword.toString().equals(plain.toString())) {
                    continue;
                }
                spelledTwice++;
                final String keywordError = compileError("SELECT " + keyword + " FROM t");
                final String plainError = compileError("SELECT " + plain + " FROM t");
                Assert.assertEquals(
                        "keyword spelling: " + keyword + "\nplain spelling: " + plain
                                + "\nkeyword error: " + keywordError + "\nplain error: " + plainError,
                        plainError == null,
                        keywordError == null
                );
                if (keywordError == null) {
                    bothCompiled++;
                }
            }
            for (int s = 0; s < SHAPES.length; s++) {
                Assert.assertTrue("the generator produced no " + SHAPES[s] + " in " + CORPUS_SIZE + " expressions",
                        shapeCounts[s] > 0);
            }
            Assert.assertTrue("only " + bothCompiled + " of " + spelledTwice + " expressions spelled two ways compiled",
                    bothCompiled * 2 > spelledTwice);
        });
    }

    // constant folding can fail a cast at compile time, as in '2024::DECIMAL(2, 1)'
    private static String compileError(String sql) {
        try (RecordCursorFactory ignore = select(sql)) {
            return null;
        } catch (SqlException | ImplicitCastException e) {
            return e.getFlyweightMessage().toString();
        }
    }
}
