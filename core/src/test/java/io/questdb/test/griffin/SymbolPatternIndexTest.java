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

package io.questdb.test.griffin;

import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.regex.SymbolKeySetProvider;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.IntList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SymbolPatternIndexTest extends AbstractCairoTest {

    /**
     * Compiles {@code predicate} (e.g. {@code "sym like 'A%'"}) as a standalone
     * boolean function bound to table {@code t}'s reader, then returns the matched
     * symbol keys that the fast index path would use.
     * <p>
     * Mechanism:
     * <ol>
     *   <li>Open a {@link TableReader} for table {@code t}.</li>
     *   <li>Build a {@link GenericRecordMetadata} copy of the reader's metadata so
     *       that the {@code sym} column carries {@code isSymbolTableStatic=true}.</li>
     *   <li>Parse the expression via {@link FunctionParser} backed by the engine's
     *       full {@link io.questdb.griffin.FunctionFactoryCache}.</li>
     *   <li>Call {@code f.init(reader, sqlExecutionContext)} — {@link TableReader}
     *       implements {@link io.questdb.cairo.sql.SymbolTableSource}, so this
     *       gives the function access to the real symbol table.</li>
     *   <li>Assert the compiled function implements {@link SymbolKeySetProvider},
     *       then read and return its key list.</li>
     * </ol>
     * Using this approach the test directly exercises the provider interface rather
     * than running an end-to-end SQL query (which would pass regardless of this
     * interface because the per-row filter path already works).
     */
    private IntList matchedKeys(String predicate) throws Exception {
        try (TableReader reader = engine.getReader("t")) {
            // Copy metadata so FunctionParser sees the real symbolTableStatic flag
            GenericRecordMetadata meta = GenericRecordMetadata.copyOf(reader.getMetadata());

            // Build a parser backed by the full engine factory cache (includes LIKE/~ factories)
            FunctionParser functionParser = new FunctionParser(configuration, engine.getFunctionFactoryCache());

            // Parse the expression AST
            ExpressionNode node;
            QueryModel qm = QueryModel.FACTORY.newInstance();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                node = compiler.testParseExpression(predicate, qm);
            }

            // Compile to a Function; may throw if predicate is malformed
            Function f = functionParser.parseFunction(node, meta, sqlExecutionContext);

            Assert.assertTrue(
                    predicate + " did not compile to a SymbolKeySetProvider: " + f.getClass().getName(),
                    f instanceof SymbolKeySetProvider
            );

            // Bind the function to the reader's static symbol table
            // TableReader implements SymbolTableSource directly
            f.init(reader, sqlExecutionContext);

            IntList out = new IntList();
            out.addAll(((SymbolKeySetProvider) f).getMatchedSymbolKeys());
            f.close();
            return out;
        }
    }

    @Test
    public void testConfigDefaults() {
        Assert.assertTrue(configuration.isSymbolPatternIndexEnabled());
        Assert.assertEquals(100, configuration.getSymbolPatternIndexThreshold());
    }

    @Test
    public void testProviderExposesStartsWithKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index, ts timestamp) timestamp(ts)");
            execute("insert into t values ('AA', 0::timestamp),('AB',1::timestamp),('BA',2::timestamp),('BB',3::timestamp)");
            // symbol keys are assigned in insertion order: AA=0, AB=1, BA=2, BB=3
            IntList keys = matchedKeys("sym like 'A%'");
            Assert.assertEquals("[0,1]", keys.toString());
        });
    }
}
