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

package io.questdb.test.griffin.engine.functions;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class InCharTest extends AbstractCairoTest {

    @Test
    public void testBindVarTypedCastInList() throws Exception {
        // Regression: InCharFunctionFactory used to read every list element
        // at compile time via getChar(null) / getStrA(null). A bind variable
        // wrapped in ::CHAR was not yet bound at that point, so
        // NamedParameterLinkFunction.getBase()'s assertion fired. The list
        // now defers runtime-constants to init() and rebuilds the set there.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c CHAR)");
            execute("INSERT INTO t VALUES ('Y'), ('I'), ('Z')");
            bindVariableService.clear();
            bindVariableService.setStr("b0", "Z");
            assertQuery("SELECT c FROM t WHERE c IN ('Y', 'I', :b0::CHAR) ORDER BY 1")
                    .noLeakCheck()
                    .returns("c\nI\nY\nZ\n");
            // Bind value that doesn't appear in the data should not match,
            // verifying the deferred set is rebuilt per init().
            bindVariableService.clear();
            bindVariableService.setStr("b0", "Q");
            assertQuery("SELECT c FROM t WHERE c IN ('Y', 'I', :b0::CHAR) ORDER BY 1")
                    .noLeakCheck()
                    .returns("c\nI\nY\n");
        });
    }

    @Test
    public void testBindVarLiteralDivergenceForNulChar() throws Exception {
        // Regression: query fuzzer caught literal vs bind divergence on
        //   (0.49)::CHAR IN ((0.49)::CHAR, 'Z')
        // The literal form folds the LHS to a constant CHAR(0), and the dispatcher
        // picks InStrFunctionFactory because the CHAR constant matches in(Sv) with
        // implicit cast. InStrFunctionFactory wraps the LHS in CastCharToStr, which
        // maps CHAR(0) to NULL string, but parseToString used to add CHAR(0) list
        // elements as the 1-char NUL string; the asymmetric handling made the WHERE
        // evaluate to false, while the bind form (where the LHS is a runtime
        // constant and InCharFunctionFactory wins) correctly returned true.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT)");
            execute("INSERT INTO t VALUES (1), (2)");
            // Literal form: fold-time constant CHAR(0) IN list with CHAR(0) and 'Z'.
            assertQuery("SELECT k FROM t WHERE (0.49)::CHAR IN ((0.49)::CHAR, 'Z')")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("k\n1\n2\n");
            // Bind form: same query with the LHS routed through a bind variable.
            bindVariableService.clear();
            bindVariableService.setStr("b0", "0.49");
            assertQuery("SELECT k FROM t WHERE (:b0::DOUBLE)::CHAR IN ((0.49)::CHAR, 'Z')")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("k\n1\n2\n");
            // CHAR(0) should not match a list with no NUL element.
            assertQuery("SELECT k FROM t WHERE (0.49)::CHAR IN ('Z', 'X')")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("k\n");
        });
    }

    @Test
    public void testBindVarTypedCastInListMixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c CHAR)");
            execute("INSERT INTO t VALUES ('Y'), ('I'), ('Z')");
            bindVariableService.clear();
            bindVariableService.setStr("b0", "Y");
            bindVariableService.setStr("b1", "Z");
            assertQuery("SELECT c FROM t WHERE c IN ('I', :b0::CHAR, :b1::STRING) ORDER BY 1")
                    .noLeakCheck()
                    .returns("c\nI\nY\nZ\n");
        });
    }
}
