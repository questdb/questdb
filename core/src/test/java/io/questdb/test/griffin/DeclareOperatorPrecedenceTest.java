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

import io.questdb.PropertyKey;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class DeclareOperatorPrecedenceTest extends AbstractCairoTest {
    private final boolean isLegacyPrecedence;

    public DeclareOperatorPrecedenceTest(boolean isLegacyPrecedence) {
        this.isLegacyPrecedence = isLegacyPrecedence;
    }

    @Parameterized.Parameters(name = "legacy={0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    @Before
    public void setPrecedence() {
        setProperty(PropertyKey.CAIRO_SQL_LEGACY_OPERATOR_PRECEDENCE, Boolean.toString(isLegacyPrecedence));
    }

    @Test
    public void testDeclareAssignmentPrecedence() throws Exception {
        assertMemoryLeak(() -> {
            // A private compiler captures this method's mode, never a previous pool tenant's mode.
            try (SqlCompiler compiler = new SqlCompilerImpl(engine)) {
                assertQuery("SELECT true OR false AND false AS value").withCompiler(compiler)
                        .expectSize().returns("value\n" + !isLegacyPrecedence + "\n");
                assertQuery("DECLARE @a := true OR false AND false, @b := @a SELECT @b AS value")
                        .withCompiler(compiler).expectSize().returns("value\n" + !isLegacyPrecedence + "\n");
            }
        });
    }

    @Test
    public void testDeclareBoundVariableInSubquery() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setInt("value", 7);
            try (SqlCompiler compiler = new SqlCompilerImpl(engine)) {
                assertQuery("DECLARE @a := :value SELECT * FROM (SELECT @a AS value)")
                        .withCompiler(compiler).expectSize().returns("value\n7\n");
            }
        });
    }

    @Test
    public void testDeclareNull() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = new SqlCompilerImpl(engine)) {
                assertQuery("DECLARE @a := NULL::INT SELECT @a AS value")
                        .withCompiler(compiler).expectSize().returns("value\nnull\n");
            }
        });
    }

    @Test
    public void testDeclareUndefinedBindAndCompilerReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = new SqlCompilerImpl(engine)) {
                for (int i = 0; i < 3; i++) {
                    assertQuery("DECLARE @a := :missing SELECT @a").withCompiler(compiler)
                            .fails(14, "undefined bind variable: :missing");
                    assertQuery("DECLARE @a := 7 SELECT @a AS value").withCompiler(compiler)
                            .expectSize().returns("value\n7\n");
                }
            }
        });
    }

    @Test
    public void testDeclareUndefinedBindInsideSubquery() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = new SqlCompilerImpl(engine)) {
                assertQuery("DECLARE @a := :missing SELECT * FROM (SELECT @a)").withCompiler(compiler)
                        .fails(14, "undefined bind variable: :missing");
            }
        });
    }

    @Test
    public void testDeclareInvalidAssignmentAndCompilerReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = new SqlCompilerImpl(engine)) {
                assertQuery("DECLARE @a = 1 SELECT @a").withCompiler(compiler)
                        .fails(11, "expected variable assignment operator");
                assertQuery("DECLARE @a :=").withCompiler(compiler)
                        .fails(11, "too few arguments for ':='");
                assertQuery("DECLARE @a := (1, 2) SELECT @a").withCompiler(compiler)
                        .fails(21, "bracket lists are not supported");
                assertQuery("SELECT 7 AS value").withCompiler(compiler).expectSize().returns("value\n7\n");
            }
        });
    }
}
