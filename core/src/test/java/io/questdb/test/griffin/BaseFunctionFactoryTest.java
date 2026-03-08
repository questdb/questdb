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

import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.*;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;

import java.util.ArrayList;

public class BaseFunctionFactoryTest extends AbstractCairoTest {
    protected static final ArrayList<FunctionFactory> functions = new ArrayList<>();
    protected final static QueryModel queryModel = QueryModel.FACTORY.newInstance();

    @Before
    public void setUp4() {
        functions.clear();
    }

    protected static ExpressionNode expr(CharSequence expression) throws SqlException {
        queryModel.clear();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            return compiler.testParseExpression(expression, queryModel);
        }
    }

    protected static Function parseFunction(CharSequence expression, GenericRecordMetadata metadata, FunctionParser functionParser) throws SqlException {
        return functionParser.parseFunction(expr(expression), metadata, sqlExecutionContext);
    }

    @NotNull
    protected FunctionParser createFunctionParser() {
        return new FunctionParser(configuration, new FunctionFactoryCache(configuration, functions));
    }
}
