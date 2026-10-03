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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FunctionBinderHiddenColumnTest extends AbstractCairoTest {
    @Test
    public void testHiddenColumnsCannotBeNamedAndDoNotMakeVisibleNamesAmbiguous() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema()
                    .add(1, "ts", ColumnType.TIMESTAMP, null, false, "source")
                    .add(2, "id", ColumnType.INT, null, false, "source")
                    .add(3, "ID", ColumnType.INT, null, true, "source")
                    .add(4, "", ColumnType.TIMESTAMP, null, false, "source");
            final ObjList<String> references = new ObjList<>("ts", "source.ts", "\"ts\"", "source.\"ts\"", "\"\"", "source.\"\"");
            try (FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                for (int i = 0; i < references.size(); i++) {
                    final ExpressionNode reference = literal(references.getQuick(i));
                    final SqlException error = Assert.assertThrows(SqlException.class,
                            () -> binder.bind(reference, input, null, sqlExecutionContext));
                    Assert.assertEquals(13, error.getPosition());
                    TestUtils.assertContains(error.getFlyweightMessage(), "Invalid column:");
                }
                final BoundExpression expression = binder.bind(literal("source.id"), input, null, sqlExecutionContext);
                Assert.assertEquals(3, ((ColumnExpression) expression).getColumnId());
                try (Function function = binder.instantiate(expression, input, sqlExecutionContext)) {
                    binder.clear();
                    Assert.assertEquals(42, function.getInt(new Record() {
                        @Override
                        public int getInt(int columnIndex) {
                            Assert.assertEquals(2, columnIndex);
                            return 42;
                        }
                    }));
                }
            }
        });
    }

    @Test
    public void testInternalColumnIdentityStillBindsAndRelocatesHiddenTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final OutputSchema input = new OutputSchema()
                    .add(70, "", ColumnType.TIMESTAMP_NANO, false)
                    .add(80, "id", ColumnType.INT, true);
            final ExpressionNode node = literal("internal_timestamp");
            final ObjList<ExpressionNode> nodes = new ObjList<>(node);
            final ObjList<ColumnExpression> replacements = new ObjList<>(new ColumnExpression().of(70, ColumnType.TIMESTAMP_NANO, 13));
            try (FunctionBindingHarness binder = new FunctionBindingHarness(parser)) {
                final BoundExpression expression = binder.bind(node, input, null, nodes, replacements, sqlExecutionContext);
                Assert.assertEquals(ColumnType.TIMESTAMP_NANO, expression.getDataType());
                final OutputSchema layout = new OutputSchema()
                        .add(80, "id", ColumnType.INT, true)
                        .add(70, "", ColumnType.TIMESTAMP_NANO, false);
                try (Function function = binder.instantiate(expression, layout, sqlExecutionContext)) {
                    binder.clear();
                    input.clear();
                    layout.clear();
                    Assert.assertEquals(123_456_789L, function.getTimestamp(new Record() {
                        @Override
                        public long getTimestamp(int columnIndex) {
                            Assert.assertEquals(1, columnIndex);
                            return 123_456_789L;
                        }
                    }));
                }
            }
        });
    }

    private static ExpressionNode literal(String name) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, name, 0, 13);
    }
}
