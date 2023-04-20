/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.cairo.PartitionBy;
import io.questdb.test.cairo.TableModel;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

public class AbstractSqlParserTest extends AbstractGriffinTest {
    private static void assertSyntaxError(
            SqlCompiler compiler,
            String query,
            int position,
            String contains,
            TableModel... tableModels
    ) throws Exception {
        try {
            assertMemoryLeak(() -> {
                try {
                    for (int i = 0, n = tableModels.length; i < n; i++) {
                        CreateTableTestUtils.create(tableModels[i]);
                    }
                    compiler.compile(query, sqlExecutionContext);
                    Assert.fail("Exception expected");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), contains);
                    Assert.assertEquals("position", position, e.getPosition());
                }
            });
        } finally {
            for (int i = 0, n = tableModels.length; i < n; i++) {
                TableModel tableModel = tableModels[i];
                TableToken tableToken = engine.verifyTableName(tableModel.getName());
                Path path = tableModel.getPath().of(tableModel.getConfiguration().getRoot()).concat(tableToken).slash$();
                configuration.getFilesFacade().rmdir(path);
                tableModel.close();
            }
        }
    }

    private static void checkLiteralIsInSet(
            ExpressionNode node,
            ObjList<LowerCaseCharSequenceHashSet> nameSets,
            LowerCaseCharSequenceIntHashMap modelAliasSet
    ) {
        if (node.type == ExpressionNode.LITERAL) {
            final CharSequence tok = node.token;
            final int dot = Chars.indexOf(tok, '.');
            if (dot == -1) {
                boolean found = false;
                for (int i = 0, n = nameSets.size(); i < n; i++) {
                    boolean f = nameSets.getQuick(i).contains(tok);
                    if (f) {
                        Assert.assertFalse(found);
                        found = true;
                    }
                }
                if (!found) {
                    Assert.fail("column: " + tok);
                }
            } else {
                int index = modelAliasSet.keyIndex(tok, 0, dot);
                Assert.assertTrue(index < 0);
                LowerCaseCharSequenceHashSet set = nameSets.getQuick(modelAliasSet.valueAt(index));
                Assert.assertFalse(set.excludes(tok, dot + 1, tok.length()));
            }
        } else {
            if (node.paramCount < 3) {
                if (node.lhs != null) {
                    AbstractSqlParserTest.checkLiteralIsInSet(node.lhs, nameSets, modelAliasSet);
                }

                if (node.rhs != null) {
                    AbstractSqlParserTest.checkLiteralIsInSet(node.rhs, nameSets, modelAliasSet);
                }
            } else {
                for (int j = 0, k = node.args.size(); j < k; j++) {
                    AbstractSqlParserTest.checkLiteralIsInSet(node.args.getQuick(j), nameSets, modelAliasSet);
                }
            }
        }
    }

    private void assertColumnNames(SqlCompiler compiler, String query, String... columns) throws SqlException {
        CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
        try (RecordCursorFactory factory = cc.getRecordCursorFactory()) {
            RecordMetadata metadata = factory.getMetadata();
            for (int idx = 0; idx < columns.length; idx++) {
                TestUtils.assertEquals(metadata.getColumnName(idx), columns[idx]);
            }
        }
    }

    private void createModelsAndRun(SqlParserTest.CairoAware runnable, TableModel... tableModels) throws SqlException {
        try {
            for (int i = 0, n = tableModels.length; i < n; i++) {
                CreateTableTestUtils.create(tableModels[i]);
            }
            runnable.run();
        } finally {
            Assert.assertTrue(engine.releaseAllReaders());
            FilesFacade filesFacade = configuration.getFilesFacade();
            for (int i = 0, n = tableModels.length; i < n; i++) {
                TableModel tableModel = tableModels[i];
                TableToken tableToken = engine.verifyTableName(tableModel.getName());
                Path path = tableModel.getPath().of(tableModel.getConfiguration().getRoot()).concat(tableToken).slash$();
                Assert.assertEquals(0, filesFacade.rmdir(path));
                tableModel.close();
            }
            engine.reloadTableNames();
        }
    }

    private void validateTopDownColumns(QueryModel model) {
        ObjList<QueryColumn> columns = model.getColumns();
        final ObjList<LowerCaseCharSequenceHashSet> nameSets = new ObjList<>();

        QueryModel nested = model.getNestedModel();
        while (nested != null) {
            nameSets.clear();

            for (int i = 0, n = nested.getJoinModels().size(); i < n; i++) {
                LowerCaseCharSequenceHashSet set = new LowerCaseCharSequenceHashSet();
                final QueryModel m = nested.getJoinModels().getQuick(i);
                final ObjList<QueryColumn> cols = m.getTopDownColumns();
                for (int j = 0, k = cols.size(); j < k; j++) {
                    QueryColumn qc = cols.getQuick(j);
                    Assert.assertTrue(set.add(qc.getName()));
                }
                nameSets.add(set);
            }

            for (int i = 0, n = columns.size(); i < n; i++) {
                AbstractSqlParserTest.checkLiteralIsInSet(columns.getQuick(i).getAst(), nameSets, nested.getModelAliasIndexes());
            }

            columns = nested.getTopDownColumns();
            nested = nested.getNestedModel();
        }
    }

    protected static void assertSyntaxError(String query, int position, String contains, TableModel... tableModels) throws Exception {
        refreshTablesInBaseEngine();
        AbstractSqlParserTest.assertSyntaxError(compiler, query, position, contains, tableModels);
    }

    protected static TableModel modelOf(String tableName) {
        return new TableModel(configuration, tableName, PartitionBy.NONE);
    }

    protected void assertColumnNames(String query, String... columns) throws SqlException {
        assertColumnNames(compiler, query, columns);
    }

    protected void assertInsertQuery(TableModel... tableModels) throws SqlException {
        assertModel(
                "insert into test (test_timestamp, test_value) values (cast('2020-12-31 15:15:51.663+00:00',timestamp), '256')",
                "insert into test (test_timestamp, test_value) values (timestamp with time zone '2020-12-31 15:15:51.663+00:00', '256')",
                ExecutionModel.INSERT,
                tableModels
        );
    }

    protected void assertModel(String expected, String query, int modelType, TableModel... tableModels) throws SqlException {
        createModelsAndRun(() -> {
            sink.clear();
            ExecutionModel model = compiler.testCompileModel(query, sqlExecutionContext);
            Assert.assertEquals(model.getModelType(), modelType);
            ((Sinkable) model).toSink(sink);
            TestUtils.assertEquals(expected, sink);
            if (model instanceof QueryModel && model.getModelType() == ExecutionModel.QUERY) {
                validateTopDownColumns((QueryModel) model);
            }
        }, tableModels);
    }

    protected void assertQuery(String expected, String query, TableModel... tableModels) throws SqlException {
        assertModel(expected, query, ExecutionModel.QUERY, tableModels);
    }

    protected void assertUpdate(String expected, String query, TableModel... tableModels) throws SqlException {
        assertModel(expected, query, ExecutionModel.UPDATE, tableModels);
    }
}
