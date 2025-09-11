/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

public class AbstractSqlParserTest extends AbstractCairoTest {

    private static void assertSyntaxError0(
            String query,
            int position,
            String contains,
            TableModel... tableModels
    ) throws Exception {
        try {
            assertMemoryLeak(() -> {
                for (int i = 0, n = tableModels.length; i < n; i++) {
                    AbstractCairoTest.create(tableModels[i]);
                }
                assertExceptionNoLeakCheck(query, position, contains, false);
            });
        } finally {
            try (Path path = new Path()) {
                for (int i = 0, n = tableModels.length; i < n; i++) {
                    TableModel tableModel = tableModels[i];
                    TableToken tableToken = engine.verifyTableName(tableModel.getName());
                    path.of(tableModel.getConfiguration().getDbRoot()).concat(tableToken).slash$();
                    configuration.getFilesFacade().rmdir(path);
                }
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
                        Assert.assertFalse("ambiguous column: " + tok, found);
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

    private void addColumnToNameSets(ObjList<LowerCaseCharSequenceHashSet> nameSets, CharSequence columnName) {
        // Add column to name set 0 (it always exists, we are assuming this column can be referenced by the projection)
        // unless that is, column already exists in one of the sets. If we don't check column existence, it might
        // cause "ambiguous" column error.
        boolean found = false;
        for (int i = 0, n = nameSets.size(); i < n; i++) {
            if (nameSets.getQuick(i).contains(columnName)) {
                found = true;
                break;
            }
        }
        if (!found) {
            nameSets.getQuick(0).add(columnName);
        }
    }

    protected static void assertSyntaxError(String query, int position, String contains, TableModel... tableModels) throws Exception {
        refreshTablesInBaseEngine();
        assertSyntaxError0(query, position, contains, tableModels);
    }

    protected static TableModel modelOf(String tableName) {
        return new TableModel(configuration, tableName, PartitionBy.NONE);
    }

    protected void assertColumnNames(String query, String... columns) throws SqlException {
        try (RecordCursorFactory factory = select(query)) {
            RecordMetadata metadata = factory.getMetadata();
            for (int idx = 0; idx < columns.length; idx++) {
                TestUtils.assertEquals(metadata.getColumnName(idx), columns[idx]);
            }
        }
    }

    protected void assertInsertQuery(TableModel... tableModels) throws SqlException {
        assertModel(
                "insert into test (test_timestamp, test_value) values ('2020-12-31 15:15:51.663+00:00'::timestamp, '256')",
                "insert into test (test_timestamp, test_value) values (timestamp with time zone '2020-12-31 15:15:51.663+00:00', '256')",
                ExecutionModel.INSERT,
                tableModels
        );
    }

    protected void assertModel(String expected, String query, int modelType, TableModel... tableModels) throws SqlException {
        createModelsAndRun(
                () -> {
                    sink.clear();
                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        ExecutionModel model = compiler.testCompileModel(query, sqlExecutionContext);
                        Assert.assertEquals(model.getModelType(), modelType);
                        ((Sinkable) model).toSink(sink);
                        TestUtils.assertEquals(expected, sink);
                        if (model instanceof QueryModel && model.getModelType() == ExecutionModel.QUERY) {
                            validateTopDownColumns((QueryModel) model);
                        }
                    }
                },
                tableModels
        );
    }

    protected void assertQuery(String expected, String query, TableModel... tableModels) throws SqlException {
        assertModel(expected, query, ExecutionModel.QUERY, tableModels);
    }

    protected void assertUpdate(String expected, String query, TableModel... tableModels) throws SqlException {
        assertModel(expected, query, ExecutionModel.UPDATE, tableModels);
    }

    protected void assertCreate(String expected, String query) throws SqlException {
        assertModel(expected, query, ExecutionModel.CREATE_TABLE);
    }

    protected void createModelsAndRun(SqlParserTest.CairoAware runnable, TableModel... tableModels) throws SqlException {
        try {
            for (int i = 0, n = tableModels.length; i < n; i++) {
                AbstractCairoTest.create(tableModels[i]);
            }
            runnable.run();
        } finally {
            Assert.assertTrue(engine.releaseAllReaders());
            FilesFacade filesFacade = configuration.getFilesFacade();
            try (Path path = new Path()) {
                for (int i = 0, n = tableModels.length; i < n; i++) {
                    TableModel tableModel = tableModels[i];
                    TableToken tableToken = engine.verifyTableName(tableModel.getName());
                    path.of(tableModel.getConfiguration().getDbRoot()).concat(tableToken).slash$();
                    Assert.assertTrue(filesFacade.rmdir(path));
                }
            }
            engine.reloadTableNames();
        }
    }

    protected void validateTopDownColumns(QueryModel model) {
        ObjList<QueryColumn> columns = model.getColumns();
        final ObjList<LowerCaseCharSequenceHashSet> nameSets = new ObjList<>();

        QueryModel nested = model.getNestedModel();
        while (nested != null) {
            nameSets.clear();

            for (int i = 0, n = nested.getJoinModels().size(); i < n; i++) {
                LowerCaseCharSequenceHashSet set = new LowerCaseCharSequenceHashSet();
                final QueryModel m = nested.getJoinModels().getQuick(i);
                // validate uniqueness of top-down column names.
                final ObjList<QueryColumn> cols = m.getTopDownColumns();
                for (int j = 0, k = cols.size(); j < k; j++) {
                    Assert.assertTrue(set.add(cols.getQuick(j).getName()));
                }
                nameSets.add(set);
            }

            for (int i = 0, n = columns.size(); i < n; i++) {
                AbstractSqlParserTest.checkLiteralIsInSet(columns.getQuick(i).getAst(), nameSets, nested.getModelAliasIndexes());
                addColumnToNameSets(nameSets, columns.getQuick(i).getName());
            }

            columns = nested.getTopDownColumns();
            nested = nested.getNestedModel();
        }
    }
}
