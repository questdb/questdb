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

package io.questdb.test.griffin.model;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.CompileViewModel;
import io.questdb.griffin.model.CreateTableColumnModel;
import io.questdb.griffin.model.ExplainModel;
import io.questdb.griffin.model.ExportModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.InsertModel;
import io.questdb.griffin.model.IntrinsicModel;
import io.questdb.griffin.model.JoinContext;
import io.questdb.griffin.model.PivotForColumn;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.RenameTableModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.model.WindowJoinContext;
import io.questdb.griffin.model.WithClauseModel;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Tests that mutable model classes properly reset all fields in their clear() method.
 * Uses reflection to compare cleared instances against fresh instances.
 */
public class MutableModelsTest {

    @Test
    public void testCompileViewModelClear() {
        CompileViewModel model = CompileViewModel.FACTORY.newInstance();
        model.setTableNameExpr(newExpressionNode());
        model.setQueryModel(QueryModel.FACTORY.newInstance());
        assertDifferentFromFresh(model, CompileViewModel.FACTORY.newInstance());

        model.clear();
        assertFieldsEqual(CompileViewModel.FACTORY.newInstance(), model);
    }

    @Test
    public void testCreateTableColumnModelClear() {
        CreateTableColumnModel model = CreateTableColumnModel.FACTORY.newInstance();
        model.setColumnNamePos(10);
        model.setColumnType(ColumnType.STRING);
        model.setCastType(ColumnType.INT, 20);
        model.setIndexType(IndexType.SYMBOL, 30, 256);
        model.setIsDedupKey();
        model.setSymbolCacheFlag(true);
        model.setSymbolCapacity(1024);
        assertDifferentFromFresh(model, CreateTableColumnModel.FACTORY.newInstance());

        model.clear();
        assertFieldsEqual(CreateTableColumnModel.FACTORY.newInstance(), model);
    }

    @Test
    public void testExplainModelClear() {
        ExplainModel model = ExplainModel.FACTORY.newInstance();
        model.setFormat(ExplainModel.FORMAT_JSON);
        model.setModel(QueryModel.FACTORY.newInstance());
        assertDifferentFromFresh(model, ExplainModel.FACTORY.newInstance());

        model.clear();
        assertFieldsEqual(ExplainModel.FACTORY.newInstance(), model);
    }

    @Test
    public void testExportModelClear() {
        ExportModel model = ExportModel.FACTORY.newInstance();
        model.setTarget(newExpressionNode());
        model.setFileName(newExpressionNode());
        model.setHeader(true);
        model.setCancel(true);
        model.setTimestampFormat("yyyy-MM-dd");
        model.setTimestampColumnName("ts");
        model.setPartitionBy(1);
        model.setSelectText("select * from t", 10);
        model.setDelimiter((byte) ',');
        model.setAtomicity(1);
        model.setType(ExportModel.COPY_TYPE_TO);
        model.setFormat(ExportModel.COPY_FORMAT_PARQUET);
        model.setCompressionCodec(1);
        model.setCompressionLevel(5, 20);
        model.setRowGroupSize(1000);
        model.setDataPageSize(4096);
        model.setParquetVersion(ExportModel.PARQUET_VERSION_V2);
        model.setStatisticsEnabled(false);
        model.setRawArrayEncoding(true);
        model.setNoDelayTo(true);
        assertDifferentFromFresh(model, ExportModel.FACTORY.newInstance());

        model.clear();
        assertFieldsEqual(ExportModel.FACTORY.newInstance(), model);
    }

    @Test
    public void testExpressionNodeClear() {
        ExpressionNode node = ExpressionNode.FACTORY.newInstance();
        node.args.add(ExpressionNode.FACTORY.newInstance());
        node.token = "test";
        node.precedence = 10;
        node.position = 20;
        node.lhs = ExpressionNode.FACTORY.newInstance();
        node.rhs = ExpressionNode.FACTORY.newInstance();
        node.type = ExpressionNode.FUNCTION;
        node.paramCount = 3;
        node.intrinsicValue = 42;
        node.queryModel = QueryModel.FACTORY.newInstance();
        node.innerPredicate = true;
        node.implemented = true;
        assertDifferentFromFresh(node, ExpressionNode.FACTORY.newInstance());

        node.clear();
        assertFieldsEqual(ExpressionNode.FACTORY.newInstance(), node);
    }

    @Test
    public void testInsertModelClear() throws SqlException {
        InsertModel model = InsertModel.FACTORY.newInstance();
        model.setTableName(newExpressionNode());
        model.setQueryModel(QueryModel.FACTORY.newInstance());
        model.addColumn("col1", 10);
        model.addColumn("col2", 20);
        ObjList<ExpressionNode> row = new ObjList<>();
        row.add(newExpressionNode());
        model.addRowTupleValues(row);
        model.addEndOfRowTupleValuesPosition(30);
        model.setSelectKeywordPosition(40);
        model.setBatchSize(1000);
        model.setO3MaxLag(5000);
        assertDifferentFromFresh(model, InsertModel.FACTORY.newInstance());

        model.clear();
        assertFieldsEqual(InsertModel.FACTORY.newInstance(), model);
    }

    @Test
    public void testIntrinsicModelClear() {
        IntrinsicModel model = IntrinsicModel.FACTORY.newInstance();
        model.keyColumn = "symbol";
        model.filter = newExpressionNode();
        model.intrinsicValue = IntrinsicModel.TRUE;
        model.keySubQuery = QueryModel.FACTORY.newInstance();
        model.keyExcludedNodes.add(newExpressionNode());
        // Note: keyValueFuncs and keyExcludedValueFuncs require Function instances
        // which are harder to create, but setting other fields is sufficient
        // to verify the clear() method works correctly
        assertDifferentFromFresh(model, IntrinsicModel.FACTORY.newInstance());

        model.clear();
        assertFieldsEqual(IntrinsicModel.FACTORY.newInstance(), model);
    }

    @Test
    public void testJoinContextClear() {
        JoinContext ctx = JoinContext.FACTORY.newInstance();
        ctx.aIndexes.add(1);
        ctx.aNames.add("aName");
        ctx.aNodes.add(newExpressionNode());
        ctx.bIndexes.add(2);
        ctx.bNames.add("bName");
        ctx.bNodes.add(newExpressionNode());
        ctx.parents.add(3);
        ctx.inCount = 5;
        ctx.slaveIndex = 10;
        assertDifferentFromFresh(ctx, JoinContext.FACTORY.newInstance());

        ctx.clear();
        assertFieldsEqual(JoinContext.FACTORY.newInstance(), ctx);
    }

    @Test
    public void testPivotForColumnClear() {
        PivotForColumn col = PivotForColumn.FACTORY.newInstance();
        col.of(newExpressionNode(), false);
        col.addValue(newExpressionNode(), "alias1");
        col.setInExprAlias("inAlias");
        col.setSelectSubqueryExpr(newExpressionNode());
        assertDifferentFromFresh(col, PivotForColumn.FACTORY.newInstance());

        col.clear();
        assertFieldsEqual(PivotForColumn.FACTORY.newInstance(), col);
    }

    @Test
    public void testQueryColumnClear() {
        QueryColumn column = QueryColumn.FACTORY.newInstance();
        column.of("alias", newExpressionNode(), false, 42);
        column.setAlias("newAlias", 10);
        assertDifferentFromFresh(column, QueryColumn.FACTORY.newInstance());

        column.clear();
        assertFieldsEqual(QueryColumn.FACTORY.newInstance(), column);
    }

    @Test
    public void testQueryModelClear() throws SqlException {
        QueryModel model = QueryModel.FACTORY.newInstance();
        model.setAlias(newExpressionNode());
        model.setArtificialStar(true);
        model.setTableNameExpr(newExpressionNode());
        model.setJoinType(QueryModel.JOIN_LT);
        model.setJoinCriteria(newExpressionNode());
        model.setModelPosition(42);
        model.setLimit(newExpressionNode(), newExpressionNode());
        model.setLimitAdvice(newExpressionNode(), newExpressionNode());
        model.setLimitPosition(42);
        model.setTimestamp(newExpressionNode());
        model.setContext(new JoinContext());
        model.setIsUpdate(true);
        model.setSelectModelType(QueryModel.SELECT_MODEL_VIRTUAL);
        model.setSelectTranslation(true);
        model.setSetOperationType(QueryModel.SET_OPERATION_EXCEPT);
        model.setSampleByOffset(newExpressionNode());
        model.setSampleBy(newExpressionNode(), newExpressionNode());
        model.setSampleByTimezoneName(newExpressionNode());
        model.setLatestByType(QueryModel.LATEST_BY_NEW);
        model.setOrderByAdviceMnemonic(42);
        model.setOrderByPosition(42);
        IntList jm = model.nextOrderedJoinModels();
        jm.add(42);
        model.setOrderedJoinModels(jm);
        model.addBottomUpColumn(newQueryColumn());
        model.addTopDownColumn(newQueryColumn(), "foobar");
        model.addField(newQueryColumn());
        model.setNestedModel(QueryModel.FACTORY.newInstance());
        model.addJoinModel(QueryModel.FACTORY.newInstance());
        model.addJoinColumn(newExpressionNode());
        model.setWhereClause(newExpressionNode());
        model.setPostJoinWhereClause(newExpressionNode());
        model.addParsedWhereNode(newExpressionNode(), true);
        model.addOrderBy(newExpressionNode(), QueryModel.ORDER_DIRECTION_DESCENDING);
        model.addGroupBy(newExpressionNode());
        ObjList<ExpressionNode> orderByAdvice = new ObjList<>();
        orderByAdvice.add(newExpressionNode());
        model.copyOrderByAdvice(orderByAdvice);
        IntList orderByDirectionAdvice = new IntList();
        orderByDirectionAdvice.add(42);
        model.copyOrderByDirectionAdvice(orderByDirectionAdvice);
        model.setFillValues(new ObjList<>(newExpressionNode()));
        model.setFillTo(newExpressionNode());
        model.setFillFrom(newExpressionNode());
        model.setFillStride(newExpressionNode());
        model.setAllowPropagationOfOrderByAdvice(false);
        model.getAliasSequenceMap().put("foobar", 1);
        assertDifferentFromFresh(model, QueryModel.FACTORY.newInstance());

        model.clear();
        assertFieldsEqual(QueryModel.FACTORY.newInstance(), model);
    }

    @Test
    public void testRenameTableModelClear() {
        RenameTableModel model = RenameTableModel.FACTORY.newInstance();
        model.setFrom(newExpressionNode());
        model.setTo(newExpressionNode());
        assertDifferentFromFresh(model, RenameTableModel.FACTORY.newInstance());

        model.clear();
        assertFieldsEqual(RenameTableModel.FACTORY.newInstance(), model);
    }

    @Test
    public void testWindowExpressionClear() {
        WindowExpression column = WindowExpression.FACTORY.newInstance();
        // QueryColumn fields
        column.of("alias", newExpressionNode(), false, 42);
        // WindowColumn fields
        column.getPartitionBy().add(newExpressionNode());
        column.addOrderBy(newExpressionNode(), QueryModel.ORDER_DIRECTION_DESCENDING);
        column.setRowsLoExpr(newExpressionNode(), 10);
        column.setRowsLoExprTimeUnit(WindowExpression.TIME_UNIT_SECOND);
        column.setRowsHiExpr(newExpressionNode(), 20);
        column.setRowsHiExprTimeUnit(WindowExpression.TIME_UNIT_MINUTE);
        column.setRowsLoKind(WindowExpression.FOLLOWING, 30);
        column.setRowsHiKind(WindowExpression.FOLLOWING, 40);
        column.setFramingMode(WindowExpression.FRAMING_ROWS);
        column.setRowsLo(100);
        column.setRowsHi(200);
        column.setExclusionKind(WindowExpression.EXCLUDE_CURRENT_ROW, 50);
        column.setIgnoreNulls(true);
        column.setNullsDescPos(60);
        assertDifferentFromFresh(column, WindowExpression.FACTORY.newInstance());

        column.clear();
        assertFieldsEqual(WindowExpression.FACTORY.newInstance(), column);
    }

    @Test
    public void testWindowJoinContextClear() {
        WindowJoinContext context = new WindowJoinContext();
        WindowJoinContext freshContext = new WindowJoinContext();
        context.setLoExpr(newExpressionNode(), 10);
        context.setLoExprTimeUnit(WindowExpression.TIME_UNIT_SECOND);
        context.setLo(100);
        context.setLoKind(WindowJoinContext.FOLLOWING, 20);
        context.setHiExpr(newExpressionNode(), 30);
        context.setHiExprTimeUnit(WindowExpression.TIME_UNIT_MINUTE);
        context.setHi(200);
        context.setHiKind(WindowJoinContext.FOLLOWING, 40);
        context.setIncludePrevailing(false, 50);
        context.setParentModel(QueryModel.FACTORY.newInstance());
        assertDifferentFromFresh(context, freshContext);

        context.clear();
        assertFieldsEqual(new WindowJoinContext(), context);
    }

    @Test
    public void testWithClauseModelClear() {
        WithClauseModel model = WithClauseModel.FACTORY.newInstance();
        LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses = new LowerCaseCharSequenceObjHashMap<>();
        withClauses.put("cte1", WithClauseModel.FACTORY.newInstance());
        model.of(42, withClauses, QueryModel.FACTORY.newInstance());
        // Force initialization of withClauses field
        model.getWithClauses();
        assertDifferentFromFresh(model, WithClauseModel.FACTORY.newInstance());

        model.clear();
        assertFieldsEqual(WithClauseModel.FACTORY.newInstance(), model);
    }

    private static void assertDifferentFromFresh(Object modified, Object fresh) {
        try {
            assertFieldsEqual(fresh, modified);
            throw new IllegalStateException("Objects should differ before clear()");
        } catch (AssertionError ignored) {
            // expected
        }
    }

    /**
     * Asserts that all instance fields of two objects have equal values.
     * Uses reflection to compare all declared fields (including private),
     * walking up the class hierarchy to include inherited fields.
     */
    private static void assertFieldsEqual(Object expected, Object actual) {
        if (expected == null && actual == null) {
            return;
        }
        Assert.assertNotNull("Expected object is null but actual is not", expected);
        Assert.assertNotNull("Actual object is null but expected is not", actual);
        Assert.assertEquals("Objects are of different classes",
                expected.getClass(), actual.getClass());

        Class<?> clazz = expected.getClass();
        while (clazz != null && clazz != Object.class) {
            for (Field field : clazz.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                compareField(field, expected, actual);
            }
            clazz = clazz.getSuperclass();
        }
    }

    private static void compareField(Field field, Object expected, Object actual) {
        field.setAccessible(true);
        String fieldName = field.getDeclaringClass().getSimpleName() + "." + field.getName();

        try {
            Object expectedValue = field.get(expected);
            Object actualValue = field.get(actual);

            if (expectedValue == actualValue) {
                return;
            }

            // Handle nulls
            if (expectedValue == null || actualValue == null) {
                Assert.fail("Field '" + fieldName + "' mismatch: expected=<" +
                        expectedValue + ">, actual=<" + actualValue + ">");
                return;
            }

            // For embedded Mutable objects, compare fields recursively
            String className = expectedValue.getClass().getSimpleName();
            if (className.equals("RuntimeIntervalModelBuilder") || className.equals("WindowJoinContext")) {
                assertFieldsEqual(expectedValue, actualValue);
                return;
            }

            // For collections, compare only sizes rather than deep equality.
            // This handles both java.util collections and QuestDB collections.
            Integer expectedSize = getCollectionSize(expectedValue);
            Integer actualSize = getCollectionSize(actualValue);

            if (expectedSize != null && actualSize != null) {
                Assert.assertEquals("Field '" + fieldName + "' collection size mismatch",
                        (int) expectedSize, (int) actualSize);
                return;
            }

            // For other objects, use equals comparison
            if (!Objects.equals(expectedValue, actualValue)) {
                Assert.fail("Field '" + fieldName + "' mismatch: expected=<" +
                        expectedValue + ">, actual=<" + actualValue + ">");
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to access field: " + fieldName, e);
        }
    }

    /**
     * Gets the size of a collection-like object.
     * Supports java.util collections/maps and QuestDB collections (via size() method).
     */
    private static Integer getCollectionSize(Object obj) {
        if (obj instanceof Collection) {
            return ((Collection<?>) obj).size();
        }
        if (obj instanceof Map) {
            return ((Map<?, ?>) obj).size();
        }

        // Try QuestDB collections via reflection (they have size() method)
        try {
            Method sizeMethod = obj.getClass().getMethod("size");
            if (sizeMethod.getReturnType() == int.class) {
                return (Integer) sizeMethod.invoke(obj);
            }
        } catch (NoSuchMethodException e) {
            // Not a collection-like object
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke size() on " + obj.getClass(), e);
        }

        return null;
    }

    private static ExpressionNode newExpressionNode() {
        ExpressionNode node = ExpressionNode.FACTORY.newInstance();
        node.token = "foobar";
        return node;
    }

    private static QueryColumn newQueryColumn() {
        QueryColumn column = QueryColumn.FACTORY.newInstance();
        column.of("foobar", newExpressionNode());
        return column;
    }
}
