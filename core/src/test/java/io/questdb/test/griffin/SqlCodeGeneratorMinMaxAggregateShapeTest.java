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

import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pure-predicate tests for {@link SqlCodeGenerator#tryGetMinMaxAggregateColumn}.
 * No SQL execution, no parquet, no metadata - just synthetic AST trees that
 * exercise the predicate's match and reject paths.
 * <p>
 * Test inputs are built via the same {@link ExpressionNode.FACTORY} pool the
 * parser uses, so the predicate exercises real flyweight objects with normal
 * type / token / paramCount semantics.
 */
public class SqlCodeGeneratorMinMaxAggregateShapeTest {

    private final ObjectPool<ExpressionNode> exprPool = new ObjectPool<>(ExpressionNode.FACTORY, 16);
    private final ObjectPool<QueryColumn> qcPool = new ObjectPool<>(QueryColumn.FACTORY, 16);

    @Test
    public void testEmptyOrNullReturnsNull() {
        Assert.assertNull(SqlCodeGenerator.tryGetMinMaxAggregateColumn(null));
        Assert.assertNull(SqlCodeGenerator.tryGetMinMaxAggregateColumn(new ObjList<>()));
    }

    @Test
    public void testMatchesMinSingleColumn() {
        final ObjList<QueryColumn> cols = new ObjList<>();
        cols.add(aggColumn("min", "ts"));
        final CharSequence shared = SqlCodeGenerator.tryGetMinMaxAggregateColumn(cols);
        Assert.assertNotNull(shared);
        Assert.assertTrue(Chars.equals("ts", shared));
    }

    @Test
    public void testMatchesMaxSingleColumn() {
        final ObjList<QueryColumn> cols = new ObjList<>();
        cols.add(aggColumn("MAX", "ts"));
        // Case-insensitive on function name.
        final CharSequence shared = SqlCodeGenerator.tryGetMinMaxAggregateColumn(cols);
        Assert.assertNotNull(shared);
        Assert.assertTrue(Chars.equals("ts", shared));
    }

    @Test
    public void testMatchesMinAndMaxOnSameColumn() {
        final ObjList<QueryColumn> cols = new ObjList<>();
        cols.add(aggColumn("min", "ts"));
        cols.add(aggColumn("max", "ts"));
        final CharSequence shared = SqlCodeGenerator.tryGetMinMaxAggregateColumn(cols);
        Assert.assertNotNull(shared);
        Assert.assertTrue(Chars.equals("ts", shared));
    }

    @Test
    public void testRejectsMinOnDifferentColumns() {
        final ObjList<QueryColumn> cols = new ObjList<>();
        cols.add(aggColumn("min", "ts"));
        cols.add(aggColumn("max", "id"));
        Assert.assertNull(SqlCodeGenerator.tryGetMinMaxAggregateColumn(cols));
    }

    @Test
    public void testRejectsNonMinMaxFunction() {
        final ObjList<QueryColumn> cols = new ObjList<>();
        cols.add(aggColumn("sum", "ts"));
        Assert.assertNull(SqlCodeGenerator.tryGetMinMaxAggregateColumn(cols));
    }

    @Test
    public void testRejectsMinWithNonLiteralArg() {
        // min(ts + 1) is FUNCTION whose arg is OPERATION, not LITERAL.
        final ExpressionNode arg = exprPool.next();
        arg.type = ExpressionNode.OPERATION;
        arg.token = "+";
        arg.paramCount = 2;

        final ExpressionNode minCall = exprPool.next();
        minCall.type = ExpressionNode.FUNCTION;
        minCall.token = "min";
        minCall.paramCount = 1;
        minCall.rhs = arg;

        final QueryColumn col = qcPool.next();
        col.of("min", minCall);

        final ObjList<QueryColumn> cols = new ObjList<>();
        cols.add(col);
        Assert.assertNull(SqlCodeGenerator.tryGetMinMaxAggregateColumn(cols));
    }

    @Test
    public void testRejectsMinWithMultipleArgs() {
        final ExpressionNode arg = exprPool.next();
        arg.type = ExpressionNode.LITERAL;
        arg.token = "ts";

        final ExpressionNode minCall = exprPool.next();
        minCall.type = ExpressionNode.FUNCTION;
        minCall.token = "min";
        minCall.paramCount = 2; // wrong arity for min(col)
        minCall.rhs = arg;

        final QueryColumn col = qcPool.next();
        col.of("min", minCall);

        final ObjList<QueryColumn> cols = new ObjList<>();
        cols.add(col);
        Assert.assertNull(SqlCodeGenerator.tryGetMinMaxAggregateColumn(cols));
    }

    @Test
    public void testRejectsMixedMinMaxAndNonAggregate() {
        // min(ts), min(ts), bare_literal - the third entry isn't an aggregate
        // shape so the whole list rejects.
        final ObjList<QueryColumn> cols = new ObjList<>();
        cols.add(aggColumn("min", "ts"));
        cols.add(aggColumn("max", "ts"));

        final ExpressionNode lit = exprPool.next();
        lit.type = ExpressionNode.LITERAL;
        lit.token = "ts";

        final QueryColumn bare = qcPool.next();
        bare.of("ts", lit);
        cols.add(bare);

        Assert.assertNull(SqlCodeGenerator.tryGetMinMaxAggregateColumn(cols));
    }

    /**
     * Builds a synthetic min(col) / max(col) QueryColumn. Function-name
     * comparison is case-insensitive; column name comparison is exact.
     */
    private QueryColumn aggColumn(String funcName, String columnName) {
        final ExpressionNode arg = exprPool.next();
        arg.type = ExpressionNode.LITERAL;
        arg.token = columnName;

        final ExpressionNode call = exprPool.next();
        call.type = ExpressionNode.FUNCTION;
        call.token = funcName;
        call.paramCount = 1;
        call.rhs = arg;

        final QueryColumn col = qcPool.next();
        col.of(funcName, call);
        return col;
    }
}
