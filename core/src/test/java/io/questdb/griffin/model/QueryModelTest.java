/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.model;

import io.questdb.griffin.SqlException;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

public class QueryModelTest {

    @Test
    public void testAllFieldsResetOnClear() throws SqlException {
        QueryModel model = QueryModel.FACTORY.newInstance();

        model.setAlias(newExpressionNode());
        model.setArtificialStar(true);
        model.setTableName(newExpressionNode());
        model.setJoinType(QueryModel.JOIN_LT);
        model.setJoinCriteria(newExpressionNode());
        model.setModelPosition(42);
        model.setLimit(newExpressionNode(), newExpressionNode());
        model.setLimitAdvice(newExpressionNode(), newExpressionNode());
        model.setLimitImplemented(true);
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

        Assert.assertNotEquals(QueryModel.FACTORY.newInstance(), model);

        model.clear();

        Assert.assertEquals(QueryModel.FACTORY.newInstance(), model);
    }

    private static ExpressionNode newExpressionNode() {
        ExpressionNode node = ExpressionNode.FACTORY.newInstance();
        node.token = "foobar";
        return node;
    }

    private static QueryColumn newQueryColumn() {
        QueryColumn column = new QueryColumn();
        column.of("foobar", newExpressionNode());
        return column;
    }
}