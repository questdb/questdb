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

package io.questdb.test.griffin.engine;

import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.QueryModelWrapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class SqlCodeGeneratorCleanupTest {

    @Test
    public void testFreeTableNameFunctionsTraversesSharedGraphAndPreservesPrimaryError() {
        final AtomicInteger closeCount = new AtomicInteger();
        final QueryModel root = QueryModel.FACTORY.newInstance();
        final QueryModel expressionModel = QueryModel.FACTORY.newInstance();
        final QueryModel joinModel = QueryModel.FACTORY.newInstance();
        final QueryModel sharedModel = QueryModel.FACTORY.newInstance();
        final QueryModel unionModel = QueryModel.FACTORY.newInstance();
        final QueryModelWrapper sharedWrapper = new QueryModelWrapper().of(sharedModel, 1);

        root.setTableNameFunction(newFactory(closeCount, false));
        expressionModel.setTableNameFunction(newFactory(closeCount, false));
        joinModel.setTableNameFunction(newFactory(closeCount, false));
        sharedModel.setTableNameFunction(newFactory(closeCount, true));
        unionModel.setTableNameFunction(newFactory(closeCount, false));

        final ExpressionNode expressionNode = ExpressionNode.FACTORY.newInstance();
        expressionNode.queryModel = expressionModel;
        root.addExpressionModel(expressionNode);
        final ExpressionNode sharedExpressionNode = ExpressionNode.FACTORY.newInstance();
        sharedExpressionNode.queryModel = sharedWrapper;
        root.addExpressionModel(sharedExpressionNode);
        root.setNestedModel(sharedWrapper);
        root.addJoinModel(root);
        root.addJoinModel(joinModel);
        root.setUnionModel(unionModel);

        final RuntimeException primaryError = new RuntimeException("primary");
        SqlCodeGenerator.freeTableNameFunctionsForTesting(root, primaryError);

        Assert.assertEquals(5, closeCount.get());
        Assert.assertNull(root.getTableNameFunction());
        Assert.assertNull(expressionModel.getTableNameFunction());
        Assert.assertNull(joinModel.getTableNameFunction());
        Assert.assertNull(sharedModel.getTableNameFunction());
        Assert.assertNull(unionModel.getTableNameFunction());
        Assert.assertEquals(1, primaryError.getSuppressed().length);
        Assert.assertEquals("cleanup", primaryError.getSuppressed()[0].getMessage());
    }

    private static RecordCursorFactory newFactory(AtomicInteger closeCount, boolean isThrowing) {
        return new RecordCursorFactory() {
            private final RecordMetadata metadata = new GenericRecordMetadata();

            @Override
            public void close() {
                closeCount.incrementAndGet();
                if (isThrowing) {
                    throw new RuntimeException("cleanup");
                }
            }

            @Override
            public RecordMetadata getMetadata() {
                return metadata;
            }

            @Override
            public boolean recordCursorSupportsRandomAccess() {
                return false;
            }

            @Override
            public void toPlan(PlanSink sink) {
            }
        };
    }
}
