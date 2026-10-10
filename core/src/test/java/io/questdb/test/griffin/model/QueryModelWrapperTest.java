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

package io.questdb.test.griffin.model;

import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.QueryModelWrapper;
import org.junit.Assert;
import org.junit.Test;

public class QueryModelWrapperTest {

    @Test
    public void testEarliestByDelegatesAndMutatorsThrow() {
        QueryModel delegate = QueryModel.FACTORY.newInstance();
        ExpressionNode col = ExpressionNode.FACTORY.newInstance();
        col.of(ExpressionNode.LITERAL, "s", 0, 0);
        delegate.addEarliestBy(col);
        delegate.setEarliestByType(QueryModel.EARLIEST_BY_NEW);

        QueryModelWrapper wrapper = QueryModelWrapper.FACTORY.newInstance().of(delegate, 1);

        Assert.assertSame(delegate.getEarliestBy(), wrapper.getEarliestBy());
        Assert.assertEquals(1, wrapper.getEarliestBy().size());
        Assert.assertEquals(QueryModel.EARLIEST_BY_NEW, wrapper.getEarliestByType());

        try {
            wrapper.addEarliestBy(col);
            Assert.fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException ignore) {
        }

        try {
            wrapper.setEarliestByType(QueryModel.EARLIEST_BY_NONE);
            Assert.fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException ignore) {
        }
    }
}
