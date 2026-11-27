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

import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.QueryModelWrapper;
import org.junit.Assert;
import org.junit.Test;

public class QueryModelWrapperTest {

    @Test
    public void testMovableAfterOuterJoinsReadsDelegate() {
        // A wrapper reads the model that its shared cursor shares, and leaves the model to its owner
        final QueryModel model = QueryModel.FACTORY.newInstance();
        final QueryModelWrapper wrapper = new QueryModelWrapper().of(model, 1);
        Assert.assertFalse(wrapper.isMovableAfterOuterJoins());
        model.setMovableAfterOuterJoins(true);
        Assert.assertTrue(wrapper.isMovableAfterOuterJoins());
        try {
            wrapper.setMovableAfterOuterJoins(false);
            Assert.fail("a wrapper must not change the model that it shares");
        } catch (UnsupportedOperationException ignore) {
        }
        Assert.assertTrue(model.isMovableAfterOuterJoins());
    }
}
