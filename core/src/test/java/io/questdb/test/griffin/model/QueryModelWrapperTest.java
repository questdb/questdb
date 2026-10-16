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
import io.questdb.std.IntList;
import org.junit.Assert;
import org.junit.Test;

public class QueryModelWrapperTest {

    // A wrapper reads the lateral NULL check groups of its delegate, each its size followed by the
    // indices of its checks, and does not change them
    @Test
    public void testLateralNullCheckGroups() {
        final QueryModel model = QueryModel.FACTORY.newInstance();
        final IntList group = new IntList();
        group.add(1);
        group.add(0);
        model.addLateralNullCheckGroup(group);
        final QueryModelWrapper wrapper = new QueryModelWrapper().of(model, 1);
        final IntList groups = wrapper.getLateralNullCheckGroups();
        Assert.assertSame(model.getLateralNullCheckGroups(), groups);
        Assert.assertEquals(3, groups.size());
        Assert.assertEquals(2, groups.getQuick(0));
        Assert.assertEquals(1, groups.getQuick(1));
        Assert.assertEquals(0, groups.getQuick(2));
        Assert.assertThrows(UnsupportedOperationException.class, () -> wrapper.addLateralNullCheckGroup(group));
        Assert.assertEquals(3, groups.size());
    }
}
