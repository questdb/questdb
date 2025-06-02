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

import io.questdb.griffin.SqlHints;
import io.questdb.griffin.model.QueryModel;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlHintsTest extends AbstractTest {

    @Test
    public void testAsOfJoinBinarySearchHint() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QueryModel model = new QueryModel.QueryModelFactory().newInstance();
            Assert.assertFalse(SqlHints.hasAsOfJoinBinarySearchHint(model, "tableA", "tableB"));

            model.addHint(SqlHints.ASOF_JOIN_BINARY_SEARCH_HINT, "tableA tableB");
            Assert.assertTrue(SqlHints.hasAsOfJoinBinarySearchHint(model, "tableA", "tableB"));

            // case insensitive
            Assert.assertTrue(SqlHints.hasAsOfJoinBinarySearchHint(model, "tablea", "tableb"));
            Assert.assertTrue(SqlHints.hasAsOfJoinBinarySearchHint(model, "TABLEA", "TABLEB"));

            // different order
            Assert.assertTrue(SqlHints.hasAsOfJoinBinarySearchHint(model, "tableB", "tableA"));
            Assert.assertTrue(SqlHints.hasAsOfJoinBinarySearchHint(model, "TABLEB", "TABLEA"));

            model.clear();
            Assert.assertFalse(SqlHints.hasAsOfJoinBinarySearchHint(model, "tableA", "tableB"));
        });
    }
}
