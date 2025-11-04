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
    public void testAsOfJoinUseDrivebyCacheHint() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QueryModel model = new QueryModel.QueryModelFactory().newInstance();
            Assert.assertFalse(SqlHints.hasAsOfDrivebyCacheHint(model, "tableA", "tableB"));

            model.addHint(SqlHints.ASOF_DRIVEBY_CACHE_HINT, "tableA tableB");
            Assert.assertTrue(SqlHints.hasAsOfDrivebyCacheHint(model, "tableA", "tableB"));

            // case-insensitive
            Assert.assertTrue(SqlHints.hasAsOfDrivebyCacheHint(model, "tablea", "tableb"));
            Assert.assertTrue(SqlHints.hasAsOfDrivebyCacheHint(model, "TABLEA", "TABLEB"));

            // different order
            Assert.assertTrue(SqlHints.hasAsOfDrivebyCacheHint(model, "tableB", "tableA"));
            Assert.assertTrue(SqlHints.hasAsOfDrivebyCacheHint(model, "TABLEB", "TABLEA"));

            model.clear();
            Assert.assertFalse(SqlHints.hasAsOfDrivebyCacheHint(model, "tableA", "tableB"));
        });
    }

    @Test
    public void testAsOfJoinUseIndexSearchHint() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QueryModel model = new QueryModel.QueryModelFactory().newInstance();
            Assert.assertFalse(SqlHints.hasAsOfIndexHint(model, "tableA", "tableB"));

            model.addHint(SqlHints.ASOF_INDEX_HINT, "tableA tableB");
            Assert.assertTrue(SqlHints.hasAsOfIndexHint(model, "tableA", "tableB"));

            // case-insensitive
            Assert.assertTrue(SqlHints.hasAsOfIndexHint(model, "tablea", "tableb"));
            Assert.assertTrue(SqlHints.hasAsOfIndexHint(model, "TABLEA", "TABLEB"));

            // different order
            Assert.assertTrue(SqlHints.hasAsOfIndexHint(model, "tableB", "tableA"));
            Assert.assertTrue(SqlHints.hasAsOfIndexHint(model, "TABLEB", "TABLEA"));

            model.clear();
            Assert.assertFalse(SqlHints.hasAsOfIndexHint(model, "tableA", "tableB"));
        });
    }

    @Test
    public void testAsOfJoinUseLinearSearchHint() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QueryModel model = new QueryModel.QueryModelFactory().newInstance();
            Assert.assertFalse(SqlHints.hasAsOfLinearHint(model, "tableA", "tableB"));

            model.addHint(SqlHints.ASOF_LINEAR_HINT, "tableA tableB");
            Assert.assertTrue(SqlHints.hasAsOfLinearHint(model, "tableA", "tableB"));

            // case-insensitive
            Assert.assertTrue(SqlHints.hasAsOfLinearHint(model, "tablea", "tableb"));
            Assert.assertTrue(SqlHints.hasAsOfLinearHint(model, "TABLEA", "TABLEB"));

            // different order
            Assert.assertTrue(SqlHints.hasAsOfLinearHint(model, "tableB", "tableA"));
            Assert.assertTrue(SqlHints.hasAsOfLinearHint(model, "TABLEB", "TABLEA"));

            model.clear();
            Assert.assertFalse(SqlHints.hasAsOfLinearHint(model, "tableA", "tableB"));
        });
    }

    @Test
    public void testColumnPreTouchHint() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QueryModel model = new QueryModel.QueryModelFactory().newInstance();
            Assert.assertFalse(SqlHints.hasEnablePreTouchHint(model, "myTable"));

            model.addHint(SqlHints.ENABLE_PRE_TOUCH_HINT, "myTable");
            Assert.assertTrue(SqlHints.hasEnablePreTouchHint(model, "myTable"));

            // case-insensitive
            Assert.assertTrue(SqlHints.hasEnablePreTouchHint(model, "mytable"));
            Assert.assertTrue(SqlHints.hasEnablePreTouchHint(model, "MYTABLE"));

            model.clear();
            Assert.assertFalse(SqlHints.hasEnablePreTouchHint(model, "myTable"));

            // parameter-less hint is ignored
            model.addHint(SqlHints.ENABLE_PRE_TOUCH_HINT, null);
            Assert.assertFalse(SqlHints.hasEnablePreTouchHint(model, "myTable"));
        });
    }
}
