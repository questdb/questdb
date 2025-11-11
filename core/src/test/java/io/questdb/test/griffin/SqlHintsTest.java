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
    public void testAsOfDenseHint() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QueryModel model = new QueryModel.QueryModelFactory().newInstance();
            Assert.assertFalse(SqlHints.hasAsOfDenseHint(model, "tableA", "tableB"));

            model.addHint(SqlHints.ASOF_DENSE_HINT, "tableA tableB");
            Assert.assertTrue(SqlHints.hasAsOfDenseHint(model, "tableA", "tableB"));

            // case-insensitive
            Assert.assertTrue(SqlHints.hasAsOfDenseHint(model, "tablea", "tableb"));
            Assert.assertTrue(SqlHints.hasAsOfDenseHint(model, "TABLEA", "TABLEB"));

            // different order
            Assert.assertTrue(SqlHints.hasAsOfDenseHint(model, "tableB", "tableA"));
            Assert.assertTrue(SqlHints.hasAsOfDenseHint(model, "TABLEB", "TABLEA"));

            model.clear();
            Assert.assertFalse(SqlHints.hasAsOfDenseHint(model, "tableA", "tableB"));
        });
    }

    @Test
    public void testAsOfIndexHint() throws Exception {
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
    public void testAsOfLinearHint() throws Exception {
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
    public void testAsOfMemoizedDrivebyHint() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QueryModel model = new QueryModel.QueryModelFactory().newInstance();
            Assert.assertFalse(SqlHints.hasAsOfMemoizedDrivebyHint(model, "tableA", "tableB"));

            model.addHint(SqlHints.ASOF_MEMOIZED_DRIVEBY_HINT, "tableA tableB");
            Assert.assertTrue(SqlHints.hasAsOfMemoizedDrivebyHint(model, "tableA", "tableB"));

            // case-insensitive
            Assert.assertTrue(SqlHints.hasAsOfMemoizedDrivebyHint(model, "tablea", "tableb"));
            Assert.assertTrue(SqlHints.hasAsOfMemoizedDrivebyHint(model, "TABLEA", "TABLEB"));

            // different order
            Assert.assertTrue(SqlHints.hasAsOfMemoizedDrivebyHint(model, "tableB", "tableA"));
            Assert.assertTrue(SqlHints.hasAsOfMemoizedDrivebyHint(model, "TABLEB", "TABLEA"));

            model.clear();
            Assert.assertFalse(SqlHints.hasAsOfMemoizedDrivebyHint(model, "tableA", "tableB"));
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
