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

package io.questdb.test.sqllogictest.parquet;

import io.questdb.test.sqllogictest.AbstractSqllogicTestRunner;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Tests read_parquet()/parquet_scan() SQL function.
 */
public class ParquetTest extends AbstractSqllogicTestRunner {

    public ParquetTest(String testFile, boolean parallelReadParquet) {
        super(testFile, parallelReadParquet);
    }

    @Parameterized.Parameters(name = "{0} parallel={1}")
    public static Collection<Object[]> files() {
        final Collection<Object[]> files = files("parquet");
        final ArrayList<Object[]> params = new ArrayList<>();
        for (Object[] objects : files) {
            params.add(new Object[]{objects[0], true});
            params.add(new Object[]{objects[0], false});
        }
        return params;
    }
}
