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

package io.questdb.test.cairo.pool;

import io.questdb.cairo.pool.SqlCompilerPool;
import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SqlCompilerPoolTest extends AbstractCairoTest {

    @Test
    public void testDoesNotSupportRefreshAt() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    SqlCompiler compiler1 = engine.getSqlCompiler();
                    SqlCompiler compiler2 = engine.getSqlCompiler()
            ) {
                SqlCompilerPool.C c1 = (SqlCompilerPool.C) compiler1;
                SqlCompilerPool.C c2 = (SqlCompilerPool.C) compiler2;
                try {
                    c1.refreshAt(null, c2);
                    Assert.fail();
                } catch (UnsupportedOperationException ignore) {
                }
            }
        });
    }
}
