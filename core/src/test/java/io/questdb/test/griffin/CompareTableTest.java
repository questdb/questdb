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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

public class CompareTableTest {
    protected static final Log LOG = LogFactory.getLog(CompareTableTest.class);

    @Test
    @Ignore
    public void testCompareTables() throws SqlException {
        // This is an integration test stub to compare 2 tables 
        // on local disk.
        String table1Name = "cpu";
        String table2Name = "cpu_non_wal";
        String root = "/Users/alpel/questdb-root/db";

        CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
        try (
                CairoEngine engine = new CairoEngine(configuration);
                SqlCompiler compiler = engine.getSqlCompiler();
                SqlExecutionContext executionContext = new SqlExecutionContextImpl(engine, 1).with(
                        AllowAllSecurityContext.INSTANCE,
                        new BindVariableServiceImpl(configuration),
                        null,
                        -1,
                        null
                )
        ) {
            TestUtils.assertSqlCursors(compiler, executionContext, table1Name, table2Name, LOG);
        }
    }
}
