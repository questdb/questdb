/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class TxIDCurrentFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testPrefixedTxIDCurrentFunc() throws Exception {
        assertMemoryLeak(
                () -> TestUtils.assertSql(
                        compiler,
                        sqlExecutionContext,
                        "select pg_catalog.txid_current();",
                        sink,
                        "txid_current\n" + (TxIDCurrentFunctionFactory.getTxID() + 1) + "\n"
                )
        );
    }

    @Test
    public void testTxIDCurrentFunc() throws Exception {
        assertMemoryLeak(
                () -> TestUtils.assertSql(
                        compiler,
                        sqlExecutionContext,
                        "select txid_current();",
                        sink,
                        "txid_current\n" + (TxIDCurrentFunctionFactory.getTxID() + 1) + "\n"
                )
        );
    }
}