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

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

// Runs some tests from CopyTest, but without mangling private table names
public class CopyNoMangleTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        inputRoot = TestUtils.getCsvRoot();
        inputWorkRoot = TestUtils.unchecked(() -> temp.newFolder("imports" + System.nanoTime()).getAbsolutePath());
        AbstractCairoTest.setUpStatic();
        node1.initGriffin(circuitBreaker);
        bindVariableService = node1.getBindVariableService();
        sqlExecutionContext = node1.getSqlExecutionContext();
    }

    @Test
    public void testWhenWorkIsTheSameAsDataDirThenParallelCopyThrowsException() throws Exception {
        configOverrideMangleTableDirNames(false);
        String inputWorkRootTmp = inputWorkRoot;
        inputWorkRoot = temp.getRoot().getAbsolutePath();

        CopyImportTest.CopyRunnable stmt = () -> CopyImportTest.runAndFetchCopyID(
                "copy dbRoot from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' " +
                        "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' on error ABORT partition by day; ",
                sqlExecutionContext
        );

        CopyImportTest.CopyRunnable test = () -> assertQueryNoLeakCheck(
                "message\ncould not remove import work directory because it points to one of main directories\n",
                "select left(message, 83) message from " + configuration.getSystemTableNamePrefix() + "text_import_log limit -1",
                null,
                true,
                true
        );

        CopyImportTest.testCopy(stmt, test);

        inputWorkRoot = inputWorkRootTmp;
    }
}
