/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static io.questdb.griffin.CopyTest.runAndFetchImportId;
import static io.questdb.griffin.CopyTest.testCopy;

// Runs some tests from CopyTest, but without mangling system table names
public class CopyTestNoMangle extends AbstractGriffinTest {

    @BeforeClass
    public static void setUpStatic() {
        mangleTableSystemName = false;
        inputRoot = TestUtils.getCsvRoot();
        try {
            inputWorkRoot = temp.newFolder("imports" + System.nanoTime()).getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        AbstractGriffinTest.setUpStatic();
    }

    @Test
    public void testWhenWorkIsTheSameAsDataDirThenParallelCopyThrowsException() throws Exception {
        String inputWorkRootTmp = inputWorkRoot;
        inputWorkRoot = temp.getRoot().getAbsolutePath();

        CopyTest.CopyRunnable stmt = () -> runAndFetchImportId("copy dbRoot from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' " +
                "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' on error ABORT partition by day; ", sqlExecutionContext);

        CopyTest.CopyRunnable test = () -> assertQuery("message\ncould not remove import work directory because it points to one of main directories\n",
                "select left(message, 83) message from " + configuration.getSystemTableNamePrefix() + "text_import_log limit -1",
                null,
                true,
                true
        );

        testCopy(stmt, test);

        inputWorkRoot = inputWorkRootTmp;
    }
}