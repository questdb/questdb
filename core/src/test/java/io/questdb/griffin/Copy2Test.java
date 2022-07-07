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

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

public class Copy2Test extends AbstractGriffinTest {

    @BeforeClass
    public static void setUpStatic() {
        inputRoot = new File(".").getAbsolutePath();
        inputWorkRoot = temp.getRoot().getAbsolutePath();
        AbstractGriffinTest.setUpStatic();
    }

    @Test
    public void testWhenWorkIsTheSameAsDataDirThenParallelCopyThrowsException() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile("copy dbRoot from '/src/test/resources/csv/test-quotes-big.csv' with parallel header true timestamp 'ts' delimiter ',' " +
                        "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' on error ABORT partition by day; ", sqlExecutionContext);
            } catch (Exception e) {
                MatcherAssert.assertThat(e.getMessage(), CoreMatchers.containsString("cannot remove work dir because it points to one of main instance directories"));
            }
        });
    }
}
