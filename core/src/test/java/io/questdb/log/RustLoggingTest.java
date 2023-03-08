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

package io.questdb.log;

import junit.framework.TestCase;
import org.junit.Test;

public class RustLoggingTest extends TestCase {
    @Test
    public void testLogging() {
        // All info-level log messages (and above) should appear
        // in stdout when running this test.
        // If they don't, temporarily uncomment the following line:
        //     LogFactory.configureSync();
        RustLogging.logMsg(RustLogging.LEVEL_INFO, "a::b::c", "test_msg1");
        RustLogging.logMsg(RustLogging.LEVEL_WARN, "def", "test_msg2");
        RustLogging.logMsg(RustLogging.LEVEL_INFO, "a::b::c", "test_msg3");
        RustLogging.logMsg(RustLogging.LEVEL_ERROR, "def", "test_msg4");
        RustLogging.logMsg(RustLogging.LEVEL_INFO, "a::b::c", "test_msg5");
        RustLogging.logMsg(RustLogging.LEVEL_DEBUG, "ghi", "test_msg6");
        RustLogging.logMsg(RustLogging.LEVEL_TRACE, "a::b::c", "test_msg7");
        RustLogging.logMsg(RustLogging.LEVEL_ERROR, "ghi", "test_msg8");
    }
}
