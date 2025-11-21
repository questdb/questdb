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

package io.questdb.test.griffin.engine.table.parquet;

import io.questdb.griffin.engine.functions.regex.GlobStrFunctionFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;


public class GlobStrFunctionFactoryTest {

    final String[] patternMappings = new String[]{
            "*", "^.*$",
            "abc*", "^abc.*$",
            "*.txt", "^.*\\.txt$",
            "file?.csv", "^file.\\.csv$",
            "[a-z]*", "^[a-z].*$",
            "[!0-9]*", "^[^0-9].*$"
    };

    @Test
    public void testPatternMapping() {
        Assert.assertEquals(0, patternMappings.length % 2);
        StringSink sink = new StringSink();
        for (int i = 0; i < patternMappings.length; i += 2) {
            sink.clear();
            GlobStrFunctionFactory.convertGlobPatternToRegex(patternMappings[i], sink);
            TestUtils.assertEquals(patternMappings[i + 1], sink);
        }
    }
}
