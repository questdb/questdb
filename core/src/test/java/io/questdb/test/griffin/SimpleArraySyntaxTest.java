/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __\__ \ |_| |_| | |_) |
 *    \___\\__,_|\___||___/\__|____/|____/
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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Simple test to verify basic array syntax validation is working.
 */
public class SimpleArraySyntaxTest extends AbstractCairoTest {

    @Test
    public void testBasicArraySyntaxValidation() throws Exception {
        assertMemoryLeak(() -> {
            // Valid syntax should work
            execute("CREATE TABLE test_valid (id int, data double[])");
            execute("DROP TABLE test_valid");

            // Invalid syntax should fail - try a simple case first
            try {
                execute("CREATE TABLE test_invalid (id int, data double [])");
                fail("Expected SQL exception for array syntax with space");
            } catch (Exception e) {
                System.out.println("Exception message: " + e.getMessage());
                System.out.println("Exception class: " + e.getClass().getSimpleName());
            }
        });
    }
}