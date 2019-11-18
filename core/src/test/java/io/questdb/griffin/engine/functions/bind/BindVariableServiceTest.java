/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class BindVariableServiceTest {
    private final static BindVariableService bindVariableService = new BindVariableService();

    @Before
    public void setUp() {
        bindVariableService.clear();
    }

    @Test
    public void testBinIndexedOverride() {
        bindVariableService.setLong(0, 10);
        try {
            bindVariableService.setBin(0, null);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable at 0 is already defined as LONG");
        }
    }

    @Test
    public void testBinOverride() {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setBin("a", null);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testBooleanIndexedOverride() {
        bindVariableService.setLong(0, 10);
        try {
            bindVariableService.setBoolean(0, false);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable at 0 is already defined as LONG");
        }
    }

    @Test
    public void testBooleanOverride() {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setBoolean("a", false);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testByteIndexedOverride() {
        bindVariableService.setLong(0, 10);
        try {
            bindVariableService.setByte(0, (byte) 5);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable at 0 is already defined as LONG");
        }
    }

    @Test
    public void testByteOverride() {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setByte("a", (byte) 5);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testDateIndexedOverride() {
        bindVariableService.setLong(0, 10);
        try {
            bindVariableService.setDate(0, 5);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable at 0 is already defined as LONG");
        }
    }

    @Test
    public void testDateOverride() {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setDate("a", 5);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testDoubleIndexedOverride() {
        bindVariableService.setInt(2, 10);
        try {
            bindVariableService.setDouble(2, 5.4);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable at 2 is already defined as INT");
        }
    }

    @Test
    public void testCharIndexedOverride() {
        bindVariableService.setInt(2, 10);
        try {
            bindVariableService.setChar(2, 'o');
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable at 2 is already defined as INT");
        }
    }

    @Test
    public void testDoubleOverride() {
        bindVariableService.setInt("a", 10);
        try {
            bindVariableService.setDouble("a", 5.4);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as INT");
        }
    }

    @Test
    public void testFloatIndexedOverride() {
        bindVariableService.setLong(1, 10);
        try {
            bindVariableService.setFloat(1, 5);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable at 1 is already defined as LONG");
        }
    }

    @Test
    public void testFloatOverride() {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setFloat("a", 5);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testIntIndexedOverride() {
        bindVariableService.setLong(0, 10);
        try {
            bindVariableService.setInt(0, 5);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable at 0 is already defined as LONG");
        }
    }

    @Test
    public void testIntOverride() {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setInt("a", 5);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testCharOverride() {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setChar("a", 'k');
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testLongIndexedOverride() {
        bindVariableService.setInt(0, 10);
        try {
            bindVariableService.setLong(0, 5);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable at 0 is already defined as INT");
        }
    }

    @Test
    public void testLongOverride() {
        bindVariableService.setInt("a", 10);
        try {
            bindVariableService.setLong("a", 5);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as INT");
        }
    }

    @Test
    public void testShortIndexedOverride() {
        bindVariableService.setLong(0, 10);
        try {
            bindVariableService.setShort(0, (short) 5);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable at 0 is already defined as LONG");
        }
    }

    @Test
    public void testShortOverride() {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setShort("a", (short) 5);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testStrIndexedOverride() {
        bindVariableService.setLong(0, 10);
        try {
            bindVariableService.setStr(0, "ok");
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable at 0 is already defined as LONG");
        }
    }

    @Test
    public void testStrOverride() {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setStr("a", "ok");
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testTimestampIndexedOverride() {
        bindVariableService.setLong(0, 10);
        try {
            bindVariableService.setTimestamp(0, 5);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable at 0 is already defined as LONG");
        }
    }

    @Test
    public void testTimestampOverride() {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setTimestamp("a", 5);
        } catch (BindException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }
}