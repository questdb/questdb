/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.functions.bind;

import com.questdb.test.tools.TestUtils;
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