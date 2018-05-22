/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.griffin.SqlException;
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
    public void testBinOverride() throws SqlException {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setBin("a", null);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testBooleanOverride() throws SqlException {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setBoolean("a", false);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testByteOverride() throws SqlException {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setByte("a", (byte) 5);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testDateOverride() throws SqlException {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setDate("a", 5);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testDoubleOverride() throws SqlException {
        bindVariableService.setInt("a", 10);
        try {
            bindVariableService.setDouble("a", 5.4);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as INT");
        }
    }

    @Test
    public void testFloatOverride() throws SqlException {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setFloat("a", 5);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testIntOverride() throws SqlException {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setInt("a", 5);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testLongOverride() throws SqlException {
        bindVariableService.setInt("a", 10);
        try {
            bindVariableService.setLong("a", 5);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as INT");
        }
    }

    @Test
    public void testShortOverride() throws SqlException {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setShort("a", (short) 5);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testStrOverride() throws SqlException {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setStr("a", "ok");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testTimestampOverride() throws SqlException {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setTimestamp("a", 5);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "bind variable 'a' is already defined as LONG");
        }
    }
}