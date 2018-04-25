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

package com.questdb.griffin;

import com.questdb.cairo.AbstractCairoTest;
import com.questdb.cairo.Engine;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlCompilerTest extends AbstractCairoTest {
    private final static SqlCompiler compiler = new SqlCompiler(new Engine(configuration), configuration);
    private final static BindVariableService bindVariableService = new BindVariableService();

    @Test
    public void testCreateTable() throws SqlException {
        compiler.execute("create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                bindVariableService
        );
    }

    @Test
    public void testDuplicateTableName() throws SqlException {
        compiler.execute("create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "t TIMESTAMP, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                bindVariableService
        );

        try {
            compiler.execute("create table x (" +
                            "t TIMESTAMP, " +
                            "y BOOLEAN) " +
                            "timestamp(t) " +
                            "partition by MONTH",
                    bindVariableService
            );
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(13, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "table already exists");
        }
    }
}