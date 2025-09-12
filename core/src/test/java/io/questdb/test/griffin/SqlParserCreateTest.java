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

package io.questdb.test.griffin;

import org.junit.Test;

public class SqlParserCreateTest extends AbstractSqlParserTest {
    @Test
    public void testCreateDecimalTable() throws Exception {
        assertCreate("create atomic table x (v DECIMAL(18,3))", "create table x (v decimal)");
        assertCreate("create atomic table x (v DECIMAL(10,3))", "create table x (v decimal(10))");
        assertCreate("create atomic table x (v DECIMAL(10,5))", "create table x (v decimal(10, 5))");
    }

    @Test
    public void testCreateInvalidDecimalPrecision() throws Exception {
        assertSyntaxError("create table x (v decimal(abc))", 26, "invalid DECIMAL type, precision must be a number");
    }

    @Test
    public void testCreateInvalidDecimalScale() throws Exception {
        assertSyntaxError("create table x (v decimal(5,abc))", 28, "invalid DECIMAL type, scale must be a number");
    }

    @Test
    public void testCreateMissingDecimalClosingParen() throws Exception {
        assertSyntaxError("create table x (v decimal(5", 26, "invalid DECIMAL type, missing ')'");
    }

    @Test
    public void testCreateMissingDecimalPrecision() throws Exception {
        assertSyntaxError("create table x (v decimal())", 26, "invalid DECIMAL type, missing precision");
        assertSyntaxError("create table x (v decimal(", 25, "invalid DECIMAL type, missing precision");
    }

    @Test
    public void testCreateMissingDecimalScale() throws Exception {
        assertSyntaxError("create table x (v decimal(5,))", 28, "invalid DECIMAL type, missing scale");
        assertSyntaxError("create table x (v decimal(5,", 27, "invalid DECIMAL type, missing scale");
    }
}
