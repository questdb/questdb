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
        assertSyntaxError("create table x (v decimal(abc))", 26, "Invalid decimal type. The precision ('abc') must be a number");
    }

    @Test
    public void testCreateInvalidDecimalScale() throws Exception {
        assertSyntaxError("create table x (v decimal(5,abc))", 28, "Invalid decimal type. The scale ('abc') must be a number");
    }

    @Test
    public void testCreateMissingDecimalClosingParen() throws Exception {
        assertSyntaxError("create table x (v decimal(5", 26, "Invalid decimal type. Missing ')'");
    }

    @Test
    public void testCreateMissingDecimalPrecision() throws Exception {
        assertSyntaxError("create table x (v decimal())", 26, "Invalid decimal type. The precision is missing");
        assertSyntaxError("create table x (v decimal(", 25, "Invalid decimal type. The precision is missing");
    }

    @Test
    public void testCreateMissingDecimalScale() throws Exception {
        assertSyntaxError("create table x (v decimal(5,))", 28, "Invalid decimal type. The scale is missing");
        assertSyntaxError("create table x (v decimal(5,", 27, "Invalid decimal type. The scale is missing");
    }

    @Test
    public void testCreatePrecisionTooBig() throws Exception {
        assertSyntaxError("create table x (v decimal(80, 6))", 25, "Invalid decimal type. The precision (80) must be less than ");
    }

    @Test
    public void testCreatePrecisionTooSmall() throws Exception {
        assertSyntaxError("create table x (v decimal(0, 6))", 25, "Invalid decimal type. The precision (0) must be greater than zero");
    }

    @Test
    public void testCreateScalingGreaterPrecision() throws Exception {
        assertSyntaxError("create table x (v decimal(5, 6))", 25, "Invalid decimal type. The precision (5) must be greater than or equal to the scale (6)");
    }
}
