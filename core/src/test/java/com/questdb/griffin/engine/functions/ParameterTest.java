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

package com.questdb.griffin.engine.functions;

import com.questdb.cairo.sql.Record;
import com.questdb.common.ColumnType;
import com.questdb.griffin.BaseFunctionFactoryTest;
import com.questdb.griffin.Function;
import com.questdb.griffin.FunctionParser;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.math.AddIntVVFunctionFactory;
import com.questdb.ql.CollectionRecordMetadata;
import org.junit.Assert;
import org.junit.Test;

public class ParameterTest extends BaseFunctionFactoryTest {

    @Test
    public void testIntParameter() throws SqlException {
        functions.add(new AddIntVVFunctionFactory());

        final CollectionRecordMetadata metadata = new CollectionRecordMetadata();
        metadata.add(new TestColumnMetadata("a", ColumnType.INT));
        FunctionParser functionParser = createFunctionParser();
        Function function = parseFunction("a + :xyz", metadata, functionParser);
        Parameter param = params.get(":xyz");
        Assert.assertNotNull(param);
        param.setInt(10);
        Assert.assertEquals(32, function.getInt(new Record() {
            @Override
            public int getInt(int col) {
                return 22;
            }
        }));

    }
}