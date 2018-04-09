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

package com.questdb.griffin.engine.functions.constants;

import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class NullConstantTest {
    @Test
    public void testConstant() {
        NullConstant constant = NullConstant.INSTANCE;
        Assert.assertTrue(constant.isConstant());
        Assert.assertNull(constant.getStr(null));
        Assert.assertNull(constant.getStrB(null));
        Assert.assertEquals(-1, constant.getStrLen(null));

        CharSink sink = new StringSink();
        constant.getStr(null, sink);
        Assert.assertEquals(0, ((StringSink) sink).length());
    }
}