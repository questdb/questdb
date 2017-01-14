/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.ql.impl.join;

import com.questdb.ql.impl.AbstractAllTypeTest;
import org.junit.Test;

public class JoinComplianceTest extends AbstractAllTypeTest {

    @Test
    public void testAsOfSymbolBehaviour() throws Exception {
        assertSymbol("select a.sym, b.sym from abc a asof join abc b", 0);
        assertSymbol("select a.sym, b.sym from abc a asof join abc b", 1);
    }

    @Test
    public void testCrossReset() throws Exception {
        assertThat("BZ\tBZ\n" +
                "BZ\tBZ\n" +
                "BZ\tXX\n" +
                "BZ\tXX\n" +
                "BZ\tXX\n" +
                "BZ\tBZ\n" +
                "BZ\tXX\n" +
                "BZ\tKK\n" +
                "BZ\tKK\n" +
                "BZ\tAX\n" +
                "BZ\tKK\n" +
                "BZ\tAX\n" +
                "BZ\tAX\n" +
                "BZ\tBZ\n" +
                "BZ\tAX\n" +
                "BZ\tAX\n" +
                "BZ\tKK\n" +
                "BZ\tAX\n" +
                "BZ\tBZ\n" +
                "BZ\tAX\n" +
                "BZ\tBZ\n" +
                "BZ\tBZ\n" +
                "BZ\tXX\n" +
                "BZ\tXX\n" +
                "BZ\tXX\n" +
                "BZ\tBZ\n" +
                "BZ\tXX\n" +
                "BZ\tKK\n" +
                "BZ\tKK\n" +
                "BZ\tAX\n" +
                "BZ\tKK\n" +
                "BZ\tAX\n" +
                "BZ\tAX\n" +
                "BZ\tBZ\n" +
                "BZ\tAX\n" +
                "BZ\tAX\n" +
                "BZ\tKK\n" +
                "BZ\tAX\n" +
                "BZ\tBZ\n" +
                "BZ\tAX\n" +
                "XX\tBZ\n" +
                "XX\tBZ\n" +
                "XX\tXX\n" +
                "XX\tXX\n" +
                "XX\tXX\n" +
                "XX\tBZ\n" +
                "XX\tXX\n" +
                "XX\tKK\n" +
                "XX\tKK\n" +
                "XX\tAX\n" +
                "XX\tKK\n" +
                "XX\tAX\n" +
                "XX\tAX\n" +
                "XX\tBZ\n" +
                "XX\tAX\n" +
                "XX\tAX\n" +
                "XX\tKK\n" +
                "XX\tAX\n" +
                "XX\tBZ\n" +
                "XX\tAX\n" +
                "XX\tBZ\n" +
                "XX\tBZ\n" +
                "XX\tXX\n" +
                "XX\tXX\n" +
                "XX\tXX\n" +
                "XX\tBZ\n" +
                "XX\tXX\n" +
                "XX\tKK\n" +
                "XX\tKK\n" +
                "XX\tAX\n" +
                "XX\tKK\n" +
                "XX\tAX\n" +
                "XX\tAX\n" +
                "XX\tBZ\n" +
                "XX\tAX\n" +
                "XX\tAX\n" +
                "XX\tKK\n" +
                "XX\tAX\n" +
                "XX\tBZ\n" +
                "XX\tAX\n" +
                "XX\tBZ\n" +
                "XX\tBZ\n" +
                "XX\tXX\n" +
                "XX\tXX\n" +
                "XX\tXX\n" +
                "XX\tBZ\n" +
                "XX\tXX\n" +
                "XX\tKK\n" +
                "XX\tKK\n" +
                "XX\tAX\n" +
                "XX\tKK\n" +
                "XX\tAX\n" +
                "XX\tAX\n" +
                "XX\tBZ\n" +
                "XX\tAX\n" +
                "XX\tAX\n" +
                "XX\tKK\n" +
                "XX\tAX\n" +
                "XX\tBZ\n" +
                "XX\tAX\n" +
                "BZ\tBZ\n" +
                "BZ\tBZ\n" +
                "BZ\tXX\n" +
                "BZ\tXX\n" +
                "BZ\tXX\n" +
                "BZ\tBZ\n" +
                "BZ\tXX\n" +
                "BZ\tKK\n" +
                "BZ\tKK\n" +
                "BZ\tAX\n" +
                "BZ\tKK\n" +
                "BZ\tAX\n" +
                "BZ\tAX\n" +
                "BZ\tBZ\n" +
                "BZ\tAX\n" +
                "BZ\tAX\n" +
                "BZ\tKK\n" +
                "BZ\tAX\n" +
                "BZ\tBZ\n" +
                "BZ\tAX\n" +
                "XX\tBZ\n" +
                "XX\tBZ\n" +
                "XX\tXX\n" +
                "XX\tXX\n" +
                "XX\tXX\n" +
                "XX\tBZ\n" +
                "XX\tXX\n" +
                "XX\tKK\n" +
                "XX\tKK\n" +
                "XX\tAX\n" +
                "XX\tKK\n" +
                "XX\tAX\n" +
                "XX\tAX\n" +
                "XX\tBZ\n" +
                "XX\tAX\n" +
                "XX\tAX\n" +
                "XX\tKK\n" +
                "XX\tAX\n" +
                "XX\tBZ\n" +
                "XX\tAX\n" +
                "KK\tBZ\n" +
                "KK\tBZ\n" +
                "KK\tXX\n" +
                "KK\tXX\n" +
                "KK\tXX\n" +
                "KK\tBZ\n" +
                "KK\tXX\n" +
                "KK\tKK\n" +
                "KK\tKK\n" +
                "KK\tAX\n" +
                "KK\tKK\n" +
                "KK\tAX\n" +
                "KK\tAX\n" +
                "KK\tBZ\n" +
                "KK\tAX\n" +
                "KK\tAX\n" +
                "KK\tKK\n" +
                "KK\tAX\n" +
                "KK\tBZ\n" +
                "KK\tAX\n" +
                "KK\tBZ\n" +
                "KK\tBZ\n" +
                "KK\tXX\n" +
                "KK\tXX\n" +
                "KK\tXX\n" +
                "KK\tBZ\n" +
                "KK\tXX\n" +
                "KK\tKK\n" +
                "KK\tKK\n" +
                "KK\tAX\n" +
                "KK\tKK\n" +
                "KK\tAX\n" +
                "KK\tAX\n" +
                "KK\tBZ\n" +
                "KK\tAX\n" +
                "KK\tAX\n" +
                "KK\tKK\n" +
                "KK\tAX\n" +
                "KK\tBZ\n" +
                "KK\tAX\n" +
                "AX\tBZ\n" +
                "AX\tBZ\n" +
                "AX\tXX\n" +
                "AX\tXX\n" +
                "AX\tXX\n" +
                "AX\tBZ\n" +
                "AX\tXX\n" +
                "AX\tKK\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tAX\n" +
                "AX\tAX\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tAX\n" +
                "KK\tBZ\n" +
                "KK\tBZ\n" +
                "KK\tXX\n" +
                "KK\tXX\n" +
                "KK\tXX\n" +
                "KK\tBZ\n" +
                "KK\tXX\n" +
                "KK\tKK\n" +
                "KK\tKK\n" +
                "KK\tAX\n" +
                "KK\tKK\n" +
                "KK\tAX\n" +
                "KK\tAX\n" +
                "KK\tBZ\n" +
                "KK\tAX\n" +
                "KK\tAX\n" +
                "KK\tKK\n" +
                "KK\tAX\n" +
                "KK\tBZ\n" +
                "KK\tAX\n" +
                "AX\tBZ\n" +
                "AX\tBZ\n" +
                "AX\tXX\n" +
                "AX\tXX\n" +
                "AX\tXX\n" +
                "AX\tBZ\n" +
                "AX\tXX\n" +
                "AX\tKK\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tAX\n" +
                "AX\tAX\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tBZ\n" +
                "AX\tXX\n" +
                "AX\tXX\n" +
                "AX\tXX\n" +
                "AX\tBZ\n" +
                "AX\tXX\n" +
                "AX\tKK\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tAX\n" +
                "AX\tAX\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tAX\n" +
                "BZ\tBZ\n" +
                "BZ\tBZ\n" +
                "BZ\tXX\n" +
                "BZ\tXX\n" +
                "BZ\tXX\n" +
                "BZ\tBZ\n" +
                "BZ\tXX\n" +
                "BZ\tKK\n" +
                "BZ\tKK\n" +
                "BZ\tAX\n" +
                "BZ\tKK\n" +
                "BZ\tAX\n" +
                "BZ\tAX\n" +
                "BZ\tBZ\n" +
                "BZ\tAX\n" +
                "BZ\tAX\n" +
                "BZ\tKK\n" +
                "BZ\tAX\n" +
                "BZ\tBZ\n" +
                "BZ\tAX\n" +
                "AX\tBZ\n" +
                "AX\tBZ\n" +
                "AX\tXX\n" +
                "AX\tXX\n" +
                "AX\tXX\n" +
                "AX\tBZ\n" +
                "AX\tXX\n" +
                "AX\tKK\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tAX\n" +
                "AX\tAX\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tBZ\n" +
                "AX\tXX\n" +
                "AX\tXX\n" +
                "AX\tXX\n" +
                "AX\tBZ\n" +
                "AX\tXX\n" +
                "AX\tKK\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tAX\n" +
                "AX\tAX\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tAX\n" +
                "KK\tBZ\n" +
                "KK\tBZ\n" +
                "KK\tXX\n" +
                "KK\tXX\n" +
                "KK\tXX\n" +
                "KK\tBZ\n" +
                "KK\tXX\n" +
                "KK\tKK\n" +
                "KK\tKK\n" +
                "KK\tAX\n" +
                "KK\tKK\n" +
                "KK\tAX\n" +
                "KK\tAX\n" +
                "KK\tBZ\n" +
                "KK\tAX\n" +
                "KK\tAX\n" +
                "KK\tKK\n" +
                "KK\tAX\n" +
                "KK\tBZ\n" +
                "KK\tAX\n" +
                "AX\tBZ\n" +
                "AX\tBZ\n" +
                "AX\tXX\n" +
                "AX\tXX\n" +
                "AX\tXX\n" +
                "AX\tBZ\n" +
                "AX\tXX\n" +
                "AX\tKK\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tAX\n" +
                "AX\tAX\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tAX\n" +
                "BZ\tBZ\n" +
                "BZ\tBZ\n" +
                "BZ\tXX\n" +
                "BZ\tXX\n" +
                "BZ\tXX\n" +
                "BZ\tBZ\n" +
                "BZ\tXX\n" +
                "BZ\tKK\n" +
                "BZ\tKK\n" +
                "BZ\tAX\n" +
                "BZ\tKK\n" +
                "BZ\tAX\n" +
                "BZ\tAX\n" +
                "BZ\tBZ\n" +
                "BZ\tAX\n" +
                "BZ\tAX\n" +
                "BZ\tKK\n" +
                "BZ\tAX\n" +
                "BZ\tBZ\n" +
                "BZ\tAX\n" +
                "AX\tBZ\n" +
                "AX\tBZ\n" +
                "AX\tXX\n" +
                "AX\tXX\n" +
                "AX\tXX\n" +
                "AX\tBZ\n" +
                "AX\tXX\n" +
                "AX\tKK\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tAX\n" +
                "AX\tAX\n" +
                "AX\tKK\n" +
                "AX\tAX\n" +
                "AX\tBZ\n" +
                "AX\tAX\n", "select a.sym, b.sym from abc a cross join abc b");

    }

    @Test
    public void testCrossSymbolBehaviour() throws Exception {
        assertSymbol("select a.sym, b.sym from abc a cross join abc b", 0);
        assertSymbol("select a.sym, b.sym from abc a cross join abc b", 1);
    }
}