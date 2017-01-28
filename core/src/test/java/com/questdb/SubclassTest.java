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

package com.questdb;

import com.questdb.model.SubQuote;
import com.questdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class SubclassTest extends AbstractTest {

    @Test
    public void testSubclass() throws Exception {

        try (JournalWriter<SubQuote> w = getFactory().writer(SubQuote.class)) {

            SubQuote q = new SubQuote().setType((byte) 10);
            q.setTimestamp(System.currentTimeMillis());
            q.setSym("ABC");

            w.append(q);

            SubQuote q2 = w.read(0);
            Assert.assertEquals(q.getSym(), q2.getSym());
            Assert.assertEquals(q.getTimestamp(), q2.getTimestamp());
            Assert.assertEquals(q.getType(), q2.getType());
        }
    }
}
