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

import com.questdb.std.Hash;
import com.questdb.std.IntHashSet;
import com.questdb.std.Rnd;
import com.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class HashTest {

    @Test
    public void testStringHash() {
        Rnd rnd = new Rnd();
        IntHashSet hashes = new IntHashSet(100000);
        final int LEN = 64;

        long address = Unsafe.malloc(LEN);

        for (int i = 0; i < 100000; i++) {
            rnd.nextChars(address, LEN / 2);
            hashes.add(Hash.hashMem(address, LEN));
        }
        Assert.assertTrue("Hash function distribution dropped", hashes.size() > 99990);
    }
}
