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

package com.questdb.net.http;

import org.junit.Assert;
import org.junit.Test;

public class RangeParserTest {

    private final RangeParser parser = RangeParser.FACTORY.newInstance();

    @Test
    public void testFmt1() {
        Assert.assertTrue(parser.of("bytes=79990-"));
        Assert.assertEquals(79990L, parser.getLo());
        Assert.assertEquals(Long.MAX_VALUE, parser.getHi());
    }

    @Test
    public void testFmt2() {
        Assert.assertTrue(parser.of("bytes=79990-43343245"));
        Assert.assertEquals(79990L, parser.getLo());
        Assert.assertEquals(43343245L, parser.getHi());
    }

    @Test
    public void testInvalid1() {
        Assert.assertFalse(parser.of("zeroes=79990-43343245"));
    }

    @Test
    public void testInvalid2() {
        Assert.assertFalse(parser.of("bytes=79990x-43343245"));
    }

    @Test
    public void testInvalid3() {
        Assert.assertFalse(parser.of("bytes=7999-0-43343245"));
    }

    @Test
    public void testInvalid4() {
        Assert.assertFalse(parser.of("bytes=7999"));
    }
}