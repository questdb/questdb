/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.net.http;

import org.junit.Assert;
import org.junit.Test;

public class RangeParserTest {

    private final RangeParser parser = RangeParser.FACTORY.newInstance();

    @Test
    public void testFmt1() throws Exception {
        Assert.assertTrue(parser.of("bytes=79990-"));
        Assert.assertEquals(79990L, parser.getLo());
        Assert.assertEquals(Long.MAX_VALUE, parser.getHi());
    }

    @Test
    public void testFmt2() throws Exception {
        Assert.assertTrue(parser.of("bytes=79990-43343245"));
        Assert.assertEquals(79990L, parser.getLo());
        Assert.assertEquals(43343245L, parser.getHi());
    }

    @Test
    public void testInvalid1() throws Exception {
        Assert.assertFalse(parser.of("zeroes=79990-43343245"));
    }

    @Test
    public void testInvalid2() throws Exception {
        Assert.assertFalse(parser.of("bytes=79990x-43343245"));
    }

    @Test
    public void testInvalid3() throws Exception {
        Assert.assertFalse(parser.of("bytes=7999-0-43343245"));
    }

    @Test
    public void testInvalid4() throws Exception {
        Assert.assertFalse(parser.of("bytes=7999"));
    }
}