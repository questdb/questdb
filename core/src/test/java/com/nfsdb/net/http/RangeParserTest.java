/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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