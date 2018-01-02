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

import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class MimeTypesTest {
    @Test
    public void testLoading() throws Exception {
        MimeTypes mimeTypes = new MimeTypes(new File(this.getClass().getResource("/mime_test.types").getFile()));
        Assert.assertEquals(6, mimeTypes.size());
        TestUtils.assertEquals("application/andrew-inset", mimeTypes.get("ez"));
        TestUtils.assertEquals("application/inkml+xml", mimeTypes.get("ink"));
        TestUtils.assertEquals("application/inkml+xml", mimeTypes.get("inkml"));
        TestUtils.assertEquals("application/mp21", mimeTypes.get("m21"));
        TestUtils.assertEquals("application/mp21", mimeTypes.get("mp21"));
        TestUtils.assertEquals("application/mp4", mimeTypes.get("mp4s"));
    }
}