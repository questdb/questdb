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

import com.nfsdb.test.tools.TestUtils;
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