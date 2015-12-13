/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.http;

import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MimeTypesTest {
    @Test
    public void testLoading() throws Exception {
        MimeTypes mimeTypes = new MimeTypes(this.getClass().getResource("/mime_test.types").getFile());
        Assert.assertEquals(6, mimeTypes.size());
        TestUtils.assertEquals("application/andrew-inset", mimeTypes.get("ez"));
        TestUtils.assertEquals("application/inkml+xml", mimeTypes.get("ink"));
        TestUtils.assertEquals("application/inkml+xml", mimeTypes.get("inkml"));
        TestUtils.assertEquals("application/mp21", mimeTypes.get("m21"));
        TestUtils.assertEquals("application/mp21", mimeTypes.get("mp21"));
        TestUtils.assertEquals("application/mp4", mimeTypes.get("mp4s"));
    }
}