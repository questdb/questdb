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

package com.nfsdb.std;

import com.nfsdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class FileNameExtractorCharSequenceTest {

    private static char separator;
    private static FileNameExtractorCharSequence extractor = new FileNameExtractorCharSequence();

    @BeforeClass
    public static void setUp() throws Exception {
        separator = System.getProperty("file.separator").charAt(0);
    }

    @Test
    public void testEmptyString() throws Exception {
        TestUtils.assertEquals("", extractor.of(""));
    }

    @Test
    public void testNameFromPath() throws Exception {
        StringBuilder name = new StringBuilder();
        name.append(separator).append("xyz").append(separator).append("dir1").append(separator).append("dir2").append(separator).append("this is my name");
        TestUtils.assertEquals("this is my name", extractor.of(name));
    }

    @Test
    public void testPlainName() throws Exception {
        TestUtils.assertEquals("xyz.txt", extractor.of("xyz.txt"));
    }
}