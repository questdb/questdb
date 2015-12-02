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

package com.nfsdb.io.parser;

import com.nfsdb.io.TextFileFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FormatParserTest {

    private static final int maxLen = 16 * 1024 * 1024;
    private final FormatParser p = new FormatParser();
    private long address;
    private int len;
    private RandomAccessFile raf;

    @After
    public void tearDown() throws Exception {
        close();
    }

    @Test
    public void testCsv() throws Exception {
        open("/csv/test-import.csv");
        p.of(this.address, this.len);
        Assert.assertEquals(TextFileFormat.CSV, p.getFormat());
        Assert.assertEquals(0d, p.getStdDev(), 0.0000001d);
        Assert.assertEquals(104, p.getAvgRecLen());
    }

    @Test
    public void testRandomText() throws Exception {
        open("/upload.html");
        p.of(this.address, this.len);
        Assert.assertTrue(p.getStdDev() > 1d);
    }

    private void close() throws IOException {
        if (this.raf != null) {
            this.raf.close();
        }
    }

    private void open(String resource) throws IOException {
        URL url = this.getClass().getResource(resource);
        if (url == null) {
            throw new IllegalArgumentException("Not found: " + resource);
        }
        String file = url.getFile();
        this.raf = new RandomAccessFile(file, "r");
        FileChannel channel = raf.getChannel();
        this.len = maxLen < raf.length() ? maxLen : (int) raf.length();
        ByteBuffer b = channel.map(FileChannel.MapMode.READ_ONLY, 0, this.len);
        this.address = ((DirectBuffer) b).address();
    }
}