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

package com.questdb.txt.parser;

import com.questdb.misc.ByteBuffers;
import com.questdb.txt.TextFileDelimiter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DelimiterDetectorTest {

    private static final int maxLen = 16 * 1024 * 1024;
    private final DelimiterDetector p = DelimiterDetector.FACTORY.newInstance();
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
        Assert.assertEquals(TextFileDelimiter.CSV, p.getDelimiter());
        Assert.assertEquals(0d, p.getStdDev(), 0.0000001d);
        Assert.assertEquals(105, p.getAvgRecLen());
    }

    @Test
    public void testRandomText() throws Exception {
        open("/site/public/upload.html");
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
        this.address = ByteBuffers.getAddress(b);
    }
}