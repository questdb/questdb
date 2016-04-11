/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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

package com.nfsdb.io.parser;

import com.nfsdb.io.TextFileFormat;
import com.nfsdb.misc.ByteBuffers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FormatParserTest {

    private static final int maxLen = 16 * 1024 * 1024;
    private final FormatParser p = FormatParser.FACTORY.newInstance();
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