/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb.journal;

import com.nfsdb.journal.test.tools.TestCharSink;
import com.nfsdb.journal.utils.ByteBuffers;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Dates2;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Dates2Test {

    private final TestCharSink sink = new TestCharSink();

    public static void main(String[] args) throws IOException {
        FileOutputStream out = new FileOutputStream(FileDescriptor.out);
        ByteBuffer bb = ByteBuffer.allocate(1024);
        ByteBuffers.putStr(bb, "hello");
        bb.flip();
        out.getChannel().write(bb);
        out.close();
    }

    @Test
    public void testFormatISO() throws Exception {
        assertTrue("2014-11-30T12:34:55.332Z");
        assertTrue("2008-03-15T11:22:30.500Z");
        assertTrue("1917-10-01T11:22:30.500Z");
        assertTrue("900-01-01T01:02:00.005Z");
    }

    private void assertTrue(String date) {
        Dates2.appendDateISO(sink, Dates.toMillis(date));
        Assert.assertEquals(date, sink.toString());
        sink.clear();
    }
}
