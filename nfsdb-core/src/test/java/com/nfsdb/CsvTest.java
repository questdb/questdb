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

package com.nfsdb;

import com.nfsdb.imp.Csv;
import com.nfsdb.utils.ByteBuffers;
import org.junit.Test;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

public class CsvTest {

    public static final int BUF_SIZE = 1024 * 1024 * 10;

    public static void main(String[] args) throws IOException {
        RandomAccessFile raf = new RandomAccessFile("D:\\data\\worldcitiespop.txt", "r");
        FileChannel channel = raf.getChannel();
        long size = channel.size();

        Csv csv = new Csv(false, new Csv.CsvListener() {
            @Override
            public void onError(int line) {
                System.out.println("Error on line: " + line);
            }

            @Override
            public void onField(CharSequence value, int line, boolean eol) {
/*
                System.out.print(value.toString());
                if (eol) {
                    System.out.println();
                } else {
                    System.out.print(",");
                }
*/
            }

            @Override
            public void onNames(List<String> names) {

            }
        });


        for (int i = 0; i < 100; i++) {
            long p = 0;
            long t = System.nanoTime();
            while (p < size) {
                MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, p, size - p < BUF_SIZE ? size - p : BUF_SIZE);
                try {
                    p += buf.remaining();
                    csv.parse(
                            ((DirectBuffer) buf).address(), buf.remaining()

                    );
                } finally {
                    ByteBuffers.release(buf);
                }
            }
            System.out.println(csv.getLineCount() + " ~ " + (System.nanoTime() - t));
            csv.reset();
        }

    }

    @Test
    public void testParseTab() throws Exception {


    }
}
