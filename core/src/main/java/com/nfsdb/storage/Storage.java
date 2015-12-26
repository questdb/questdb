/*
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
 */

package com.nfsdb.storage;

import com.nfsdb.collections.LPSZ;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Dates;
import com.nfsdb.misc.Files;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Storage {

    public static void main(String[] args) throws IOException {

        LPSZ lpsz = new LPSZ();

        long fd = Files.openRO(lpsz.of("x.txt"));
        System.out.println(fd);
        try {
            ByteBuffer buf = ByteBuffer.allocateDirect(1024);
            ByteBuffers.putStr(buf, "hello from java");
            int len = buf.position();
            Files.write(fd, ByteBuffers.getAddress(buf), len, 0);

            buf.clear();

            ByteBuffers.putStr(buf, ", awesome");
            Files.write(fd, ByteBuffers.getAddress(buf), buf.position(), len);
            System.out.println("hello");
        } finally {
            Files.close(fd);
        }


//        FileOutputStream s = new FileOutputStream("x.txt");
//        int fd = Unsafe.getUnsafe().getInt(s.getFD(), FD);
//
//        ByteBuffer buf = ByteBuffer.allocateDirect(1024);
//        ByteBuffers.putStr(buf, "hello from java");
//        write(fd, ByteBuffers.getAddress(buf), buf.remaining(), 0);
//        System.out.println("hello");

        long t = Files.getLastModified(new LPSZ("pom.xml").address());
        System.out.println(Dates.toString(t));
//        System.out.println(getLastModified(new LPSZ("/Users/vlad/dev/nfsdb/x.txt").address()));
//        s.close();

        System.out.println("Hello");
    }


    static {
        System.loadLibrary("nfsdb");
    }
}
