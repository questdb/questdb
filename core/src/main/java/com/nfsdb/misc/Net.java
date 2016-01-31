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

package com.nfsdb.misc;

import com.nfsdb.ex.NumericException;
import com.nfsdb.net.Kqueue;

public final class Net {
    private Net() {
    }

    public native static int accept(int fd);

    public native static boolean bind(int fd, int address, int port);

    public static boolean bind(int fd, CharSequence address, int port) {
        return bind(fd, parseIPv4(address), port);
    }

    public native static void listen(int fd, int backlog);

    public static void main(String[] args) {
        Os.init();
        int fd = socketTcp(false);
        System.out.println(fd);
        System.out.println(bind(fd, "127.0.0.1", 5000));
//        System.out.println(accept(fd));

        listen(fd, 1024);
        Kqueue kqueue = new Kqueue();
        kqueue.readFD(0, fd);
        System.out.println(kqueue.register(1));

        System.out.println(kqueue.getFlags(0));

        while (kqueue.poll() == 0) {
            Thread.yield();
        }
        System.out.println("ok");
    }

    public static int parseIPv4(CharSequence address) {
        int ip = 0;
        int count = 0;
        int lo = 0;
        int hi;
        try {
            while ((hi = Chars.indexOf(address, lo, '.')) > -1) {
                int n = Numbers.parseInt(address, lo, hi);
                ip = (ip << 8) | n;
                count++;
                lo = hi + 1;
            }

            if (count != 3) {
                throw new RuntimeException("bad dog");
            }

            return (ip << 8) | Numbers.parseInt(address, lo, address.length());
        } catch (NumericException e) {
            throw new RuntimeException("bad dog");
        }
    }

    public native static int socketTcp(boolean blocking);
}
