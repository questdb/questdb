/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package org.questdb;

import io.questdb.cutlass.text.CsvTextLexer;
import io.questdb.cutlass.text.DefaultTextConfiguration;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import java.util.concurrent.atomic.AtomicInteger;

public class CsvLexerBenchmark {
    public static void main(String[] args) {
        long BUF_SIZE = 64 * 1024;
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        long fd = Files.openRO(new Path().of("C:\\Users\\Vlad\\dev\\csv-parsers-comparison\\worldcitiespop.txt").$());
        assert fd > 0;
        AtomicInteger counter = new AtomicInteger();

        CsvTextLexer lexer = new CsvTextLexer(new DefaultTextConfiguration());
        CsvTextLexer.Listener listener = (line, fields, hi) -> counter.incrementAndGet();


        for (int i = 0; i < 20; i++) {
            long o = 0;
            long t = System.nanoTime();
            counter.set(0);
            lexer.setupLimits(Integer.MAX_VALUE, listener);
            while (true) {
                long r = Files.read(fd, buf, BUF_SIZE, o);
                if (r < 1) {
                    break;
                }
                lexer.parse(buf, buf + r);
                o += r;
            }
            lexer.parseLast();
            System.out.println(System.nanoTime() - t);
        }
    }
}
