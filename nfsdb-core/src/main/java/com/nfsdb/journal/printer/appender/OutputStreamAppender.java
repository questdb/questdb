/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.printer.appender;

import java.io.IOException;
import java.io.OutputStream;

public class OutputStreamAppender implements Appender {
    private final OutputStream out;
    private byte[] lineSeparator;

    public OutputStreamAppender(OutputStream out) {
        this.out = out;

        String separator = System.getProperty("line.separator");
        lineSeparator = new byte[separator.length()];
        for (int i = 0; i < separator.length(); i++) {
            lineSeparator[i] = (byte) separator.charAt(i);
        }
    }

    @Override
    public void append(StringBuilder stringBuilder) throws IOException {
        for (int i = 0; i < stringBuilder.length(); i++) {
            out.write(stringBuilder.charAt(i));
        }
        out.write(this.lineSeparator);
    }

    @Override
    public void close() throws IOException {
        out.flush();
    }
}
