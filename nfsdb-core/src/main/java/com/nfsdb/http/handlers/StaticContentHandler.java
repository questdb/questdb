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

package com.nfsdb.http.handlers;

import com.nfsdb.http.ContextHandler;
import com.nfsdb.http.IOContext;
import com.nfsdb.misc.Misc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class StaticContentHandler implements ContextHandler {
    public void _continue(IOContext context) throws IOException {
        if (context.raf == null) {
            return;
        }
        FileChannel ch = context.raf.getChannel();
        ByteBuffer out = context.response.getOut();

        while (ch.read(out) > 0) {
            out.flip();
            context.response.sendBody();
        }

        context.raf = Misc.free(context.raf);
    }

    @Override
    public void handle(IOContext context) throws IOException {
        context.raf = new RandomAccessFile("/Users/vlad/Downloads/Stats19-Data1979-2004/Accidents7904.csv", "r");
        context.response.setFragmented(true);
        context.response.status(200, "text/plain; charset=utf-8", context.raf.length());
        context.response.headers().put("Content-Disposition: attachment; filename=\"").put("Accidents7904.csv").put("\"").put(Misc.EOL);
        //Content-Disposition: attachment; filename="filename.pdf"

        context.response.sendHeader();
        _continue(context);
    }
}
