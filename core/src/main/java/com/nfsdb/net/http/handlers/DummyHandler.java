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

package com.nfsdb.net.http.handlers;

import com.nfsdb.ex.ResponseContentBufferTooSmallException;
import com.nfsdb.misc.Misc;
import com.nfsdb.misc.Numbers;
import com.nfsdb.net.http.ChunkedResponse;
import com.nfsdb.net.http.ContextHandler;
import com.nfsdb.net.http.IOContext;

import java.io.IOException;

public class DummyHandler implements ContextHandler {
    private int counter = -1;

    @Override
    public void handle(IOContext context) throws IOException {
        ChunkedResponse r = context.chunkedResponse();
//        r.setCompressed(true);
        r.status(200, "text/plain; charset=utf-8");
        r.sendHeader();
        counter = -1;
        resume(context);
    }

    @Override
    public void resume(IOContext context) throws IOException {
        System.out.println("resuming");
        ChunkedResponse r = context.chunkedResponse();
        for (int i = counter + 1; i < 2; i++) {
            counter = i;
            try {
                for (int k = 0; k < 16 * 1024 * 1024 - 4; k++) {
                    Numbers.append(r, i);
                }
//                r.put("This is chunk ");
//                Numbers.append(r, i);
                r.put(Misc.EOL);
            } catch (ResponseContentBufferTooSmallException ignore) {
                // ignore, send as much as we can in one chunk
            }
            r.sendChunk();
        }
        r.done();
    }
}
