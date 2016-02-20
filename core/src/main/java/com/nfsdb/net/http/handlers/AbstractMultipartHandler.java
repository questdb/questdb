/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.net.http.handlers;

import com.nfsdb.ex.DisconnectedChannelException;
import com.nfsdb.ex.SlowWritableChannelException;
import com.nfsdb.net.http.ContextHandler;
import com.nfsdb.net.http.IOContext;
import com.nfsdb.net.http.MultipartListener;
import com.nfsdb.net.http.RequestHeaderBuffer;
import com.nfsdb.std.ByteSequence;
import com.nfsdb.std.DirectByteCharSequence;
import com.nfsdb.std.LocalValue;
import com.nfsdb.std.Mutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.IOException;

public abstract class AbstractMultipartHandler implements ContextHandler, MultipartListener {

    private final LocalValue<MultipartContext> lvContext = new LocalValue<>();

    @Override
    public final void handle(IOContext context) throws IOException {
        onPartEnd(context);
        onComplete0(context);
    }

    @SuppressFBWarnings("ACEM_ABSTRACT_CLASS_EMPTY_METHODS")
    @Override
    public void resume(IOContext context) throws IOException {
    }

    @Override
    public final void onChunk(IOContext context, RequestHeaderBuffer hb, DirectByteCharSequence data, boolean continued) throws IOException {
        if (!continued) {
            MultipartContext h = lvContext.get(context);
            if (h == null) {
                lvContext.set(context, h = new MultipartContext());
            }

            if (h.chunky) {
                onPartEnd(context);
            }
            h.chunky = true;
            onPartBegin(context, hb);
        }
        onData(context, hb, data);
    }

    protected abstract void onComplete0(IOContext context) throws IOException;

    protected abstract void onData(IOContext context, RequestHeaderBuffer hb, ByteSequence data) throws DisconnectedChannelException, SlowWritableChannelException;

    protected abstract void onPartBegin(IOContext context, RequestHeaderBuffer hb) throws IOException;

    protected abstract void onPartEnd(IOContext context) throws IOException;

    private static class MultipartContext implements Mutable, Closeable {
        private boolean chunky = false;

        @Override
        public void clear() {
            chunky = false;
        }

        @Override
        public void close() {
            clear();
        }
    }
}
