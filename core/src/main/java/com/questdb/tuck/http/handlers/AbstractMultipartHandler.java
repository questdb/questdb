/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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
 ******************************************************************************/

package com.questdb.tuck.http.handlers;

import com.questdb.std.LocalValue;
import com.questdb.std.Mutable;
import com.questdb.std.str.ByteSequence;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.tuck.http.ContextHandler;
import com.questdb.tuck.http.IOContext;
import com.questdb.tuck.http.MultipartListener;
import com.questdb.tuck.http.RequestHeaderBuffer;

import java.io.Closeable;
import java.io.IOException;

public abstract class AbstractMultipartHandler implements ContextHandler, MultipartListener {

    private final LocalValue<MultipartContext> lvContext = new LocalValue<>();

    @Override
    public final void handle(IOContext context) throws IOException {
        onPartEnd(context);
        onComplete0(context);
    }

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
        onData(context, data);
    }

    protected abstract void onComplete0(IOContext context) throws IOException;

    protected abstract void onData(IOContext context, ByteSequence data) throws IOException;

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
