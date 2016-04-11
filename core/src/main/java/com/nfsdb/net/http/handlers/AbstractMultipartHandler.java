/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

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
        onData(context, data);
    }

    protected abstract void onComplete0(IOContext context) throws IOException;

    protected abstract void onData(IOContext context, ByteSequence data) throws DisconnectedChannelException, SlowWritableChannelException;

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
