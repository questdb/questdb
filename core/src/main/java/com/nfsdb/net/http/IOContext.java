/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb.net.http;

import com.nfsdb.ex.DisconnectedChannelException;
import com.nfsdb.ex.SlowWritableChannelException;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.parser.TextParser;
import com.nfsdb.io.parser.listener.JournalImportListener;
import com.nfsdb.iter.clock.Clock;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.Files;
import com.nfsdb.misc.Misc;
import com.nfsdb.mp.WorkerContext;
import com.nfsdb.net.NetworkChannel;
import com.nfsdb.ql.Record;
import com.nfsdb.std.AssociativeCache;
import com.nfsdb.std.FlyweightCharSequence;
import com.nfsdb.std.Mutable;
import com.nfsdb.std.ObjectFactory;
import com.nfsdb.store.PlainFile;

import java.io.Closeable;
import java.io.RandomAccessFile;
import java.util.Iterator;

public class IOContext implements Closeable, Mutable {
    private static final Log LOG = LogFactory.getLog(IOContext.class);

    public final NetworkChannel channel;
    public final Request request;
    public final FlyweightCharSequence ext = new FlyweightCharSequence();
    private final Response response;
    public WorkerContext threadContext;
    // multipart generic
    public boolean chunky = false;
    // file upload fields
    public PlainFile mf;
    public long wptr = 0;
    // import handler fields
    public boolean analysed = false;
    public boolean dataFormatValid = false;
    public TextParser textParser;
    public JournalImportListener importer;
    public long fd = -1;
    public long bytesSent;
    public long sendMax;
    // query sending fields
    public Iterator<? extends Record> records;
    public RecordMetadata metadata;
    public long count;
    public long skip;
    public long stop;
    public Record current;
    public boolean includeCount;
    // static sending fields
    private RandomAccessFile raf;

    public IOContext(NetworkChannel channel, Clock clock, int reqHeaderSize, int reqContentSize, int reqMultipartHeaderSize, int respHeaderSize, int respContentSize) {
        this.channel = channel;
        this.request = new Request(channel, reqHeaderSize, reqContentSize, reqMultipartHeaderSize);
        this.response = new Response(channel, respHeaderSize, respContentSize, clock);
    }

    public ChunkedResponse chunkedResponse() {
        return response.asChunked();
    }

    @Override
    public void clear() {
        request.clear();
        response.clear();
        this.chunky = false;
        freeResources();
    }

    @Override
    public void close() {
        Misc.free(channel);
        request.close();
        response.close();
        freeResources();
    }

    public FixedSizeResponse fixedSizeResponse() {
        return response.asFixedSize();
    }

    @SuppressWarnings("unchecked")
    public <T> T getThreadLocal(CharSequence key, ObjectFactory<T> factory) {
        AssociativeCache<Object> cache = threadContext.getCache();
        Object result = cache.get(key);
        if (result == null) {
            cache.put(key, result = factory.newInstance());
        }
        return (T) result;
    }

    public ResponseSink responseSink() {
        return response.asSink();
    }

    public void resume() throws DisconnectedChannelException, SlowWritableChannelException {
        response.resume();
    }

    public SimpleResponse simpleResponse() {
        return response.asSimple();
    }

    private void freeResources() {
        mf = Misc.free(mf);
        raf = Misc.free(raf);
        textParser = Misc.free(textParser);
        importer = Misc.free(importer);
        if (fd != -1) {
            if (Files.close(fd) != 0) {
                LOG.error().$("Could not close file").$();
            }
        }
    }
}
