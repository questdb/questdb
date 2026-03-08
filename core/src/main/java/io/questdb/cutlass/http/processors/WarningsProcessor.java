/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.net.HttpURLConnection;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.ErrorTag.*;

public class WarningsProcessor implements HttpRequestProcessor, HttpRequestHandler {
    private static final long RECOMMENDED_FILE_LIMIT = 1048576;
    private static final long RECOMMENDED_MAP_COUNT_LIMIT = 1048576;
    private static final String TAG = "tag";
    private static final String WARNING = "warning";
    private static final AtomicReference<StringSink> sinkRef = new AtomicReference<>(new StringSink());

    public WarningsProcessor(CairoConfiguration configuration) {
        synchronized (sinkRef) {
            final StringSink sink = sinkRef.get();
            sink.clear();
            sink.putAscii('[');

            final FilesFacade ff = configuration.getFilesFacade();
            final String rootDir = configuration.getDbRoot();
            try (Path path = new Path()) {
                final long fsStatus = ff.getFileSystemStatus(path.of(rootDir).$());
                if (fsStatus >= 0 && !(fsStatus == 0 && Os.type == Os.DARWIN && Os.arch == Os.ARCH_AARCH64)) {
                    sink.putAscii('{').putQuoted(TAG).putAscii(':').putQuoted(UNSUPPORTED_FILE_SYSTEM.text())
                            .putAscii(',').putQuoted(WARNING).putAscii(":\"")
                            .putAscii("Unsupported file system [dir=").put(rootDir).putAscii(", magic=0x");
                    Numbers.appendHex(sink, fsStatus, false);
                    sink.putAscii("]\"}");
                }
            }

            final long fileLimit = ff.getFileLimit();
            if (fileLimit < 0) {
                throw CairoException.nonCritical().put("Could not read fs.file-max [errno=").put(Os.errno()).put("]");
            }
            if (fileLimit > 0 && fileLimit < RECOMMENDED_FILE_LIMIT) {
                if (sink.length() > 1) {
                    sink.putAscii(',');
                }
                sink.putAscii('{').putQuoted(TAG).putAscii(':').putQuoted(TOO_MANY_OPEN_FILES.text())
                        .putAscii(',').putQuoted(WARNING).putAscii(":\"")
                        .putAscii("fs.file-max limit is too low [current=").put(fileLimit)
                        .putAscii(", recommended=").put(RECOMMENDED_FILE_LIMIT).putAscii("]\"}");
            }

            final long mapCountLimit = ff.getMapCountLimit();
            if (mapCountLimit < 0) {
                throw CairoException.nonCritical().put("Could not read vm.max_map_count [errno=").put(Os.errno()).put("]");
            }
            if (mapCountLimit > 0 && mapCountLimit < RECOMMENDED_MAP_COUNT_LIMIT) {
                if (sink.length() > 1) {
                    sink.putAscii(',');
                }
                sink.putAscii('{').putQuoted(TAG).putAscii(':').putQuoted(OUT_OF_MMAP_AREAS.text())
                        .putAscii(',').putQuoted(WARNING).putAscii(":\"")
                        .putAscii("vm.max_map_count limit is too low [current=").put(mapCountLimit)
                        .putAscii(", recommended=").put(RECOMMENDED_MAP_COUNT_LIMIT).putAscii("]\"}");
            }

            sink.putAscii(']');
        }
    }

    @TestOnly
    public static void override(@NotNull String tag, @NotNull String warning) {
        final StringSink current = sinkRef.get();
        final StringSink sink = new StringSink();

        synchronized (sinkRef) {
            if (tag.isEmpty()) {
                sink.putAscii("[]");
            } else {
                if (current.length() == 0) {
                    sink.putAscii('[');
                } else {
                    sink.put(current.subSequence(0, current.length() - 1));
                }
                if (sink.length() > 1) {
                    sink.putAscii(',');
                }
                sink.putAscii('{')
                        .putQuoted(TAG).putAscii(':').putQuoted(ErrorTag.resolveTag(tag).text())
                        .putAscii(',')
                        .putQuoted(WARNING).putAscii(':').putQuoted(warning)
                        .putAscii('}')
                        .putAscii(']');
            }
            sinkRef.set(sink);
        }
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        return this;
    }

    @Override
    public byte getRequiredAuthType() {
        return SecurityContext.AUTH_TYPE_NONE;
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpChunkedResponse r = context.getChunkedResponse();
        r.status(HttpURLConnection.HTTP_OK, "application/json");
        r.sendHeader();
        r.put(sinkRef.get());
        r.sendChunk(true);
    }
}
