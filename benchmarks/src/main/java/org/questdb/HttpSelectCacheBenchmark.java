/*+*****************************************************************************
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

package org.questdb;

import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.http.ActiveConnectionTracker;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpHeaderParser;
import io.questdb.cutlass.http.processors.JsonQueryProcessorState;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ConcurrentAssociativeCache;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_QUERY;

/**
 * Decodes the SQL parameters, looks up a factory and releases the request, without timing socket I/O,
 * query compilation or execution. A zero-length value omits the variables parameter.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED"
})
public class HttpSelectCacheBenchmark {
    private static final RecordCursorFactory FACTORY = new RecordCursorFactory() {
        @Override
        public RecordMetadata getMetadata() {
            return null;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }
    };
    @Param({"true", "false"})
    public boolean isCacheEnabled;
    @Param({"0", "16", "1024"})
    public int valueLength;
    private HttpConnectionContext context;
    private DirectUtf8Sequence query;
    private JsonQueryProcessorState state;

    @Benchmark
    public RecordCursorFactory lookupAndReturn() throws Exception {
        state.configure(context.getRequestHeader(), query, 0, Long.MAX_VALUE);
        state.parseQueryVariables(context.getRequestHeader());
        final RecordCursorFactory hit = context.getSelectCache().poll(state.getSelectCacheKey(AllowAllSecurityContext.INSTANCE));
        state.setFactoryForTest(hit != null ? hit : FACTORY, true);
        state.clear();
        return hit;
    }

    @Setup
    public void setup() {
        final DefaultHttpServerConfiguration config = new DefaultHttpServerConfiguration(new DefaultCairoConfiguration("."));
        context = isCacheEnabled
                ? new HttpConnectionContext(
                        config,
                        PlainSocketFactory.INSTANCE,
                        new ConcurrentAssociativeCache<>(config.getConcurrentCacheConfiguration()),
                        ActiveConnectionTracker.NO_TRACKING
                )
                : new HttpConnectionContext(config, PlainSocketFactory.INSTANCE);
        final HttpHeaderParser header = (HttpHeaderParser) context.getRequestHeader();
        try (DirectUtf8Sink request = new DirectUtf8Sink(128 + valueLength)) {
            request.putAscii("GET /exec?query=");
            if (valueLength == 0) {
                request.putAscii("SELECT+1");
            } else {
                request.putAscii("SELECT+%40v&variables=%40v+%3A%3D+%27").putAscii("x".repeat(valueLength)).putAscii("%27");
            }
            request.putAscii(" HTTP/1.1\r\n\r\n");
            header.parse(request.ptr(), request.ptr() + request.size(), true, false);
        }
        query = header.getUrlParam(URL_PARAM_QUERY);
        state = new JsonQueryProcessorState(context, config.getJsonQueryProcessorConfiguration().getNanosecondClock(), null);
    }

    @TearDown
    public void tearDown() {
        state.close();
        context.getSelectCache().close();
        context.close();
    }
}
