package io.questdb.cutlass.http.processors;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpResponseSink;
import io.questdb.metrics.HealthMetricsService;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.ServerDisconnectedException;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;

import java.io.IOException;
import java.util.Map;

public class HealthMetricsProcessor implements HttpRequestProcessor {
    private final CairoEngine engine;
    private final HealthMetricsService healthMetricsService;

    public HealthMetricsProcessor(CairoEngine engine) {
        this.engine = engine;
        this.healthMetricsService = new HealthMetricsService(engine, engine.getConfiguration());
    }

    @Override
    public void onRequest(HttpConnectionContext context) throws IOException, PeerDisconnectedException, ServerDisconnectedException {
        HttpResponseSink responseSink = context.getResponseSink();
        responseSink.status(200, "OK");
        responseSink.header("Content-Type", "application/json");

        StringSink sink = new StringSink();
        sink.put('{');

        Map<String, Object> metrics = healthMetricsService.collectHealthMetrics();
        boolean first = true;
        for (Map.Entry<String, Object> entry : metrics.entrySet()) {
            if (!first) {
                sink.put(',');
            }
            first = false;
            sink.put('"').put(entry.getKey()).put("\":");
            serializeObject(sink, entry.getValue());
        }

        sink.put('}');
        responseSink.put(sink);
        responseSink.flush();
    }

    private void serializeObject(StringSink sink, Object value) {
        if (value == null) {
            sink.put("null");
        } else if (value instanceof Number) {
            sink.put(value.toString());
        } else if (value instanceof String) {
            sink.put('"').put(Chars.escapeJson((String) value)).put('"');
        } else if (value instanceof Map) {
            sink.put('{');
            boolean first = true;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!first) {
                    sink.put(',');
                }
                first = false;
                sink.put('"').put(entry.getKey().toString()).put("\":");
                serializeObject(sink, entry.getValue());
            }
            sink.put('}');
        } else {
            sink.put('"').put(value.toString()).put('"');
        }
    }

    @Override
    public void close() {
        Misc.free(engine);
    }
} 