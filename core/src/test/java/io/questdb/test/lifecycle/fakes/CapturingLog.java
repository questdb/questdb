package io.questdb.test.lifecycle.fakes;

import io.questdb.cairo.TimestampDriver;
import io.questdb.log.Log;
import io.questdb.log.LogRecord;
import io.questdb.std.Misc;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;

/**
 * Test-only Log fake that records every emitted message into a {@link StringSink}.
 * Mirrors the MockLog pattern in {@code LogAlertSocketTest.java:477}.
 * <p>
 * Both {@link #info()} and {@link #error()} return the recording {@link CapturingRecord}
 * because the orchestrator emits info on non-FAILED transitions and error on FAILED.
 * All other Log methods throw {@link UnsupportedOperationException} -- the orchestrator
 * does not call them in Phase 2; if a future change does, the test gains a UOE assertion
 * that flags the new path.
 */
public final class CapturingLog implements Log {

    public final CapturingRecord record;
    public final StringSink sink;

    public CapturingLog() {
        this.sink = new StringSink();
        this.record = new CapturingRecord(sink);
    }

    @Override
    public LogRecord advisory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogRecord advisoryW() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogRecord critical() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogRecord debug() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogRecord debugW() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogRecord error() {
        return record;
    }

    @Override
    public LogRecord errorW() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogRecord info() {
        return record;
    }

    @Override
    public LogRecord infoW() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogRecord xDebugW() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogRecord xInfoW() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogRecord xadvisory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogRecord xcritical() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogRecord xdebug() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogRecord xerror() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogRecord xinfo() {
        throw new UnsupportedOperationException();
    }

    /**
     * LogRecord fake that appends every argument into a shared {@link StringSink}.
     * Implements the FULL {@link LogRecord} interface (which extends {@code Utf8Sink}).
     * <p>
     * Methods that take native pointers ($safe(long, long), $hex(long)) record a
     * stable textual marker rather than dereferencing the pointer -- they never receive
     * real native memory in the lifecycle tests. {@code Utf8Sink} / {@code CharSink}
     * abstract methods are implemented as no-ops; the orchestrator's emit path uses
     * only $(CharSequence), $(int|long|char), $(Object), and I$().
     */
    public static final class CapturingRecord implements LogRecord {

        private final StringSink sink;

        public CapturingRecord(StringSink sink) {
            this.sink = sink;
        }

        @Override
        public void $() {
            sink.put(Misc.EOL);
        }

        @Override
        public LogRecord $(@Nullable CharSequence sequence) {
            if (sequence != null) {
                sink.put(sequence);
            } else {
                sink.put("null");
            }
            return this;
        }

        @Override
        public LogRecord $(@Nullable Utf8Sequence sequence) {
            if (sequence != null) {
                sink.put(sequence.toString());
            } else {
                sink.put("null");
            }
            return this;
        }

        @Override
        public LogRecord $(@Nullable DirectUtf8Sequence sequence) {
            if (sequence != null) {
                sink.put(sequence.toString());
            } else {
                sink.put("null");
            }
            return this;
        }

        @Override
        public LogRecord $(int x) {
            sink.put(x);
            return this;
        }

        @Override
        public LogRecord $(double x) {
            sink.put(x);
            return this;
        }

        @Override
        public LogRecord $(long l) {
            sink.put(l);
            return this;
        }

        @Override
        public LogRecord $(boolean x) {
            sink.put(x);
            return this;
        }

        @Override
        public LogRecord $(char c) {
            sink.put(c);
            return this;
        }

        @Override
        public LogRecord $(@Nullable Throwable e) {
            if (e != null) {
                sink.put(e.toString());
            } else {
                sink.put("null");
            }
            return this;
        }

        @Override
        public LogRecord $(@Nullable File x) {
            if (x != null) {
                sink.put(x.getPath());
            } else {
                sink.put("null");
            }
            return this;
        }

        @Override
        public LogRecord $(@Nullable Object x) {
            if (x != null) {
                sink.put(x.toString());
            } else {
                sink.put("null");
            }
            return this;
        }

        @Override
        public LogRecord $(@Nullable Sinkable x) {
            if (x != null) {
                x.toSink(sink);
            } else {
                sink.put("null");
            }
            return this;
        }

        @Override
        public LogRecord $256(long a, long b, long c, long d) {
            sink.put(a).put(':').put(b).put(':').put(c).put(':').put(d);
            return this;
        }

        @Override
        public LogRecord $hex(long value) {
            sink.put(Long.toHexString(value));
            return this;
        }

        @Override
        public LogRecord $hexPadded(long value) {
            sink.put(Long.toHexString(value));
            return this;
        }

        @Override
        public LogRecord $ip(long ip) {
            sink.put(ip);
            return this;
        }

        @Override
        public LogRecord $safe(@NotNull CharSequence sequence, int lo, int hi) {
            sink.put(sequence, lo, hi);
            return this;
        }

        @Override
        public LogRecord $safe(@Nullable DirectUtf8Sequence sequence) {
            return $(sequence);
        }

        @Override
        public LogRecord $safe(@Nullable Utf8Sequence sequence) {
            return $(sequence);
        }

        @Override
        public LogRecord $safe(long lo, long hi) {
            sink.put("[ptr:").put(lo).put('-').put(hi).put(']');
            return this;
        }

        @Override
        public LogRecord $safe(@Nullable CharSequence sequence) {
            return $(sequence);
        }

        @Override
        public LogRecord $size(long memoryBytes) {
            sink.put(memoryBytes);
            return this;
        }

        @Override
        public LogRecord $substr(int from, @Nullable DirectUtf8Sequence sequence) {
            return $(sequence);
        }

        @Override
        public LogRecord $ts(long x) {
            sink.put(x);
            return this;
        }

        @Override
        public LogRecord $ts(TimestampDriver driver, long x) {
            sink.put(x);
            return this;
        }

        @Override
        public LogRecord $uuid(long lo, long hi) {
            sink.put(lo).put(':').put(hi);
            return this;
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public LogRecord microTime(long x) {
            sink.put(x);
            return this;
        }

        @Override
        public int[] ryuScratch() {
            return sink.ryuScratch();
        }

        @Override
        public LogRecord ts() {
            return this;
        }

        // Utf8Sink / CharSink abstract methods -- orchestrator emit path does not use these.
        // No-ops keep compile happy without polluting the captured text with placeholder bytes.
        @Override
        public io.questdb.std.str.Utf8Sink put(byte b) {
            return this;
        }

        @Override
        public io.questdb.std.str.Utf8Sink put(@Nullable Utf8Sequence us) {
            if (us != null) {
                sink.put(us.toString());
            }
            return this;
        }

        @Override
        public io.questdb.std.str.Utf8Sink putNonAscii(long lo, long hi) {
            return this;
        }
    }
}
