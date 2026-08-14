package io.questdb.test.tools;

import io.questdb.log.Log;
import io.questdb.log.LogConsoleWriter;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecordUtf8Sink;
import io.questdb.log.LogWriter;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogCapture {
    private static final long DRAIN_TIMEOUT_MS = 2_000;
    private static final Log LOG = LogFactory.getLog(LogCapture.class);
    private static final AtomicLong SENTINEL_SEQ = new AtomicLong();
    private final LogConsoleWriter consoleWriter;
    private final StringSink sink = new SynchronizedSink();
    private final LogConsoleWriter.LogInterceptor interceptor = this::onLog;

    public LogCapture() {
        consoleWriter = getFirstConsoleWriter();
        consoleWriter.setInterceptor(logRecordSink -> logRecordSink.toSink(sink));
    }

    @SuppressWarnings("unused")
    // used in the Ent, will be useful in OSS eventually
    public void assertLogged(String message) {
        final int idx = sink.indexOf(message);
        if (idx < 0) {
            Assert.fail("Message '" + message + "' was not logged, captured log: " + sink);
        }
    }

    public void assertLoggedRE(String regex) {
        Matcher matcher = Pattern.compile(regex).matcher(sink.toString());
        if (!matcher.find()) {
            Assert.fail("Message '" + regex + "' was not logged, captured log: " + sink);
        }
    }

    @SuppressWarnings("unused")
    // used in the Ent
    public void assertNotLogged(String message) {
        final int idx = sink.indexOf(message);
        if (idx > -1) {
            int lo = sink.lastIndexOf("\n", idx);
            int hi = sink.indexOf("\n", idx);
            Assert.fail("Message '" + message + "' was logged: " + sink.subSequence(lo, hi));
        }
    }

    public void assertOnlyOnce(String regex) {
        Matcher m = Pattern.compile(regex).matcher(sink);
        Assert.assertTrue("Message '" + regex + "' was not logged", m.find());
        Assert.assertEquals("Message '" + regex + "' was not more than once", 0, m.groupCount());
    }

    /**
     * Snapshot of the captured log lines. Read-only view — the underlying
     * sink keeps accumulating after this call, so tests that scan for
     * "which of these events fired" should either call {@link #stop()}
     * first or filter the returned text themselves.
     */
    public String captured() {
        return sink.toString();
    }

    /**
     * Blocks until the console writer has handed every record enqueued so far to
     * the interceptor. ADVISORY is the top level, so it is in every writer's mask,
     * and all levels one writer subscribes to share a single ring queue.
     */
    public void drain() {
        final String sentinel = "log-capture-drain-" + SENTINEL_SEQ.incrementAndGet();
        LOG.advisory().$(sentinel).$();
        // a full ring silently drops the sentinel, and a backed-up writer is what
        // fills it -- give up rather than fail on the symptom
        final long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        while (sink.indexOf(sentinel) == -1 && System.currentTimeMillis() < deadline) {
            Os.sleep(1);
        }
    }

    public void start() {
        consoleWriter.setInterceptor(interceptor);
        drain();
        sink.clear();
    }

    public void stop() {
        consoleWriter.setInterceptor(null);
    }

    public void waitFor(String value) {
        long start = System.currentTimeMillis();
        int maxWait = 120_000;
        while (sink.indexOf(value) == -1 && (System.currentTimeMillis() - start) < maxWait) {
            Os.sleep(1);
        }
        if ((System.currentTimeMillis() - start) > maxWait) {
            throw new AssertionError("timed out waiting for log to populate");
        }
    }

    public void waitForRegex(String regex) {
        long start = System.currentTimeMillis();
        int maxWait = 120_000;
        Matcher m = Pattern.compile(regex).matcher(sink);
        while (!m.find() && (System.currentTimeMillis() - start) < maxWait) {
            Os.sleep(1);
            m.reset(sink);
        }
        if ((System.currentTimeMillis() - start) > maxWait) {
            throw new AssertionError("timed out waiting for log to populate");
        }
    }

    private static @NotNull LogConsoleWriter getFirstConsoleWriter() {
        ObjHashSet<LogWriter> jobs = LogFactory.getInstance().getJobs();
        for (int i = 0, n = jobs.size(); i < n; i++) {
            LogWriter logWriter = jobs.get(i);
            if (logWriter instanceof LogConsoleWriter) {
                return (LogConsoleWriter) logWriter;
            }
        }
        Assert.fail();
        return null;
    }

    private void onLog(LogRecordUtf8Sink sink) {
        sink.toSink(this.sink);
    }

    static class SynchronizedSink extends StringSink {
        @Override
        public synchronized void clear() {
            super.clear();
        }

        @Override
        public synchronized int indexOf(@NotNull String s) {
            return super.indexOf(s);
        }

        @Override
        public synchronized int indexOf(@NotNull String s, int fromIndex) {
            return super.indexOf(s, fromIndex);
        }

        @Override
        public synchronized int lastIndexOf(@NotNull String s, int fromIndex) {
            return super.lastIndexOf(s, fromIndex);
        }

        @Override
        public synchronized Utf16Sink put(char c) {
            return super.put(c);
        }

        @Override
        public synchronized @NotNull CharSequence subSequence(int lo, int hi) {
            return super.subSequence(lo, hi);
        }
    }

}
