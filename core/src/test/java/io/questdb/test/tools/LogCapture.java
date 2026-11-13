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
import org.jetbrains.annotations.TestOnly;
import org.junit.Assert;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogCapture {
    public static final long DRAIN_TIMEOUT_MS = 2_000;
    // stop() must not lose a record a starved logging worker has not delivered
    // yet; the price of this deadline is paid only when the worker is stuck.
    public static final long STOP_DRAIN_TIMEOUT_MS = 30_000;
    private static final Log LOG = LogFactory.getLog(LogCapture.class);
    private static final AtomicLong SENTINEL_SEQ = new AtomicLong();
    // The capture whose interceptor LogCapture last installed on the console writer, null once a
    // stop() removed it. Volatile: a rule reads it on the thread that wraps a test, and the test may
    // run on another one (a Timeout rule's).
    private static volatile LogCapture installedCapture;
    private final LogConsoleWriter consoleWriter;
    private final StringSink sink = new SynchronizedSink();
    private final LogConsoleWriter.LogInterceptor interceptor = this::onLog;
    private boolean isCapturing;
    // Real by default; a test pins the exact deadline boundary in waitFor(CharSequence, long), or
    // the moment drain(long) gives up, by swapping these for a fake clock/sleeper instead of racing
    // real wall-clock granularity.
    private LongSupplier clockMillis = System::currentTimeMillis;
    private LongConsumer sleeper = Os::sleep;

    public LogCapture() {
        consoleWriter = getFirstConsoleWriter();
        consoleWriter.setInterceptor(logRecordSink -> logRecordSink.toSink(sink));
        installedCapture = this;
    }

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
        // The logging worker may still be appending the final line. Inspect one stable snapshot.
        assertNotLogged(message, sink.toString());
    }

    /**
     * Fails as {@link #assertNotLogged(String)} does when {@code message} occurs after the end of
     * the first occurrence of {@code marker}, read off one stable snapshot. A capture that does not
     * hold {@code marker} has nothing after it to check, and passes: the console writer delivers
     * records in the order they were logged, so nothing logged after the marker can have arrived
     * ahead of it.
     */
    public void assertNotLoggedAfter(String marker, String message) {
        final String snapshot = sink.toString();
        final int markerIndex = snapshot.indexOf(marker);
        if (markerIndex > -1) {
            assertNotLogged(message, snapshot, markerIndex + marker.length());
        }
    }

    /**
     * How many times {@code message} appears in the captured log, read off one stable snapshot.
     */
    public int countOccurrences(String message) {
        final String snapshot = sink.toString();
        int count = 0;
        for (int i = snapshot.indexOf(message); i >= 0; i = snapshot.indexOf(message, i + message.length())) {
            count++;
        }
        return count;
    }

    public void assertOnlyOnce(String regex) {
        Matcher matcher = Pattern.compile(regex).matcher(sink.toString());
        Assert.assertTrue("Message '" + regex + "' was not logged", matcher.find());
        Assert.assertFalse("Message '" + regex + "' was logged more than once", matcher.find());
    }

    @TestOnly
    public void setClockForTest(LongSupplier clockMillis) {
        this.clockMillis = clockMillis;
    }

    @TestOnly
    public void setSleeperForTest(LongConsumer sleeper) {
        this.sleeper = sleeper;
    }

    /**
     * Blocks until the console writer has handed every record enqueued so far to
     * the interceptor. ADVISORY is the top level, so it is in every writer's mask,
     * and all levels one writer subscribes to share a single ring queue.
     */
    public void drain() {
        drain(DRAIN_TIMEOUT_MS);
    }

    public void drain(long timeoutMs) {
        final String sentinel = "log-capture-drain-" + SENTINEL_SEQ.incrementAndGet();
        // advisory() waits for a ring slot, so a full ring cannot drop the sentinel;
        // the deadline only bounds delivery once the record is queued
        LOG.advisory().$(sentinel).$();
        final long deadline = clockMillis.getAsLong() + timeoutMs;
        while (sink.indexOf(sentinel) == -1 && clockMillis.getAsLong() < deadline) {
            sleeper.accept(1);
        }
    }

    /**
     * Whether this capture's interceptor is the one LogCapture last installed on the console
     * writer: the constructor and {@link #start()} install it, and the {@link #stop()} of any
     * capture removes whatever interceptor is there. A direct
     * {@link LogConsoleWriter#setInterceptor} call bypasses this record. The tests that make one
     * park the logging worker with it, and the capture they create or start next installs over it.
     */
    public boolean isInstalled() {
        return installedCapture == this;
    }

    public void start() {
        consoleWriter.setInterceptor(interceptor);
        installedCapture = this;
        isCapturing = true;
        drain();
        sink.clear();
    }

    public void stop() {
        stop(STOP_DRAIN_TIMEOUT_MS);
    }

    public void stop(long drainTimeoutMs) {
        // A second stop() (a test's own finally plus assertMemoryLeak's) has nothing
        // to drain: the sentinel could no longer reach the sink and would only wait
        // out the deadline.
        if (!isCapturing) {
            return;
        }
        // Records are delivered by the logging worker after the caller's log call
        // returns. Without this drain a record logged just before stop() can still
        // be in the ring when the interceptor is removed, and it then reaches the
        // console instead of the capture.
        drain(drainTimeoutMs);
        consoleWriter.setInterceptor(null);
        installedCapture = null;
        isCapturing = false;
    }

    public void waitFor(String value) {
        waitFor(value, 120_000);
    }

    public void waitFor(CharSequence value, long timeoutMs) {
        final String needle = value.toString();
        long start = clockMillis.getAsLong();
        while (sink.indexOf(needle) == -1) {
            if ((clockMillis.getAsLong() - start) >= timeoutMs) {
                throw new AssertionError("timed out waiting for log to contain '" + value + "', captured log: " + sink);
            }
            sleeper.accept(1);
        }
    }

    public void waitForRegex(String regex) {
        // Same deadline shape as waitFor(CharSequence, long): the timeout check runs BEFORE the
        // sleep and treats elapsed == maxWait as timed out. The old shape looped on
        // `elapsed < maxWait` but threw on `elapsed > maxWait`, so landing exactly on the deadline
        // left the loop with no match and returned silently -- a timed-out wait passing as a
        // successful one.
        final long maxWait = 120_000;
        final long start = clockMillis.getAsLong();
        final Pattern pattern = Pattern.compile(regex);
        // Snapshot the sink into an immutable String before matching. Matching a Matcher
        // directly against the live SynchronizedSink would race an in-flight put() because
        // CharSequence#charAt() isn't one of the methods SynchronizedSink guards; toString()
        // is also synchronized (below), so this snapshot is a single atomic read of the sink's
        // current contents, same as the synchronized indexOf() reads in waitFor(CharSequence,
        // long).
        Matcher m = pattern.matcher(sink.toString());
        while (!m.find()) {
            if ((clockMillis.getAsLong() - start) >= maxWait) {
                throw new AssertionError("timed out waiting for log to match '" + regex + "', captured log: " + sink);
            }
            sleeper.accept(1);
            m = pattern.matcher(sink.toString());
        }
    }

    static void assertNotLogged(String message, String capturedLog) {
        assertNotLogged(message, capturedLog, 0);
    }

    // Scans from fromIndex on, without copying the tail of a capture that can hold millions of
    // chars, and quotes the whole line of the first match.
    static void assertNotLogged(String message, String capturedLog, int fromIndex) {
        final int idx = capturedLog.indexOf(message, fromIndex);
        if (idx > -1) {
            final int lo = capturedLog.lastIndexOf('\n', idx - 1) + 1;
            final int hi = capturedLog.indexOf('\n', idx);
            Assert.fail("Message '" + message + "' was logged: "
                    + capturedLog.substring(lo, hi > -1 ? hi : capturedLog.length()));
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

        @Override
        public synchronized @NotNull String toString() {
            return super.toString();
        }
    }

}
