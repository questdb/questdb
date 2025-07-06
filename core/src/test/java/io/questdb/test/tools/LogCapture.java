package io.questdb.test.tools;

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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogCapture {
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
            Assert.fail("Message '" + message + "' was not logged");
        }
    }

    public void assertLoggedRE(String regex) {
        Matcher matcher = Pattern.compile(regex).matcher(sink.toString());
        if (!matcher.find()) {
            Assert.fail("Message '" + regex + "' was not logged");
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

    public void start() {
        sink.clear();
        consoleWriter.setInterceptor(interceptor);
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
