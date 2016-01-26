/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 */

package com.nfsdb.logging;

import com.nfsdb.collections.*;
import com.nfsdb.concurrent.*;
import com.nfsdb.exceptions.NumericException;
import com.nfsdb.misc.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class LoggerFactory implements Closeable {

    public static final LoggerFactory INSTANCE = new LoggerFactory();

    public static final int LOG_LEVEL_DEBUG = 1;
    public static final int LOG_LEVEL_INFO = 2;
    public static final int LOG_LEVEL_ERROR = 4;

    public static final int DEFAULT_QUEUE_DEPTH = 1024;
    public static final int DEFAULT_MSG_SIZE = 4 * 1024;
    private static final String DEFAULT_CONFIG = "/nfslog.conf";
    private static final String EMPTY_STR = "";
    private static final Holder NOP = new Holder();
    private static final CharSequenceHashSet reserved = new CharSequenceHashSet();
    private static final LengthDescendingComparator LDC = new LengthDescendingComparator();
    private final CharSequenceObjHashMap<Holder> debug = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<Holder> info = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<Holder> error = new CharSequenceObjHashMap<>();
    private final ObjHashSet<LogWriter> jobs = new ObjHashSet<>();
    private final CountDownLatch workerHaltLatch = new CountDownLatch(1);
    private Worker worker = null;
    private boolean configured = false;

    public static void configureFromSystemProperties(LoggerFactory factory) {
        String conf = System.getProperty("nfslog");
        if (conf == null) {
            conf = DEFAULT_CONFIG;
        }
        try (InputStream is = LoggerFactory.class.getResourceAsStream(conf)) {
            if (is != null) {
                Properties properties = new Properties();
                properties.load(is);
                setup(factory, properties);
            } else {
                factory.configureDefaultWriter();
            }
        } catch (IOException e) {
            if (!DEFAULT_CONFIG.equals(conf)) {
                throw new LoggerError("Cannot read " + conf, e);
            } else {
                factory.configureDefaultWriter();
            }
        }
        factory.startThread();
    }

    public static AsyncLogger getLogger(CharSequence key) {
        if (!INSTANCE.configured) {
            configureFromSystemProperties(INSTANCE);
        }
        return INSTANCE.create(key);
    }

    public static void setup(LoggerFactory factory, Properties properties) {

        String writers = properties.getProperty("writers");

        if (writers == null) {
            factory.configured = true;
            return;
        }

        for (String w : writers.split(",")) {
            LogWriterConfig conf = createWriter(properties, w.trim());
            if (conf != null) {
                factory.add(conf);
            }
        }

        factory.bind();
    }

    public void add(final LogWriterConfig config) {

        checkConfigured();

        int level = config.getLevel();

        if (level == 0) {
            createEverywhere(config);
        } else {

            if ((level & LOG_LEVEL_DEBUG) == LOG_LEVEL_DEBUG) {
                createAt(config, debug);
            }

            if ((level & LOG_LEVEL_INFO) == LOG_LEVEL_INFO) {
                createAt(config, info);
            }

            if ((level & LOG_LEVEL_ERROR) == LOG_LEVEL_ERROR) {
                createAt(config, error);
            }
        }
    }

    public void add(String scope, int level, LogWriterFactory factory) {
        add(new LogWriterConfig(scope, level, factory, DEFAULT_QUEUE_DEPTH, DEFAULT_MSG_SIZE));
    }

    public void add(int level, LogWriterFactory factory) {
        add(new LogWriterConfig(level, factory, DEFAULT_QUEUE_DEPTH, DEFAULT_MSG_SIZE));
    }

    public void add(LogWriterFactory factory) {
        add(new LogWriterConfig(0, factory, DEFAULT_QUEUE_DEPTH, DEFAULT_MSG_SIZE));
    }

    public void bind() {
        if (configured) {
            return;
        }

        for (int i = 0, n = jobs.size(); i < n; i++) {
            LogWriter w = jobs.get(i);
            w.bindProperties();
        }

        debug.sortKeys(LDC);
        info.sortKeys(LDC);
        error.sortKeys(LDC);

        bindSequences(debug);
        bindSequences(info);
        bindSequences(error);

        configured = true;
    }

    @Override
    public void close() {
        for (int i = 0, n = jobs.size(); i < n; i++) {
            Misc.free(jobs.get(i));
        }
    }

    public AsyncLogger create(CharSequence key) {
        if (!configured) {
            throw new LoggerError("Not configured");
        }
        Holder inf = findHolder(key, info);
        Holder dbg = findHolder(key, debug);
        Holder err = findHolder(key, error);
        return new AsyncLogger(dbg.ring, dbg.lSeq, inf.ring, inf.lSeq, err.ring, err.lSeq);
    }

    public ObjHashSet<LogWriter> getJobs() {
        return jobs;
    }

    public void haltThread() {
        if (worker != null) {
            worker.halt();
            try {
                workerHaltLatch.await();
            } catch (InterruptedException ignore) {
            }
            worker = null;
        }
    }

    public void startThread() {
        this.worker = new Worker(jobs, workerHaltLatch);
        worker.setDaemon(true);
        worker.setName("nfsdb-log-writer");
        worker.start();
    }

    private static LogWriterConfig createWriter(final Properties properties, String w) {
        final String writer = "w." + w + '.';
        final String clazz = properties.getProperty(writer + "class");
        final String levelStr = properties.getProperty(writer + "level");
        final String scope = properties.getProperty(writer + "scope");
        final String queueDepthStr = properties.getProperty(writer + "queueDepth");
        final String maxMsgSizeStr = properties.getProperty(writer + "maxMsgSize");

        if (clazz == null) {
            return null;
        }

        int queueDepth;

        try {
            queueDepth = Numbers.parseInt(queueDepthStr);
        } catch (NumericException e) {
            queueDepth = DEFAULT_QUEUE_DEPTH;
        }

        int recordLength;

        try {
            recordLength = Numbers.parseInt(maxMsgSizeStr);
        } catch (NumericException e) {
            recordLength = DEFAULT_MSG_SIZE;
        }

        final Class<?> cl;
        final Constructor constructor;
        try {
            cl = Class.forName(clazz);
            constructor = cl.getDeclaredConstructor(RingQueue.class, Sequence.class);
        } catch (ClassNotFoundException e) {
            throw new LoggerError("Class not found " + clazz, e);
        } catch (NoSuchMethodException e) {
            throw new LoggerError("Constructor(RingQueue, Sequence) expected: " + clazz, e);
        }

        int level = 0;
        if (levelStr != null) {
            for (String s : levelStr.split(",")) {
                switch (s.toUpperCase()) {
                    case "DEBUG":
                        level |= LOG_LEVEL_DEBUG;
                        break;
                    case "INFO":
                        level |= LOG_LEVEL_INFO;
                        break;
                    case "ERROR":
                        level |= LOG_LEVEL_ERROR;
                        break;
                    default:
                        throw new LoggerError("Unknown level: " + s);
                }
            }
        }

        return new LogWriterConfig(scope == null ? EMPTY_STR : scope, level, new LogWriterFactory() {
            @Override
            public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq) {
                try {
                    LogWriter w = (LogWriter) constructor.newInstance(ring, seq);

                    for (String n : properties.stringPropertyNames()) {
                        if (n.startsWith(writer)) {
                            String p = n.substring(writer.length());

                            if (reserved.contains(p)) {
                                continue;
                            }

                            try {
                                Field f = cl.getDeclaredField(p);
                                if (f != null && f.getType() == String.class) {
                                    long offset = Unsafe.getUnsafe().objectFieldOffset(f);
                                    Unsafe.getUnsafe().putObject(w, offset, properties.getProperty(n));
                                }
                            } catch (Exception e) {
                                throw new LoggerError("Unknown property: " + n, e);
                            }
                        }
                    }
                    return w;
                } catch (Exception e) {
                    throw new LoggerError("Error creating log writer", e);
                }
            }
        }, queueDepth, recordLength);
    }

    private Holder addNewHolder(CharSequenceObjHashMap<Holder> map,
                                final LogWriterConfig config,
                                RingQueue<LogRecordSink> ring,
                                Sequence wSeq,
                                Sequence lSeq) {
        Holder h = new Holder();
        h.ring = ring != null ? ring : new RingQueue<>(new ObjectFactory<LogRecordSink>() {
            @Override
            public LogRecordSink newInstance() {
                return new LogRecordSink(config.getQueueDepth());
            }
        }, config.getRecordLength());
        h.wSeq = wSeq != null ? wSeq : new SCSequence();
        h.lSeq = lSeq != null ? lSeq : new MPSequence(config.getQueueDepth());
        map.put(config.getScope(), h);
        return h;
    }

    private void bindSequences(CharSequenceObjHashMap<Holder> map) {
        // cycle sequences
        for (int i = 0, n = map.size(); i < n; i++) {
            Holder h = map.valueQuick(i);

            if (h.fanOut != null) {
                h.lSeq.followedBy(h.fanOut);
                h.fanOut.followedBy(h.lSeq);
            } else {
                h.lSeq.followedBy(h.wSeq);
                h.wSeq.followedBy(h.lSeq);
            }
        }
    }

    private void checkConfigured() {
        if (configured) {
            throw new LoggerError("Already configured");
        }
    }

    private void configureDefaultWriter() {
        add(LOG_LEVEL_INFO | LOG_LEVEL_ERROR, new LogWriterFactory() {
            @Override
            public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq) {
                return new StdOutWriter(ring, seq);
            }
        });
        bind();
    }

    private void createAt(final LogWriterConfig config, CharSequenceObjHashMap<Holder> m) {
        final Sequence sequence = new SCSequence();
        Holder h = m.get(config.getScope());
        if (h == null) {
            h = addNewHolder(m, config, null, sequence, null);
        } else {
            if (h.fanOut == null) {
                h.fanOut = new FanOut(h.wSeq, sequence);
            } else {
                h.fanOut.add(sequence);
            }
        }
        jobs.add(config.getFactory().createLogWriter(h.ring, sequence));
    }

    private void createEverywhere(final LogWriterConfig config) {
        Sequence wSeq = null;
        Sequence lSeq = null;
        RingQueue<LogRecordSink> ring = null;

        if (debug.get(config.getScope()) == null) {
            Holder h = addNewHolder(debug, config, null, null, null);
            wSeq = h.wSeq;
            lSeq = h.lSeq;
            ring = h.ring;
        }

        if (info.get(config.getScope()) == null) {
            Holder h = addNewHolder(info, config, ring, wSeq, lSeq);
            wSeq = h.wSeq;
            lSeq = h.lSeq;
            ring = h.ring;
        }

        if (error.get(config.getScope()) == null) {
            addNewHolder(error, config, ring, wSeq, lSeq);
        }
        jobs.add(config.getFactory().createLogWriter(ring, wSeq));

    }

    private Holder findHolder(CharSequence key, CharSequenceObjHashMap<Holder> map) {
        ObjList<CharSequence> keys = map.keys();
        CharSequence k = null;

        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence s = keys.getQuick(i);
            if (Chars.startsWith(key, s)) {
                k = s;
                break;
            }
        }

        if (k == null) {
            return NOP;
        }

        return map.get(k);
    }

    private static class LengthDescendingComparator implements Comparator<CharSequence>, Serializable {
        @Override
        public int compare(CharSequence o1, CharSequence o2) {
            int l1, l2;
            if ((l1 = o1.length()) < (l2 = o2.length())) {
                return 1;
            }

            if (l1 > l2) {
                return -11;
            }

            return 0;
        }
    }

    private static class Holder {
        private RingQueue<LogRecordSink> ring;
        private Sequence wSeq;
        private Sequence lSeq;
        private FanOut fanOut;
    }

    static {
        Os.init();
        reserved.add("scope");
        reserved.add("class");
        reserved.add("level");
        reserved.add("queueDepth");
        reserved.add("maxMsgSize");
    }
}
