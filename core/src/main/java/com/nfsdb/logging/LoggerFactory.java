/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb.logging;

import com.nfsdb.collections.CharSequenceObjHashMap;
import com.nfsdb.collections.ObjHashSet;
import com.nfsdb.collections.ObjList;
import com.nfsdb.collections.ObjectFactory;
import com.nfsdb.concurrent.*;
import com.nfsdb.misc.Chars;

import java.util.Comparator;
import java.util.concurrent.CountDownLatch;

public class LoggerFactory {

    private final CharSequenceObjHashMap<Holder> debug = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<Holder> info = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<Holder> error = new CharSequenceObjHashMap<>();
    private final ObjHashSet<Job<Object>> jobs = new ObjHashSet<>();
    private final Holder all = new Holder();
    private final CountDownLatch workerHaltLatch = new CountDownLatch(1);
    private Worker<Object> worker = null;

    public static void main(String[] args) {
        LoggerFactory factory = new LoggerFactory();

        factory.add(new LogWriterConfig("com.nfsdb", LogLevel.ALL, new LogWriterFactory() {
            @Override
            public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq) {
                return new LogFileWriter(ring, seq, "x.log");
            }
        }, 1024, 4096));

        factory.add(new LogWriterConfig("com", LogLevel.INFO, new LogWriterFactory() {
            @Override
            public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq) {
                return new LogFileWriter(ring, seq, "y.log");
            }
        }, 1024, 4096));

        factory.addDefault(new LogWriterConfig("", LogLevel.ALL, new LogWriterFactory() {
            @Override
            public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq) {
                return new LogFileWriter(ring, seq, "z.log");
            }
        }, 1024, 4096));

        factory.bind();

        factory.startThread();

        try {
            AsyncLogger logger = factory.getLogger("com.nfsdb.x");
            for (int i = 0; i < 1000000; i++) {
                logger.error()._("damn: ")._(i).$();
            }
        } finally {
            factory.haltThread();
        }
    }

    public void add(final LogWriterConfig config) {
        switch (config.getLevel()) {
            case ALL:
                createEverywhere(config);
                break;
            case DEBUG:
                createAt(config, debug);
                break;
            case INFO:
                createAt(config, info);
                break;
            case ERROR:
                createAt(config, error);
                break;
        }
    }

    public void addDefault(final LogWriterConfig config) {
        if (all.ring == null) {
            all.ring = new RingQueue<>(new ObjectFactory<LogRecordSink>() {
                @Override
                public LogRecordSink newInstance() {
                    return new LogRecordSink(config.getQueueDepth());
                }
            }, config.getRecordLength());
        }

        if (all.lSeq == null) {
            all.lSeq = new MPSequence(config.getQueueDepth());
        }

        Sequence sequence = new SCSequence();

        if (all.wSeq == null && all.fanOut == null) {
            all.wSeq = sequence;
        } else if (all.wSeq != null && all.fanOut == null) {
            all.fanOut = new FanOut(all.wSeq, sequence);
        } else {
            all.fanOut.add(sequence);
        }

        jobs.add(config.getFactory().createLogWriter(all.ring, sequence));
    }

    public void bind() {
        debug.sortKeys(new Comparator<CharSequence>() {
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
        });

        bindSequences(debug);
        bindSequences(info);
        bindSequences(error);

        if (all.fanOut != null) {
            all.lSeq.followedBy(all.fanOut);
            all.fanOut.followedBy(all.lSeq);
        } else {
            all.lSeq.followedBy(all.wSeq);
            all.wSeq.followedBy(all.lSeq);
        }

    }

    public AsyncLogger getLogger(CharSequence key) {
        Holder inf = findHolder(key, info);
        Holder dbg = findHolder(key, debug);
        Holder err = findHolder(key, error);
        return new AsyncLogger(dbg.ring, dbg.lSeq, inf.ring, inf.lSeq, err.ring, err.lSeq);
    }

    public void haltThread() {
        if (worker != null) {
            worker.halt();
            try {
                workerHaltLatch.await();
            } catch (InterruptedException ignore) {
            }
        }
    }

    public void startThread() {
        Worker<Object> worker = new Worker<>(jobs, workerHaltLatch, null);
        worker.setDaemon(true);
        worker.setName("nfsdb-log-writer");
        worker.start();
    }

    private Holder addNewHolder(CharSequenceObjHashMap<Holder> map,
                                final LogWriterConfig config,
                                RingQueue<LogRecordSink> ring,
                                Sequence wSeq,
                                Sequence lSeq,
                                LogWriter w) {
        Holder h = new Holder();
        h.ring = ring != null ? ring : new RingQueue<>(new ObjectFactory<LogRecordSink>() {
            @Override
            public LogRecordSink newInstance() {
                return new LogRecordSink(config.getQueueDepth());
            }
        }, config.getRecordLength());
        h.wSeq = wSeq != null ? wSeq : new SCSequence();
        h.lSeq = lSeq != null ? lSeq : new MPSequence(config.getQueueDepth());
        jobs.add(h.w = (w != null ? w : config.getFactory().createLogWriter(h.ring, h.wSeq)));
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

    private void createAt(final LogWriterConfig config, CharSequenceObjHashMap<Holder> m) {
        final Sequence sequence = new SCSequence();
        Holder h = m.get(config.getScope());
        if (h == null) {
            addNewHolder(m, config, null, null, null, null);
        } else {
            if (h.fanOut == null) {
                h.fanOut = new FanOut(h.wSeq, sequence);
            } else {
                h.fanOut.add(sequence);
            }
            jobs.add(config.getFactory().createLogWriter(h.ring, sequence));
        }
    }

    private void createEverywhere(final LogWriterConfig config) {
        Sequence wSeq = null;
        Sequence lSeq = null;
        RingQueue<LogRecordSink> ring = null;
        LogWriter w = null;

        if (debug.get(config.getScope()) == null) {
            Holder h = addNewHolder(debug, config, null, null, null, null);
            wSeq = h.wSeq;
            lSeq = h.lSeq;
            ring = h.ring;
            w = h.w;
        }

        if (info.get(config.getScope()) == null) {
            Holder h = addNewHolder(info, config, ring, wSeq, lSeq, w);
            wSeq = h.wSeq;
            lSeq = h.lSeq;
            ring = h.ring;
            w = h.w;
        }

        if (error.get(config.getScope()) == null) {
            addNewHolder(error, config, ring, wSeq, lSeq, w);
        }
    }

    private Holder findHolder(CharSequence key, CharSequenceObjHashMap<Holder> map) {
        ObjList<CharSequence> keys = map.keys();
        CharSequence k = null;

        for (int i = 0, n = keys.size(); i < n; i++) {
            k = keys.getQuick(i);
            if (Chars.containts(key, k)) {
                break;
            }
        }

        if (k == null) {
            return all;
        }

        return map.get(k);
    }

    private static class Holder {
        private RingQueue<LogRecordSink> ring;
        private Sequence wSeq;
        private Sequence lSeq;
        private FanOut fanOut;
        private LogWriter w;
    }
}
