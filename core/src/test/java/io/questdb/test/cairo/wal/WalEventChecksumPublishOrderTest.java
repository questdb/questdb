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
 ******************************************************************************/

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.Chars;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A reader that follows the tail of a WAL segment's {@code _event} file must never see a record the
 * sidecar has no entry for yet.
 * <p>
 * {@code WalEventWriter.finishRecord()} writes the record's length at {@code startOffset} FIRST -- the
 * store that flips the slot from the {@code -1} end-of-events marker to a real length, i.e. the store
 * that PUBLISHES the record -- and only then fills the {@code _event.c} entry that names it. Between the
 * two stores the record is readable and its sidecar entry is still the preallocated zeros, so
 * {@code WalEventCursor.verifyRecordChecksum()} reports {@code storedOffset=0, storedLen=0, expected=0}
 * and calls the record TORN. Nothing crashed; the reader simply arrived mid-publication.
 */
public class WalEventChecksumPublishOrderTest extends AbstractCairoTest {

    @Test
    public void testTailReaderNeverSeesRecordAheadOfItsSidecarEntry() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken tt = engine.verifyTableName("x");

            final int commits = 5_000;
            final AtomicInteger written = new AtomicInteger();
            final AtomicReference<Throwable> readerError = new AtomicReference<>();
            final AtomicInteger tornSeen = new AtomicInteger();
            final CyclicBarrier start = new CyclicBarrier(2);

            final Thread reader = new Thread(() -> {
                try (
                        Path path = new Path();
                        WalEventReader eventReader = new WalEventReader(engine.getConfiguration())
                ) {
                    start.await();
                    while (written.get() < commits) {
                        path.of(engine.getConfiguration().getDbRoot())
                                .concat(tt)
                                .concat(WalUtils.WAL_NAME_BASE).put(1).slash().put(0);
                        try {
                            // segmentTxn=-1: read from the head and walk to the tail, exactly as a
                            // tail-following reader does; every record it walks past is verified.
                            final WalEventCursor cursor = eventReader.of(path, -1);
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                        } catch (CairoException e) {
                            if (Chars.contains(e.getFlyweightMessage(), "torn WAL event record")) {
                                tornSeen.incrementAndGet();
                                readerError.compareAndSet(null, e);
                                return;
                            }
                            // a segment that is not there yet / being rolled is not what this asserts
                        } catch (Throwable t) {
                            readerError.compareAndSet(null, t);
                            return;
                        }
                    }
                } catch (Throwable t) {
                    readerError.compareAndSet(null, t);
                }
            });
            reader.start();
            start.await();

            for (int i = 0; i < commits; i++) {
                execute("INSERT INTO x VALUES (" + (1_700_000_000_000_000L + i) + "::timestamp, " + i + ")");
                written.incrementAndGet();
            }
            written.set(commits);
            reader.join();

            final Throwable err = readerError.get();
            Assert.assertEquals(
                    "a tail reader saw a published record whose sidecar entry was still zeros"
                            + (err != null ? ": " + err.getMessage() : ""),
                    0,
                    tornSeen.get()
            );
            if (err != null && tornSeen.get() == 0) {
                throw new AssertionError(err);
            }
        });
    }
}
