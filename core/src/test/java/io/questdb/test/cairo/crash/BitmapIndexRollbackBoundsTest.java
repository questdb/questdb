package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A key entry whose value-block offset points PAST the mapped value extent must fail with a clean
 * {@link CairoException}, not take the process down.
 *
 * <h3>Why this exists</h3>
 * Found by the VM crash harness ({@code tools/durability/vm}), not by the modelled suite. Replaying a
 * recorded adaptive W=50ms run to flush boundary 13776 and opening the result reproduced, deterministically:
 * <pre>
 *   TableWriter switched partition [path=/t~1/2024-01-16.1295, rowCount=54000]
 *   TableWriter recovering index [fd=...]
 *   SIGSEGV (0xb) ... J io.questdb.cairo.vm.api.MemoryCR.getLong(J)J
 * </pre>
 * The filesystem was clean (ext4 recovery completed, mounted r/w) and the reconstruction healthy, so the
 * engine was handed a coherent crash state and still walked off a mapping.
 *
 * <h3>The gap</h3>
 * {@link BitmapIndexWriter}'s open path validates both files, but only at HEADER level:
 * <pre>
 *   key:   keyMemSize &lt; keyCount * KEY_ENTRY_SIZE + KEY_FILE_RESERVED   -&gt; throw
 *   value: valueMemSize &gt; ff.length(valueFd)                            -&gt; throw
 *   valueMem.of(..., valueMemSize, ...)                                  // maps exactly valueMemSize
 * </pre>
 * {@code rollbackValues} then trusts a PER-ENTRY pointer with no bounds check at all:
 * <pre>
 *   long blockOffset = keyMem.getLong(offset + KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);
 *   BitmapIndexUtils.seekValueBlockRTL(valueCount, blockOffset, valueMem, ...);
 * </pre>
 * Both header guards pass, and the seek then reads outside {@code valueMem}. This is a pointer that is
 * durable ahead of the data it names -- the invariant class adaptive commit is built around -- surfacing on
 * the index rather than on {@code _txn}.
 *
 <h3>ROOT CAUSE: the bounds check is an assert, so it does not exist in production</h3>
 * <pre>
 *   // AbstractMemoryCR
 *   public long addressOf(long offset) {
 *       offset -= shiftAddressRight;
 *       assert checkOffsetMapped(offset);   // &lt;-- disabled without -ea
 *       return pageAddress + offset;
 *   }
 * </pre>
 * The only thing standing between a bad {@code blockOffset} and a wild pointer is a Java assertion:
 * <ul>
 *   <li><b>under {@code -ea}</b> (how this repo runs tests) it raises {@code AssertionError} -- survivable,
 *       and what this test observes;</li>
 *   <li><b>without {@code -ea}</b> (the shipped binary, and the {@code CrashVerifier} the VM harness runs)
 *       it is a no-op, {@code addressOf} returns {@code pageAddress + offset} unchecked, and
 *       {@code Unsafe.getLong} takes SIGSEGV.</li>
 * </ul>
 * That asymmetry is why no modelled test catches this: every JUnit run has assertions ON, so the failure
 * mode the shipped binary actually exhibits is never the one exercised. It took a real mapping, with
 * assertions off, to show it as a segfault.
 *
 * <h3>The required fix</h3>
 * An explicit bounds check -- {@code blockOffset} within the mapped value extent -- raising
 * {@link CairoException}. An assertion is not a guard for a value read off disk that a crash can corrupt.
 *
 * <h3>State before the fix</h3>
 * This test FAILS with {@code AssertionError} from {@code addressOf} (under {@code -ea}). That is the
 * defect reproduced, and it is the same call chain the VM harness saw as SIGSEGV:
 * {@code rollbackValues -> seekValueBlockRTL -> MemoryCR.getLong -> addressOf}.
 */
public class BitmapIndexRollbackBoundsTest extends AbstractCairoTest {

    private static final long COLUMN_NAME_TXN_NONE = TableUtils.COLUMN_NAME_TXN_NONE;
    private static final int KEY = 0;
    private static final int N = 4096;

    /** Mirrors BitmapIndexTest#create: the 3-arg writer ctor OPENS an index, it does not create one. */
    private static void create(CairoConfiguration configuration, Path path, CharSequence name, int valueBlockCapacity) {
        int plen = path.size();
        try {
            FilesFacade ff = configuration.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(
                    ff,
                    BitmapIndexUtils.keyFileName(path, name, COLUMN_NAME_TXN_NONE),
                    MemoryTag.MMAP_DEFAULT,
                    configuration.getWriterFileOpenOpts())) {
                BitmapIndexWriter.initKeyMemory(mem, Numbers.ceilPow2(valueBlockCapacity));
            }
            ff.touch(BitmapIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
        } finally {
            path.trimTo(plen);
        }
    }

    @Test
    public void testKeyEntryPointingPastValueExtentFailsCleanly() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;

            try (Path path = new Path().of(root)) {
                final int plen = path.size();

                create(configuration, path.trimTo(plen), "x", 4);

                // 1. Build a genuine index: one key, enough values to span several
                //    value blocks so a block offset is a meaningful distance in.
                try (BitmapIndexWriter w = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < N; i++) {
                        w.add(KEY, i);
                    }
                    w.commit();
                }

                // 2. Corrupt ONE key entry's last-value-block offset so it points
                //    past the value extent, leaving every HEADER field untouched.
                //    Both open-time guards therefore still pass -- which is the
                //    point: the state is reachable, not obviously malformed.
                final long entry = BitmapIndexUtils.getKeyEntryOffset(KEY);
                long valueMemSize;
                try (MemoryMARW km = Vm.getCMARWInstance()) {
                    km.of(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE),
                            ff.getPageSize(), ff.length(BitmapIndexUtils.keyFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)),
                            MemoryTag.MMAP_DEFAULT);
                    valueMemSize = km.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
                    Assert.assertTrue("index should have values", valueMemSize > 0);

                    long blockOffset = km.getLong(entry + BitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);
                    Assert.assertTrue("block offset should start inside the extent",
                            blockOffset < valueMemSize);

                    // Point it well past the end of what will be mapped.
                    km.putLong(entry + BitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET,
                            valueMemSize + 1024 * 1024);
                }

                // 3. Reopen and roll back. The header guards pass; the per-entry
                //    pointer is what is out of bounds.
                try (BitmapIndexWriter w = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                    try {
                        w.rollbackValues(N - 1);
                        Assert.fail("rollbackValues accepted a key entry pointing past the value extent; "
                                + "on a real mapping this is a SIGSEGV, not a recoverable state");
                    } catch (CairoException e) {
                        // The required behaviour: named, recoverable, process intact.
                        TestUtils.assertContains(e.getFlyweightMessage(), "value block offset");
                    }
                }
            }
        });
    }

    @Test
    public void testHealthyIndexStillRollsBack() throws Exception {
        // Negative control. A bounds check that rejects VALID indexes would be
        // worse than none: it would turn every recovery into a failure.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(root)) {
                final int plen = path.size();
                create(configuration, path.trimTo(plen), "y", 4);
                try (BitmapIndexWriter w = new BitmapIndexWriter(configuration, path.trimTo(plen), "y", COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < N; i++) {
                        w.add(KEY, i);
                    }
                    w.commit();
                }
                try (BitmapIndexWriter w = new BitmapIndexWriter(configuration, path.trimTo(plen), "y", COLUMN_NAME_TXN_NONE)) {
                    w.rollbackValues(N / 2);
                    // Counted through the public cursor: rollbackValues(m) must
                    // leave exactly the values 0..m.
                    long count = 0;
                    long max = -1;
                    RowCursor cur = w.getCursor(KEY);
                    while (cur.hasNext()) {
                        long v = cur.next();
                        if (v > max) {
                            max = v;
                        }
                        count++;
                    }
                    Assert.assertEquals("rollback should retain values 0..N/2", N / 2 + 1, count);
                    Assert.assertEquals(N / 2, max);
                }
            }
        });
    }
}
