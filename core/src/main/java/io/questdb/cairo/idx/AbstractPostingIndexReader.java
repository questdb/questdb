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

package io.questdb.cairo.idx;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.DirectBinarySequence;
import io.questdb.std.DirectBitSet;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;

public abstract class AbstractPostingIndexReader implements IndexReader {
    // Number of consecutive values decoded per FSST decompressBlock0 call.
    // The block as a whole is symbol-table-trained once (imported on first
    // access), then chunks are decoded on demand as the cursor walks the
    // block's ordinals. Bounds the anonymous-heap decode scratch to
    // FSST_DECODE_CHUNK_SIZE * worstCaseValueSize regardless of total block
    // size -- the previous "decompress whole block" path sized dstCap to
    // 4 * totalCompressed, which on a multi-GiB sealed block ran the
    // process out of RSS budget before a single value could be served.
    private static final int FSST_DECODE_CHUNK_SIZE = 256;
    private static final String INDEX_CORRUPT = "posting index is corrupt";
    private static final Log LOG = LogFactory.getLog(AbstractPostingIndexReader.class);
    protected final PostingIndexChainEntry.Snapshot entryScratch = new PostingIndexChainEntry.Snapshot();
    protected final PostingGenLookup genLookup = new PostingGenLookup();
    // Reusable ascending-gen-order scratch for populateCacheForKey's metadata-only
    // cache warm. Owned by the reader's operating thread (same single-owner discipline
    // as the cursors); cleared at the start of every populateCacheForKey call.
    private final LongList cacheBuilderEntries = new LongList();
    protected final PostingIndexChainHeader.Snapshot headerScratch = new PostingIndexChainHeader.Snapshot();
    protected final MemoryCMR infoMem = Vm.getCMRInstance();
    protected final MemoryMR keyMem = Vm.getCMRInstance();
    protected final Path sidecarBasePath = new Path();
    protected final IntList sidecarColumnIndices = new IntList();
    protected final IntList sidecarColumnTypes = new IntList();
    protected final LongList sidecarCovTs = new LongList();
    // Per-cover-column file extents picked from the chain entry. Slot c is
    // the writer's published append offset in .pc{c}.<...>.<sealTxn> at the
    // moment the picked entry was (re)published. Sized by openSidecarFilesIfPresent
    // to coverCount; refreshed on each readIndexMetadataFromChain() success.
    protected final LongList sidecarFileEndOffsets = new LongList();
    protected final ObjList<MemoryMR> sidecarMems = new ObjList<>();
    protected final MemoryMR valueMem = Vm.getCMRInstance();
    protected long columnTop;
    protected int coverCount;
    protected boolean[] coveredAvailable;
    // Highest row id the picked chain entry's index data covers
    // (V2_ENTRY_OFFSET_MAX_VALUE). Cursor reads must not surface row ids
    // beyond this value: the writer can leave dirty entries in .pv past
    // the chain's tracked maxValue (e.g. after an O3 split shrinks the
    // partition before the next reseal evicts them).
    // readIndexMetadataFromChain() refreshes this field on each success;
    // -1 indicates the picker has no visible chain entry, in which case
    // the cursor degrades to the empty path and ignores entryMaxValue.
    protected long entryMaxValue = -1L;
    protected int genCount;
    protected int keyCount;
    protected RecordMetadata metadata;
    // Assertion-only stamp of the thread that last checked out a cursor; see
    // assertStampOperatingThread() / assertSameOperatingThread().
    private long assertOperatingThreadId = -1L;
    // Last successfully observed seqlock value of the chain header's active
    // page. Used by reloadConditionally to detect any publish (appendNewEntry
    // or extendHead — both republish the header) and skip the picker walk
    // when nothing has changed since our last read.
    private long chainSequence;
    private MillisecondClock clock;
    private long columnTxn;
    private ColumnVersionReader columnVersionReader;
    private FilesFacade ff;
    // While true, reloadConditionally() is a no-op. Set by the parallel-decode
    // pipeline for the duration that async worker cursors hold raw page
    // addresses into valueMem / sidecar mappings, so a concurrent writer's
    // publish cannot trigger a remap (free+mmap / changeSize / sidecar
    // close+resize) that would invalidate those in-flight addresses.
    private boolean frozen = false;
    // Byte offset of the entry currently driving this reader's snapshot
    // (V2_NO_HEAD if the chain is empty / no visible entry).
    private long headEntryOffset = PostingIndexUtils.V2_NO_HEAD;
    private CharSequence indexColumnName;
    private int keyCountIncludingNulls;
    // Pin value used at the most recent successful pick. reloadConditionally
    // compares against pinnedTableTxn to force a re-pick on pin change even
    // when the chain seqlock has not advanced.
    private long lastPickedPinnedTxn = Long.MIN_VALUE;
    private long partitionTimestamp;
    private long partitionTxn;
    // Strict-pin: the table txn this reader is pinned at via the scoreboard.
    // Picker selects the entry with the largest {@code txnAtSeal <= pinnedTableTxn};
    // computeVisibleGenCount trims the head entry's visible genCount to slots
    // with {@code slot.TXN_AT_SEAL <= pinnedTableTxn}. Default Long.MAX_VALUE
    // is "unpinned"; production callers replace it via setPinnedTableTxn.
    private long pinnedTableTxn = Long.MAX_VALUE;
    private long spinLockTimeoutMs;
    private long valueFileTxn;
    private long valueMemSize = -1;

    @Override
    public void close() {
        Misc.free(genLookup);
        Misc.free(infoMem);
        Misc.free(keyMem);
        Misc.free(valueMem);
        closeSidecarMems();
        Misc.free(sidecarBasePath);
        keyCount = 0;
        keyCountIncludingNulls = 0;
        genCount = 0;
        valueMemSize = -1;
        chainSequence = 0;
        entryMaxValue = -1L;
        headEntryOffset = PostingIndexUtils.V2_NO_HEAD;
        pinnedTableTxn = Long.MAX_VALUE;
        lastPickedPinnedTxn = Long.MIN_VALUE;
    }

    @Override
    public int collectDistinctKeys(DirectBitSet foundKeys) {
        if (genCount == 0 || keyCount == 0) {
            return 0;
        }
        if (entryMaxValue >= 0) {
            // The key-directory fast path can only tell that a key has encoded
            // row ids somewhere in .pv; it cannot tell whether all of them sit
            // past the picked chain entry's MAX_VALUE. Use the ranged scanner
            // so full-partition DISTINCT observes the same clamp as cursors.
            return collectDistinctKeysInRange(foundKeys, 0, Long.MAX_VALUE);
        }
        int newlyFound = 0;
        for (int g = 0; g < genCount; g++) {
            int genKeyCount = genLookup.getGenKeyCount(g);
            long genFileOffset = genLookup.getGenFileOffset(g);
            if (genKeyCount >= 0) {
                newlyFound += collectDenseGenKeys(genFileOffset, genKeyCount, foundKeys);
            } else {
                newlyFound += collectSparseGenKeys(genFileOffset, -genKeyCount, foundKeys);
            }
        }
        return newlyFound;
    }

    @Override
    public int collectDistinctKeysInRange(DirectBitSet foundKeys, long rowLo, long rowHi) {
        if (genCount == 0 || keyCount == 0) {
            return 0;
        }
        // Clamp rowHi to the picked chain entry's MAX_VALUE for the same
        // reason PostingIndexFwdReader#getCursor does: dirty (key, rowId)
        // pairs in .pv past the entry's coverage must not surface as
        // keys "present in range".
        if (entryMaxValue >= 0 && rowHi > entryMaxValue) {
            rowHi = entryMaxValue;
        }
        if (rowHi < rowLo) {
            return 0;
        }
        int newlyFound = 0;
        for (int g = 0; g < genCount; g++) {
            int genKeyCount = genLookup.getGenKeyCount(g);
            long genFileOffset = genLookup.getGenFileOffset(g);
            if (genKeyCount >= 0) {
                newlyFound += scanDenseGenForRange(genFileOffset, genKeyCount, foundKeys, rowLo, rowHi);
            } else {
                newlyFound += scanSparseGenForRange(genFileOffset, -genKeyCount, foundKeys, rowLo, rowHi);
            }
        }
        return newlyFound;
    }

    @Override
    public long getColumnTop() {
        return columnTop;
    }

    @Override
    public long getColumnTxn() {
        return columnTxn;
    }

    /**
     * Highest row id the picked chain entry's index data covers (its
     * {@code V2_ENTRY_OFFSET_MAX_VALUE}), or {@code -1} when the picker has no
     * visible chain entry (empty partition / not yet visible at our pin). This
     * is the inclusive clamp {@link #getCursor}/{@code getDetachedCursor} fold
     * into a cursor's upper bound ({@code min(callerMax, entryMaxValue)} when
     * {@code >= 0}); the covered-frame dispatcher reads it to pass
     * {@link #selectKthMatch} the IDENTICAL {@code maxValueClamped} the cursor
     * (and its {@code size()}) used, so the O(genCount) frame metadata matches
     * the traverse byte for byte.
     */
    public long getEntryMaxValue() {
        return entryMaxValue;
    }

    /**
     * O(genCount) covered-frame metadata primitive: the EXACT number of {@code key}'s
     * matching postings within {@code [minValue, maxValueClamped]} — the SAME count the
     * forward cursor reaches at natural exhaustion over that clamped range, WITHOUT the
     * O(rows) traverse. It is the companion to {@link #selectKthMatch}: it walks gens in
     * forward (cursor) order applying the IDENTICAL per-gen EXACT coverage predicate
     * (true first/last posting read directly via {@code select*KthValue}, not the slack
     * max bound {@code size()} uses), summing the per-gen counts of fully-covered gens
     * and skipping ALL_DIRTY gens.
     * <p>
     * Returns {@link Numbers#LONG_NULL} (so the caller falls back to the traverse) on a
     * GENUINELY clipped (MIXED) gen — exactly the cases {@link #selectKthMatch} also
     * sentinels — so that {@code countMatchesClamped} and {@code selectKthMatch} agree on
     * which layouts are metadata-resolvable and compose with no sentinel surprise.
     * <p>
     * Unlike {@code coveringCursor.size()}, this does NOT false-bail when the encoding's
     * slack max upper bound straddles {@code maxValueClamped} while the true max is within
     * it (the common freshly-resealed partition, where {@code entryMaxValue} is the genuine
     * last row id) — that is the case the covered dispatcher must keep on the cheap path.
     *
     * @param key             column key (>= 0); null-prefix handled when {@code key == 0 && columnTop > 0}
     * @param minValue        inclusive lower bound of the cursor's range
     * @param nullMaxValue    UNCLAMPED inclusive caller max; bounds ONLY the implicit-null prefix
     *                        ({@code nullCount = min(columnTop, nullMaxValue + 1)}), mirroring the
     *                        cursor's {@code NullCursor} — implicit nulls are independent of the
     *                        index and clamped by {@code columnTop} only, NOT by {@code entryMaxValue}
     * @param maxValueClamped inclusive upper bound for the GEN walk, already clamped to {@code entryMaxValue}
     * @return the exact clamped match count, or {@link Numbers#LONG_NULL} to signal "fall back to traverse"
     */
    public long countMatchesClamped(int key, long minValue, long nullMaxValue, long maxValueClamped) {
        if (key < 0 || keyCount == 0 || genCount == 0 || maxValueClamped < minValue) {
            return Numbers.LONG_NULL;
        }

        long total = 0;

        // Null prefix: synthetic contiguous null row ids the cursor emits for key 0
        // before any posting (mirrors selectKthMatch / NullCursor). The implicit
        // nulls are independent of the index, so the cursor (getCursor / NullCursor)
        // bounds them by the UNCLAMPED caller max and columnTop only:
        // nullCount = min(columnTop, nullMaxValue + 1) — NOT maxValueClamped, which
        // would under-count when entryMaxValue < columnTop. Matches are those at/after minValue.
        if (key == 0 && columnTop > 0 && minValue < columnTop) {
            long nullCount = Math.min(columnTop, nullMaxValue == Long.MAX_VALUE ? Long.MAX_VALUE : nullMaxValue + 1);
            total += Math.max(0L, nullCount - minValue);
        }

        if (key >= keyCount) {
            // Only the null prefix is addressable for a key past keyCount; if the key
            // had real postings the cursor would visit gens, so a key past keyCount with
            // no null prefix contributes nothing — total is exact (possibly 0).
            return total;
        }

        // Forward gen walk (cursor order), applying selectKthMatch's EXACT coverage check.
        for (int g = 0; g < genCount; g++) {
            int gkc = genLookup.getGenKeyCount(g);
            long count;
            if (gkc >= 0) {
                if (key >= gkc) {
                    continue;
                }
                count = selectDenseKeyCount(key, g, gkc);
            } else {
                if (genLookup.notContainKey(valueMem, g, key)) {
                    continue;
                }
                count = selectSparseKeyCount(key, g, -gkc);
            }
            if (count <= 0) {
                continue; // key absent / empty in this gen
            }
            // The per-gen ordinal is int-indexed throughout the index format (the cursor
            // itself walks int ordinals), so a single (key, gen) never holds > 2^31
            // postings. Assert it so the (int) casts below fail loud rather than silently
            // truncating if that cap is ever raised.
            assert count <= Integer.MAX_VALUE : "per-gen match count exceeds int range: " + count;
            long exactMin = gkc >= 0
                    ? selectDenseKthValue(key, g, gkc, 0)
                    : selectSparseKthValue(key, g, -gkc, 0);
            long exactMax = gkc >= 0
                    ? selectDenseKthValue(key, g, gkc, (int) (count - 1))
                    : selectSparseKthValue(key, g, -gkc, (int) (count - 1));
            if (exactMin == Numbers.LONG_NULL || exactMax == Numbers.LONG_NULL) {
                return Numbers.LONG_NULL;
            }
            // Fully past the clamp: the cursor skips the whole gen (ALL_DIRTY).
            if (exactMin > maxValueClamped) {
                continue;
            }
            // Fully below the low bound: every posting in this gen is < minValue, so the
            // cursor filters them all out and the gen contributes 0 — skip it (the symmetric
            // counterpart of the fully-above-clamp case above). Without this, such a gen would
            // trip the exactMin < minValue MIXED check below and force the whole (key,
            // partition) onto the O(rows) traverse, even though it is cleanly empty here.
            if (exactMax < minValue) {
                continue;
            }
            // Partially clipped (genuine MIXED — straddles minValue or the clamp): bail to the
            // traverse fallback. exactMax >= minValue here, so exactMin < minValue means a true
            // straddle (some postings below minValue, some at/above), which the full count would
            // over-count.
            if (exactMax > maxValueClamped || exactMin < minValue) {
                return Numbers.LONG_NULL;
            }
            total += count;
        }
        return total;
    }

    /**
     * O(genCount) covered-frame metadata primitive: returns the absolute row id of the
     * 0-based {@code k}-th matching posting of {@code key} within {@code [minValue, maxValueClamped]},
     * WITHOUT the O(rows) cursor traverse. {@code maxValueClamped} is the SAME inclusive clamp
     * the cursor applies ({@code min(callerHi - 1, entryMaxValue)}); callers pass it pre-computed.
     * <p>
     * Equivalence contract: the returned row id is IDENTICAL to the one the forward cursor's
     * {@code next()} yields at iteration position {@code k} over the same clamped range, PROVIDED
     * the range covers every visited gen's postings for the key fully. The method walks gens in
     * forward (cursor) order using the SAME O(1) per-gen count reads {@link AbstractCoveringCursor#size}
     * uses; the holding gen is the first where {@code acc + c_g > k}, with within-gen index
     * {@code j = k - acc}. The within-gen value is then read by random access: FLAT stride via a
     * single {@code unpackValue}; per-key Elias-Fano via the high-bits select + low-bits read;
     * per-key delta-FoR by locating the owning block from the {@code valueCounts} prefix and
     * accumulating that block's deltas up to the in-block index (O(j / BLOCK_CAPACITY + j % BLOCK_CAPACITY)).
     * <p>
     * Coverage is verified EXACTLY (not via the slack max bound {@code size()} uses): the gen's true
     * first and last postings for the key are read directly. Returns the sentinel
     * {@link Numbers#LONG_NULL} (so the caller falls back to the traverse) when exact equivalence
     * cannot be guaranteed by metadata alone:
     * <ul>
     *   <li>a visited gen is partially clipped by {@code minValue} or {@code maxValueClamped}
     *       (genuine MIXED — e.g. dirty rows past the chain entry's MAX_VALUE, or a narrow caller
     *       range mid-gen) — the cursor would trim it but the O(1) full-gen count would over-count;</li>
     *   <li>{@code k} is out of range for the clamped match set.</li>
     * </ul>
     * Never returns a wrong row id. Asserts {@code minValue <= result <= maxValueClamped} on success.
     *
     * @param key             column key (>= 0); null-prefix handled when {@code key == 0 && columnTop > 0}
     * @param minValue        inclusive lower bound of the cursor's range
     * @param nullMaxValue    UNCLAMPED inclusive caller max; bounds ONLY the implicit-null prefix
     *                        ({@code nullCount = min(columnTop, nullMaxValue + 1)}), mirroring the
     *                        cursor's {@code NullCursor} — implicit nulls are independent of the
     *                        index and clamped by {@code columnTop} only, NOT by {@code entryMaxValue}
     * @param maxValueClamped inclusive upper bound for the GEN walk, already clamped to {@code entryMaxValue}
     * @param k               0-based match ordinal within the clamped range
     * @return the absolute row id, or {@link Numbers#LONG_NULL} to signal "fall back to traverse"
     */
    public long selectKthMatch(int key, long minValue, long nullMaxValue, long maxValueClamped, long k) {
        if (key < 0 || k < 0 || keyCount == 0 || genCount == 0 || maxValueClamped < minValue) {
            return Numbers.LONG_NULL;
        }

        long acc = 0;

        // Null prefix: when the requested key is 0 and the column has a null
        // (columnTop) prefix, rows [minValue .. nullCount - 1] are synthetic
        // contiguous null row ids the cursor emits BEFORE any index posting,
        // exactly mirroring NullCursor.hasNext()/getCursor's nullCount =
        // min(columnTop, callerHi). Implicit nulls are independent of the index,
        // so the bound is the UNCLAMPED caller max (columnTop only), NOT
        // maxValueClamped: nullCount = min(columnTop, nullMaxValue + 1). A null row
        // can therefore exceed maxValueClamped (when entryMaxValue < columnTop), so
        // the result is asserted against nullMaxValue, the bound that actually clamps it.
        if (key == 0 && columnTop > 0 && minValue < columnTop) {
            long nullCount = Math.min(columnTop, nullMaxValue == Long.MAX_VALUE ? Long.MAX_VALUE : nullMaxValue + 1);
            long nullMatches = Math.max(0L, nullCount - minValue);
            if (k < nullMatches) {
                long result = minValue + k;
                assert result >= minValue && result <= nullMaxValue;
                return result;
            }
            acc = nullMatches;
        }

        if (key >= keyCount) {
            // Only the null prefix is addressable for a key past keyCount.
            return Numbers.LONG_NULL;
        }

        // Forward gen walk (cursor order). Gen g's rows precede gen g+1's and are
        // concatenated in gen order, so the k-th match lives in the first gen whose
        // running count crosses k. Per-gen counts use the same O(1) reads size() uses.
        for (int g = 0; g < genCount; g++) {
            int gkc = genLookup.getGenKeyCount(g);
            long count;
            if (gkc >= 0) {
                if (key >= gkc) {
                    continue;
                }
                count = selectDenseKeyCount(key, g, gkc);
            } else {
                if (genLookup.notContainKey(valueMem, g, key)) {
                    continue;
                }
                count = selectSparseKeyCount(key, g, -gkc);
            }
            if (count <= 0) {
                continue; // key absent / empty in this gen
            }
            // Per-gen ordinals are int-indexed (see countMatchesClamped); assert so the
            // (int) casts of count-1 / k-acc below fail loud rather than silently truncate.
            assert count <= Integer.MAX_VALUE : "per-gen match count exceeds int range: " + count;
            // Exact (not slack) coverage check: read the gen's first and last posting
            // for the key directly. If the whole list sits inside [minValue, clamp] the
            // O(1) full count equals the cursor's range-filtered count, so the walk is
            // exact. Otherwise the gen is partially clipped — by a narrow caller range or
            // by dirty rows past the chain entry's MAX_VALUE — and the metadata-only count
            // would diverge from the cursor; return the sentinel so the caller traverses.
            long exactMin = gkc >= 0
                    ? selectDenseKthValue(key, g, gkc, 0)
                    : selectSparseKthValue(key, g, -gkc, 0);
            long exactMax = gkc >= 0
                    ? selectDenseKthValue(key, g, gkc, (int) (count - 1))
                    : selectSparseKthValue(key, g, -gkc, (int) (count - 1));
            if (exactMin == Numbers.LONG_NULL || exactMax == Numbers.LONG_NULL) {
                return Numbers.LONG_NULL;
            }
            // Fully past the clamp: the cursor skips the whole gen (ALL_DIRTY).
            if (exactMin > maxValueClamped) {
                continue;
            }
            // Fully below the low bound: every posting is < minValue, so the cursor skips the
            // whole gen and it contributes 0 matches — skip it without disturbing the running
            // acc (the symmetric counterpart of the fully-above-clamp case). Must mirror
            // countMatchesClamped exactly, or the k-th-match index would desync from the count.
            if (exactMax < minValue) {
                continue;
            }
            // Partially clipped (genuine MIXED — straddles minValue or the clamp): bail to the
            // traverse fallback.
            if (exactMax > maxValueClamped || exactMin < minValue) {
                return Numbers.LONG_NULL;
            }
            if (acc + count > k) {
                int j = (int) (k - acc);
                long result = gkc >= 0
                        ? selectDenseKthValue(key, g, gkc, j)
                        : selectSparseKthValue(key, g, -gkc, j);
                assert result == Numbers.LONG_NULL || (result >= minValue && result <= maxValueClamped)
                        : "selectKthMatch out of range: result=" + result + " min=" + minValue + " clamp=" + maxValueClamped;
                return result;
            }
            acc += count;
        }
        // k is past the end of the clamped match set.
        return Numbers.LONG_NULL;
    }

    @Override
    public long getKeyBaseAddress() {
        return keyMem.addressOf(0);
    }

    @Override
    public int getKeyCount() {
        return keyCountIncludingNulls;
    }

    /**
     * O(genCount) covered-frame metadata primitive: pre-populates the per-reader genLookup
     * cache for {@code key} IDENTICALLY to the cursor traverse's
     * {@code genLookup.putCacheEntries(key, builderEntries)} at natural exhaustion, but via a
     * metadata-only gen walk (no O(rows) decode). After this call a same-key cursor over this
     * reader replays the cache instead of re-walking the SBBF/prefix-sum path, exactly as if a
     * full traverse had warmed it.
     * <p>
     * The cache only ever holds sparse-gen hits, so this is gated on a layout with at
     * least one sparse gen ({@code anySparseGen}); an all-dense layout (incl. single-gen-dense)
     * never touches the cache, and dense gens are never cached. The entry list is emitted in
     * ascending gen order — the canonical form: the forward traverse commits ascending, and the
     * backward traverse builds descending then {@code reverse()}s, so both converge on ascending.
     * <p>
     * Byte-for-byte equivalent to the traverse: it applies the SAME per-gen predicates the
     * traverse's {@code advanceTo*RelevantGen} applies — gen-key-range, SBBF {@code notContainKey},
     * and {@code start != end} (the key is genuinely present in the sparse gen) — and packs the
     * SAME {@code packCacheEntry(gen, start)} values. {@code putCacheEntries} itself is idempotent and
     * budget-guarded, so a redundant call (or one over budget) is a safe no-op.
     *
     * @param key             column key (>= 0)
     * @param maxValueClamped inclusive clamp the cursor uses; reserved for symmetry with
     *                        {@link #selectKthMatch} — the cache predicate is value-independent,
     *                        so it does not currently affect which gens are cached
     */
    public void populateCacheForKey(int key, long maxValueClamped) {
        if (key < 0 || !genLookup.anySparseGen()) {
            return;
        }
        cacheBuilderEntries.clear();
        for (int g = 0; g < genCount; g++) {
            if (genLookup.getGenKeyCount(g) >= 0) {
                continue; // dense gen — never cached
            }
            if (key < genLookup.getGenMinKey(g) || key > genLookup.getGenMaxKey(g)) {
                continue;
            }
            if (genLookup.notContainKey(valueMem, g, key)) {
                continue;
            }
            long prefixSumAddr = valueMem.addressOf(genLookup.getGenPrefixSumOffset(g, valueMem));
            int minKey = genLookup.getGenMinKey(g);
            int start = Unsafe.getInt(prefixSumAddr + (long) (key - minKey) * Integer.BYTES);
            int end = Unsafe.getInt(prefixSumAddr + (long) (key - minKey + 1) * Integer.BYTES);
            // EXACT traverse predicate: the cursor's loadSparseGenByPrefixSum (and
            // selectSparseKeyCount) record an entry for the key iff start != end. counts[start] > 0
            // is NOT equivalent: for a key in [minKey, maxKey] but ABSENT from this gen (an SBBF
            // false-positive that reaches here) start == end, yet counts[start] is the NEXT active
            // key's count (> 0) — caching a spurious entry that points at a different key's postings.
            if (start == end) {
                continue;
            }
            cacheBuilderEntries.add(PostingGenLookup.packCacheEntry(g, start));
        }
        genLookup.putCacheEntries(key, cacheBuilderEntries);
    }

    @Override
    public long getKeyMemorySize() {
        return keyMem.size();
    }

    @Override
    public long getPartitionTxn() {
        return partitionTxn;
    }

    @Override
    public long getValueBaseAddress() {
        return valueMem.addressOf(0);
    }

    @Override
    public int getValueBlockCapacity() {
        return 0;
    }

    @Override
    public long getValueMemorySize() {
        return valueMem.size();
    }

    @Override
    public boolean isOpen() {
        return keyMem.getFd() != -1;
    }

    @Override
    public void of(
            CairoConfiguration configuration,
            @Transient Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp
    ) {
        this.columnTop = columnTop;
        this.columnTxn = columnNameTxn;
        this.partitionTxn = partitionTxn;
        this.metadata = metadata;
        this.columnVersionReader = columnVersionReader;
        this.partitionTimestamp = partitionTimestamp;
        this.spinLockTimeoutMs = configuration.getSpinLockTimeout();
        this.clock = configuration.getMillisecondClock();
        this.ff = configuration.getFilesFacade();
        this.indexColumnName = columnName;
        this.sidecarBasePath.of(path);
        // Self-healing freeze reset: re-initialising a pooled reader always starts it
        // unfrozen, so a reused reader can never inherit a stale freeze if a prior query's
        // reset()/setFrozen(false) was skipped (a permanently-frozen reader would silently
        // no-op reloadConditionally() and miss writer republishes). genLookup.reopen()
        // clears the mirrored genLookup freeze. See setFrozen().
        this.frozen = false;
        genLookup.reopen();
        final int pLen = path.size();

        try {
            LPSZ keyFile = PostingIndexUtils.keyFileName(path, columnName, columnNameTxn);
            long keyFileSize = ff.length(keyFile);
            if (keyFileSize >= 0 && keyFileSize < PostingIndexUtils.KEY_FILE_RESERVED) {
                throw CairoException.critical(0)
                        .put("posting index key file too short [expected>=")
                        .put(PostingIndexUtils.KEY_FILE_RESERVED)
                        .put(", actual=").put(keyFileSize)
                        .put(", path=").put(keyFile).put(']');
            }
            // Map the entire key file. v2 chain entries live past the
            // reserved header region (>= KEY_FILE_RESERVED) so the mapping
            // must cover the file's current length, not just the header.
            keyMem.of(
                    ff,
                    keyFile,
                    ff.getMapPageSize(),
                    -1,
                    MemoryTag.MMAP_INDEX_READER,
                    CairoConfiguration.O_NONE,
                    -1
            );

            // Discover coverCount from .pci before reading the chain entry
            // — the entry's cover end-offset footer length is sized by
            // coverCount, and PostingIndexChainPicker.pick() needs it to
            // populate the snapshot's coverFileEndOffsets list.
            openSidecarFilesIfPresent(path.trimTo(pLen), columnName, columnNameTxn);

            readIndexMetadataFromChain();

            if (headEntryOffset == PostingIndexUtils.V2_NO_HEAD || valueMemSize <= 0) {
                // Chain is empty or not yet visible at our pin. Skip mapping
                // the value file — readers will treat the partition as empty.
                // A subsequent reloadConditionally() will lazily map .pv if
                // the writer publishes an entry while we're open.
                return;
            }

            mapValueMem(path.trimTo(pLen), columnName, columnNameTxn);
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(pLen);
        }
    }

    @Override
    public void reloadConditionally() {
        if (frozen) {
            // Parallel decode in progress: in-flight worker cursors hold raw
            // page addresses into valueMem / sidecar mappings. Suppress the
            // entire reload (seqlock read, picker walk, and any remap) so those
            // mmaps stay stable until the pipeline clears the freeze. This also
            // makes reloadConditionally() calls nested inside getCursor /
            // getDetachedCursor / warmForKeys no-ops while frozen, which is
            // intended.
            return;
        }
        // Cheap pre-check: peek at the header's seqlock. If the writer
        // hasn't republished since our last pick, nothing to do. The
        // sequence advances on every publish — both appendNewEntry (new
        // chain entry) and extendHead (in-place head mutation for sparse
        // gens) — so this gate covers both update paths.
        Unsafe.loadFence();
        if (!PostingIndexChainHeader.readUnderSeqlock(keyMem, headerScratch)) {
            // Header was unreadable this attempt; the next reload will try
            // again. We keep the existing snapshot in the meantime.
            return;
        }
        // A pin change can move the picker to a different entry even when
        // the chain hasn't been republished.
        boolean chainAdvanced = headerScratch.sequence != chainSequence;
        boolean pinChanged = lastPickedPinnedTxn != pinnedTableTxn;
        if (!chainAdvanced && !pinChanged) {
            return;
        }

        // File can only have grown when the chain advanced.
        if (chainAdvanced && ff != null) {
            long fd = keyMem.getFd();
            if (fd > 0) {
                long fileLen = ff.length(fd);
                if (fileLen > 0 && fileLen > keyMem.size()) {
                    keyMem.extend(fileLen);
                }
            }
        }

        long prevValueMemSize = valueMemSize;
        long prevValueFileTxn = valueFileTxn;
        readIndexMetadataFromChain();
        if (valueMemSize <= 0) {
            return;
        }
        boolean sealTxnAdvanced = valueFileTxn != prevValueFileTxn;
        if (sealTxnAdvanced || valueMem.size() == 0) {
            // sealTxn advanced (new .pv.{N} filename) or .pv was never opened
            // because the chain was previously empty. Close and reopen
            // against the new path / size.
            Misc.free(valueMem);
            mapValueMem(sidecarBasePath, indexColumnName, columnTxn);
        } else if (valueMemSize != prevValueMemSize) {
            // Same .pv file, just grew or shrank in place. extendHead reuses
            // the head entry but advances valueMemSize, so we must resize
            // valueMem here even though headEntryOffset is unchanged.
            ((MemoryCMR) this.valueMem).changeSize(valueMemSize);
        }
        // Refresh sidecar mappings to track .pcN file changes.
        // sealTxn advance: drop existing mappings; the new sealTxn names
        // different .pcN files (sealTxn is part of coverDataFileName), so
        // ensureSidecarOpen() must lazy-remap on the next covering read.
        // Same sealTxn, gen flushed: writeSidecarGenData() may have extended
        // .pcN past the writer's previous extent; resize each mapping to the
        // chain-published end offset so covered reads in the new gen stay
        // in bounds.
        for (int c = 0, n = sidecarMems.size(); c < n; c++) {
            MemoryMR mem = sidecarMems.getQuick(c);
            if (mem == null || mem.size() == 0) {
                continue;
            }
            if (sealTxnAdvanced) {
                mem.close();
                continue;
            }
            long publishedEnd = c < sidecarFileEndOffsets.size() ? sidecarFileEndOffsets.getQuick(c) : 0L;
            if (publishedEnd > mem.size()) {
                ((MemoryCMR) mem).changeSize(publishedEnd);
            }
        }
    }

    /**
     * While frozen, {@link #reloadConditionally()} is a no-op so the value /
     * sidecar mmaps stay stable for in-flight worker cursors that hold raw page
     * addresses into them. The parallel-decode pipeline sets this around the
     * window in which it dispatches async decode work and clears it once all
     * worker cursors have finished.
     * <p>
     * {@code frozen} (and the warm-pass state it guards: the genLookup cache,
     * {@code coveredAvailable}, the pre-extended mmaps) is a plain field. Visibility to
     * worker threads relies entirely on the dispatch sequence's release/acquire: the
     * freeze and all warm writes happen-before the {@code pubSeq.done(cursor)} publish on
     * the single dispatch thread, and a worker observes them after its {@code Sequence}
     * dequeue. Any future handoff of a covered reader to a worker through a channel OTHER
     * than the reduce/collect sequences would have no happens-before and must add its own
     * fence (or make this field volatile).
     */
    @Override
    public void setFrozen(boolean frozen) {
        this.frozen = frozen;
        // Defence-in-depth for parallel decode: while frozen, no cursor may mutate the shared
        // genLookup cache (workers run concurrently against this one reader). putCacheEntries
        // asserts on a frozen write so any future regression of the read-only-worker invariant
        // fails loud in tests rather than racing.
        genLookup.setFrozen(frozen);
    }

    @Override
    public boolean isFrozen() {
        return frozen;
    }

    @TestOnly
    public void setGenLookupCacheBudget(long budget) {
        genLookup.setCacheMemoryBudget(budget);
    }

    @Override
    public void setPinnedTableTxn(long pinnedTableTxn) {
        this.pinnedTableTxn = pinnedTableTxn;
    }

    private static boolean deltaKeyHasValueInRange(long baseAddr, long encodedOffset, long rowLo, long rowHi) {
        int firstWord = Unsafe.getInt(baseAddr + encodedOffset);
        if (firstWord == PostingIndexUtils.EF_FORMAT_SENTINEL) {
            return efKeyHasValueInRange(baseAddr, encodedOffset, rowLo, rowHi);
        }
        if (firstWord <= 0) {
            return false;
        }
        long valueCountsOff = encodedOffset + 4;
        long firstValuesOff = valueCountsOff + firstWord;
        long minDeltasOff = firstValuesOff + (long) firstWord * Long.BYTES;
        long bitWidthsOff = minDeltasOff + (long) firstWord * Long.BYTES;
        long packedOffsetsOff = bitWidthsOff + firstWord;
        long packedDataStartOff = firstWord > 1
                ? packedOffsetsOff + (long) firstWord * Long.BYTES
                : bitWidthsOff + firstWord;

        long firstFV = Unsafe.getLong(baseAddr + firstValuesOff);
        if (firstFV > rowHi) {
            return false;
        }
        int lo = 0, hi = firstWord;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            long fv = Unsafe.getLong(baseAddr + firstValuesOff + (long) mid * Long.BYTES);
            if (fv < rowLo) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo < firstWord) {
            long fv = Unsafe.getLong(baseAddr + firstValuesOff + (long) lo * Long.BYTES);
            if (fv <= rowHi) {
                return true;
            }
        }

        int b = lo < firstWord ? lo - 1 : firstWord - 1;
        int count = Unsafe.getByte(baseAddr + valueCountsOff + b) & 0xFF;
        int numDeltas = count - 1;
        if (numDeltas == 0) {
            return false;
        }
        long firstValue = Unsafe.getLong(baseAddr + firstValuesOff + (long) b * Long.BYTES);
        long minD = Unsafe.getLong(baseAddr + minDeltasOff + (long) b * Long.BYTES);
        int bitWidth = Unsafe.getByte(baseAddr + bitWidthsOff + b) & 0xFF;

        if (bitWidth == 0) {
            // Constant arithmetic progression: values are firstValue + k*minD
            // for k = 0..numDeltas. minD == 0 with count > 1 would mean the
            // writer emitted duplicate row indices, which it doesn't.
            if (minD == 0) {
                return false;
            }
            long k = (rowLo - firstValue + minD - 1) / minD;
            if (k > numDeltas) {
                return false;
            }
            long value = firstValue + k * minD;
            return value <= rowHi;
        }

        long packedOffset = b > 0
                ? Unsafe.getLong(baseAddr + packedOffsetsOff + (long) b * Long.BYTES)
                : 0;
        long packedDataAddr = baseAddr + packedDataStartOff + packedOffset;
        long cum = firstValue;
        for (int k = 0; k < numDeltas; k++) {
            cum += BitpackUtils.unpackValue(packedDataAddr, k, bitWidth, minD);
            if (cum >= rowLo) {
                return cum <= rowHi;
            }
        }
        return false;
    }

    private static int denseIndexFromWriter(RecordMetadata metadata, int writerIdx) {
        for (int d = 0, n = metadata.getColumnCount(); d < n; d++) {
            if (metadata.getColumnMetadata(d).getWriterIndex() == writerIdx) {
                return d;
            }
        }
        return -1;
    }

    // True iff the EF-encoded posting list at encodedOffset contains a value
    // in [rowLo, rowHi]. Walks the EF stream in sorted order, short-circuiting
    // on the first value >= rowLo (in range iff also <= rowHi) or on the first
    // value > rowHi (out of range).
    private static boolean efKeyHasValueInRange(long baseAddr, long encodedOffset, long rowLo, long rowHi) {
        long pos = encodedOffset + 4; // skip EF_FORMAT_SENTINEL
        int totalCount = Unsafe.getInt(baseAddr + pos);
        pos += 4;
        int bitsL = Unsafe.getByte(baseAddr + pos) & 0xFF;
        pos += 1;
        long universe = Unsafe.getLong(baseAddr + pos);
        pos += 8;
        if (totalCount == 0 || universe < rowLo) {
            return false;
        }
        long lowMask = (bitsL < 64) ? (1L << bitsL) - 1 : -1L;
        long lowOffset = pos;
        long highOffset = pos + PostingIndexUtils.efLowBytesAligned(totalCount, bitsL);
        int numHighWords = (int) ((totalCount + (universe >>> bitsL) + 63) / 64);

        int outputCount = 0;
        int highWordIdx = 0;
        long lowWordAddr = baseAddr + lowOffset;
        int lowBitOffset = 0;

        while (highWordIdx < numHighWords && outputCount < totalCount) {
            long word = Unsafe.getLong(baseAddr + highOffset + (long) highWordIdx * 8);
            if (word == 0) {
                highWordIdx++;
                continue;
            }
            // base + trail recovers the high part of value at the current
            // outputCount; base-- after each consumed bit absorbs the i offset
            // baked into bit position (high_i + i) of the high-bits bitset.
            long base = (long) highWordIdx * 64 - outputCount;
            while (word != 0 && outputCount < totalCount) {
                int trail = Long.numberOfTrailingZeros(word);
                long low;
                if (bitsL == 0) {
                    low = 0;
                } else {
                    long lowWord = Unsafe.getLong(lowWordAddr);
                    low = (lowWord >>> lowBitOffset) & lowMask;
                    if (lowBitOffset + bitsL > 64) {
                        low |= (Unsafe.getLong(lowWordAddr + 8) << (64 - lowBitOffset)) & lowMask;
                    }
                    lowBitOffset += bitsL;
                    if (lowBitOffset >= 64) {
                        lowWordAddr += 8;
                        lowBitOffset -= 64;
                    }
                }
                long value = ((base + trail) << bitsL) | low;
                if (value > rowHi) {
                    return false;
                }
                if (value >= rowLo) {
                    return true;
                }
                outputCount++;
                base--;
                word &= word - 1;
            }
            highWordIdx++;
        }
        return false;
    }

    private static boolean flatKeyHasValueInRange(
            long dataAddr, int bitWidth, long baseValue,
            int startCount, int endCount, long rowLo, long rowHi
    ) {
        if (bitWidth == 0) {
            return baseValue >= rowLo && baseValue <= rowHi;
        }
        int lo = startCount, hi = endCount;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            long val = BitpackUtils.unpackValue(dataAddr, mid, bitWidth, baseValue);
            if (val < rowLo) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo < endCount) {
            long val = BitpackUtils.unpackValue(dataAddr, lo, bitWidth, baseValue);
            return val <= rowHi;
        }
        return false;
    }

    /**
     * Returns a (possibly slack) upper bound on the largest row id encoded
     * for one key, given the per-key encoded blob at {@code encodedOffset}
     * inside the value memory mapping. Returns {@code -1L} when the key
     * holds no values, and {@code Long.MAX_VALUE} when bit-width arithmetic
     * would overflow (handled as MIXED by {@code Cursor.size()}).
     * <p>
     * Flat-mode strides do not use the delta layout, so this helper covers
     * only the per-key delta-FoR / Elias-Fano blob format that
     * {@code deltaKeyHasValueInRange} consumes.
     */
    private static long peekDeltaKeyMaxValueUpperBound(long baseAddr, long encodedOffset) {
        int blockCount = Unsafe.getInt(baseAddr + encodedOffset);
        if (blockCount == PostingIndexUtils.EF_FORMAT_SENTINEL) {
            // EF: writer stores universe == lastValue + 1, so max == universe - 1 exactly.
            long universe = Unsafe.getLong(baseAddr + encodedOffset + 9);
            return universe - 1;
        }
        if (blockCount <= 0) {
            return -1L;
        }
        long valueCountsOff = encodedOffset + 4;
        long firstValuesOff = valueCountsOff + blockCount;
        long minDeltasOff = firstValuesOff + (long) blockCount * Long.BYTES;
        long bitWidthsOff = minDeltasOff + (long) blockCount * Long.BYTES;
        int last = blockCount - 1;
        long lastFv = Unsafe.getLong(baseAddr + firstValuesOff + (long) last * Long.BYTES);
        int lastCount = Unsafe.getByte(baseAddr + valueCountsOff + last) & 0xFF;
        int numDeltas = lastCount - 1;
        if (numDeltas == 0) {
            return lastFv;
        }
        long lastMinD = Unsafe.getLong(baseAddr + minDeltasOff + (long) last * Long.BYTES);
        int lastBitWidth = Unsafe.getByte(baseAddr + bitWidthsOff + last) & 0xFF;
        if (lastBitWidth == 0) {
            // Constant-stride block: writer guarantees minD > 0 when count > 1, so the
            // arithmetic progression's last value is exact.
            return lastFv + (long) numDeltas * lastMinD;
        }
        // Slack upper bound: assume every delta in the last block packs to (1<<bw)-1.
        // Real values are <= this, so a CLEAN classification stays correct; the cost
        // is a stricter cutoff for MIXED detection at high bitwidth.
        if (lastBitWidth >= 32) {
            return Long.MAX_VALUE;
        }
        long maxDelta = lastMinD + (1L << lastBitWidth) - 1;
        if (maxDelta < lastMinD) {
            return Long.MAX_VALUE;
        }
        long span = (long) numDeltas * maxDelta;
        if (maxDelta != 0 && span / maxDelta != numDeltas) {
            return Long.MAX_VALUE;
        }
        long max = lastFv + span;
        if (max < lastFv) {
            return Long.MAX_VALUE;
        }
        return max;
    }

    /**
     * Exact smallest row id encoded for one key, taken from the encoded blob
     * at {@code encodedOffset}. Returns {@code -1L} when the key holds no
     * values. Mirror to {@link #peekDeltaKeyMaxValueUpperBound}.
     */
    private static long peekDeltaKeyMinValue(long baseAddr, long encodedOffset) {
        int firstWord = Unsafe.getInt(baseAddr + encodedOffset);
        if (firstWord == PostingIndexUtils.EF_FORMAT_SENTINEL) {
            return peekEFKeyMinValue(baseAddr, encodedOffset);
        }
        if (firstWord <= 0) {
            return -1L;
        }
        long firstValuesOff = encodedOffset + 4 + firstWord;
        return Unsafe.getLong(baseAddr + firstValuesOff);
    }

    private static long peekEFKeyMinValue(long baseAddr, long encodedOffset) {
        long pos = encodedOffset + 4; // skip EF_FORMAT_SENTINEL
        int totalCount = Unsafe.getInt(baseAddr + pos);
        pos += 4;
        if (totalCount == 0) {
            return -1L;
        }
        int bitsL = Unsafe.getByte(baseAddr + pos) & 0xFF;
        pos += 1;
        long universe = Unsafe.getLong(baseAddr + pos);
        pos += 8;
        long lowOffset = pos;
        long highOffset = pos + PostingIndexUtils.efLowBytesAligned(totalCount, bitsL);
        int numHighWords = (int) ((totalCount + (universe >>> bitsL) + 63) / 64);
        for (int i = 0; i < numHighWords; i++) {
            long word = Unsafe.getLong(baseAddr + highOffset + (long) i * 8);
            if (word == 0) {
                continue;
            }
            // First set bit's absolute index in the high-bit stream is the high
            // part of value 0 (outputCount == 0 at the start, so base == i*64).
            long high = (long) i * 64 + Long.numberOfTrailingZeros(word);
            long low = 0;
            if (bitsL > 0) {
                long lowMask = (bitsL < 64) ? (1L << bitsL) - 1 : -1L;
                long lowWord = Unsafe.getLong(baseAddr + lowOffset);
                low = lowWord & lowMask;
            }
            return (high << bitsL) | low;
        }
        return -1L;
    }

    private void closeSidecarMems() {
        Misc.freeObjListAndKeepObjects(sidecarMems);
        coverCount = 0;
        sidecarColumnIndices.clear();
        sidecarColumnTypes.clear();
        sidecarCovTs.clear();
        sidecarFileEndOffsets.clear();
    }

    private int collectDenseGenKeys(long genFileOffset, int genKeyCount, DirectBitSet foundKeys) {
        long genAddr = valueMem.addressOf(genFileOffset);
        int sc = PostingIndexUtils.strideCount(genKeyCount);
        int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
        int newlyFound = 0;

        for (int s = 0; s < sc; s++) {
            long strideOff = Unsafe.getLong(genAddr + (long) s * Long.BYTES);
            long nextStrideOff = Unsafe.getLong(genAddr + (long) (s + 1) * Long.BYTES);
            // Empty stride: writer records strideOff[s] == strideOff[s+1] when
            // stride s contributed no bytes. Reading on would interpret the next
            // stride's bytes here.
            if (nextStrideOff == strideOff) {
                continue;
            }
            long strideAddr = genAddr + siSize + strideOff;
            int ks = PostingIndexUtils.keysInStride(genKeyCount, s);
            int keyBase = s * PostingIndexUtils.DENSE_STRIDE;
            byte mode = Unsafe.getByte(strideAddr);

            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                for (int j = 0; j < ks; j++) {
                    int startCount = Unsafe.getInt(prefixAddr + (long) j * Integer.BYTES);
                    int endCount = Unsafe.getInt(prefixAddr + (long) (j + 1) * Integer.BYTES);
                    if (endCount > startCount && !foundKeys.getAndSet(keyBase + j)) {
                        newlyFound++;
                    }
                }
            } else {
                long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                for (int j = 0; j < ks; j++) {
                    if (Unsafe.getInt(countsAddr + (long) j * Integer.BYTES) > 0
                            && !foundKeys.getAndSet(keyBase + j)) {
                        newlyFound++;
                    }
                }
            }
        }
        return newlyFound;
    }

    private int collectSparseGenKeys(long genFileOffset, int activeKeyCount, DirectBitSet foundKeys) {
        long genAddr = valueMem.addressOf(genFileOffset);
        int newlyFound = 0;
        for (int i = 0; i < activeKeyCount; i++) {
            int key = Unsafe.getInt(genAddr + (long) i * Integer.BYTES);
            if (!foundKeys.getAndSet(key)) {
                newlyFound++;
            }
        }
        return newlyFound;
    }

    private long computeVisibleEntryMaxValue(PostingIndexChainEntry.Snapshot e, int visibleGenCount) {
        if (visibleGenCount == 0) {
            return -1L;
        }
        return visibleGenCount == e.genCount
                ? e.maxValue
                : genLookup.getGenMaxValue(visibleGenCount - 1);
    }

    private int computeVisibleGenCount(PostingIndexChainEntry.Snapshot e) {
        for (int g = 0; g < e.genCount; g++) {
            if (genLookup.getGenTxnAtSeal(g) > pinnedTableTxn) {
                return g;
            }
        }
        return e.genCount;
    }

    /**
     * Open the .pv value file using the picked entry's sealTxn-suffixed
     * filename and the entry's recorded valueMemSize. The path is taken from
     * {@code basePath} and trimmed back before returning.
     */
    private void mapValueMem(Path basePath, CharSequence columnName, long columnNameTxn) {
        int pLen = basePath.size();
        try {
            valueMem.of(
                    ff,
                    PostingIndexUtils.valueFileName(basePath, columnName, columnNameTxn, valueFileTxn),
                    valueMemSize,
                    valueMemSize,
                    MemoryTag.MMAP_INDEX_READER
            );
        } finally {
            basePath.trimTo(pLen);
        }
    }

    private void openSidecarFilesIfPresent(
            Path path,
            CharSequence columnName,
            long columnNameTxn
    ) {
        int plen = path.size();
        try {
            // A rowid-only reader (null metadata) can't map writer indices to dense
            // columns, so it serves no covered reads: skip sidecar setup and leave
            // coverCount at 0. Otherwise denseIndexFromWriter() NPEs on the null
            // metadata and the propagating catch below fails the query.
            if (metadata == null) {
                return;
            }
            LPSZ pciFile = PostingIndexUtils.coverInfoFileName(path, columnName, columnNameTxn);
            if (!ff.exists(pciFile)) {
                return;
            }

            infoMem.of(ff, pciFile, ff.getMapPageSize(), -1, MemoryTag.MMAP_INDEX_READER, CairoConfiguration.O_NONE, -1);
            if (infoMem.size() < 8) {
                return;
            }
            int magic = infoMem.getInt(0);
            if (magic != PostingIndexUtils.COVER_INFO_MAGIC) {
                return;
            }
            int count = infoMem.getInt(4);
            if (count <= 0) {
                return;
            }
            sidecarColumnIndices.clear();
            sidecarColumnTypes.clear();
            sidecarCovTs.clear();
            closeSidecarMems();
            for (int i = 0; i < count; i++) {
                int writerIdx = infoMem.getInt(8 + (long) i * Integer.BYTES);
                sidecarColumnIndices.add(writerIdx);
                if (sidecarMems.getQuiet(i) == null) {
                    sidecarMems.extendAndSet(i, Vm.getCMRInstance());
                }
                if (writerIdx < 0) {
                    sidecarColumnTypes.add(-1);
                    sidecarCovTs.add(TableUtils.COLUMN_NAME_TXN_NONE);
                    continue;
                }
                int denseIdx = denseIndexFromWriter(metadata, writerIdx);
                if (denseIdx < 0) {
                    sidecarColumnTypes.add(-1);
                    sidecarCovTs.add(TableUtils.COLUMN_NAME_TXN_NONE);
                    continue;
                }
                sidecarColumnTypes.add(metadata.getColumnType(denseIdx));
                sidecarCovTs.add(columnVersionReader.getColumnNameTxn(partitionTimestamp, writerIdx));
            }
            coverCount = count;
        } catch (Throwable e) {
            // The covered read path has no base-table fallback, so degrading here
            // would leave a covered read walking an absent sidecar. Propagate so
            // the caller fails the query and closes the reader.
            LOG.error().$("failed to open sidecar files").$(e).$();
            closeSidecarMems();
            throw e;
        } finally {
            Misc.free(infoMem);
            path.trimTo(plen);
        }
    }

    /**
     * Walk the v2 chain and pick the entry visible to this reader's pin,
     * then promote its metadata into the active snapshot.
     * <p>
     * On chain-header unreadable failures we spin briefly and retry; the
     * picker has its own bounded internal retry loop, so transient writer-
     * mid-publish states are absorbed inside it.
     */
    private void readIndexMetadataFromChain() {
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            // The picker reads at offsets up to keyMem.size(); when a
            // concurrent writer has published a new chain entry past our
            // mmap, the picker returns HEADER_UNREADABLE rather than
            // dereferencing past the mapping. Extend the mmap to the
            // current file length before each attempt so the retry sees
            // the writer's latest publish.
            if (ff != null) {
                long fd = keyMem.getFd();
                if (fd > 0) {
                    long fileLen = ff.length(fd);
                    if (fileLen > 0 && fileLen > keyMem.size()) {
                        keyMem.extend(fileLen);
                    }
                }
            }
            int result = PostingIndexChainPicker.pick(keyMem, pinnedTableTxn, coverCount, headerScratch, entryScratch);
            if (result == PostingIndexChainPicker.RESULT_HEADER_UNREADABLE) {
                if (clock.getTicks() > deadline) {
                    LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms]").$();
                    return;
                }
                Os.pause();
                continue;
            }

            // headerScratch.formatVersion is populated regardless of pick
            // outcome (read under seqlock at the start of the picker).
            if (headerScratch.formatVersion != PostingIndexUtils.V2_FORMAT_VERSION) {
                throw CairoException.critical(0)
                        .put("Unsupported Posting index version: ").put(headerScratch.formatVersion);
            }

            if (result == PostingIndexChainPicker.RESULT_OK) {
                // Fill staging gen-dir snapshot from the picked entry's payload.
                // Torn reads here are harmless — the active snapshot from the
                // previous successful read is still in place until we commit.
                genLookup.snapshotMetadata(keyMem, entryScratch.genCount, entryScratch.offset);
                // Re-validate the chain header seqlock. extendHead mutates the
                // head entry (GEN_COUNT, VALUE_MEM_SIZE) in place via separate
                // aligned stores and republishes the header. Without this
                // check the picker can observe e.g. new GEN_COUNT with old
                // VALUE_MEM_SIZE, leading to a snapshot whose gen-dir entries
                // reference offsets past the recorded valueMemSize. Retry on
                // any concurrent publish.
                if (!PostingIndexChainHeader.stillStable(keyMem, headerScratch.pageOffset, headerScratch.sequence)) {
                    if (clock.getTicks() > deadline) {
                        LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms]").$();
                        return;
                    }
                    Os.pause();
                    continue;
                }
                genLookup.commitSnapshot();
                genLookup.invalidateCache();

                this.headEntryOffset = entryScratch.offset;
                this.chainSequence = headerScratch.sequence;
                this.genCount = computeVisibleGenCount(entryScratch);
                this.entryMaxValue = computeVisibleEntryMaxValue(entryScratch, this.genCount);
                this.valueMemSize = entryScratch.valueMemSize;
                this.keyCount = entryScratch.keyCount;
                this.valueFileTxn = entryScratch.sealTxn;
                this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;
                // Promote the picked entry's per-cover end offsets into the
                // active snapshot. Slots are zero-padded to coverCount so
                // callers can read them by includeIdx without bounds checks.
                sidecarFileEndOffsets.clear();
                sidecarFileEndOffsets.setPos(coverCount);
                int picked = entryScratch.coverFileEndOffsets.size();
                for (int c = 0; c < coverCount; c++) {
                    sidecarFileEndOffsets.setQuick(c, c < picked ? entryScratch.coverFileEndOffsets.getQuick(c) : 0L);
                }
                this.lastPickedPinnedTxn = this.pinnedTableTxn;
                return;
            }

            // RESULT_EMPTY_CHAIN or RESULT_NO_VISIBLE_ENTRY: chain has nothing
            // visible to this reader yet (pre-first-seal or
            // post-snapshot-restore). Promote an empty snapshot. The reader
            // will report keyCount/genCount=0 and the caller skips mapping
            // the .pv file in of().
            this.headEntryOffset = PostingIndexUtils.V2_NO_HEAD;
            this.chainSequence = headerScratch.sequence;
            this.entryMaxValue = -1L;
            this.valueMemSize = 0;
            this.keyCount = 0;
            this.genCount = 0;
            this.valueFileTxn = 0;
            this.keyCountIncludingNulls = columnTop > 0 ? 1 : 0;
            sidecarFileEndOffsets.clear();
            sidecarFileEndOffsets.setPos(coverCount);
            for (int c = 0; c < coverCount; c++) {
                sidecarFileEndOffsets.setQuick(c, 0L);
            }
            // Reset gen lookup to an empty staging snapshot and promote it.
            genLookup.snapshotMetadata(keyMem, 0, 0L);
            genLookup.commitSnapshot();
            genLookup.invalidateCache();
            this.lastPickedPinnedTxn = this.pinnedTableTxn;
            return;
        }
    }

    private int scanDenseGenForRange(long genFileOffset, int genKeyCount, DirectBitSet foundKeys, long rowLo, long rowHi) {
        long genAddr = valueMem.addressOf(genFileOffset);
        long baseAddr = valueMem.addressOf(0);
        int sc = PostingIndexUtils.strideCount(genKeyCount);
        int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
        int newlyFound = 0;

        for (int s = 0; s < sc; s++) {
            long strideOff = Unsafe.getLong(genAddr + (long) s * Long.BYTES);
            long nextStrideOff = Unsafe.getLong(genAddr + (long) (s + 1) * Long.BYTES);
            if (nextStrideOff == strideOff) {
                continue;
            }
            long strideAddr = genAddr + siSize + strideOff;
            long strideFileOffset = genFileOffset + siSize + strideOff;
            int ks = PostingIndexUtils.keysInStride(genKeyCount, s);
            int keyBase = s * PostingIndexUtils.DENSE_STRIDE;
            byte mode = Unsafe.getByte(strideAddr);

            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                int bitWidth = Unsafe.getByte(strideAddr + 1) & 0xFF;
                long baseValue = Unsafe.getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                long dataAddr = strideAddr + PostingIndexUtils.strideFlatHeaderSize(ks);
                for (int j = 0; j < ks; j++) {
                    int globalKey = keyBase + j;
                    if (foundKeys.get(globalKey)) {
                        continue;
                    }
                    int startCount = Unsafe.getInt(prefixAddr + (long) j * Integer.BYTES);
                    int endCount = Unsafe.getInt(prefixAddr + (long) (j + 1) * Integer.BYTES);
                    if (endCount == startCount) {
                        continue;
                    }
                    if (flatKeyHasValueInRange(dataAddr, bitWidth, baseValue, startCount, endCount, rowLo, rowHi)
                            && !foundKeys.getAndSet(globalKey)) {
                        newlyFound++;
                    }
                }
            } else {
                long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
                int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
                for (int j = 0; j < ks; j++) {
                    int globalKey = keyBase + j;
                    if (foundKeys.get(globalKey)) {
                        continue;
                    }
                    int totalCount = Unsafe.getInt(countsAddr + (long) j * Integer.BYTES);
                    if (totalCount == 0) {
                        continue;
                    }
                    long dataOffset = Unsafe.getLong(offsetsBase + (long) j * Long.BYTES);
                    long encodedOffset = strideFileOffset + deltaHeaderSize + dataOffset;
                    if (deltaKeyHasValueInRange(baseAddr, encodedOffset, rowLo, rowHi)
                            && !foundKeys.getAndSet(globalKey)) {
                        newlyFound++;
                    }
                }
            }
        }
        return newlyFound;
    }

    private int scanSparseGenForRange(long genFileOffset, int activeKeyCount, DirectBitSet foundKeys, long rowLo, long rowHi) {
        long genAddr = valueMem.addressOf(genFileOffset);
        long baseAddr = valueMem.addressOf(0);
        long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
        long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
        int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
        int newlyFound = 0;
        for (int i = 0; i < activeKeyCount; i++) {
            int key = Unsafe.getInt(genAddr + (long) i * Integer.BYTES);
            if (foundKeys.get(key)) {
                continue;
            }
            int totalCount = Unsafe.getInt(countsBase + (long) i * Integer.BYTES);
            if (totalCount == 0) {
                continue;
            }
            long dataOffset = Unsafe.getLong(offsetsBase + (long) i * Long.BYTES);
            long encodedOffset = genFileOffset + headerSize + dataOffset;
            if (deltaKeyHasValueInRange(baseAddr, encodedOffset, rowLo, rowHi)
                    && !foundKeys.getAndSet(key)) {
                newlyFound++;
            }
        }
        return newlyFound;
    }

    // ----- selectKthMatch support: per-gen random-access reads, parameterized by
    // ----- key (the outer reader has no requestedKey field). These mirror, byte for
    // ----- byte, the offset arithmetic the cursor's loadDenseGenerationCached /
    // ----- loadSparseGenByPrefixSum / readDeltaBlockMetadata / decode*Block use, so a
    // ----- random-access read at index j yields exactly the cursor's j-th value.

    /**
     * Count of {@code key}'s postings in dense gen {@code gen}. Mirrors the cursor's
     * {@code getDenseGenKeyCount}: FLAT stride -> prefix {@code end - start}; DELTA stride ->
     * {@code counts[localKey]}. Returns 0 when the stride is empty or the key absent.
     */
    private long selectDenseKeyCount(int key, int gen, int genKeyCount) {
        if (key >= genKeyCount) {
            return 0;
        }
        int stride = key / PostingIndexUtils.DENSE_STRIDE;
        int localKey = key % PostingIndexUtils.DENSE_STRIDE;
        long genAddr = valueMem.addressOf(genLookup.getGenFileOffset(gen));
        long strideOff = Unsafe.getLong(genAddr + (long) stride * Long.BYTES);
        long nextStrideOff = Unsafe.getLong(genAddr + (long) (stride + 1) * Long.BYTES);
        if (nextStrideOff == strideOff) {
            return 0;
        }
        long strideAddr = genAddr + PostingIndexUtils.strideIndexSize(genKeyCount) + strideOff;
        byte mode = Unsafe.getByte(strideAddr);
        if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
            long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
            int start = Unsafe.getInt(prefixAddr + (long) localKey * Integer.BYTES);
            int end = Unsafe.getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES);
            return end - start;
        }
        if (mode != PostingIndexUtils.STRIDE_MODE_DELTA) {
            throw CairoException.critical(0).put(INDEX_CORRUPT).put(" [bad stride mode=").put(mode).put(']');
        }
        long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
        return Unsafe.getInt(countsAddr + (long) localKey * Integer.BYTES);
    }

    /**
     * Absolute row id of the 0-based {@code j}-th posting of {@code key} in dense gen {@code gen}.
     * FLAT stride -> single {@code unpackValue} at {@code startCount + j}; DELTA stride -> resolve
     * the per-key blob and delegate to {@link #selectFromKeyBlob}. Caller guarantees
     * {@code 0 <= j < count}.
     */
    private long selectDenseKthValue(int key, int gen, int genKeyCount, int j) {
        int stride = key / PostingIndexUtils.DENSE_STRIDE;
        int localKey = key % PostingIndexUtils.DENSE_STRIDE;
        long genFileOffset = genLookup.getGenFileOffset(gen);
        long genAddr = valueMem.addressOf(genFileOffset);
        long strideOff = Unsafe.getLong(genAddr + (long) stride * Long.BYTES);
        int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
        long strideAddr = genAddr + siSize + strideOff;
        long strideFileOffset = genFileOffset + siSize + strideOff;
        byte mode = Unsafe.getByte(strideAddr);
        int ks = PostingIndexUtils.keysInStride(genKeyCount, stride);
        if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
            int bitWidth = Unsafe.getByte(strideAddr + 1) & 0xFF;
            long baseValue = Unsafe.getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
            long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
            int startCount = Unsafe.getInt(prefixAddr + (long) localKey * Integer.BYTES);
            if (bitWidth == 0) {
                // Stride-wide FoR with zero range: every value equals baseValue.
                return baseValue;
            }
            long dataAddr = strideAddr + PostingIndexUtils.strideFlatHeaderSize(ks);
            return BitpackUtils.unpackValue(dataAddr, startCount + j, bitWidth, baseValue);
        }
        if (mode != PostingIndexUtils.STRIDE_MODE_DELTA) {
            throw CairoException.critical(0).put(INDEX_CORRUPT).put(" [bad stride mode=").put(mode).put(']');
        }
        long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
        long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
        long dataOffset = Unsafe.getLong(offsetsBase + (long) localKey * Long.BYTES);
        long encodedOffset = strideFileOffset + PostingIndexUtils.strideDeltaHeaderSize(ks) + dataOffset;
        return selectFromKeyBlob(encodedOffset, j);
    }

    /**
     * Absolute row id of the 0-based {@code j}-th value in a per-key delta-FoR blob at
     * {@code encodedOffset}. Locates the owning block by walking the {@code valueCounts} prefix,
     * positions on its packed data via {@code packedOffsets} (implicitly 0 for a single-block key),
     * then accumulates the block's deltas up to the in-block index — exactly the cumulative the
     * cursor's {@code decodeBlock} performs. O(j / BLOCK_CAPACITY + j % BLOCK_CAPACITY).
     */
    private long selectFromDeltaBlob(long encodedOffset, long baseAddr, int blockCount, int j) {
        long valueCountsOff = encodedOffset + 4;
        long firstValuesOff = valueCountsOff + blockCount;
        long minDeltasOff = firstValuesOff + (long) blockCount * Long.BYTES;
        long bitWidthsOff = minDeltasOff + (long) blockCount * Long.BYTES;
        long packedOffsetsOff = bitWidthsOff + blockCount;
        long packedDataStartOff = blockCount > 1
                ? packedOffsetsOff + (long) blockCount * Long.BYTES
                : bitWidthsOff + blockCount;

        // Locate the block that owns global index j by accumulating per-block counts.
        int b = 0;
        int blockStartIdx = 0;
        while (b < blockCount) {
            int c = Unsafe.getByte(baseAddr + valueCountsOff + b) & 0xFF;
            if (blockStartIdx + c > j) {
                break;
            }
            blockStartIdx += c;
            b++;
        }
        if (b >= blockCount) {
            return Numbers.LONG_NULL; // j past the blob — guarded against by the caller's count check
        }
        int r = j - blockStartIdx; // in-block index, 0-based
        long firstValue = Unsafe.getLong(baseAddr + firstValuesOff + (long) b * Long.BYTES);
        if (r == 0) {
            return firstValue;
        }
        long minD = Unsafe.getLong(baseAddr + minDeltasOff + (long) b * Long.BYTES);
        int bitWidth = Unsafe.getByte(baseAddr + bitWidthsOff + b) & 0xFF;
        if (bitWidth == 0) {
            // Constant arithmetic progression block: value at in-block index r.
            return firstValue + (long) r * minD;
        }
        long packedOffset = b > 0
                ? Unsafe.getLong(baseAddr + packedOffsetsOff + (long) b * Long.BYTES)
                : 0;
        long packedDataAddr = baseAddr + packedDataStartOff + packedOffset;
        // unpackValue(addr, i, bw, minD) == minD + residual_i == the delta the cursor adds at step i.
        long cum = firstValue;
        for (int i = 0; i < r; i++) {
            cum += BitpackUtils.unpackValue(packedDataAddr, i, bitWidth, minD);
        }
        return cum;
    }

    /**
     * Absolute row id of the 0-based {@code j}-th value in a per-key Elias-Fano blob at
     * {@code encodedOffset}. Selects the {@code j}-th set bit in the high-bits bitset
     * ({@code high = bitPosition - j}) and reads the matching {@code L} low bits at {@code j*L};
     * this is the random-access analogue of the cursor's forward EF chunk decode and the backward
     * reverse decode. O(numHighWords) worst case, typically far less.
     */
    private long selectFromEFBlob(long baseAddr, long encodedOffset, int j) {
        long pos = encodedOffset + 4; // skip EF_FORMAT_SENTINEL
        int totalCount = Unsafe.getInt(baseAddr + pos);
        pos += 4;
        if (j >= totalCount) {
            return Numbers.LONG_NULL;
        }
        int bitsL = Unsafe.getByte(baseAddr + pos) & 0xFF;
        pos += 1;
        long universe = Unsafe.getLong(baseAddr + pos);
        pos += 8;
        long lowOffset = pos;
        long highOffset = pos + PostingIndexUtils.efLowBytesAligned(totalCount, bitsL);
        int numHighWords = (int) ((totalCount + (universe >>> bitsL) + 63) / 64);
        // Find the high-bits word holding the j-th set bit, then its in-word position.
        int consumed = 0;
        for (int w = 0; w < numHighWords; w++) {
            long word = Unsafe.getLong(baseAddr + highOffset + (long) w * 8);
            int bits = Long.bitCount(word);
            if (consumed + bits <= j) {
                consumed += bits;
                continue;
            }
            // The (j - consumed)-th set bit within this word.
            int within = j - consumed;
            long masked = word;
            for (int t = 0; t < within; t++) {
                masked &= masked - 1; // clear lowest set bit
            }
            int trail = Long.numberOfTrailingZeros(masked);
            long high = (long) w * 64 + trail - j; // global set-bit position minus ordinal
            long low = PostingIndexUtils.readBitsWord(baseAddr + lowOffset, (long) j * bitsL, bitsL);
            if (bitsL < 64) {
                low &= (1L << bitsL) - 1;
            }
            return (high << bitsL) | low;
        }
        return Numbers.LONG_NULL;
    }

    /**
     * Dispatches a per-key encoded blob at {@code encodedOffset} to the matching select:
     * EF (sentinel first word) or delta-FoR (positive block count). Returns {@link Numbers#LONG_NULL}
     * for an empty/corrupt blob (the caller's count check makes this unreachable in practice).
     */
    private long selectFromKeyBlob(long encodedOffset, int j) {
        long baseAddr = valueMem.addressOf(0);
        int firstWord = Unsafe.getInt(baseAddr + encodedOffset);
        if (firstWord == PostingIndexUtils.EF_FORMAT_SENTINEL) {
            return selectFromEFBlob(baseAddr, encodedOffset, j);
        }
        if (firstWord <= 0) {
            return Numbers.LONG_NULL;
        }
        return selectFromDeltaBlob(encodedOffset, baseAddr, firstWord, j);
    }

    /**
     * Count of {@code key}'s postings in sparse gen {@code gen}. Mirrors the cursor's
     * {@code getSparseGenKeyCount}: prefix-sum {@code start = prefixSum[key - minKey]}, then
     * {@code counts[start]}. Returns 0 when the key is out of the gen's key range or absent.
     */
    private long selectSparseKeyCount(int key, int gen, int activeKeyCount) {
        int minKey = genLookup.getGenMinKey(gen);
        int maxKey = genLookup.getGenMaxKey(gen);
        if (key < minKey || key > maxKey) {
            return 0;
        }
        long genFileOffset = genLookup.getGenFileOffset(gen);
        long prefixSumAddr = valueMem.addressOf(genLookup.getGenPrefixSumOffset(gen, valueMem));
        int kk = key - minKey;
        int start = Unsafe.getInt(prefixSumAddr + (long) kk * Integer.BYTES);
        int end = Unsafe.getInt(prefixSumAddr + (long) (kk + 1) * Integer.BYTES);
        // Equivalent to the cursor's start == end empty-key test (PostingIndexFwdReader's
        // loadSparseGenByPrefixSum): prefix sums are monotonic non-decreasing so start > end
        // cannot occur; >= is the same check, kept defensive against a corrupt prefix sum.
        if (start >= end) {
            return 0;
        }
        long countsBase = valueMem.addressOf(genFileOffset) + (long) activeKeyCount * Integer.BYTES;
        return Unsafe.getInt(countsBase + (long) start * Integer.BYTES);
    }

    /**
     * Absolute row id of the 0-based {@code j}-th posting of {@code key} in sparse gen {@code gen}.
     * Resolves the per-key blob via the prefix-sum {@code start} index (mirroring
     * {@code loadSparseGenByPrefixSum}) and delegates to {@link #selectFromKeyBlob}.
     * Caller guarantees {@code 0 <= j < count}.
     */
    private long selectSparseKthValue(int key, int gen, int activeKeyCount, int j) {
        int minKey = genLookup.getGenMinKey(gen);
        long genFileOffset = genLookup.getGenFileOffset(gen);
        long prefixSumAddr = valueMem.addressOf(genLookup.getGenPrefixSumOffset(gen, valueMem));
        int start = Unsafe.getInt(prefixSumAddr + (long) (key - minKey) * Integer.BYTES);
        long genAddr = valueMem.addressOf(genFileOffset);
        long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
        long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
        long dataOffset = Unsafe.getLong(offsetsBase + (long) start * Long.BYTES);
        long encodedOffset = genFileOffset + PostingIndexUtils.genHeaderSizeSparse(activeKeyCount) + dataOffset;
        return selectFromKeyBlob(encodedOffset, j);
    }

    protected static long readVarBlockOffset(long offsetsAddr, int ordinal, boolean longOffsets) {
        if (longOffsets) {
            return Unsafe.getLong(offsetsAddr + (long) ordinal * Long.BYTES);
        }
        // Zero-extend to long; offsets in int blocks are non-negative.
        return Unsafe.getInt(offsetsAddr + (long) ordinal * Integer.BYTES) & 0xFFFFFFFFL;
    }

    protected static long varBlockOffsetsSize(int count, boolean longOffsets) {
        return (long) (count + 1) * (longOffsets ? Long.BYTES : Integer.BYTES);
    }

    // Single-owner tripwire (assertion-only). A posting reader and its pooled
    // cursors are driven by exactly one thread at a time: the thread that owns the
    // enclosing TableReader between pool acquire/release. getCursor() stamps that
    // thread via assertStampOperatingThread() and every cursor close() checks it
    // here. A posting cursor whose close() runs after the reader was released to the
    // pool and re-acquired by another thread -- the lifecycle hazard that
    // CoveringIndexRecordCursorFactory.CoveringCursor.close() avoids by freeing the
    // row cursor BEFORE the frame cursor -- trips this assert instead of silently
    // re-pooling into / racing a concurrently-reloaded reader. Never relied upon for
    // correctness: the isOpen() guard in each cursor close() is the actual leak
    // mitigation, and this stamp is only written under -ea.
    protected boolean assertSameOperatingThread() {
        final long owner = assertOperatingThreadId;
        return owner == -1L || owner == Thread.currentThread().threadId();
    }

    protected boolean assertStampOperatingThread() {
        assertOperatingThreadId = Thread.currentThread().threadId();
        return true;
    }

    /**
     * Single-threaded warm-up so the reader can later be read concurrently by N worker
     * cursors without any of them mutating shared state. For each key, drives a full cursor
     * pass to natural exhaustion (populates the idempotent genLookup cache so later same-key
     * cursors run read-only), pre-opens required sidecars, and pre-extends valueMem to its full
     * published size so iteration-time extend() calls become no-ops.
     */
    public void warmForKeys(int[] keys, int[] requiredCoverColumns) {
        reloadConditionally();
        if (valueMemSize > 0) {
            valueMem.extend(valueMemSize);
        }
        // Open sidecars up front so the pre-warm side effect (sidecar mmaps + coveredAvailable)
        // holds even when keys is empty; getCursor below re-runs this idempotently per key.
        openRequiredSidecars(requiredCoverColumns);
        for (int i = 0, n = keys.length; i < n; i++) {
            warmCacheForKey(keys[i], requiredCoverColumns);
        }
    }

    private void warmCacheForKey(int key, int[] requiredCoverColumns) {
        if (key < 0) {
            return;
        }
        // Open a cursor over the full key range and iterate to natural exhaustion:
        // the genLookup cache is only committed (putCacheEntries) when the gen walk
        // reaches its end, so we must not stop early. Closing the cursor returns it
        // to the reader's free list, which is safe because warming is single-threaded.
        RowCursor cursor = getCursor(key, 0, Long.MAX_VALUE, requiredCoverColumns);
        try {
            while (cursor.hasNext()) {
                cursor.next();
            }
        } finally {
            cursor.close();
        }
    }

    protected void ensureSidecarOpen(int c) {
        MemoryMR mem = sidecarMems.getQuick(c);
        long publishedEnd = c < sidecarFileEndOffsets.size() ? sidecarFileEndOffsets.getQuick(c) : 0L;
        if (publishedEnd <= 0) {
            // Writer has not (yet) published any sidecar bytes for this slot.
            // Nothing safe to map.
            return;
        }
        if (mem.size() > 0) {
            return;
        }
        int pLen = sidecarBasePath.size();
        try {
            LPSZ pcFile = PostingIndexUtils.coverDataFileName(
                    sidecarBasePath.trimTo(pLen),
                    indexColumnName,
                    c,
                    columnTxn,
                    sidecarCovTs.getQuick(c),
                    valueFileTxn
            );
            if (!ff.exists(pcFile)) {
                return;
            }
            // Use the chain-published end offset rather than ff.length(fd):
            // the file may be page-pre-allocated past valid data, and the
            // chain's value is the only authoritative upper bound that's
            // ordered with the seqlock that brought us here.
            mem.of(ff, pcFile, ff.getMapPageSize(), publishedEnd, MemoryTag.MMAP_INDEX_READER, CairoConfiguration.O_NONE, -1);
        } finally {
            sidecarBasePath.trimTo(pLen);
        }
    }

    protected void openRequiredSidecars(int[] requiredCoverColumns) {
        if (coverCount == 0) {
            return;
        }
        if (frozen) {
            // Parallel decode in progress. warmForKeys (single-threaded, pre-freeze)
            // already opened every required sidecar and populated coveredAvailable[]
            // for the query's full cover-column set. Detached worker cursors call this
            // from N threads concurrently, so it MUST NOT mutate the shared reader state
            // here: zeroing-then-rewriting coveredAvailable[] would race sibling workers
            // and momentarily publish a false availability. The array is already correct,
            // so this is a no-op while frozen.
            assert allRequiredCoveredAvailable(requiredCoverColumns)
                    : "frozen reader missing a warm-opened sidecar for the requested cover columns";
            return;
        }
        if (coveredAvailable == null || coveredAvailable.length < coverCount) {
            coveredAvailable = new boolean[coverCount];
        } else {
            for (int i = 0; i < coverCount; i++) {
                coveredAvailable[i] = false;
            }
        }
        if (requiredCoverColumns == null) {
            return;
        }
        for (int c : requiredCoverColumns) {
            if (c >= 0 && c < coverCount) {
                ensureSidecarOpen(c);
                coveredAvailable[c] = sidecarMems.getQuick(c).getFd() != -1;
            }
        }
    }

    // -ea-only invariant check used by the frozen no-op path of openRequiredSidecars:
    // every column a detached worker cursor requests must already have been opened and
    // marked available by the single-threaded warm pass, so the frozen reader never needs
    // to (and never may) rewrite the shared coveredAvailable[] from a worker thread.
    private boolean allRequiredCoveredAvailable(int[] requiredCoverColumns) {
        if (requiredCoverColumns == null) {
            return true;
        }
        if (coveredAvailable == null) {
            return false;
        }
        for (int c : requiredCoverColumns) {
            if (c >= 0 && c < coverCount && !coveredAvailable[c]) {
                return false;
            }
        }
        return true;
    }

    protected abstract class AbstractCoveringCursor implements CoveringRowCursor {
        protected final BorrowedArray arrayView = new BorrowedArray();
        protected final DirectBinarySequence binView = new DirectBinarySequence();
        protected final LongList currentGenSidecarOffsets = new LongList();
        protected final DirectString stringViewA = new DirectString();
        protected final DirectString stringViewB = new DirectString();
        protected final DirectUtf8String varcharViewA = new DirectUtf8String();
        protected final DirectUtf8String varcharViewB = new DirectUtf8String();
        protected int cachedKeyBlockStride = -1;
        protected int cachedSidecarIdx;
        protected int cursorGenCount;
        protected long decodeWorkspaceAddr;
        protected int decodeWorkspaceCapacity;
        protected int denseVarKeyStartCount;
        protected long[] fsstCachedBlockBases;
        // First ordinal of the currently-decoded chunk for each cover column,
        // or -1 if no chunk is decoded for this block. Used together with
        // fsstCachedBlockBases to address the chunk cache.
        protected long[] fsstCachedChunkStarts;
        protected long[] fsstDecoderAddrs;
        protected long[] fsstDstAddrs;
        protected long[] fsstDstCapacities;
        protected long[] fsstOffsetsAddrs;
        protected long[] fsstOffsetsCapacities;
        protected boolean isCurrentGenDense;
        protected long[] keyBlockAddrs;
        protected int requestedKey;
        protected int sealedGenKeyCount;
        protected int sidecarOrdinal;
        protected int sidecarStrideKeyStart;
        private long[] colCacheAddrs;
        private long[] colCacheBlockAddrs;
        private int[] colCacheCapacities;
        private long[] colPointBlockAddrs;

        @Override
        public ArrayView getCoveredArray(int includeIdx, int columnType) {
            return getVarSidecarArray(includeIdx, columnType);
        }

        @Override
        public BinarySequence getCoveredBin(int includeIdx) {
            return getVarSidecarBin(includeIdx);
        }

        @Override
        public long getCoveredBinLen(int includeIdx) {
            return getVarSidecarBinLen(includeIdx);
        }

        @Override
        public byte getCoveredByte(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarByte(includeIdx);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return 0;
            }
            if (ensureColumnDecoded(includeIdx)) {
                return Unsafe.getByte(colCacheAddrs[includeIdx] + idx);
            }
            return CoveringCompressor.readByteAt(keyBlockAddrs[includeIdx], idx);
        }

        @Override
        public double getCoveredDouble(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarDouble(includeIdx);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Double.NaN;
            }
            if (ensureColumnDecoded(includeIdx)) {
                return Unsafe.getDouble(colCacheAddrs[includeIdx] + (long) idx * Double.BYTES);
            }
            return CoveringCompressor.readDoubleAt(keyBlockAddrs[includeIdx], idx);
        }

        @Override
        public float getCoveredFloat(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarFloat(includeIdx);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Float.NaN;
            }
            if (ensureColumnDecoded(includeIdx)) {
                return Unsafe.getFloat(colCacheAddrs[includeIdx] + (long) idx * Float.BYTES);
            }
            return CoveringCompressor.readFloatAt(keyBlockAddrs[includeIdx], idx);
        }

        @Override
        public int getCoveredInt(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarInt(includeIdx);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Numbers.INT_NULL;
            }
            if (ensureColumnDecoded(includeIdx)) {
                return Unsafe.getInt(colCacheAddrs[includeIdx] + (long) idx * Integer.BYTES);
            }
            return CoveringCompressor.readIntAt(keyBlockAddrs[includeIdx], idx);
        }

        @Override
        public long getCoveredLong(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarLong(includeIdx);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Numbers.LONG_NULL;
            }
            if (ensureColumnDecoded(includeIdx)) {
                return Unsafe.getLong(colCacheAddrs[includeIdx] + (long) idx * Long.BYTES);
            }
            return CoveringCompressor.readLongAt(keyBlockAddrs[includeIdx], idx);
        }

        @Override
        public long getCoveredLong128Hi(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarMultiLong(includeIdx, 16, 1);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Numbers.LONG_NULL;
            }
            // UUID/DECIMAL128: raw 16 bytes per value, skip 4-byte count header
            return Unsafe.getLong(keyBlockAddrs[includeIdx] + 4 + (long) idx * 16 + 8);
        }

        @Override
        public long getCoveredLong128Lo(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarMultiLong(includeIdx, 16, 0);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Numbers.LONG_NULL;
            }
            return Unsafe.getLong(keyBlockAddrs[includeIdx] + 4 + (long) idx * 16);
        }

        @Override
        public long getCoveredLong256_0(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarMultiLong(includeIdx, 32, 0);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Numbers.LONG_NULL;
            }
            return Unsafe.getLong(keyBlockAddrs[includeIdx] + 4 + (long) idx * 32);
        }

        @Override
        public long getCoveredLong256_1(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarMultiLong(includeIdx, 32, 1);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Numbers.LONG_NULL;
            }
            return Unsafe.getLong(keyBlockAddrs[includeIdx] + 4 + (long) idx * 32 + 8);
        }

        @Override
        public long getCoveredLong256_2(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarMultiLong(includeIdx, 32, 2);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Numbers.LONG_NULL;
            }
            return Unsafe.getLong(keyBlockAddrs[includeIdx] + 4 + (long) idx * 32 + 16);
        }

        @Override
        public long getCoveredLong256_3(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarMultiLong(includeIdx, 32, 3);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Numbers.LONG_NULL;
            }
            return Unsafe.getLong(keyBlockAddrs[includeIdx] + 4 + (long) idx * 32 + 24);
        }

        @Override
        public short getCoveredShort(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarShort(includeIdx);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return 0;
            }
            if (ensureColumnDecoded(includeIdx)) {
                return Unsafe.getShort(colCacheAddrs[includeIdx] + (long) idx * Short.BYTES);
            }
            return CoveringCompressor.readShortAt(keyBlockAddrs[includeIdx], idx);
        }

        @Override
        public CharSequence getCoveredStrA(int includeIdx) {
            return getVarSidecarStr(includeIdx, stringViewA);
        }

        @Override
        public CharSequence getCoveredStrB(int includeIdx) {
            return getVarSidecarStr(includeIdx, stringViewB);
        }

        @Override
        public Utf8Sequence getCoveredVarcharA(int includeIdx) {
            return getVarSidecarUtf8(includeIdx, varcharViewA);
        }

        @Override
        public Utf8Sequence getCoveredVarcharB(int includeIdx) {
            return getVarSidecarUtf8(includeIdx, varcharViewB);
        }

        @Override
        public boolean isCoveredAvailable(int includeIdx) {
            return includeIdx >= 0 && includeIdx < coverCount
                    && coveredAvailable != null && coveredAvailable[includeIdx];
        }

        @Override
        public long seekToLast() {
            throw new UnsupportedOperationException(
                    "seekToLast: use a backward index reader; forward iteration is O(n)");
        }

        @Override
        public long size() {
            if (requestedKey < 0) {
                return 0;
            }
            // Per-gen classification when the chain entry advertises a tracked
            // coverage: CLEAN gens (max <= entryMaxValue) contribute their count,
            // ALL_DIRTY gens (min > entryMaxValue) contribute zero, MIXED gens
            // force a -1 bail so the caller falls back to the clamped iteration.
            // entryMaxValue == -1 means an empty chain entry where no clamping
            // applies; the original count fast path is taken verbatim.
            long total = 0;
            for (int g = 0; g < cursorGenCount; g++) {
                int gkc = genLookup.getGenKeyCount(g);
                if (gkc >= 0) {
                    if (entryMaxValue >= 0) {
                        long minV = peekDenseKeyMinValue(g, gkc);
                        if (minV < 0) {
                            continue;
                        }
                        if (minV > entryMaxValue) {
                            continue;
                        }
                        long maxV = peekDenseKeyMaxValueUpperBound(g, gkc);
                        if (maxV > entryMaxValue) {
                            return -1;
                        }
                    }
                    total += getDenseGenKeyCount(g, gkc);
                } else if (!genLookup.notContainKey(valueMem, g, requestedKey)) {
                    int activeKeyCount = -gkc;
                    if (entryMaxValue >= 0) {
                        long minV = peekSparseKeyMinValue(g, activeKeyCount);
                        if (minV < 0) {
                            continue;
                        }
                        if (minV > entryMaxValue) {
                            continue;
                        }
                        long maxV = peekSparseKeyMaxValueUpperBound(g, activeKeyCount);
                        if (maxV > entryMaxValue) {
                            return -1;
                        }
                    }
                    total += getSparseGenKeyCount(g);
                }
            }
            return total;
        }

        private CharSequence decompressFsstStr(MemoryMR mem, long blockBase, int count, int ordinal, int includeIdx, DirectString view, boolean longOffsets) {
            if (isFsstChunkUnavailable(mem, blockBase, count, ordinal, includeIdx, longOffsets)) {
                return null;
            }
            int chunkOrdinal = (int) (ordinal - fsstCachedChunkStarts[includeIdx]);
            long offsBase = fsstOffsetsAddrs[includeIdx];
            long lo = Unsafe.getLong(offsBase + (long) chunkOrdinal * Long.BYTES);
            long hi = Unsafe.getLong(offsBase + (long) (chunkOrdinal + 1) * Long.BYTES);
            if (lo == hi) {
                return null;
            }
            long valAddr = fsstDstAddrs[includeIdx] + lo;
            int len = Unsafe.getInt(valAddr);
            if (len < 0) {
                return null;
            }
            return view.of(valAddr + Integer.BYTES, len);
        }

        private Utf8Sequence decompressFsstUtf8(MemoryMR mem, long blockBase, int count, int ordinal, int includeIdx, DirectUtf8String view, boolean longOffsets) {
            if (isFsstChunkUnavailable(mem, blockBase, count, ordinal, includeIdx, longOffsets)) {
                return null;
            }
            int chunkOrdinal = (int) (ordinal - fsstCachedChunkStarts[includeIdx]);
            long offsBase = fsstOffsetsAddrs[includeIdx];
            long lo = Unsafe.getLong(offsBase + (long) chunkOrdinal * Long.BYTES);
            long hi = Unsafe.getLong(offsBase + (long) (chunkOrdinal + 1) * Long.BYTES);
            if (lo == hi) {
                return null;
            }
            // Strip the 1-byte non-null sentinel emitted by writeVarcharValue.
            // See the matching comment there for why VARCHAR (unlike STRING
            // and BINARY) needs an explicit empty-vs-NULL marker.
            long valAddr = fsstDstAddrs[includeIdx] + lo + 1;
            return view.of(valAddr, valAddr + (hi - lo - 1));
        }

        private void ensureFsstCacheCapacity() {
            if (fsstCachedBlockBases == null) {
                fsstCachedBlockBases = new long[coverCount];
                Arrays.fill(fsstCachedBlockBases, -1L);
                fsstCachedChunkStarts = new long[coverCount];
                Arrays.fill(fsstCachedChunkStarts, -1L);
                fsstDecoderAddrs = new long[coverCount];
                fsstDstAddrs = new long[coverCount];
                fsstDstCapacities = new long[coverCount];
                fsstOffsetsAddrs = new long[coverCount];
                fsstOffsetsCapacities = new long[coverCount];
            }
        }

        private long findDenseVarBlockBase(int includeIdx) {
            if (includeIdx >= sidecarMems.size() || sealedGenKeyCount <= 0) {
                return -1;
            }
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return -1;
            }
            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int siSize = PostingIndexUtils.strideIndexSize(sealedGenKeyCount);
            long strideIdxOffset = PostingIndexUtils.PC_HEADER_SIZE + (long) stride * Long.BYTES;
            // Read this stride's offset and the next one. The stride index has
            // strideCount + 1 entries, so reading [stride+1] is in-bounds for
            // any stride < strideCount.
            if (strideIdxOffset + 2 * Long.BYTES > mem.size()) {
                return -1;
            }
            long strideOff = Unsafe.getLong(mem.addressOf(strideIdxOffset));
            long nextStrideOff = Unsafe.getLong(mem.addressOf(strideIdxOffset + Long.BYTES));
            // Empty stride: writer records strideOff[s] == strideOff[s+1] when
            // the stride contributed no bytes. Returning siSize + strideOff
            // would land in the next stride's data — see the patched peer at
            // getDenseGenKeyCount(). Mirrors that guard.
            if (nextStrideOff == strideOff) {
                return -1;
            }
            return siSize + strideOff;
        }

        private void freeFsstCache() {
            if (fsstCachedBlockBases == null) {
                return;
            }
            for (int i = 0, n = fsstCachedBlockBases.length; i < n; i++) {
                if (fsstDecoderAddrs[i] != 0) {
                    Unsafe.free(fsstDecoderAddrs[i], FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_INDEX_READER);
                    fsstDecoderAddrs[i] = 0;
                }
                if (fsstDstAddrs[i] != 0) {
                    Unsafe.free(fsstDstAddrs[i], fsstDstCapacities[i], MemoryTag.NATIVE_INDEX_READER);
                    fsstDstAddrs[i] = 0;
                    fsstDstCapacities[i] = 0;
                }
                if (fsstOffsetsAddrs[i] != 0) {
                    Unsafe.free(fsstOffsetsAddrs[i], fsstOffsetsCapacities[i], MemoryTag.NATIVE_INDEX_READER);
                    fsstOffsetsAddrs[i] = 0;
                    fsstOffsetsCapacities[i] = 0;
                }
                fsstCachedBlockBases[i] = -1;
                fsstCachedChunkStarts[i] = -1;
            }
        }

        private int getDenseGenKeyCount(int gen, int genKeyCount) {
            if (requestedKey >= genKeyCount) {
                return 0;
            }
            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int localKey = requestedKey % PostingIndexUtils.DENSE_STRIDE;
            long genAddr = valueMem.addressOf(genLookup.getGenFileOffset(gen));
            long strideOff = Unsafe.getLong(genAddr + (long) stride * Long.BYTES);
            long nextStrideOff = Unsafe.getLong(genAddr + (long) (stride + 1) * Long.BYTES);
            // Empty stride in this gen: writer records strideOff[s] == strideOff[s+1].
            // Reading on would interpret the next stride's bytes here.
            if (nextStrideOff == strideOff) {
                return 0;
            }
            long strideAddr = genAddr + PostingIndexUtils.strideIndexSize(genKeyCount) + strideOff;
            byte mode = Unsafe.getByte(strideAddr);
            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                int start = Unsafe.getInt(prefixAddr + (long) localKey * Integer.BYTES);
                int end = Unsafe.getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES);
                return end - start;
            }
            if (mode != PostingIndexUtils.STRIDE_MODE_DELTA) {
                throw CairoException.critical(0).put(INDEX_CORRUPT).put(" [bad stride mode=").put(mode).put(']');
            }
            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
            return Unsafe.getInt(countsAddr + (long) localKey * Integer.BYTES);
        }

        private byte getRawSidecarByte(int includeIdx) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return 0;
            }
            long addr = mem.addressOf(currentGenSidecarOffsets.getQuick(includeIdx) + 4 + (long) cachedSidecarIdx * Byte.BYTES);
            return Unsafe.getByte(addr);
        }

        private double getRawSidecarDouble(int includeIdx) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return Double.NaN;
            }
            long addr = mem.addressOf(currentGenSidecarOffsets.getQuick(includeIdx) + 4 + (long) cachedSidecarIdx * Double.BYTES);
            return Unsafe.getDouble(addr);
        }

        private float getRawSidecarFloat(int includeIdx) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return Float.NaN;
            }
            long addr = mem.addressOf(currentGenSidecarOffsets.getQuick(includeIdx) + 4 + (long) cachedSidecarIdx * Float.BYTES);
            return Unsafe.getFloat(addr);
        }

        private int getRawSidecarInt(int includeIdx) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return Integer.MIN_VALUE;
            }
            long addr = mem.addressOf(currentGenSidecarOffsets.getQuick(includeIdx) + 4 + (long) cachedSidecarIdx * Integer.BYTES);
            return Unsafe.getInt(addr);
        }

        private long getRawSidecarLong(int includeIdx) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return Long.MIN_VALUE;
            }
            long addr = mem.addressOf(currentGenSidecarOffsets.getQuick(includeIdx) + 4 + (long) cachedSidecarIdx * Long.BYTES);
            return Unsafe.getLong(addr);
        }

        private long getRawSidecarMultiLong(int includeIdx, int valueSize, int longIndex) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return Long.MIN_VALUE;
            }
            long addr = mem.addressOf(
                    currentGenSidecarOffsets.getQuick(includeIdx) + 4
                            + (long) cachedSidecarIdx * valueSize
                            + (long) longIndex * Long.BYTES
            );
            return Unsafe.getLong(addr);
        }

        private short getRawSidecarShort(int includeIdx) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return 0;
            }
            long addr = mem.addressOf(currentGenSidecarOffsets.getQuick(includeIdx) + 4 + (long) cachedSidecarIdx * Short.BYTES);
            return Unsafe.getShort(addr);
        }

        private int getSparseGenKeyCount(int gen) {
            int minKey = genLookup.getGenMinKey(gen);
            int maxKey = genLookup.getGenMaxKey(gen);
            if (requestedKey < minKey || requestedKey > maxKey) {
                return 0;
            }
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long prefixSumAddr = valueMem.addressOf(genLookup.getGenPrefixSumOffset(gen, valueMem));
            int k = requestedKey - minKey;
            int start = Unsafe.getInt(prefixSumAddr + (long) k * Integer.BYTES);
            int end = Unsafe.getInt(prefixSumAddr + (long) (k + 1) * Integer.BYTES);
            if (start >= end) {
                return 0;
            }
            int activeKeyCount = -genLookup.getGenKeyCount(gen);
            long countsBase = valueMem.addressOf(genFileOffset) + (long) activeKeyCount * Integer.BYTES;
            return Unsafe.getInt(countsBase + (long) start * Integer.BYTES);
        }

        private ArrayView getVarSidecarArray(int includeIdx, int columnType) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return null;
            }
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + cachedSidecarIdx
                    : cachedSidecarIdx;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets.getQuick(includeIdx);
            if (blockBase < 0) {
                return null;
            }
            int rawCount = Unsafe.getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSSTNative.FSST_BLOCK_FLAG) != 0;
            boolean longOffsets = (rawCount & PostingIndexUtils.LONG_OFFSETS_FLAG) != 0;
            int count = rawCount & ~(FSSTNative.FSST_BLOCK_FLAG | PostingIndexUtils.LONG_OFFSETS_FLAG);
            if (ordinal >= count) {
                return null;
            }

            long dataAddr;
            int dataLen;
            if (fsst) {
                if (isFsstChunkUnavailable(mem, blockBase, count, ordinal, includeIdx, longOffsets)) {
                    return null;
                }
                int chunkOrdinal = (int) (ordinal - fsstCachedChunkStarts[includeIdx]);
                long offsBase = fsstOffsetsAddrs[includeIdx];
                long lo = Unsafe.getLong(offsBase + (long) chunkOrdinal * Long.BYTES);
                long hi = Unsafe.getLong(offsBase + (long) (chunkOrdinal + 1) * Long.BYTES);
                if (lo == hi) {
                    arrayView.ofNull();
                    return arrayView;
                }
                dataAddr = fsstDstAddrs[includeIdx] + lo;
                dataLen = (int) (hi - lo);
            } else {
                long offsetsAddr = mem.addressOf(blockBase + 4);
                long lo = readVarBlockOffset(offsetsAddr, ordinal, longOffsets);
                long hi = readVarBlockOffset(offsetsAddr, ordinal + 1, longOffsets);
                if (lo == hi) {
                    arrayView.ofNull();
                    return arrayView;
                }
                long dataBase = blockBase + 4 + varBlockOffsetsSize(count, longOffsets);
                dataAddr = mem.addressOf(dataBase + lo);
                // A single var value is always well under 2 GB
                dataLen = (int) (hi - lo);
            }
            int dims = ColumnType.decodeArrayDimensionality(columnType);
            short elemType = ColumnType.decodeArrayElementType(columnType);
            int elemSize = ColumnType.sizeOf(elemType);
            int cardinality = 1;
            for (int d = 0; d < dims; d++) {
                cardinality *= Unsafe.getInt(dataAddr + (long) d * Integer.BYTES);
            }
            int valueSize = cardinality * elemSize;
            long valuePtr = dataAddr + dataLen - valueSize;
            return arrayView.of(columnType, dataAddr, valuePtr, valueSize);
        }

        private BinarySequence getVarSidecarBin(int includeIdx) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return null;
            }
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + cachedSidecarIdx
                    : cachedSidecarIdx;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets.getQuick(includeIdx);
            if (blockBase < 0) {
                return null;
            }
            int rawCount = Unsafe.getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSSTNative.FSST_BLOCK_FLAG) != 0;
            boolean longOffsets = (rawCount & PostingIndexUtils.LONG_OFFSETS_FLAG) != 0;
            int count = rawCount & ~(FSSTNative.FSST_BLOCK_FLAG | PostingIndexUtils.LONG_OFFSETS_FLAG);
            if (ordinal >= count) {
                return null;
            }

            if (fsst) {
                if (isFsstChunkUnavailable(mem, blockBase, count, ordinal, includeIdx, longOffsets)) {
                    return null;
                }
                int chunkOrdinal = (int) (ordinal - fsstCachedChunkStarts[includeIdx]);
                long offsBase = fsstOffsetsAddrs[includeIdx];
                long lo = Unsafe.getLong(offsBase + (long) chunkOrdinal * Long.BYTES);
                long hi = Unsafe.getLong(offsBase + (long) (chunkOrdinal + 1) * Long.BYTES);
                if (lo == hi) {
                    return null;
                }
                long valAddr = fsstDstAddrs[includeIdx] + lo;
                long len = Unsafe.getLong(valAddr);
                if (len < 0) {
                    return null;
                }
                return binView.of(valAddr + Long.BYTES, len);
            }

            long offsetsAddr = mem.addressOf(blockBase + 4);
            long lo = readVarBlockOffset(offsetsAddr, ordinal, longOffsets);
            long hi = readVarBlockOffset(offsetsAddr, ordinal + 1, longOffsets);
            if (lo == hi) {
                return null;
            }
            long dataBase = blockBase + 4 + varBlockOffsetsSize(count, longOffsets);
            long dataAddr = mem.addressOf(dataBase + lo);
            long len = Unsafe.getLong(dataAddr);
            if (len < 0) {
                return null;
            }
            return binView.of(dataAddr + Long.BYTES, len);
        }

        private long getVarSidecarBinLen(int includeIdx) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return -1;
            }
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + cachedSidecarIdx
                    : cachedSidecarIdx;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets.getQuick(includeIdx);
            if (blockBase < 0) {
                return -1;
            }
            int rawCount = Unsafe.getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSSTNative.FSST_BLOCK_FLAG) != 0;
            boolean longOffsets = (rawCount & PostingIndexUtils.LONG_OFFSETS_FLAG) != 0;
            int count = rawCount & ~(FSSTNative.FSST_BLOCK_FLAG | PostingIndexUtils.LONG_OFFSETS_FLAG);
            if (ordinal >= count) {
                return -1;
            }

            if (fsst) {
                if (isFsstChunkUnavailable(mem, blockBase, count, ordinal, includeIdx, longOffsets)) {
                    return -1;
                }
                int chunkOrdinal = (int) (ordinal - fsstCachedChunkStarts[includeIdx]);
                long offsBase = fsstOffsetsAddrs[includeIdx];
                long lo = Unsafe.getLong(offsBase + (long) chunkOrdinal * Long.BYTES);
                long hi = Unsafe.getLong(offsBase + (long) (chunkOrdinal + 1) * Long.BYTES);
                if (lo == hi) {
                    return -1;
                }
                return Unsafe.getLong(fsstDstAddrs[includeIdx] + lo);
            }

            long offsetsAddr = mem.addressOf(blockBase + 4);
            long lo = readVarBlockOffset(offsetsAddr, ordinal, longOffsets);
            long hi = readVarBlockOffset(offsetsAddr, ordinal + 1, longOffsets);
            if (lo == hi) {
                return -1;
            }
            long dataBase = blockBase + 4 + varBlockOffsetsSize(count, longOffsets);
            long dataAddr = mem.addressOf(dataBase + lo);
            return Unsafe.getLong(dataAddr);
        }

        private CharSequence getVarSidecarStr(int includeIdx, DirectString view) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return null;
            }
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + cachedSidecarIdx
                    : cachedSidecarIdx;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets.getQuick(includeIdx);
            if (blockBase < 0) {
                return null;
            }
            int rawCount = Unsafe.getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSSTNative.FSST_BLOCK_FLAG) != 0;
            boolean longOffsets = (rawCount & PostingIndexUtils.LONG_OFFSETS_FLAG) != 0;
            int count = rawCount & ~(FSSTNative.FSST_BLOCK_FLAG | PostingIndexUtils.LONG_OFFSETS_FLAG);
            if (ordinal >= count) {
                return null;
            }

            if (fsst) {
                return decompressFsstStr(mem, blockBase, count, ordinal, includeIdx, view, longOffsets);
            }

            long offsetsAddr = mem.addressOf(blockBase + 4);
            long lo = readVarBlockOffset(offsetsAddr, ordinal, longOffsets);
            long hi = readVarBlockOffset(offsetsAddr, ordinal + 1, longOffsets);
            if (lo == hi) {
                return null;
            }
            long dataBase = blockBase + 4 + varBlockOffsetsSize(count, longOffsets);
            long dataAddr = mem.addressOf(dataBase + lo);
            int len = Unsafe.getInt(dataAddr);
            if (len < 0) {
                return null;
            }
            return view.of(dataAddr + Integer.BYTES, len);
        }

        private Utf8Sequence getVarSidecarUtf8(int includeIdx, DirectUtf8String view) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return null;
            }
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + cachedSidecarIdx
                    : cachedSidecarIdx;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets.getQuick(includeIdx);
            if (blockBase < 0) {
                return null;
            }
            int rawCount = Unsafe.getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSSTNative.FSST_BLOCK_FLAG) != 0;
            boolean longOffsets = (rawCount & PostingIndexUtils.LONG_OFFSETS_FLAG) != 0;
            int count = rawCount & ~(FSSTNative.FSST_BLOCK_FLAG | PostingIndexUtils.LONG_OFFSETS_FLAG);
            if (ordinal >= count) {
                return null;
            }

            if (fsst) {
                return decompressFsstUtf8(mem, blockBase, count, ordinal, includeIdx, view, longOffsets);
            }

            long offsetsAddr = mem.addressOf(blockBase + 4);
            long lo = readVarBlockOffset(offsetsAddr, ordinal, longOffsets);
            long hi = readVarBlockOffset(offsetsAddr, ordinal + 1, longOffsets);
            if (lo == hi) {
                return null;
            }
            long dataBase = blockBase + 4 + varBlockOffsetsSize(count, longOffsets);
            // Strip the 1-byte non-null sentinel emitted by writeVarcharValue
            // (see comment there). VARCHAR has no length prefix, so we need
            // an explicit marker to tell empty apart from NULL on read.
            long dataAddr = mem.addressOf(dataBase + lo + 1);
            return view.of(dataAddr, dataAddr + (hi - lo - 1));
        }

        /**
         * Returns true if the FSST chunk covering {@code ordinal} cannot
         * be decoded -- the block's table won't import, the chunk's
         * decompression failed, or the decoder is in a corrupted state.
         * Otherwise leaves the chunk's bytes in {@code fsstDstAddrs[includeIdx]}
         * and chunk-relative offsets in {@code fsstOffsetsAddrs[includeIdx]}.
         * Callers index using {@code (ordinal - fsstCachedChunkStarts[includeIdx])}.
         * <p>
         * Decoding is chunked at {@link #FSST_DECODE_CHUNK_SIZE} values. The
         * symbol-table import happens once per block (cached via
         * {@code fsstCachedBlockBases}); chunk decode happens per FSST chunk
         * (cached via {@code fsstCachedChunkStarts}). Anonymous-heap scratch
         * is bounded to a chunk's worth of decoded bytes plus
         * {@code (chunkSize + 1) * 8} for the offset table -- a few tens of
         * KiB in typical workloads, never the multi-GiB the whole-block
         * decode required.
         */
        private boolean isFsstChunkUnavailable(MemoryMR mem, long blockBase, int count, int ordinal, int includeIdx, boolean longOffsets) {
            ensureFsstCacheCapacity();
            // Compute the chunk that owns this ordinal. CHUNK_SIZE is a
            // power of two so the divide and modulo both compile to shifts.
            final int chunkStart = (ordinal / FSST_DECODE_CHUNK_SIZE) * FSST_DECODE_CHUNK_SIZE;

            // Fast path: same block and same chunk as last access.
            if (fsstCachedBlockBases[includeIdx] == blockBase
                    && fsstCachedChunkStarts[includeIdx] == chunkStart) {
                return false;
            }

            long pos = blockBase + 4;
            int tableLen = Unsafe.getShort(mem.addressOf(pos)) & 0xFFFF;
            long tableAddr = mem.addressOf(pos + 2);
            long blockOffsetsAddr = mem.addressOf(pos + 2 + tableLen);
            long offsetsTableSize = varBlockOffsetsSize(count, longOffsets);
            long dataBase = pos + 2 + tableLen + offsetsTableSize;
            int srcOffsetsWidth = longOffsets ? Long.BYTES : Integer.BYTES;

            // Re-import the symbol table only when the block changed. The
            // table is small (<= FSST_MAXHEADER, ~2 KiB) and reusable across
            // every chunk inside this block.
            if (fsstCachedBlockBases[includeIdx] != blockBase) {
                long decoderAddr = fsstDecoderAddrs[includeIdx];
                if (decoderAddr == 0) {
                    decoderAddr = Unsafe.malloc(FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_INDEX_READER);
                    fsstDecoderAddrs[includeIdx] = decoderAddr;
                }
                if (FSSTNative.importTable(decoderAddr, tableAddr) < 0) {
                    fsstCachedBlockBases[includeIdx] = -1;
                    fsstCachedChunkStarts[includeIdx] = -1;
                    return true;
                }
                fsstCachedBlockBases[includeIdx] = blockBase;
                // Force chunk decode below; the new block invalidates any
                // chunk position cached from the previous block.
                fsstCachedChunkStarts[includeIdx] = -1;
            }

            // chunkCount is the number of values in this chunk. The last
            // chunk is short when count is not a multiple of CHUNK_SIZE.
            final int chunkCount = Math.min(FSST_DECODE_CHUNK_SIZE, count - chunkStart);

            // Offsets buffer for this chunk: (chunkCount + 1) longs.
            // Bounded to (CHUNK_SIZE + 1) * 8 = ~2 KiB.
            long offsetsBytes = (long) (chunkCount + 1) * Long.BYTES;
            if (fsstOffsetsCapacities[includeIdx] < offsetsBytes) {
                fsstOffsetsAddrs[includeIdx] = Unsafe.realloc(
                        fsstOffsetsAddrs[includeIdx], fsstOffsetsCapacities[includeIdx],
                        offsetsBytes, MemoryTag.NATIVE_INDEX_READER);
                fsstOffsetsCapacities[includeIdx] = offsetsBytes;
            }

            // Estimate the chunk's decoded size from this chunk's compressed
            // span (chunk-local, not block-wide). Pointer-shift the source
            // offsets so the JNI sees the chunk's slice as a self-contained
            // block; src*Addr stays at the absolute compressed-bytes base so
            // fsst_decompress reads from the right file position.
            long chunkOffsetsAddr = blockOffsetsAddr + (long) chunkStart * srcOffsetsWidth;
            long compressedChunkStart = readVarBlockOffset(blockOffsetsAddr, chunkStart, longOffsets);
            long compressedChunkEnd = readVarBlockOffset(blockOffsetsAddr, chunkStart + chunkCount, longOffsets);
            long compressedChunkLen = compressedChunkEnd - compressedChunkStart;
            // FSST worst-case expansion is 8x; pad up so the first attempt
            // usually succeeds without a realloc-retry round-trip. Floor at
            // 256 bytes for very short chunks.
            long initialDstCap = Math.max(compressedChunkLen * 8L, 256L);
            if (fsstDstCapacities[includeIdx] < initialDstCap) {
                fsstDstAddrs[includeIdx] = Unsafe.realloc(
                        fsstDstAddrs[includeIdx], fsstDstCapacities[includeIdx],
                        initialDstCap, MemoryTag.NATIVE_INDEX_READER);
                fsstDstCapacities[includeIdx] = initialDstCap;
            }

            while (true) {
                long decoded = FSSTNative.decompressBlock(
                        fsstDecoderAddrs[includeIdx],
                        mem.addressOf(dataBase), chunkOffsetsAddr, srcOffsetsWidth, chunkCount,
                        fsstDstAddrs[includeIdx], fsstDstCapacities[includeIdx],
                        fsstOffsetsAddrs[includeIdx]
                );
                if (decoded >= 0) {
                    break;
                }
                long newCap = fsstDstCapacities[includeIdx] * 2L;
                fsstDstAddrs[includeIdx] = Unsafe.realloc(
                        fsstDstAddrs[includeIdx], fsstDstCapacities[includeIdx],
                        newCap, MemoryTag.NATIVE_INDEX_READER);
                fsstDstCapacities[includeIdx] = newCap;
            }

            fsstCachedChunkStarts[includeIdx] = chunkStart;
            return false;
        }

        private long peekDenseKeyMaxValueUpperBound(int gen, int genKeyCount) {
            if (requestedKey >= genKeyCount) {
                return -1L;
            }
            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int localKey = requestedKey % PostingIndexUtils.DENSE_STRIDE;
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long genAddr = valueMem.addressOf(genFileOffset);
            long strideOff = Unsafe.getLong(genAddr + (long) stride * Long.BYTES);
            long nextStrideOff = Unsafe.getLong(genAddr + (long) (stride + 1) * Long.BYTES);
            if (nextStrideOff == strideOff) {
                return -1L;
            }
            int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
            long strideAddr = genAddr + siSize + strideOff;
            long strideFileOffset = genFileOffset + siSize + strideOff;
            byte mode = Unsafe.getByte(strideAddr);
            int ks = PostingIndexUtils.keysInStride(genKeyCount, stride);
            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                int bitWidth = Unsafe.getByte(strideAddr + 1) & 0xFF;
                long baseValue = Unsafe.getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                int start = Unsafe.getInt(prefixAddr + (long) localKey * Integer.BYTES);
                int end = Unsafe.getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES);
                if (end == start) {
                    return -1L;
                }
                if (bitWidth == 0) {
                    return baseValue;
                }
                long dataAddr = strideAddr + PostingIndexUtils.strideFlatHeaderSize(ks);
                return BitpackUtils.unpackValue(dataAddr, end - 1, bitWidth, baseValue);
            }
            if (mode != PostingIndexUtils.STRIDE_MODE_DELTA) {
                throw CairoException.critical(0).put(INDEX_CORRUPT).put(" [bad stride mode=").put(mode).put(']');
            }
            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
            int totalCount = Unsafe.getInt(countsAddr + (long) localKey * Integer.BYTES);
            if (totalCount == 0) {
                return -1L;
            }
            long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
            long dataOffset = Unsafe.getLong(offsetsBase + (long) localKey * Long.BYTES);
            long encodedOffset = strideFileOffset + PostingIndexUtils.strideDeltaHeaderSize(ks) + dataOffset;
            long baseAddr = valueMem.addressOf(0);
            return peekDeltaKeyMaxValueUpperBound(baseAddr, encodedOffset);
        }

        private long peekDenseKeyMinValue(int gen, int genKeyCount) {
            if (requestedKey >= genKeyCount) {
                return -1L;
            }
            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int localKey = requestedKey % PostingIndexUtils.DENSE_STRIDE;
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long genAddr = valueMem.addressOf(genFileOffset);
            long strideOff = Unsafe.getLong(genAddr + (long) stride * Long.BYTES);
            long nextStrideOff = Unsafe.getLong(genAddr + (long) (stride + 1) * Long.BYTES);
            if (nextStrideOff == strideOff) {
                return -1L;
            }
            int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
            long strideAddr = genAddr + siSize + strideOff;
            long strideFileOffset = genFileOffset + siSize + strideOff;
            byte mode = Unsafe.getByte(strideAddr);
            int ks = PostingIndexUtils.keysInStride(genKeyCount, stride);
            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                int bitWidth = Unsafe.getByte(strideAddr + 1) & 0xFF;
                long baseValue = Unsafe.getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                int start = Unsafe.getInt(prefixAddr + (long) localKey * Integer.BYTES);
                int end = Unsafe.getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES);
                if (end == start) {
                    return -1L;
                }
                if (bitWidth == 0) {
                    return baseValue;
                }
                long dataAddr = strideAddr + PostingIndexUtils.strideFlatHeaderSize(ks);
                return BitpackUtils.unpackValue(dataAddr, start, bitWidth, baseValue);
            }
            if (mode != PostingIndexUtils.STRIDE_MODE_DELTA) {
                throw CairoException.critical(0).put(INDEX_CORRUPT).put(" [bad stride mode=").put(mode).put(']');
            }
            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
            int totalCount = Unsafe.getInt(countsAddr + (long) localKey * Integer.BYTES);
            if (totalCount == 0) {
                return -1L;
            }
            long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
            long dataOffset = Unsafe.getLong(offsetsBase + (long) localKey * Long.BYTES);
            long encodedOffset = strideFileOffset + PostingIndexUtils.strideDeltaHeaderSize(ks) + dataOffset;
            long baseAddr = valueMem.addressOf(0);
            return peekDeltaKeyMinValue(baseAddr, encodedOffset);
        }

        private long peekSparseKeyMaxValueUpperBound(int gen, int activeKeyCount) {
            int minKey = genLookup.getGenMinKey(gen);
            int maxKey = genLookup.getGenMaxKey(gen);
            if (requestedKey < minKey || requestedKey > maxKey) {
                return -1L;
            }
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long prefixSumAddr = valueMem.addressOf(genLookup.getGenPrefixSumOffset(gen, valueMem));
            int k = requestedKey - minKey;
            int start = Unsafe.getInt(prefixSumAddr + (long) k * Integer.BYTES);
            int end = Unsafe.getInt(prefixSumAddr + (long) (k + 1) * Integer.BYTES);
            if (start >= end) {
                return -1L;
            }
            long genAddr = valueMem.addressOf(genFileOffset);
            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            int totalCount = Unsafe.getInt(countsBase + (long) start * Integer.BYTES);
            if (totalCount == 0) {
                return -1L;
            }
            long dataOffset = Unsafe.getLong(offsetsBase + (long) start * Long.BYTES);
            long encodedOffset = genFileOffset + PostingIndexUtils.genHeaderSizeSparse(activeKeyCount) + dataOffset;
            long baseAddr = valueMem.addressOf(0);
            return peekDeltaKeyMaxValueUpperBound(baseAddr, encodedOffset);
        }

        private long peekSparseKeyMinValue(int gen, int activeKeyCount) {
            int minKey = genLookup.getGenMinKey(gen);
            int maxKey = genLookup.getGenMaxKey(gen);
            if (requestedKey < minKey || requestedKey > maxKey) {
                return -1L;
            }
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long prefixSumAddr = valueMem.addressOf(genLookup.getGenPrefixSumOffset(gen, valueMem));
            int k = requestedKey - minKey;
            int start = Unsafe.getInt(prefixSumAddr + (long) k * Integer.BYTES);
            int end = Unsafe.getInt(prefixSumAddr + (long) (k + 1) * Integer.BYTES);
            if (start >= end) {
                return -1L;
            }
            long genAddr = valueMem.addressOf(genFileOffset);
            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            int totalCount = Unsafe.getInt(countsBase + (long) start * Integer.BYTES);
            if (totalCount == 0) {
                return -1L;
            }
            long dataOffset = Unsafe.getLong(offsetsBase + (long) start * Long.BYTES);
            long encodedOffset = genFileOffset + PostingIndexUtils.genHeaderSizeSparse(activeKeyCount) + dataOffset;
            long baseAddr = valueMem.addressOf(0);
            return peekDeltaKeyMinValue(baseAddr, encodedOffset);
        }

        protected void cacheSidecarKeyAddrs(int stride, int localKey) {
            if (coverCount == 0 || sealedGenKeyCount <= 0) {
                return;
            }
            if (stride == cachedKeyBlockStride) {
                return; // already cached for this stride
            }
            int sc = PostingIndexUtils.strideCount(sealedGenKeyCount);
            if (stride >= sc) {
                return;
            }
            int siSize = PostingIndexUtils.strideIndexSize(sealedGenKeyCount);
            int ks = PostingIndexUtils.keysInStride(sealedGenKeyCount, stride);
            if (localKey >= ks) {
                return;
            }

            if (keyBlockAddrs == null || keyBlockAddrs.length < coverCount) {
                keyBlockAddrs = new long[coverCount];
            }

            for (int c = 0; c < coverCount; c++) {
                MemoryMR mem = sidecarMems.getQuick(c);
                if (mem.size() == 0) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                long strideIdxOffset = PostingIndexUtils.PC_HEADER_SIZE + (long) stride * Long.BYTES;
                // Need both this stride's offset and the next one for the
                // empty-stride guard. The stride index has strideCount + 1
                // entries so reading [stride+1] is in-bounds.
                if (strideIdxOffset + 2 * Long.BYTES > mem.size()) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                long strideOff = mem.getLong(strideIdxOffset);
                long nextStrideOff = mem.getLong(strideIdxOffset + Long.BYTES);
                // Empty stride: writer records strideOff[s] == strideOff[s+1]
                // when the stride contributed no bytes. Continuing would
                // interpret the next stride's keyOffsets as ours and yield
                // garbage keyBlockStart pointers. Mirrors the patched peer at
                // PostingIndexFwdReader/BwdReader.
                if (nextStrideOff == strideOff) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                long strideDataStart = siSize + strideOff;
                if (strideDataStart >= mem.size()) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                long keyOffsetsEnd = strideDataStart + (long) ks * Long.BYTES;
                if (keyOffsetsEnd > mem.size()) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                long keyOffsetsAddr = mem.addressOf(strideDataStart);
                long keyBlockOff = Unsafe.getLong(keyOffsetsAddr + (long) localKey * Long.BYTES);
                long keyBlockStart = keyOffsetsEnd + keyBlockOff;
                if (keyBlockStart + 4 > mem.size()) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                keyBlockAddrs[c] = mem.addressOf(keyBlockStart);
            }
            cachedKeyBlockStride = stride;
        }

        protected void closeCoveringResources() {
            if (colCacheAddrs != null) {
                for (int i = 0; i < colCacheAddrs.length; i++) {
                    if (colCacheAddrs[i] != 0) {
                        Unsafe.free(colCacheAddrs[i], colCacheCapacities[i], MemoryTag.NATIVE_INDEX_READER);
                        colCacheAddrs[i] = 0;
                        colCacheCapacities[i] = 0;
                    }
                    colCacheBlockAddrs[i] = 0;
                }
            }
            if (decodeWorkspaceAddr != 0) {
                Unsafe.free(decodeWorkspaceAddr, (long) decodeWorkspaceCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                decodeWorkspaceAddr = 0;
                decodeWorkspaceCapacity = 0;
            }
            freeFsstCache();
        }

        protected void computePerColumnSidecarOffsets(int gen) {
            if (coverCount == 0) {
                return;
            }
            currentGenSidecarOffsets.setPos(coverCount);
            // For dense gens the header slot is 0 (never written, mmap zero-filled).
            // Dense reads go through stride_index (findDenseVarBlockBase), so the
            // 0 here is harmless — sparse reads are the only consumer of this cache.
            for (int c = 0; c < coverCount; c++) {
                MemoryMR mem = sidecarMems.getQuick(c);
                currentGenSidecarOffsets.setQuick(c, mem.size() == 0 ? 0L : mem.getLong((long) gen * Long.BYTES));
            }
        }

        /**
         * Lazy per-column decode cache. Returns true when the column's block
         * has been bulk-decoded into colCacheAddrs[includeIdx]. On the first
         * access to a new block the method returns false (caller should use
         * point read). On the second access to the same block it bulk-decodes
         * and returns true. Scan cursors auto-promote after one point read;
         * point queries never pay the bulk-decode cost.
         */
        protected boolean ensureColumnDecoded(int includeIdx) {
            if (colCacheBlockAddrs == null) {
                colCacheAddrs = new long[coverCount];
                colCacheBlockAddrs = new long[coverCount];
                colCacheCapacities = new int[coverCount];
                colPointBlockAddrs = new long[coverCount];
            }
            long blockAddr = keyBlockAddrs[includeIdx];
            if (blockAddr == 0) {
                return false;
            }
            if (colCacheBlockAddrs[includeIdx] == blockAddr) {
                return true;
            }
            if (colPointBlockAddrs[includeIdx] != blockAddr) {
                colPointBlockAddrs[includeIdx] = blockAddr;
                return false;
            }
            int rawCount = Unsafe.getInt(blockAddr);
            int count = rawCount & ~CoveringCompressor.RAW_BLOCK_FLAG;
            if (count == 0) {
                colCacheBlockAddrs[includeIdx] = blockAddr;
                return true;
            }
            int colType = sidecarColumnTypes.getQuick(includeIdx);
            int elemSize = ColumnType.sizeOf(colType);
            int needed = count * elemSize;
            if (needed > colCacheCapacities[includeIdx]) {
                colCacheAddrs[includeIdx] = Unsafe.realloc(
                        colCacheAddrs[includeIdx], colCacheCapacities[includeIdx],
                        needed, MemoryTag.NATIVE_INDEX_READER);
                colCacheCapacities[includeIdx] = needed;
            }
            ensureDecodeWorkspaceCapacity(count);
            switch (ColumnType.tagOf(colType)) {
                case ColumnType.DOUBLE ->
                        CoveringCompressor.decompressDoublesToAddr(blockAddr, colCacheAddrs[includeIdx], decodeWorkspaceAddr);
                case ColumnType.FLOAT ->
                        CoveringCompressor.decompressFloatsToAddr(blockAddr, colCacheAddrs[includeIdx], decodeWorkspaceAddr);
                case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.GEOLONG, ColumnType.DECIMAL64 ->
                        CoveringCompressor.decompressLongsToAddr(blockAddr, colCacheAddrs[includeIdx], decodeWorkspaceAddr);
                case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT, ColumnType.SYMBOL, ColumnType.DECIMAL32 ->
                        CoveringCompressor.decompressIntsToAddr(blockAddr, colCacheAddrs[includeIdx], decodeWorkspaceAddr);
                case ColumnType.SHORT, ColumnType.CHAR, ColumnType.GEOSHORT, ColumnType.DECIMAL16 ->
                        CoveringCompressor.decompressShortsToAddr(blockAddr, colCacheAddrs[includeIdx], decodeWorkspaceAddr);
                case ColumnType.BYTE, ColumnType.BOOLEAN, ColumnType.GEOBYTE, ColumnType.DECIMAL8 ->
                        CoveringCompressor.decompressBytesToAddr(blockAddr, colCacheAddrs[includeIdx], decodeWorkspaceAddr);
                case ColumnType.LONG128, ColumnType.UUID, ColumnType.DECIMAL128, ColumnType.LONG256,
                     ColumnType.DECIMAL256 ->
                        Unsafe.copyMemory(blockAddr + 4, colCacheAddrs[includeIdx], (long) count * elemSize);
            }
            colCacheBlockAddrs[includeIdx] = blockAddr;
            return true;
        }

        protected void ensureDecodeWorkspaceCapacity(int count) {
            if (count > decodeWorkspaceCapacity) {
                decodeWorkspaceAddr = Unsafe.realloc(
                        decodeWorkspaceAddr,
                        (long) decodeWorkspaceCapacity * Long.BYTES,
                        (long) count * Long.BYTES,
                        MemoryTag.NATIVE_INDEX_READER
                );
                decodeWorkspaceCapacity = count;
            }
        }

        protected void resetCoveringState() {
            this.sidecarOrdinal = 0;
            this.sidecarStrideKeyStart = 0;
            this.cachedKeyBlockStride = -1;
            this.cachedSidecarIdx = 0;
            this.isCurrentGenDense = false;
            if (colCacheBlockAddrs != null) {
                for (int i = 0; i < colCacheBlockAddrs.length; i++) {
                    colCacheBlockAddrs[i] = 0;
                    colPointBlockAddrs[i] = 0;
                }
            }
        }
    }
}
