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

package io.questdb.cairo.lv;

import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Mutable runtime state of a live view, persisted in {@code _lv.s}.
 * <p>
 * Mirrors {@link io.questdb.cairo.mv.MatViewState}'s file shape: BlockFile with
 * typed blocks, rewritten on every state change, durable across restarts.
 * <p>
 * The state file holds CORE_STATE. Every view CREATEs in {@code SEEDING} with the
 * base sequencer head as {@code seedTargetSeqTxn}: the seed sweep is what materialises
 * the rows already in the base that satisfy the view's START FROM boundary, whichever
 * boundary that is. The sweep flips {@code seedState} to {@code ACTIVE} and wipes
 * {@code seedTargetSeqTxn} back to {@code Numbers.LONG_NULL} on completion, so those
 * two values are also the resting state of a view that has finished seeding.
 */
public class LiveViewState {
    public static final String LIVE_VIEW_STATE_FILE_NAME = "_lv.s";
    public static final int LIVE_VIEW_STATE_CORE_MSG_TYPE = 0;
    // Format version stamped as the first field of the CORE_STATE block. A reader
    // that finds a higher value refuses to load the view and surfaces it as
    // version_unsupported. Bump when the CORE_STATE layout changes incompatibly.
    public static final int LIVE_VIEW_STATE_FORMAT_VERSION = 1;

    public static final byte SEED_STATE_ACTIVE = 0;
    public static final byte SEED_STATE_SEEDING = 1;

    /**
     * Writes the CORE_STATE block from a {@link LiveViewStateReader} snapshot, or
     * default values when {@code reader} is null (used at CREATE).
     */
    public static void append(@Nullable LiveViewStateReader reader, @NotNull BlockFileWriter writer) {
        if (reader != null) {
            append(
                    reader.isInvalid(),
                    reader.getInvalidationReason(),
                    reader.getInvalidationTimestampUs(),
                    reader.getSubscribeFromSeqTxn(),
                    reader.getLastProcessedSeqTxn(),
                    reader.getAppliedWatermark(),
                    reader.getLvConsumedSeqTxn(),
                    reader.getSeedState(),
                    reader.getSeedTargetSeqTxn(),
                    writer
            );
        } else {
            append(
                    false,
                    null,
                    Numbers.LONG_NULL,
                    -1L,
                    -1L,
                    -1L,
                    -1L,
                    SEED_STATE_ACTIVE,
                    Numbers.LONG_NULL,
                    writer
            );
        }
    }

    public static void append(
            boolean invalid,
            @Nullable CharSequence invalidationReason,
            long invalidationTimestampUs,
            long subscribeFromSeqTxn,
            long lastProcessedSeqTxn,
            long appliedWatermark,
            long lvConsumedSeqTxn,
            byte seedState,
            long seedTargetSeqTxn,
            @NotNull BlockFileWriter writer
    ) {
        AppendableBlock block = writer.append();
        block.putInt(LIVE_VIEW_STATE_FORMAT_VERSION);
        block.putBool(invalid);
        block.putStr(invalidationReason);
        block.putLong(invalidationTimestampUs);
        block.putLong(subscribeFromSeqTxn);
        block.putLong(lastProcessedSeqTxn);
        block.putLong(appliedWatermark);
        block.putLong(lvConsumedSeqTxn);
        block.putByte(seedState);
        block.putLong(seedTargetSeqTxn);
        block.commit(LIVE_VIEW_STATE_CORE_MSG_TYPE);
        writer.commit();
    }
}
