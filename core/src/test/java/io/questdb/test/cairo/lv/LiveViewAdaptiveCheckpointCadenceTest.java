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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;
import org.junit.Assert;
import org.junit.Test;

public class LiveViewAdaptiveCheckpointCadenceTest {

    @Test
    public void testCadenceTightensImmediatelyAndRelaxesGradually() {
        final LiveViewInstance instance = new LiveViewInstance(null, null, 1, false, -1);
        final long configured = 60 * Micros.MINUTE_MICROS;

        Assert.assertEquals(Numbers.LONG_NULL, instance.getAdaptiveCheckpointDurationMicros());
        Assert.assertEquals(configured, instance.getEffectiveCheckpointDurationMicros(configured));

        instance.recordAdaptiveCheckpointCorrection(60 * Micros.MINUTE_MICROS, configured, Micros.SECOND_MICROS);
        Assert.assertEquals(30 * Micros.MINUTE_MICROS, instance.getAdaptiveCheckpointDurationMicros());

        instance.recordAdaptiveCheckpointCorrection(10 * Micros.MINUTE_MICROS, configured, Micros.SECOND_MICROS);
        Assert.assertEquals(5 * Micros.MINUTE_MICROS, instance.getAdaptiveCheckpointDurationMicros());

        instance.recordAdaptiveCheckpointCorrection(40 * Micros.MINUTE_MICROS, configured, Micros.SECOND_MICROS);
        Assert.assertEquals(375 * Micros.SECOND_MICROS, instance.getAdaptiveCheckpointDurationMicros());
        Assert.assertEquals(3, instance.getAdaptiveCheckpointCorrectionCount());
        Assert.assertEquals(40 * Micros.MINUTE_MICROS, instance.getAdaptiveCheckpointLastCorrectionDepthMicros());
    }

    @Test
    public void testFlushIntervalFloorsLearnedCadence() {
        final LiveViewInstance instance = new LiveViewInstance(null, null, 1, false, -1);
        instance.recordAdaptiveCheckpointCorrection(
                Micros.SECOND_MICROS,
                60 * Micros.MINUTE_MICROS,
                5 * Micros.SECOND_MICROS
        );
        Assert.assertEquals(5 * Micros.SECOND_MICROS, instance.getAdaptiveCheckpointDurationMicros());

        instance.recordAdaptiveCheckpointCorrection(0, 60 * Micros.MINUTE_MICROS, Micros.SECOND_MICROS);
        Assert.assertEquals(1, instance.getAdaptiveCheckpointCorrectionCount());
    }

    @Test
    public void testRestoredHeadDoesNotRelaxCadence() {
        final LiveViewInstance instance = new LiveViewInstance(null, null, 1, false, -1);
        final long configured = 5 * Micros.MINUTE_MICROS;

        instance.recordAdaptiveCheckpointCorrection(2 * Micros.SECOND_MICROS, configured, Micros.SECOND_MICROS);
        Assert.assertEquals(Micros.SECOND_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));

        // writtenUs == LONG_NULL marks a head/seed this process restored rather than
        // sealed. It cost no seal, so it must not pay down the learned cadence.
        instance.setHeadCheckpoint(7, 7, 700, 0, Numbers.LONG_NULL);
        instance.recordSeedCheckpointWritten(70, 700, Numbers.LONG_NULL);
        Assert.assertEquals(Micros.SECOND_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));

        // A restored head must not consume the correction's exemption either: the
        // first seal this process writes is still the correction's own.
        instance.setHeadCheckpoint(8, 8, 800, 0, 8_000L);
        Assert.assertEquals(Micros.SECOND_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));
        instance.setHeadCheckpoint(9, 9, 900, 0, 9_000L);
        Assert.assertEquals(1_250_000L, instance.getEffectiveCheckpointDurationMicros(configured));
    }

    @Test
    public void testSeedSealsRelaxCadenceTowardsCeiling() {
        final LiveViewInstance instance = new LiveViewInstance(null, null, 1, false, -1);
        final long configured = 5 * Micros.MINUTE_MICROS;

        instance.recordAdaptiveCheckpointCorrection(2 * Micros.SECOND_MICROS, configured, Micros.SECOND_MICROS);
        Assert.assertEquals(Micros.SECOND_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));

        // The seal the correction's own repair forces is exempt.
        instance.recordSeedCheckpointWritten(10, 100, 1_000L);
        Assert.assertEquals(Micros.SECOND_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));

        instance.recordSeedCheckpointWritten(11, 110, 1_100L);
        Assert.assertEquals(1_250_000L, instance.getEffectiveCheckpointDurationMicros(configured));

        for (int i = 0; i < 64; i++) {
            instance.recordSeedCheckpointWritten(20 + i, 200 + i, 2_000L + i);
        }
        Assert.assertEquals(configured, instance.getEffectiveCheckpointDurationMicros(configured));
    }

    @Test
    public void testSealsWithoutCorrectionsRelaxCadenceBackToCeiling() {
        final LiveViewInstance instance = new LiveViewInstance(null, null, 1, false, -1);
        // Shipped defaults: cairo.live.view.checkpoint.max.duration.micros = 5 min,
        // against a FLUSH EVERY 1s view.
        final long configured = 5 * Micros.MINUTE_MICROS;

        // One row two seconds late pins the cadence at the flush floor - a 300x
        // seal-rate increase over the configured ceiling.
        instance.recordAdaptiveCheckpointCorrection(2 * Micros.SECOND_MICROS, configured, Micros.SECOND_MICROS);
        Assert.assertEquals(Micros.SECOND_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));

        // The seal the same correction's O3 repair forces is exempt: the tightened
        // cadence gets the one whole interval it asked for.
        instance.setHeadCheckpoint(1, 1, 100, 0, 1_000L);
        Assert.assertEquals(Micros.SECOND_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));

        // Every seal after it, with no further correction, walks the cadence one
        // 25% step back towards the ceiling, so the tightening pays for itself.
        instance.setHeadCheckpoint(2, 2, 200, 0, 2_000L);
        Assert.assertEquals(1_250_000L, instance.getEffectiveCheckpointDurationMicros(configured));

        instance.setHeadCheckpoint(3, 3, 300, 0, 3_000L);
        Assert.assertEquals(1_562_500L, instance.getEffectiveCheckpointDurationMicros(configured));

        // 1.25^n >= 300 needs 26 steps; 64 seals must land exactly on the ceiling
        // and stay there.
        for (int i = 0; i < 64; i++) {
            instance.setHeadCheckpoint(4 + i, 4 + i, 400 + i, 0, 4_000L + i);
        }
        Assert.assertEquals(configured, instance.getEffectiveCheckpointDurationMicros(configured));
        Assert.assertEquals(configured, instance.getAdaptiveCheckpointDurationMicros());

        // A fresh correction still tightens immediately.
        instance.recordAdaptiveCheckpointCorrection(2 * Micros.SECOND_MICROS, configured, Micros.SECOND_MICROS);
        Assert.assertEquals(Micros.SECOND_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));
    }
}
