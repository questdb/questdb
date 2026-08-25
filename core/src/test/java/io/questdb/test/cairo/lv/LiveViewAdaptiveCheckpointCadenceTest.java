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
}
