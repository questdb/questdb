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

package io.questdb.test.griffin;

import io.questdb.test.TestCairoConfigurationFactory;
import io.questdb.test.cairo.CairoTestConfiguration;

/**
 * Fails parallel filter factory construction while armed: every async filter atom reads the
 * pre-touch threshold from the configuration, and nothing else does.
 */
public final class AsyncFilterConstructionFault {
    public static final TestCairoConfigurationFactory CONFIGURATION_FACTORY = (root, telemetry, overrides) ->
            new CairoTestConfiguration(root, telemetry, overrides) {
                @Override
                public double getSqlParallelFilterPreTouchThreshold() {
                    final RuntimeException armed = failure;
                    if (armed != null) {
                        throw armed;
                    }
                    return super.getSqlParallelFilterPreTouchThreshold();
                }
            };
    private static volatile RuntimeException failure;

    private AsyncFilterConstructionFault() {
    }

    public static void arm(RuntimeException failure) {
        AsyncFilterConstructionFault.failure = failure;
    }

    public static void disarm() {
        failure = null;
    }
}
