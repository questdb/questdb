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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.std.Rnd;
import org.jetbrains.annotations.NotNull;
import io.questdb.std.CarrierLocal;

public class SharedRandom {
    // async random is used by SQL Async implementation in order to
    // not disturb the existing tests
    public static final CarrierLocal<Rnd> ASYNC_RANDOM = new CarrierLocal<>();
    public static final CarrierLocal<Rnd> RANDOM = new CarrierLocal<>();

    public static Rnd getAsyncRandom(CairoConfiguration configuration) {
        return getRnd(configuration, ASYNC_RANDOM);
    }

    public static Rnd getRandom(CairoConfiguration configuration) {
        return getRnd(configuration, RANDOM);
    }

    @NotNull
    private static Rnd getRnd(CairoConfiguration configuration, CarrierLocal<Rnd> tlRnd) {
        Rnd rnd = tlRnd.get();
        if (rnd == null) {
            tlRnd.set(rnd = new Rnd(
                            configuration.getNanosecondClock().getTicks(),
                            configuration.getMicrosecondClock().getTicks()
                    )
            );
        }
        return rnd;
    }
}
