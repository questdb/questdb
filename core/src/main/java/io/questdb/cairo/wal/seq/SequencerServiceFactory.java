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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;

/**
 * Factory for creating {@link SequencerService} instances. Provided via
 * {@link CairoConfiguration#getSequencerServiceFactory()} to enable pluggable
 * sequencer implementations.
 * <p>
 * When {@code null} is returned from the configuration, the default
 * {@link LocalSequencerService} is used.
 */
@FunctionalInterface
public interface SequencerServiceFactory {

    /**
     * Create a {@link SequencerService} instance. Called once during
     * {@link CairoEngine} initialization.
     *
     * @param engine        the engine instance (for accessing table tokens, listeners, etc.)
     * @param configuration the Cairo configuration
     * @return a new SequencerService instance
     */
    SequencerService create(CairoEngine engine, CairoConfiguration configuration);
}
