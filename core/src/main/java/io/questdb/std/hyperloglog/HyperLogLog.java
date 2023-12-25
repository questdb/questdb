/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.std.hyperloglog;

import static io.questdb.std.hyperloglog.HyperLogLogDenseRepresentation.MAX_PRECISION;
import static io.questdb.std.hyperloglog.HyperLogLogDenseRepresentation.MIN_PRECISION;

/**
 * This is an implementation of HyperLogLog++ described in the paper
 * <a href="http://static.googleusercontent.com/media/research.google.com/fr//pubs/archive/40671.pdf">'HyperLogLog in
 * Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm'</a>.
 */
public class HyperLogLog {
    private static final int DEFAULT_PRECISION = 14;

    private final HyperLogLogRepresentation cachedSparseRepresentation;
    private HyperLogLogRepresentation representation;
    private boolean sparse;

    public HyperLogLog() {
        this(DEFAULT_PRECISION);
    }

    public HyperLogLog(int precision) {
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new IllegalArgumentException("Precision must be within the range of 4 to 18, inclusive.");
        }
        if (HyperLogLogSparseRepresentation.calculateSparseSetMaxSize(precision) > 0) {
            cachedSparseRepresentation = new HyperLogLogSparseRepresentation(precision);
            representation = cachedSparseRepresentation;
            sparse = true;
        } else {
            cachedSparseRepresentation = null;
            representation = new HyperLogLogDenseRepresentation(precision);
            sparse = false;
        }
    }

    public void add(long hash) {
        representation.add(hash);
        if (sparse && representation.isFull()) {
            representation = representation.convertToDense();
            sparse = false;
        }
    }

    public long computeCardinality() {
        return representation.computeCardinality();
    }

    public void clear() {
        if (cachedSparseRepresentation != null) {
            representation = cachedSparseRepresentation;
        }
        representation.clear();
        sparse = true;
    }
}
