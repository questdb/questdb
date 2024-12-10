/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.std.ndarr;

import io.questdb.std.DirectIntList;
import io.questdb.std.DirectIntSlice;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;

import java.io.Closeable;

/**
 * An iterator-like utility to traverse an {@link NdArrayView} in row-major order.
 * <p>Instead of obtaining the values, this traversal class computes the coordinates.</p>
 * <p>It is down to the user to then pass those coordinates to the type-appropriate
 * getter method.</p>
 * <p>Example: <pre>{@code
 *     NdArrayView array = ...;
 *     try (NdArrayRowMajorTraversal traversal = new NdArrayRowMajorTraversal()) {
 *         traversal.of(array);
 *         DirectIntSlice coords;
 *         while ((coords = traversal.next()) != null) {
 *             final int value = array.getInt(coords);
 *             ...
 *         }
 *     }
 * }</pre></p>
 */
public class NdArrayRowMajorTraversal implements QuietCloseable {
    public static final io.questdb.std.ThreadLocal<NdArrayRowMajorTraversal> LOCAL = new io.questdb.std.ThreadLocal<>(NdArrayRowMajorTraversal::new);
    public static final Closeable THREAD_LOCAL_CLEANER = NdArrayRowMajorTraversal::clearThreadLocals;
    private final DirectIntList coordinates = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY);
    private boolean done = false;
    private int in = 0;
    private int out = 0;
    /**
     * The array's shape
     */
    private DirectIntSlice shape;

    public static void clearThreadLocals() {
        LOCAL.close();
    }

    @Override
    public void close() {
        Misc.free(coordinates);
    }

    /**
     * Number of levels of nesting to bump deeper
     * into <em>before</em> processing the coordinates returned
     * by {@link #next()}.
     */
    public int getIn() {
        return in;
    }

    /**
     * Number of levels of nesting to bump out of
     * <em>after</em> processing the coordinates returned by
     * {@link #next()}.
     */
    public int getOut() {
        return out;
    }

    /**
     * There's another coordinate after this one.
     */
    public boolean hasNext() {
        return !done;
    }

    /**
     * Get the next coordinates to iterate.
     */
    public DirectIntSlice next() {
        if (done) {
            return null;
        }

        // If we previously bumped out, we need to bump deeper
        // by the same level of nesting at the next (this) iteration.
        in = out;
        out = 0;

        // The `out` variable counts how many dims _will be_ reset to zero in the call to `next()` after this one.
        final int lastDimIndex = shape.length() - 1;
        for (int dimIndex = lastDimIndex; dimIndex >= 0; --dimIndex) {
            final int dim = shape.get(dimIndex);
            final int current = coordinates.get(dimIndex);
            if (out > 0) {
                if (current + 1 == dim) {
                    ++out;
                } else {
                    break;
                }
            } else if ((dimIndex == lastDimIndex) && (current + 2 == dim)) {
                coordinates.set(dimIndex, current + 1);
                ++out;
            } else if (current + 1 < dim) {
                coordinates.set(dimIndex, current + 1);
                break;
            } else {
                coordinates.set(dimIndex, 0);
            }
        }

        if (out == shape.length()) {
            done = true;
        }

        return coordinates.asSlice();
    }

    public NdArrayRowMajorTraversal of(NdArrayView array) {
        return of(array.getShape());
    }

    public NdArrayRowMajorTraversal of(DirectIntSlice shape) {
        reset();
        this.shape = shape;
        for (int dimIndex = shape.length() - 1; dimIndex >= 0; --dimIndex) {
            coordinates.add(0);
        }
        if (coordinates.size() > 0) {
            done = false;
            coordinates.set(coordinates.size() - 1, -1);  // one before the end.
        }

        // will be converted to `in == shape.length()` on first iteration.
        out = shape.length();
        return this;
    }

    private void reset() {
        coordinates.clear();
        shape = null;
        done = true;
        in = 0;
        out = 0;
    }
}
