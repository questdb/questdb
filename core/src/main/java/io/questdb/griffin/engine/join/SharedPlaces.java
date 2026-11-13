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

package io.questdb.griffin.engine.join;

import io.questdb.griffin.engine.table.RuntimeConstGateRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * The places of a factory tree that read a {@link SharedPrimary}, its homes and its {@link SharedRecordCursorFactory}
 * readers, in the order in which the code generator builds them, and the runtime constant gates over sub-trees that
 * hold some of them. A sub-tree that the code generator builds after it takes a mark, the number of places, holds
 * the places from the mark on.
 * <p>
 * A gate opens its base while false only when its sub-tree holds the place that reads the cursor of a primary, while
 * a place outside the sub-tree reads the state that this cursor builds, see
 * {@link io.questdb.cairo.sql.RecordCursorFactory#getSharedCursor}: the cursor of the primary then has to open in
 * every execution, see {@link RuntimeConstGateRecordCursorFactory}. While a gate over every place of a primary is
 * false, nothing reads the state of the primary, and a home outside the sub-tree opens on its own. Which place reads
 * the cursor of a primary is known only when the code generator has built the whole tree, see
 * {@link SharedPrimary#handOver()}, and the code generator can build a place outside a sub-tree after the gate over
 * it, so {@link #resolveGates(int, int)} decides then.
 */
public final class SharedPlaces implements Mutable {
    // the places [lo, hi) that the sub-tree of each gate holds, two ints per gate
    private final IntList gatePlaceRanges = new IntList();
    private final ObjList<RuntimeConstGateRecordCursorFactory> gates = new ObjList<>();
    private final ObjList<SharedPrimary> placePrimaries = new ObjList<>();
    // the reader of each place, or null for a home, which holds the primary itself
    private final ObjList<SharedRecordCursorFactory> placeReaders = new ObjList<>();

    /**
     * Records the gate over the sub-tree that the code generator built from the place mark on, when the sub-tree
     * holds places, so that {@link #resolveGates(int, int)} tells the gate whether it opens its base while false.
     *
     * @return the gate
     */
    public RuntimeConstGateRecordCursorFactory addGate(RuntimeConstGateRecordCursorFactory gate, int placeMark) {
        final int placeCount = placePrimaries.size();
        if (placeCount > placeMark) {
            gates.add(gate);
            gatePlaceRanges.add(placeMark);
            gatePlaceRanges.add(placeCount);
        }
        return gate;
    }

    /**
     * Records a place that holds the primary and reads its cursor, see {@link SharedPrimary#addHome()}.
     */
    public void addHome(SharedPrimary primary) {
        placePrimaries.add(primary);
        placeReaders.add(null);
    }

    /**
     * Records a place that reads the primary through the reader.
     */
    public void addReader(SharedPrimary primary, SharedRecordCursorFactory reader) {
        placePrimaries.add(primary);
        placeReaders.add(reader);
    }

    @Override
    public void clear() {
        truncate(0, 0);
    }

    public int getGateCount() {
        return gates.size();
    }

    public int getPlaceCount() {
        return placePrimaries.size();
    }

    /**
     * Tells each gate recorded from the gate mark on whether it opens its base while false. Call it once the code
     * generator has handed over the primaries of the places from the place mark on, see
     * {@link SharedPrimary#handOver()}, which decides the home of a primary whose tree holds no other home. A
     * generation builds a tree that holds every place of its primaries, so the gates of a generation compare their
     * places with the places from its mark on.
     */
    public void resolveGates(int placeMark, int gateMark) {
        for (int i = gateMark, n = gates.size(); i < n; i++) {
            final int lo = gatePlaceRanges.getQuick(2 * i);
            final int hi = gatePlaceRanges.getQuick(2 * i + 1);
            gates.getQuick(i).setBaseOpenRequired(isPrimaryReadOutside(placeMark, lo, hi));
        }
    }

    /**
     * Forgets the places from the place mark on and the gates from the gate mark on.
     */
    public void truncate(int placeMark, int gateMark) {
        if (placeMark == 0) {
            // the tree is built, or its generation failed: drop the references to its factories
            placePrimaries.clear();
            placeReaders.clear();
        } else {
            placePrimaries.setPos(placeMark);
            placeReaders.setPos(placeMark);
        }
        if (gateMark == 0) {
            gates.clear();
        } else {
            gates.setPos(gateMark);
        }
        gatePlaceRanges.setPos(2 * gateMark);
    }

    // Returns whether the place at the index reads the cursor of its primary in the finished tree: a home that the
    // code generator did not drop, or the reader that SharedPrimary.handOver() promoted to home
    private boolean isHomeAt(int index) {
        final SharedPrimary primary = placePrimaries.getQuick(index);
        if (primary.isClosed()) {
            // the code generator dropped every place of the primary
            return false;
        }
        final SharedRecordCursorFactory reader = placeReaders.getQuick(index);
        return reader != null ? reader.isHome() : primary.hasHome();
    }

    // Returns whether the places [lo, hi) hold the place that reads the cursor of a primary, while a reader outside
    // them, from the place mark on, reads the state of that primary
    private boolean isPrimaryReadOutside(int placeMark, int lo, int hi) {
        assert placeMark <= lo && lo < hi && hi <= placePrimaries.size();
        for (int i = lo; i < hi; i++) {
            if (!isHomeAt(i)) {
                continue;
            }
            final SharedPrimary primary = placePrimaries.getQuick(i);
            for (int j = placeMark, n = placePrimaries.size(); j < n; j++) {
                if ((j < lo || j >= hi) && placePrimaries.getQuick(j) == primary) {
                    final SharedRecordCursorFactory reader = placeReaders.getQuick(j);
                    // the place at i reads the cursor of the primary, so this reader, when the tree still holds it,
                    // reads the state of the primary
                    if (reader != null && primary.hasReader(reader)) {
                        assert !reader.isHome();
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
