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

import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Heap-only high-water scratch shared by short-lived partition-map writers.
 * Callers must serialize access; nodes and references remain valid only until
 * the next {@link #reset()}.
 */
final class LiveViewCheckpointPartitionMapObjectPool {
    private final ObjectPool<LiveViewCheckpointPageRef> decodedPageRefs = new ObjectPool<>(LiveViewCheckpointPageRef::new, 1);
    private final ObjList<LiveViewCheckpointPartitionMapNode> nodes = new ObjList<>();
    private final ObjList<LiveViewCheckpointPageRef> outputPageRefs = new ObjList<>();
    private int nodeCursor;
    private int outputPageRefCursor;

    void clear() {
        decodedPageRefs.resetCapacity();
        nodes.clear();
        outputPageRefs.clear();
        nodeCursor = 0;
        outputPageRefCursor = 0;
    }

    ObjectPool<LiveViewCheckpointPageRef> decodedPageRefs() {
        return decodedPageRefs;
    }

    int getRetainedObjectCount() {
        return decodedPageRefs.getCapacity() + nodes.size() + outputPageRefs.size();
    }

    int getFirstRetainedNodeIdentityForTest() {
        return nodes.size() == 0 ? 0 : System.identityHashCode(nodes.getQuick(0));
    }

    LiveViewCheckpointPartitionMapNode nextNode() {
        if (nodeCursor == nodes.size()) {
            nodes.add(new LiveViewCheckpointPartitionMapNode());
        }
        return nodes.getQuick(nodeCursor++);
    }

    LiveViewCheckpointPageRef nextOutputPageRef() {
        if (outputPageRefCursor == outputPageRefs.size()) {
            outputPageRefs.add(new LiveViewCheckpointPageRef());
        }
        final LiveViewCheckpointPageRef ref = outputPageRefs.getQuick(outputPageRefCursor++);
        ref.clear();
        return ref;
    }

    void reset() {
        decodedPageRefs.clear();
        nodeCursor = 0;
        outputPageRefCursor = 0;
    }
}
