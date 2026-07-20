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

import io.questdb.cairo.lv.LiveViewCheckpointContracts.DependencyKind;
import org.jetbrains.annotations.NotNull;

/**
 * Immutable compiler-visible dependency contract for one live-view function.
 * It carries the information later repair phases need without retaining parser
 * model objects in the runtime function graph.
 */
public final class LiveViewCheckpointDependency {
    private final long frameHi;
    private final long frameLo;
    private final String highBoundStrategy;
    private final DependencyKind kind;
    private final String lowBoundStrategy;
    private final NumericConvergence numericConvergence;
    private final String orderSignature;
    private final String partitionSignature;
    private final StructuralConvergence structuralConvergence;
    private final boolean supportsKeyReset;
    private final boolean supportsKeyRestore;

    public LiveViewCheckpointDependency(
            @NotNull DependencyKind kind,
            @NotNull CharSequence partitionSignature,
            @NotNull CharSequence orderSignature,
            long frameLo,
            long frameHi,
            boolean supportsKeyRestore,
            boolean supportsKeyReset,
            @NotNull StructuralConvergence structuralConvergence,
            @NotNull NumericConvergence numericConvergence
    ) {
        this.kind = kind;
        this.partitionSignature = partitionSignature.toString();
        this.orderSignature = orderSignature.toString();
        this.frameLo = frameLo;
        this.frameHi = frameHi;
        this.lowBoundStrategy = kind.getLowBoundStrategy();
        this.highBoundStrategy = kind.getHighBoundStrategy();
        this.supportsKeyRestore = supportsKeyRestore;
        this.supportsKeyReset = supportsKeyReset;
        this.structuralConvergence = structuralConvergence;
        this.numericConvergence = numericConvergence;
    }

    public long getFrameHi() {
        return frameHi;
    }

    public long getFrameLo() {
        return frameLo;
    }

    public String getHighBoundStrategy() {
        return highBoundStrategy;
    }

    public DependencyKind getKind() {
        return kind;
    }

    public String getLowBoundStrategy() {
        return lowBoundStrategy;
    }

    public NumericConvergence getNumericConvergence() {
        return numericConvergence;
    }

    public String getOrderSignature() {
        return orderSignature;
    }

    public String getPartitionSignature() {
        return partitionSignature;
    }

    public StructuralConvergence getStructuralConvergence() {
        return structuralConvergence;
    }

    public boolean supportsKeyReset() {
        return supportsKeyReset;
    }

    public boolean supportsKeyRestore() {
        return supportsKeyRestore;
    }

    public enum NumericConvergence {
        EXACT,
        FLOATING_TOLERANCE
    }

    public enum StructuralConvergence {
        EXACT
    }
}
