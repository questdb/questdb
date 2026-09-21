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

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Compiler-produced identity of one live-view window-function state stream.
 * <p>
 * The encoded form is persisted as {@code FunctionRoot.functionIdentity}. It is
 * deliberately independent of the runtime function class and object address:
 * recompiling the same definition produces the same bytes even when factory
 * traversal or allocation order changes. Every variable-width field is UTF-8
 * length-prefixed, so delimiter characters in SQL identifiers or expressions
 * cannot alias another identity.
 */
public final class LiveViewCheckpointFunctionIdentity {
    private static final int FORMAT_VERSION = 1;
    private static final int MAGIC = 0x4c564649; // LVFI
    private static final int STRING_FIELD_COUNT = 5;
    private final String canonicalWindowName;
    private final byte[] encoded;
    private final String factorySignature;
    private final String orderSignature;
    private final int outputPosition;
    private final String partitionSignature;
    private final String stateCodecIdentity;

    public LiveViewCheckpointFunctionIdentity(
            @NotNull CharSequence canonicalWindowName,
            @NotNull CharSequence factorySignature,
            int outputPosition,
            @NotNull CharSequence partitionSignature,
            @NotNull CharSequence orderSignature,
            @NotNull CharSequence stateCodecIdentity
    ) {
        if (outputPosition < 0) {
            throw new IllegalArgumentException("negative live view checkpoint function output position");
        }
        this.canonicalWindowName = canonicalWindowName.toString();
        this.factorySignature = factorySignature.toString();
        this.outputPosition = outputPosition;
        this.partitionSignature = partitionSignature.toString();
        this.orderSignature = orderSignature.toString();
        this.stateCodecIdentity = stateCodecIdentity.toString();
        if (this.factorySignature.isEmpty() || this.stateCodecIdentity.isEmpty()) {
            throw new IllegalArgumentException("live view checkpoint function signature or codec identity is empty");
        }
        this.encoded = encode();
        LiveViewCheckpointMetadata.validateByteArrayLength(encoded.length, "function identity");
    }

    public String getCanonicalWindowName() {
        return canonicalWindowName;
    }

    /**
     * Returns an owned copy suitable for passing to a persistent root builder.
     */
    public byte[] getEncoded() {
        return Arrays.copyOf(encoded, encoded.length);
    }

    public String getFactorySignature() {
        return factorySignature;
    }

    public String getOrderSignature() {
        return orderSignature;
    }

    public int getOutputPosition() {
        return outputPosition;
    }

    public String getPartitionSignature() {
        return partitionSignature;
    }

    public String getStateCodecIdentity() {
        return stateCodecIdentity;
    }

    private byte[] encode() {
        final byte[][] fields = {
                canonicalWindowName.getBytes(StandardCharsets.UTF_8),
                factorySignature.getBytes(StandardCharsets.UTF_8),
                partitionSignature.getBytes(StandardCharsets.UTF_8),
                orderSignature.getBytes(StandardCharsets.UTF_8),
                stateCodecIdentity.getBytes(StandardCharsets.UTF_8)
        };
        long size = 3L * Integer.BYTES + (long) STRING_FIELD_COUNT * Integer.BYTES;
        for (int i = 0; i < fields.length; i++) {
            size += fields[i].length;
        }
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("live view checkpoint function identity is too large");
        }
        final ByteBuffer buffer = ByteBuffer.allocate((int) size);
        buffer.putInt(MAGIC);
        buffer.putInt(FORMAT_VERSION);
        buffer.putInt(outputPosition);
        for (int i = 0; i < fields.length; i++) {
            buffer.putInt(fields[i].length);
            buffer.put(fields[i]);
        }
        return buffer.array();
    }
}
