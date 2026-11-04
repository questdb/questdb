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

import io.questdb.cairo.ColumnTypes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    private final byte[] encodedKeySchema;
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
        this(canonicalWindowName, factorySignature, outputPosition, partitionSignature,
                orderSignature, stateCodecIdentity, null);
    }

    public LiveViewCheckpointFunctionIdentity(
            @NotNull CharSequence canonicalWindowName,
            @NotNull CharSequence factorySignature,
            int outputPosition,
            @NotNull CharSequence partitionSignature,
            @NotNull CharSequence orderSignature,
            @NotNull CharSequence stateCodecIdentity,
            @Nullable ColumnTypes checkpointKeyColumnTypes
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
        this.encodedKeySchema = LiveViewCheckpointMetadata.encodeKeySchema(checkpointKeyColumnTypes);
        LiveViewCheckpointMetadata.validateByteArrayLength(encoded.length, "function identity");
    }

    public String getCanonicalWindowName() {
        return canonicalWindowName;
    }

    /**
     * Returns a defensive copy of the encoded identity.
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

    /**
     * Borrows the compiler-owned identity bytes. The compiled function owns this array
     * for its complete lifetime; package-internal checkpoint code must not mutate it or
     * retain it beyond the function/compiled-factory lifetime.
     */
    byte[] borrowEncoded() {
        return encoded;
    }

    /**
     * Borrows the compiler-owned partition-key schema. The compiled function owns this
     * array for its complete lifetime; package-internal checkpoint code must not mutate
     * it or retain it beyond the function/compiled-factory lifetime.
     */
    byte[] borrowEncodedKeySchema() {
        return encodedKeySchema;
    }

    private byte[] encode() {
        long size = 3L * Integer.BYTES + (long) STRING_FIELD_COUNT * Integer.BYTES;
        size += LiveViewCheckpointMetadata.utf8Bytes(canonicalWindowName);
        size += LiveViewCheckpointMetadata.utf8Bytes(factorySignature);
        size += LiveViewCheckpointMetadata.utf8Bytes(partitionSignature);
        size += LiveViewCheckpointMetadata.utf8Bytes(orderSignature);
        size += LiveViewCheckpointMetadata.utf8Bytes(stateCodecIdentity);
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("live view checkpoint function identity is too large");
        }
        final byte[] encoded = new byte[(int) size];
        int offset = LiveViewCheckpointMetadata.putInt(encoded, 0, MAGIC);
        offset = LiveViewCheckpointMetadata.putInt(encoded, offset, FORMAT_VERSION);
        offset = LiveViewCheckpointMetadata.putInt(encoded, offset, outputPosition);
        offset = putField(encoded, offset, canonicalWindowName);
        offset = putField(encoded, offset, factorySignature);
        offset = putField(encoded, offset, partitionSignature);
        offset = putField(encoded, offset, orderSignature);
        putField(encoded, offset, stateCodecIdentity);
        return encoded;
    }

    private static int putField(byte[] sink, int offset, CharSequence value) {
        offset = LiveViewCheckpointMetadata.putInt(sink, offset, LiveViewCheckpointMetadata.utf8Bytes(value));
        return LiveViewCheckpointMetadata.putUtf8(sink, offset, value);
    }
}
