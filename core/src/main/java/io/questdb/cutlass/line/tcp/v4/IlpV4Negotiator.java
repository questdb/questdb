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

package io.questdb.cutlass.line.tcp.v4;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * Stateless negotiator for ILP v4 protocol capabilities.
 * <p>
 * The negotiation process:
 * 1. Client sends CAPABILITY_REQUEST with supported version range and flags
 * 2. Server selects highest mutually supported version
 * 3. Server intersects supported flags
 * 4. Server sends CAPABILITY_RESPONSE with negotiated values
 * 5. If no version overlap, server sends fallback response
 * <p>
 * This class is thread-safe and can be shared across connections.
 */
public final class IlpV4Negotiator {

    // Server capabilities (can be configured)
    private final byte serverMinVersion;
    private final byte serverMaxVersion;
    private final int serverFlags;

    /**
     * Creates a negotiator with default server capabilities.
     * Supports version 1 with LZ4, Zstd, and Gorilla.
     */
    public IlpV4Negotiator() {
        this(VERSION_1, VERSION_1, FLAG_LZ4 | FLAG_ZSTD | FLAG_GORILLA);
    }

    /**
     * Creates a negotiator with custom server capabilities.
     *
     * @param serverMinVersion minimum version server supports
     * @param serverMaxVersion maximum version server supports
     * @param serverFlags      capability flags server supports
     */
    public IlpV4Negotiator(byte serverMinVersion, byte serverMaxVersion, int serverFlags) {
        this.serverMinVersion = serverMinVersion;
        this.serverMaxVersion = serverMaxVersion;
        this.serverFlags = serverFlags;
    }

    /**
     * Result of negotiation.
     */
    public static class NegotiationResult {
        public final boolean success;
        public final byte negotiatedVersion;
        public final int negotiatedFlags;

        private NegotiationResult(boolean success, byte negotiatedVersion, int negotiatedFlags) {
            this.success = success;
            this.negotiatedVersion = negotiatedVersion;
            this.negotiatedFlags = negotiatedFlags;
        }

        public static NegotiationResult success(byte version, int flags) {
            return new NegotiationResult(true, version, flags);
        }

        public static NegotiationResult failure() {
            return new NegotiationResult(false, (byte) 0, 0);
        }
    }

    /**
     * Negotiates protocol version and capabilities with a client.
     *
     * @param request the client's capability request
     * @return negotiation result
     */
    public NegotiationResult negotiate(IlpV4CapabilityRequest request) {
        return negotiate(
                request.getMinVersion(),
                request.getMaxVersion(),
                request.getFlags()
        );
    }

    /**
     * Negotiates protocol version and capabilities with a client.
     *
     * @param clientMinVersion client's minimum supported version
     * @param clientMaxVersion client's maximum supported version
     * @param clientFlags      client's capability flags
     * @return negotiation result
     */
    public NegotiationResult negotiate(byte clientMinVersion, byte clientMaxVersion, int clientFlags) {
        // Calculate version overlap
        byte negotiatedVersion = negotiateVersion(
                clientMinVersion, clientMaxVersion,
                serverMinVersion, serverMaxVersion
        );

        if (negotiatedVersion == 0) {
            // No version overlap
            return NegotiationResult.failure();
        }

        // Intersect flags
        int negotiatedFlags = clientFlags & serverFlags;

        return NegotiationResult.success(negotiatedVersion, negotiatedFlags);
    }

    /**
     * Negotiates the highest mutually supported protocol version.
     *
     * @param clientMin client minimum version
     * @param clientMax client maximum version
     * @param serverMin server minimum version
     * @param serverMax server maximum version
     * @return the negotiated version, or 0 if no overlap
     */
    public static byte negotiateVersion(byte clientMin, byte clientMax, byte serverMin, byte serverMax) {
        // Check for overlap
        if (clientMax < serverMin || serverMax < clientMin) {
            // No overlap
            return 0;
        }

        // Return highest mutually supported version
        return (byte) Math.min(clientMax & 0xFF, serverMax & 0xFF);
    }

    /**
     * Writes a capability response based on negotiation result.
     *
     * @param address destination address
     * @param result  negotiation result
     * @return address after written response
     */
    public static long writeResponse(long address, NegotiationResult result) {
        if (result.success) {
            return IlpV4CapabilityResponse.encode(address, result.negotiatedVersion, result.negotiatedFlags);
        } else {
            return IlpV4CapabilityResponse.encodeFallback(address);
        }
    }

    /**
     * Writes a capability response based on negotiation result.
     *
     * @param buf    destination buffer
     * @param offset starting offset
     * @param result negotiation result
     * @return offset after written response
     */
    public static int writeResponse(byte[] buf, int offset, NegotiationResult result) {
        if (result.success) {
            return IlpV4CapabilityResponse.encode(buf, offset, result.negotiatedVersion, result.negotiatedFlags);
        } else {
            return IlpV4CapabilityResponse.encodeFallback(buf, offset);
        }
    }

    // ==================== Getters ====================

    public byte getServerMinVersion() {
        return serverMinVersion;
    }

    public byte getServerMaxVersion() {
        return serverMaxVersion;
    }

    public int getServerFlags() {
        return serverFlags;
    }
}
