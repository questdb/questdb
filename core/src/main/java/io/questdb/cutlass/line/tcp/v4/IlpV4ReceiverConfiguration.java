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

/**
 * Configuration for ILP v4 receiver.
 * <p>
 * This interface defines the configuration options specific to ILP v4 protocol
 * handling, separate from the general TCP receiver configuration.
 */
public interface IlpV4ReceiverConfiguration {

    /**
     * Returns whether LZ4 compression is supported.
     *
     * @return true if LZ4 supported
     */
    default boolean isLz4Supported() {
        return false; // Not implemented yet
    }

    /**
     * Returns whether Zstd compression is supported.
     *
     * @return true if Zstd supported
     */
    default boolean isZstdSupported() {
        return false; // Not implemented yet
    }

    /**
     * Returns whether Gorilla timestamp encoding is supported.
     *
     * @return true if Gorilla supported
     */
    default boolean isGorillaSupported() {
        return true;
    }

    /**
     * Returns the maximum message size in bytes.
     *
     * @return max message size
     */
    default int getMaxMessageSize() {
        return 16 * 1024 * 1024; // 16 MB
    }

    /**
     * Returns the maximum number of tables per message.
     *
     * @return max tables per message
     */
    default int getMaxTablesPerMessage() {
        return 256;
    }

    /**
     * Returns the maximum rows per table in a message.
     *
     * @return max rows per table
     */
    default int getMaxRowsPerTable() {
        return 1_000_000;
    }

    /**
     * Returns the maximum number of in-flight (pipelined) messages.
     *
     * @return max pipelined messages
     */
    default int getMaxPipelinedMessages() {
        return 4;
    }

    /**
     * Returns whether auto-creation of new columns is enabled.
     *
     * @return true if auto-create columns enabled
     */
    default boolean isAutoCreateNewColumns() {
        return true;
    }

    /**
     * Returns whether auto-creation of new tables is enabled.
     *
     * @return true if auto-create tables enabled
     */
    default boolean isAutoCreateNewTables() {
        return true;
    }

    /**
     * Returns the maximum column name length.
     *
     * @return max column name length
     */
    default int getMaxFileNameLength() {
        return 127;
    }

    /**
     * Returns the handshake timeout in milliseconds.
     *
     * @return handshake timeout
     */
    default long getHandshakeTimeoutMs() {
        return 30_000; // 30 seconds
    }

    /**
     * Returns whether schema caching is enabled.
     *
     * @return true if schema caching enabled
     */
    default boolean isSchemaCachingEnabled() {
        return true;
    }

    /**
     * Returns the maximum number of cached schemas.
     *
     * @return max cached schemas
     */
    default int getMaxCachedSchemas() {
        return 256;
    }

    /**
     * Returns the receive buffer size for ILP v4 messages.
     *
     * @return buffer size in bytes
     */
    default int getRecvBufferSize() {
        return 64 * 1024; // 64 KB initial
    }

    /**
     * Returns the maximum receive buffer size.
     *
     * @return max buffer size in bytes
     */
    default int getMaxRecvBufferSize() {
        return getMaxMessageSize() + IlpV4Constants.HEADER_SIZE;
    }

    /**
     * Builds the server capabilities flags based on configuration.
     *
     * @return capability flags
     */
    default short getServerCapabilities() {
        short caps = 0;
        if (isLz4Supported()) {
            caps |= IlpV4Constants.FLAG_LZ4;
        }
        if (isZstdSupported()) {
            caps |= IlpV4Constants.FLAG_ZSTD;
        }
        if (isGorillaSupported()) {
            caps |= IlpV4Constants.FLAG_GORILLA;
        }
        return caps;
    }
}
