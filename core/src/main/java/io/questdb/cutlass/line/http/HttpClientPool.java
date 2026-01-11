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

package io.questdb.cutlass.line.http;

import io.questdb.ClientTlsConfiguration;
import io.questdb.HttpClientConfiguration;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.std.Misc;

import java.io.Closeable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe pool of HttpClient instances for concurrent HTTP requests.
 * <p>
 * This pool pre-creates a fixed number of HttpClient instances at construction time
 * and provides blocking acquire/release semantics for thread-safe access.
 * <p>
 * Usage:
 * <pre>
 * HttpClientPool pool = new HttpClientPool(config, tlsConfig, 4);
 * try {
 *     HttpClient client = pool.acquire();
 *     try {
 *         // use client
 *     } finally {
 *         pool.release(client);
 *     }
 * } finally {
 *     pool.close();
 * }
 * </pre>
 * <p>
 * Thread safety:
 * <ul>
 *     <li>acquire() blocks if no clients available until one is released or timeout</li>
 *     <li>release() returns client to pool, waking up waiting threads</li>
 *     <li>close() closes all clients, further acquire() calls will fail</li>
 * </ul>
 */
public class HttpClientPool implements Closeable {

    private final BlockingQueue<HttpClient> availableClients;
    private final int poolSize;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger createdCount = new AtomicInteger(0);
    private final HttpClientConfiguration clientConfiguration;
    private final ClientTlsConfiguration tlsConfig;

    /**
     * Creates a new HttpClient pool with the specified size.
     * All clients are created eagerly at construction time.
     *
     * @param clientConfiguration HTTP client configuration
     * @param tlsConfig           TLS configuration (null for plain HTTP)
     * @param poolSize            number of clients to create in the pool
     * @throws IllegalArgumentException if poolSize <= 0
     */
    public HttpClientPool(HttpClientConfiguration clientConfiguration, ClientTlsConfiguration tlsConfig, int poolSize) {
        if (poolSize <= 0) {
            throw new IllegalArgumentException("poolSize must be > 0, got: " + poolSize);
        }
        this.clientConfiguration = clientConfiguration;
        this.tlsConfig = tlsConfig;
        this.poolSize = poolSize;
        this.availableClients = new ArrayBlockingQueue<>(poolSize);

        // Create all clients eagerly
        for (int i = 0; i < poolSize; i++) {
            HttpClient client = createClient();
            availableClients.offer(client);
            createdCount.incrementAndGet();
        }
    }

    /**
     * Acquires an HttpClient from the pool, blocking indefinitely until one is available.
     *
     * @return an HttpClient instance
     * @throws IllegalStateException if the pool is closed
     * @throws InterruptedException  if the thread is interrupted while waiting
     */
    public HttpClient acquire() throws InterruptedException {
        checkNotClosed();
        HttpClient client = availableClients.take();
        checkNotClosed(); // Check again after acquiring in case pool was closed while waiting
        return client;
    }

    /**
     * Acquires an HttpClient from the pool, blocking up to the specified timeout.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the timeout argument
     * @return an HttpClient instance, or null if timeout elapsed
     * @throws IllegalStateException if the pool is closed
     * @throws InterruptedException  if the thread is interrupted while waiting
     */
    public HttpClient acquire(long timeout, TimeUnit unit) throws InterruptedException {
        checkNotClosed();
        HttpClient client = availableClients.poll(timeout, unit);
        if (client != null) {
            checkNotClosed(); // Check again after acquiring in case pool was closed while waiting
        }
        return client;
    }

    /**
     * Attempts to acquire an HttpClient immediately without blocking.
     *
     * @return an HttpClient instance, or null if none available
     * @throws IllegalStateException if the pool is closed
     */
    public HttpClient tryAcquire() {
        checkNotClosed();
        return availableClients.poll();
    }

    /**
     * Releases an HttpClient back to the pool.
     * <p>
     * Important: Only release clients that were acquired from this pool.
     * Releasing a client after the pool is closed will close the client instead.
     *
     * @param client the client to release
     * @throws IllegalArgumentException if client is null
     */
    public void release(HttpClient client) {
        if (client == null) {
            throw new IllegalArgumentException("client cannot be null");
        }

        if (closed.get()) {
            // Pool is closed, close the client instead of returning it
            Misc.free(client);
            return;
        }

        // Try to return to pool. If pool is full (shouldn't happen), close it.
        if (!availableClients.offer(client)) {
            Misc.free(client);
        }
    }

    /**
     * Closes the pool and all HttpClient instances.
     * <p>
     * After close():
     * <ul>
     *     <li>acquire() will throw IllegalStateException</li>
     *     <li>release() will close the released client</li>
     *     <li>Clients currently checked out should be released normally (they will be closed)</li>
     * </ul>
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // Drain and close all available clients
            HttpClient client;
            while ((client = availableClients.poll()) != null) {
                Misc.free(client);
            }
        }
    }

    /**
     * Returns the total pool size (number of clients created).
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Returns the number of currently available (not checked out) clients.
     */
    public int getAvailableCount() {
        return availableClients.size();
    }

    /**
     * Returns true if the pool has been closed.
     */
    public boolean isClosed() {
        return closed.get();
    }

    private HttpClient createClient() {
        if (tlsConfig != null) {
            return HttpClientFactory.newTlsInstance(clientConfiguration, tlsConfig);
        } else {
            return HttpClientFactory.newPlainTextInstance(clientConfiguration);
        }
    }

    private void checkNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("HttpClientPool is closed");
        }
    }
}
