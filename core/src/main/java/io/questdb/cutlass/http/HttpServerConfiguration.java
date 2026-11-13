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

package io.questdb.cutlass.http;

import io.questdb.FactoryProvider;
import io.questdb.cutlass.qwp.server.QwpBrowserAllowedOrigins;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.std.ObjHashSet;

public interface HttpServerConfiguration extends IODispatcherConfiguration, WorkerPoolConfiguration {

    /**
     * The host names this server serves when it runs without authentication;
     * custom configurations serve the ones public DNS cannot point at it.
     */
    default HttpAllowedHosts getAllowedHosts() {
        return HttpAllowedHosts.DEFAULT;
    }

    /**
     * The browser origins whose cross-site requests this server serves, on every
     * endpoint that rejects cross-site requests; custom configurations serve none.
     */
    default QwpBrowserAllowedOrigins getBrowserAllowedOrigins() {
        return QwpBrowserAllowedOrigins.EMPTY;
    }

    default ObjHashSet<String> getContextPathLifecycle() {
        return new ObjHashSet<>() {{
            add("/lifecycle");
        }};
    }

    default ObjHashSet<String> getContextPathMetrics() {
        return new ObjHashSet<>() {{
            add("/metrics");
        }};
    }

    default ObjHashSet<String> getContextPathStatus() {
        return new ObjHashSet<>() {{
            add(getHttpContextConfiguration().getMetrics().isEnabled() ? "/status" : "*");
        }};
    }

    FactoryProvider getFactoryProvider();

    HttpContextConfiguration getHttpContextConfiguration();

    byte getRequiredAuthType();

    WaitProcessorConfiguration getWaitProcessorConfiguration();

    /**
     * Enables fiber execution for this HTTP server when its resolved worker pool also
     * uses {@link io.questdb.mp.WorkerPoolMode#FIBER_HOST}.
     */
    default boolean isFiberEnabled() {
        return true;
    }

    boolean isPessimisticHealthCheckEnabled();

    boolean preAllocateBuffers();
}
