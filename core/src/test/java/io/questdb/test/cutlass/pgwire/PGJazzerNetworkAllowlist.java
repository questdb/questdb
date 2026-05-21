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

package io.questdb.test.cutlass.pgwire;

import java.lang.reflect.Method;
import java.util.function.BiPredicate;

final class PGJazzerNetworkAllowlist {
    private static volatile Object loopbackNetworkConnectionScope;

    private PGJazzerNetworkAllowlist() {
    }

    static void allowLoopbackConnections() {
        if (loopbackNetworkConnectionScope != null) {
            return;
        }
        synchronized (PGJazzerNetworkAllowlist.class) {
            if (loopbackNetworkConnectionScope != null) {
                return;
            }
            try {
                final Class<?> bugDetectors = Class.forName("com.code_intelligence.jazzer.api.BugDetectors");
                final Method method = bugDetectors.getMethod("allowNetworkConnections", BiPredicate.class);
                final BiPredicate<String, Integer> allowLoopback = (host, port) -> {
                    if (host == null || port == null || port <= 0 || port > 65535) {
                        return false;
                    }
                    return "localhost".equalsIgnoreCase(host)
                            || "127.0.0.1".equals(host)
                            || "::1".equals(host)
                            || "0:0:0:0:0:0:0:1".equals(host);
                };
                loopbackNetworkConnectionScope = method.invoke(null, allowLoopback);
            } catch (ClassNotFoundException ignored) {
                loopbackNetworkConnectionScope = PGJazzerNetworkAllowlist.class;
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Could not configure Jazzer loopback network allowance", e);
            }
        }
    }
}
