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

package io.questdb.cairo;

@FunctionalInterface
public interface LifecycleManager {
    boolean close();

    /**
     * Invoked at the very start of {@link TableWriter#close()}'s teardown ({@code doClose}),
     * before any native resource is freed. The writer pool drains in-flight async-command
     * publishers here so that a direct {@link TableWriter#destroy()} cannot free the command
     * queue underneath a publisher mid-serialize and crash the JVM with a SIGSEGV. The default
     * implementation is a no-op.
     */
    default void onBeforeClose() {
    }
}
