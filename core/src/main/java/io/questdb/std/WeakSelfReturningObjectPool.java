/*******************************************************************************
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

package io.questdb.std;

import org.jetbrains.annotations.NotNull;

/**
 * This object pool does not attempt to close pooled objects and instead assumes that
 * objects return themselves to the pool on close() method call. Note that the push()
 * method is not exposed by this class - the only way an object returns to the pool is
 * a close() call.
 */
public class WeakSelfReturningObjectPool<T extends AbstractSelfReturningObject<?>> extends WeakObjectPoolBase<T> {
    private final SelfReturningObjectFactory<T> factory;

    public WeakSelfReturningObjectPool(@NotNull SelfReturningObjectFactory<T> factory, int initSize) {
        super(initSize);
        this.factory = factory;
        fill();
    }

    @Override
    T newInstance() {
        return factory.newInstance(this);
    }
}
