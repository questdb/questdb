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

package io.questdb.std;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Wraps a resource and guards its <code>.close()</code> method so it's only
 * called once the refcount has reached 0.
 *
 * <p>
 * To share ownership:
 * <ul>
 *     <li>Call the <code>.incref()</code> method and hand that to the new shared owner.</li>
 *     <li>The internal refcount will be bumped up by one.</li>
 *     <li>The resource's <code>T::close()</code> method will be called ony once all the owners
 *         call <code>Arc&lt;T&gt;::close()</code> and the ref count drops to 0.</li>
 * </ul>
 *
 * <p>
 *
 * Call the <code>.get()</code> method to access the resource without affecting the refcount.
 *
 * @param <T> The wrapped resource
 */
public class Arc<T> implements QuietCloseable {
    private final AtomicLong refCount;
    private final T resource;

    public Arc(@NotNull T resource) {
        this.resource = resource;
        this.refCount = new AtomicLong(1);
    }

    @SuppressWarnings("unchecked")
    public static <U extends T, T> Arc<U> downcast(Arc<T> arc, Class<U> clazz) {
        if (!clazz.isInstance(arc.resource)) {
            throw new ClassCastException(arc.resource.getClass().getName()
                    + " cannot be cast to " + clazz.getName());
        }
        return (Arc<U>) arc;
    }

    @SuppressWarnings("unchecked")
    public static <T extends S, S> Arc<S> upcast(Arc<T> arc) {
        return (Arc<S>) arc;
    }

    @Override
    public void close() {
        long remaining = refCount.decrementAndGet();
        if (remaining == 0) {
            Misc.freeIfCloseable(resource);
        } else if (remaining < 0) {
            throw new IllegalStateException("`.close()` called more times than `.incref()`");
        }
    }

    public @NotNull T get() {
        if (refCount.get() <= 0) {
            throw new IllegalStateException("Resource has already been released");
        }
        return resource;
    }

    @TestOnly
    public long getRefCount() {
        return refCount.get();
    }

    public @NotNull Arc<T> incref() {
        long currentCount = refCount.incrementAndGet();
        if (currentCount <= 1) {
            throw new IllegalStateException("Resource has already been released");
        }
        return this;
    }
}
