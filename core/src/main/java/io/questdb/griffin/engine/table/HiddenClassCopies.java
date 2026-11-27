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

package io.questdb.griffin.engine.table;

import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

/**
 * Hands out instances of a template class, each of a hidden class of its own that
 * {@link #newInstance()} defines from the template's bytecode.
 * <p>
 * A hot method that calls classes each query generates for itself, such as key sinks, filters and
 * aggregate updaters, collects one type profile for every query the JVM has run when the method is
 * shared, so C2 compiles it for the queries that came first: with two receivers it inlines both and
 * may run out of its inlining budget before the current query's classes, and with more it calls
 * them through the interface. Either way, the whole JVM stays in that state until the older
 * queries' classes unload. A factory that takes its own copy of such a method profiles only its
 * own classes.
 * <p>
 * A template must be a top-level class with a no-argument constructor that this class can call, and
 * without lambdas, nested classes or static state, since each hidden copy shares nothing with it but
 * its bytecode. Its copies extend the template's superclass, so callers use them through that type.
 * When the template's class file cannot be read, the JVM cannot define a copy or the copy's
 * constructor fails, {@link #newInstance()} falls back to an instance of the template itself.
 *
 * @param <T> the template's superclass, which every copy extends
 */
public final class HiddenClassCopies<T> {
    private final Supplier<? extends T> sharedTemplate;
    private final Class<? extends T> template;
    @Nullable
    private final byte[] templateBytecode;
    private final Class<T> type;

    /**
     * @param type           the type callers use the copies through, the template's superclass
     * @param template       the class whose bytecode each copy repeats
     * @param sharedTemplate creates an instance of the template itself, which
     *                       {@link #newInstance()} falls back to
     */
    public HiddenClassCopies(Class<T> type, Class<? extends T> template, Supplier<? extends T> sharedTemplate) {
        this.type = type;
        this.template = template;
        this.sharedTemplate = sharedTemplate;
        this.templateBytecode = readBytecode(template);
    }

    /**
     * Returns an instance whose class no other caller shares, or an instance of the shared template
     * when no copy can be made.
     */
    public T newInstance() {
        if (templateBytecode != null) {
            final Class<?> clazz = Unsafe.defineAnonymousClass(template, templateBytecode);
            if (clazz != null) {
                try {
                    return type.cast(clazz.getDeclaredConstructor().newInstance());
                } catch (ReflectiveOperationException ignore) {
                    // fall back to the shared template
                }
            }
        }
        return sharedTemplate.get();
    }

    @Nullable
    private static byte[] readBytecode(Class<?> template) {
        try (InputStream in = template.getResourceAsStream(template.getSimpleName() + ".class")) {
            return in != null ? in.readAllBytes() : null;
        } catch (IOException e) {
            return null;
        }
    }
}
