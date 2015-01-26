/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.collections;

import java.util.Iterator;
import java.util.List;

public final class Lists {

    private Lists() {
    }

    public static void advance(List<?> list, int index) {
        while (list.size() <= index) {
            list.add(null);
        }
    }

    public static <T> ImmutableIteratorWrapper<T> immutableIterator(Iterable<T> source) {
        return new ImmutableIteratorWrapper<>(source);
    }

    public static class ImmutableIteratorWrapper<T> extends AbstractImmutableIterator<T> {
        private final Iterable<T> source;
        private Iterator<T> underlying;

        public ImmutableIteratorWrapper(Iterable<T> source) {
            this.source = source;
        }

        @Override
        public boolean hasNext() {
            return underlying.hasNext();
        }

        @Override
        public T next() {
            return underlying.next();
        }

        public ImmutableIteratorWrapper<T> reset() {
            underlying = source.iterator();
            return this;
        }
    }
}
