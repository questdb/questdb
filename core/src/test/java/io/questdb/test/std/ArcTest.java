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

package io.questdb.test.std;

import io.questdb.std.Arc;
import io.questdb.std.QuietCloseable;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;


public class ArcTest {
    @Test
    public void testDowncast() {
        final Arc<Dog> d1 = new Arc<>(new Dog(() -> {
        }));
        final Arc<Animal> a1 = Arc.upcast(d1);
        final Arc<Dog> d2 = Arc.downcast(a1, Dog.class);
        Assert.assertSame(d1, d2);
        final ClassCastException cce = Assert.assertThrows(ClassCastException.class, () -> {
            Arc.downcast(a1, Squid.class);
        });
        TestUtils.assertContains(cce.getMessage(), "Dog");
        TestUtils.assertContains(cce.getMessage(), "Squid");
        Assert.assertEquals(1, d1.getRefCount());
    }

    @Test
    public void testRefcount() {
        final AtomicInteger closeCount = new AtomicInteger(0);

        final Arc<Dog> d1 = new Arc<>(new Dog(closeCount::incrementAndGet));
        Assert.assertEquals(1, d1.getRefCount());

        final Arc<Dog> d2 = d1.incref();
        Assert.assertEquals(2, d1.getRefCount());
        Assert.assertEquals(2, d2.getRefCount());
        Assert.assertSame(d1, d2);

        d1.close();
        Assert.assertEquals(0, closeCount.get());
        Assert.assertEquals(1, d2.getRefCount());
        d2.close();
        Assert.assertEquals(1, closeCount.get());
        Assert.assertEquals(0, d2.getRefCount());
    }

    @Test
    public void testTryWithResource() {
        final AtomicInteger closeCount = new AtomicInteger();
        try (Arc<Dog> dog = new Arc<>(new Dog(closeCount::incrementAndGet))) {
            Assert.assertEquals("Max", dog.get().getName());
        }
        Assert.assertEquals(1, closeCount.get());
    }

    @Test
    public void testUpcast() {
        final AtomicInteger closeCount = new AtomicInteger(0);
        final Arc<Dog> d1 = new Arc<>(new Dog(closeCount::incrementAndGet));
        final Arc<Vertebrate> v1 = Arc.upcast(d1);
        Assert.assertEquals(1, v1.getRefCount());
        Assert.assertSame(d1, v1);
        v1.close();
        Assert.assertEquals(1, closeCount.get());
    }

    public static class Animal implements QuietCloseable {
        private final Runnable onClose;

        public Animal(Runnable onClose) {
            this.onClose = onClose;
        }

        @Override
        public void close() {
            this.onClose.run();
        }
    }

    public static class Dog extends Vertebrate {
        public Dog(Runnable onClose) {
            super(onClose);
        }

        public String getName() {
            return "Max";
        }
    }

    public static class Squid extends Animal {
        private final int inkQuantity;

        public Squid(Runnable onClose, int inkQuantity) {
            super(onClose);
            this.inkQuantity = inkQuantity;
        }

        public int getInkQuantity() {
            return inkQuantity;
        }
    }

    public static class Vertebrate extends Animal {
        public Vertebrate(Runnable onClose) {
            super(onClose);
        }
    }
}
