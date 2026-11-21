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


package io.questdb.test.griffin.engine.table;

import io.questdb.griffin.engine.table.HiddenClassCopies;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * The fused GROUP BY gives every factory a hidden copy of its row updater through
 * {@link HiddenClassCopies}. A copy must have a class of its own that runs the template's code, and
 * when no copy can be made, the caller gets the shared template instead of an error.
 */
public class HashJoinGroupByHiddenClassCopiesTest {

    @Test
    public void testCopiesHaveHiddenClassesOfTheirOwn() {
        HiddenClassCopies<Object> copies = new HiddenClassCopies<>(Object.class,
                HiddenClassCopiesTemplate.class, HiddenClassCopiesTemplate::new);
        Object first = copies.newInstance();
        Object second = copies.newInstance();
        assertCopy(first, HiddenClassCopiesTemplate.class);
        assertCopy(second, HiddenClassCopiesTemplate.class);
        Assert.assertNotSame(first.getClass(), second.getClass());
    }

    @Test
    public void testFallsBackWhenClassFileIsMissing() {
        // A nested class has no class file under its simple name, so its bytecode cannot be read.
        HiddenClassCopies<Object> copies = new HiddenClassCopies<>(Object.class, NestedTemplate.class, NestedTemplate::new);
        Object instance = copies.newInstance();
        Assert.assertSame(NestedTemplate.class, instance.getClass());
        Assert.assertSame(NestedTemplate.class, copies.newInstance().getClass());
    }

    @Test
    public void testFallsBackWhenClassFileIsNotAClass() throws IOException {
        // The JVM refuses to define a copy from bytes that are no class file.
        assertFallsBack(new ByteArrayInputStream(new byte[]{1, 2, 3, 4}));
    }

    @Test
    public void testFallsBackWhenClassFileIsUnreadable() throws IOException {
        assertFallsBack(new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("injected class file read failure");
            }
        });
    }

    @Test
    public void testFallsBackWhenCopyConstructorFails() {
        Object fallback = new Object();
        HiddenClassCopies<Object> copies = new HiddenClassCopies<>(Object.class,
                HiddenClassCopiesFailingTemplate.class, () -> fallback);
        Assert.assertSame(fallback, copies.newInstance());
        Assert.assertSame(fallback, copies.newInstance());
    }

    @Test
    public void testServedClassFileIsCopied() throws IOException {
        // The control for the fallbacks that serve a broken class file: serving the template's own
        // class file the same way makes copies.
        Class<?> template = new ClassFileServingLoader(new ByteArrayInputStream(templateClassFile())).defineTemplate();
        Assert.assertNotSame(HiddenClassCopiesTemplate.class, template);
        HiddenClassCopies<Object> copies = new HiddenClassCopies<>(Object.class, template, Object::new);
        assertCopy(copies.newInstance(), template);
    }

    private static void assertCopy(Object copy, Class<?> template) {
        Class<?> copyClass = copy.getClass();
        Assert.assertTrue(copyClass.getName(), copyClass.isHidden());
        Assert.assertNotSame(template, copyClass);
        // A hidden class takes the name in its bytecode, followed by a suffix of its own.
        Assert.assertTrue(copyClass.getName(), copyClass.getName().startsWith(template.getName() + '/'));
        Assert.assertSame(template.getClassLoader(), copyClass.getClassLoader());
        Assert.assertSame(Object.class, copyClass.getSuperclass());
        Assert.assertEquals("template", copy.toString());
    }

    private static void assertFallsBack(InputStream classFile) throws IOException {
        Class<?> template = new ClassFileServingLoader(classFile).defineTemplate();
        Object fallback = new Object();
        HiddenClassCopies<Object> copies = new HiddenClassCopies<>(Object.class, template, () -> fallback);
        Assert.assertSame(fallback, copies.newInstance());
        Assert.assertSame(fallback, copies.newInstance());
    }

    private static byte[] templateClassFile() throws IOException {
        try (InputStream in = HiddenClassCopiesTemplate.class.getResourceAsStream("HiddenClassCopiesTemplate.class")) {
            Assert.assertNotNull(in);
            return in.readAllBytes();
        }
    }

    /**
     * Defines {@link HiddenClassCopiesTemplate} once more, in an unnamed module of its own, and
     * answers every read of the template's class file with the stream the test passes. A class
     * outside a named module looks its resources up through its class loader, so the test decides
     * what {@link HiddenClassCopies} reads as the template's bytecode.
     */
    private static final class ClassFileServingLoader extends ClassLoader {
        private static final String CLASS_FILE = HiddenClassCopiesTemplate.class.getName().replace('.', '/') + ".class";
        private final InputStream classFile;

        private ClassFileServingLoader(InputStream classFile) {
            super(HashJoinGroupByHiddenClassCopiesTest.class.getClassLoader());
            this.classFile = classFile;
        }

        @Override
        public InputStream getResourceAsStream(String name) {
            return CLASS_FILE.equals(name) ? classFile : super.getResourceAsStream(name);
        }

        private Class<?> defineTemplate() throws IOException {
            byte[] bytecode = templateClassFile();
            return defineClass(HiddenClassCopiesTemplate.class.getName(), bytecode, 0, bytecode.length);
        }
    }

    public static class NestedTemplate {
    }
}
