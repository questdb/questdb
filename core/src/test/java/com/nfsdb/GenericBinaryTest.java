/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb;

import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.ParserException;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.misc.Rnd;
import com.nfsdb.ql.Record;
import com.nfsdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class GenericBinaryTest extends AbstractTest {

    @Test
    public void testInputObject() throws Exception {
        List<byte[]> bytes = getBytes();
        try (JournalWriter generic = getGenericWriter()) {
            writeInputStream(generic, bytes);
        }
        assertEquals(bytes, readObject(getWriter()));
    }

    @Test
    public void testInputOutput() throws Exception {
        JournalWriter writer = getGenericWriter();
        List<byte[]> expected = getBytes();
        writeInputStream(writer, expected);
        assertEquals(expected, readOutputStream());
    }

    @Test
    public void testOutputInput() throws Exception {
        JournalWriter writer = getGenericWriter();
        List<byte[]> expected = getBytes();
        writeOutputStream(writer, expected);

        List<byte[]> actual = new ArrayList<>();
        for (Record e : compiler.compile(factory, "bintest")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InputStream in = e.getBin(0);

            int b;
            while ((b = in.read()) != -1) {
                out.write(b);
            }
            actual.add(out.toByteArray());
        }

        assertEquals(expected, actual);
    }

    @Test
    public void testOutputOutput() throws Exception {
        JournalWriter writer = getGenericWriter();
        List<byte[]> expected = getBytes();
        writeOutputStream(writer, expected);
        assertEquals(expected, readOutputStream());
    }

    @Test
    public void testUnclosedOutputOutput() throws Exception {
        JournalWriter writer = getGenericWriter();
        List<byte[]> expected = getBytes();
        for (int i = 0; i < expected.size(); i++) {
            JournalEntryWriter w = writer.entryWriter();
            w.putBin(0).write(expected.get(i));
            w.append();
        }
        writer.commit();
        assertEquals(expected, readOutputStream());
    }

    private void assertEquals(List<byte[]> expected, List<byte[]> actual) {
        for (int i = 0; i < expected.size(); i++) {
            Assert.assertArrayEquals(expected.get(i), actual.get(i));
        }
    }

    private List<byte[]> getBytes() {
        Rnd r = new Rnd();
        List<byte[]> bytes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            bytes.add(r.nextBytes((3 - i) * 1024));
        }
        return bytes;
    }

    private JournalWriter getGenericWriter() throws JournalException {
        return factory.writer(new JournalStructure("bintest") {{
                                  $bin("image");
                              }}
        );
    }

    private JournalWriter<BinContainer> getWriter() throws JournalException {
        return factory.writer(BinContainer.class, "bintest");
    }

    private List<byte[]> readObject(Journal<BinContainer> reader) {
        List<byte[]> actual = new ArrayList<>();

        for (BinContainer c : reader) {
            actual.add(c.image.array());
        }

        return actual;
    }

    private List<byte[]> readOutputStream() throws JournalException, ParserException {
        List<byte[]> result = new ArrayList<>();
        for (Record e : compiler.compile(factory, "bintest")) {
            ByteArrayOutputStream o = new ByteArrayOutputStream();
            e.getBin(0, o);
            result.add(o.toByteArray());
        }
        return result;
    }

    private void writeInputStream(JournalWriter writer, List<byte[]> bytes) throws JournalException {
        JournalEntryWriter w;
        for (int i = 0; i < bytes.size(); i++) {
            w = writer.entryWriter();
            w.putBin(0, new ByteArrayInputStream(bytes.get(i)));
            w.append();
        }

        writer.commit();
    }

    private void writeOutputStream(JournalWriter writer, List<byte[]> expected) throws JournalException, IOException {
        for (int i = 0; i < expected.size(); i++) {
            JournalEntryWriter w = writer.entryWriter();
            try (OutputStream out = w.putBin(0)) {
                out.write(expected.get(i));
            }
            w.append();
        }
        writer.commit();

    }

    public static class BinContainer {
        private ByteBuffer image;
    }
}
