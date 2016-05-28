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
 ******************************************************************************/

package com.questdb.printer.appender;

public class AssertingAppender implements Appender {

    private final String[] expected;
    private int index;

    public AssertingAppender(String expected) {
        this.expected = expected.split("\n");
    }

    @Override
    public void append(StringBuilder stringBuilder) {
        if (index < expected.length) {
            String s = stringBuilder.toString();
            if (!expected[index].equals(s)) {
                throw new AssertionError(("\n\n>>>> Expected [ " + (index + 1) + " ]>>>>\n") + expected[index] + "\n<<<<  Actual  <<<<\n" + s + '\n');
            }
        } else {
            throw new AssertionError("!!! Expected " + expected.length + " lines, actual " + index);
        }
        index++;
    }

    @Override
    public void close() {
        if (index < expected.length) {
            throw new AssertionError("!!! Too few rows. Expected " + expected.length + ", actual " + index);
        }
    }
}
