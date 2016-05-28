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

package com.questdb.std;

public class FileNameExtractorCharSequence extends AbstractCharSequence {

    private final static ThreadLocal<FileNameExtractorCharSequence> SINGLETON =
            new ThreadLocal<>(new ObjectFactory<FileNameExtractorCharSequence>() {
                @Override
                public FileNameExtractorCharSequence newInstance() {
                    return new FileNameExtractorCharSequence();
                }
            });

    private static final char separator;
    private CharSequence base;
    private int lo;
    private int hi;

    public static CharSequence get(CharSequence that) {
        return SINGLETON.get().of(that);
    }

    @Override
    public int length() {
        return hi - lo;
    }

    @Override
    public char charAt(int index) {
        return base.charAt(lo + index);
    }

    public CharSequence of(CharSequence base) {
        this.base = base;
        this.hi = base.length();
        this.lo = 0;
        for (int i = hi - 1; i > -1; i--) {
            if (base.charAt(i) == separator) {
                this.lo = i + 1;
                break;
            }
        }
        return this;
    }

    static {
        separator = System.getProperty("file.separator").charAt(0);
    }
}
