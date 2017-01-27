/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.factory.configuration;

public abstract class AbstractGenericMetadataBuilder {
    final ColumnMetadata meta;
    final JournalStructure parent;

    AbstractGenericMetadataBuilder(JournalStructure parent, ColumnMetadata meta) {
        this.parent = parent;
        this.meta = meta;
    }

    public JournalStructure $() {
        return parent;
    }

    public GenericBinaryBuilder $bin(String name) {
        return parent.$bin(name);
    }

    public JournalStructure $bool(String name) {
        return parent.$bool(name);
    }

    public JournalStructure $byte(String name) {
        return parent.$byte(name);
    }

    public JournalStructure $date(String name) {
        return parent.$date(name);
    }

    public JournalStructure $double(String name) {
        return parent.$double(name);
    }

    public JournalStructure $float(String name) {
        return parent.$float(name);
    }

    public GenericIndexedBuilder $int(String name) {
        return parent.$int(name);
    }

    public GenericIndexedBuilder $long(String name) {
        return parent.$long(name);
    }

    public JournalStructure $short(String name) {
        return parent.$short(name);
    }

    public GenericStringBuilder $str(String name) {
        return parent.$str(name);
    }

    public GenericSymbolBuilder $sym(String name) {
        return parent.$sym(name);
    }

    public JournalStructure $ts(String name) {
        return parent.$ts(name);
    }

    public JournalStructure $ts() {
        return parent.$ts();
    }

    public JournalStructure recordCountHint(int hint) {
        return parent.recordCountHint(hint);
    }
}
