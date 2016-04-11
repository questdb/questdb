/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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

package com.nfsdb.query.spi;

import com.nfsdb.Journal;
import com.nfsdb.query.api.QueryHead;
import com.nfsdb.query.api.QueryHeadBuilder;

public class QueryHeadImpl<T> implements QueryHead<T> {
    private final Journal<T> journal;

    public QueryHeadImpl(Journal<T> journal) {
        this.journal = journal;
    }

    @Override
    public QueryHeadBuilder<T> withKeys(String... values) {
        return withSymValues(journal.getMetadata().getKeyColumn(), values);
    }

    @Override
    public QueryHeadBuilder<T> withSymValues(String symbol, String... values) {
        QueryHeadBuilderImpl<T> impl = new QueryHeadBuilderImpl<>(journal);
        impl.setSymbol(symbol, values);
        return impl;
    }
}
