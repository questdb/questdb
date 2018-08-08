/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.ops;

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.QueryError;
import com.questdb.ql.ops.regex.Matcher;
import com.questdb.ql.ops.regex.Pattern;
import com.questdb.ql.ops.regex.PatternSyntaxException;
import com.questdb.std.IntHashSet;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;
import com.questdb.store.StorageFacade;
import com.questdb.store.SymbolTable;

public class SymRegexOperator extends AbstractBinaryOperator {

    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new SymRegexOperator(position);
    private final IntHashSet set = new IntHashSet();
    private Matcher matcher;

    private SymRegexOperator(int position) {
        super(ColumnType.BOOLEAN, position);
    }

    @Override
    public boolean getBool(Record rec) {
        return set.contains(lhs.getInt(rec));
    }

    @Override
    public void prepare(StorageFacade facade) {
        super.prepare(facade);
        set.clear();
        SymbolTable tab = lhs.getSymbolTable();
        for (int i = 0, n = tab.size(); i < n; i++) {
            if (matcher.reset(tab.value(i)).find()) {
                set.add(i);
            }
        }
    }

    @Override
    public void setRhs(VirtualColumn rhs) throws ParserException {
        super.setRhs(rhs);
        CharSequence pattern = rhs.getFlyweightStr(null);
        if (pattern == null) {
            throw QueryError.$(rhs.getPosition(), "null regex?");
        }

        try {
            matcher = Pattern.compile(pattern.toString()).matcher("");
        } catch (PatternSyntaxException e) {
            throw QueryError.position(rhs.getPosition() + e.getIndex() + 2).$("Regex syntax error. ").$(e.getDescription()).$();
        }
    }
}
