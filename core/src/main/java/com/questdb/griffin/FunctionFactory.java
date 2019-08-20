/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.griffin;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.Function;
import com.questdb.std.ObjList;

public interface FunctionFactory {

    /**
     * Function signature in a form of "name(type...)". Name is a literal that does not
     * start with number and contains no control characters, which can be confused with
     * SQL language punctuation. Control characters include but not limited to:
     * ',', '(', ')', '*', '/', '%', '+', '-', '='.
     * <p>
     * Argument types are represented by single character from this table:
     * <ul>
     * <li>B = byte</li>
     * <li>E = short</li>
     * <li>I = int</li>
     * <li>L = long</li>
     * <li>F = float</li>
     * <li>D = double</li>
     * <li>S = string</li>
     * <li>A = char</li>
     * <li>K = symbol</li>
     * <li>T = boolean</li>
     * <li>M = date</li>
     * <li>N = timestamp</li>
     * <li>U = binary</li>
     * <li>V = variable argument list</li>
     * <li>C = cursor</li>
     * </ul>
     *
     * @return signature, for example "substr(SII)"
     */
    String getSignature();

    default boolean isGroupBy() {
        return false;
    }

    Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException;
}
