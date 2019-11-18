/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.regex.impl;

import sun.security.action.GetPropertyAction;


/**
 * Unchecked exception thrown to indicate a syntax error in a
 * regular-expression pattern.
 *
 * @author unascribed
 * @since 1.4
 */

public class PatternSyntaxException
        extends IllegalArgumentException {
    private static final long serialVersionUID = -3864639126226059218L;
    private static final String nl =
            java.security.AccessController
                    .doPrivileged(new GetPropertyAction("line.separator"));
    private final String desc;
    private final String pattern;
    private final int index;

    /**
     * Constructs a new instance of this class.
     *
     * @param desc  A description of the error
     * @param regex The erroneous pattern
     * @param index The approximate index in the pattern of the error,
     *              or <tt>-1</tt> if the index is not known
     */
    public PatternSyntaxException(String desc, String regex, int index) {
        this.desc = desc;
        this.pattern = regex;
        this.index = index;
    }

    /**
     * Retrieves the description of the error.
     *
     * @return The description of the error
     */
    public String getDescription() {
        return desc;
    }

    /**
     * Retrieves the error index.
     *
     * @return The approximate index in the pattern of the error,
     * or <tt>-1</tt> if the index is not known
     */
    public int getIndex() {
        return index;
    }

    /**
     * Returns a multi-line string containing the description of the syntax
     * error and its index, the erroneous regular-expression pattern, and a
     * visual indication of the error index within the pattern.
     *
     * @return The full detail message
     */
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append(desc);
        if (index >= 0) {
            sb.append(" near index ");
            sb.append(index);
        }
        sb.append(nl);
        sb.append(pattern);
        if (index >= 0) {
            sb.append(nl);
            for (int i = 0; i < index; i++) sb.append(' ');
            sb.append('^');
        }
        return sb.toString();
    }

    /**
     * Retrieves the erroneous regular-expression pattern.
     *
     * @return The erroneous pattern
     */
    public String getPattern() {
        return pattern;
    }

}
