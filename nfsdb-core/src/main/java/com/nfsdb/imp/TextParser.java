/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.imp;

public interface TextParser {
    int getLineCount();

    void parse(long lo, long len, int lim, Listener listener);

    /**
     * Resets parser including metadata.
     */
    void reset();

    /**
     * Prepares parser to re-parse input keeping metadata intact.
     */
    void restart();

    void setHeader(boolean header);

}
