/*******************************************************************************
 * _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 * <p/>
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.io.parser;

import com.nfsdb.io.ImportSchema;
import com.nfsdb.io.parser.listener.InputAnalysisListener;
import com.nfsdb.io.parser.listener.Listener;
import com.nfsdb.std.Mutable;

import java.io.Closeable;

public interface TextParser extends Closeable, Mutable {

    void analyse(ImportSchema hints, long addr, int len, int sampleSize, InputAnalysisListener lsnr);

    int getLineCount();

    void parse(long lo, long len, int lim, Listener listener);

    void parseLast();

    /**
     * Prepares parser to re-parse input keeping metadata intact.
     */
    void restart();

    void setHeader(boolean header);

}
