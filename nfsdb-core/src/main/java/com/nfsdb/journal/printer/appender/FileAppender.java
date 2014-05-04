/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.printer.appender;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileAppender implements Appender {

    private FileOutputStream fos;
    private OutputStreamAppender delegate;

    public FileAppender(File file) throws FileNotFoundException {
        this.fos = new FileOutputStream(file);
        this.delegate = new OutputStreamAppender(fos);
    }

    @Override
    public void append(StringBuilder stringBuilder) throws IOException {
        delegate.append(stringBuilder);
    }


    @Override
    public void close() throws IOException {
        delegate.close();
        fos.close();
    }
}
