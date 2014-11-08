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

package org.nfsdb.examples.blob;

import com.nfsdb.journal.JournalEntryWriter;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.factory.configuration.JournalStructure;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class FileSystemIndexer {
    public static void main(String[] args) throws JournalException, IOException {
        JournalFactory factory = new JournalFactory(args[0]);


        JournalWriter writer = factory.writer(new JournalStructure("files") {{
            $sym("name").index();
            $bin("data");
            $ts();
        }});

        long t = System.currentTimeMillis();
        int count = processDir(writer, new File("/Users/vlad/dev/site/nfsdb"));
        System.out.println("Added " + count + " files in " + (System.currentTimeMillis() - t) + " ms.");
    }

    public static int processDir(JournalWriter writer, File dir) throws JournalException, IOException {
        int count = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {

                if (f.isDirectory()) {
                    count += processDir(writer, f);
                    continue;
                }

                JournalEntryWriter w = writer.entryWriter(System.currentTimeMillis());
                w.putSym(0, f.getAbsolutePath());

                try (InputStream s = new FileInputStream(f)) {
                    w.putBin(1, s);
                }

                w.append();
                count++;
            }
        }

        return count;
    }
}
