/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.misc;

import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.std.LPSZ;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
public final class Files {

    public static final Charset UTF_8;

    private Files() {
    } // Prevent construction.

    public native static long append(long fd, long address, int len);

    public native static int close(long fd);

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_RETURN_FALSE")
    public static boolean delete(File file) {
        try {
            deleteOrException(file);
            return true;
        } catch (JournalException e) {
            return false;
        }
    }

    @SuppressFBWarnings({"MDM_THREAD_YIELD"})
    public static void deleteOrException(File file) throws JournalException {
        if (!file.exists()) {
            return;
        }
        deleteDirContentsOrException(file);

        int retryCount = 3;
        boolean deleted = false;
        while (retryCount > 0 && !(deleted = file.delete())) {
            retryCount--;
            Thread.yield();
        }

        if (!deleted) {
            throw new JournalException("Cannot delete file %s", file);
        }
    }

    public static boolean exists(LPSZ lpsz) {
        return getLastModified(lpsz) != -1;
    }

    public static long getLastModified(LPSZ lpsz) {
        return getLastModified(lpsz.address());
    }

    public static long length(LPSZ lpsz) {
        return length(lpsz.address());
    }

    public static File makeTempDir() {
        File result;
        try {
            result = File.createTempFile("nfsdb", "");
            deleteOrException(result);
            mkDirsOrException(result);
        } catch (Exception e) {
            throw new JournalRuntimeException("Exception when creating temp dir", e);
        }
        return result;
    }

    public static File makeTempFile() {
        try {
            return File.createTempFile("nfsdb", "");
        } catch (IOException e) {
            throw new JournalRuntimeException(e);
        }

    }

    public static void mkDirsOrException(File dir) {
        if (!dir.mkdirs()) {
            throw new JournalRuntimeException("Cannot create temp directory: %s", dir);
        }
    }

    public static long openAppend(LPSZ lpsz) {
        return openAppend(lpsz.address());
    }

    public static long openRO(LPSZ lpsz) {
        return openRO(lpsz.address());
    }

    public static long openRW(LPSZ lpsz) {
        return openRW(lpsz.address());
    }

    public native static long read(long fd, long address, int len, long offset);

    public static String readStringFromFile(File file) throws JournalException {
        try {
            try (FileInputStream fis = new FileInputStream(file)) {
                byte buffer[] = new byte[(int) fis.getChannel().size()];
                byte b;
                int index = 0;
                while ((b = (byte) fis.read()) != -1) {
                    buffer[index++] = b;
                }
                return new String(buffer, UTF_8);
            }
        } catch (IOException e) {
            throw new JournalException("Cannot read from %s", e, file.getAbsolutePath());
        }
    }

    public static boolean setLastModified(LPSZ lpsz, long millis) {
        return setLastModified(lpsz.address(), millis);
    }

    public static boolean touch(LPSZ lpsz) {
        long fd = openRW(lpsz);
        boolean result = fd > 0;
        close(fd);
        return result;
    }

    public native static long write(long fd, long address, int len, long offset);

    public static void writeStringToFile(File file, String s) throws JournalException {
        try {
            try (FileOutputStream fos = new FileOutputStream(file)) {
                fos.write(s.getBytes(UTF_8));
            }
        } catch (IOException e) {
            throw new JournalException("Cannot write to %s", e, file.getAbsolutePath());
        }
    }

    private native static long getLastModified(long lpszName);

    private native static long length(long lpszName);

    private native static long openRO(long lpszName);

    private native static long openRW(long lpszName);

    private native static long openAppend(long lpszName);

    private native static boolean setLastModified(long lpszName, long millis);

    @SuppressFBWarnings({"LEST_LOST_EXCEPTION_STACK_TRACE"})
    private static void deleteDirContentsOrException(File file) throws JournalException {
        if (!file.exists()) {
            return;
        }
        try {
            if (notSymlink(file)) {
                File[] files = file.listFiles();
                if (files != null) {
                    for (int i = 0; i < files.length; i++) {
                        deleteOrException(files[i]);
                    }
                }
            }
        } catch (IOException e) {
            throw new JournalException("Cannot delete dir contents: %s", file, e);
        }
    }

    private static boolean notSymlink(File file) throws IOException {
        if (file == null) {
            throw new NullPointerException("File must not be null");
        }
        if (File.separatorChar == '\\') {
            return true;
        }

        File fileInCanonicalDir;
        if (file.getParentFile() == null) {
            fileInCanonicalDir = file;
        } else {
            File canonicalDir = file.getParentFile().getCanonicalFile();
            fileInCanonicalDir = new File(canonicalDir, file.getName());
        }

        return fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile());
    }

    static {
        UTF_8 = Charset.forName("UTF-8");
    }
}
