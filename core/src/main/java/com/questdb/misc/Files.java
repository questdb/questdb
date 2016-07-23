/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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
 ******************************************************************************/

package com.questdb.misc;

import com.questdb.ex.JournalException;
import com.questdb.ex.JournalRuntimeException;
import com.questdb.std.LPSZ;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

public final class Files {

    public static final Charset UTF_8;

    private Files() {
    } // Prevent construction.

    public native static void append(long fd, long address, int len);

    public native static int close(long fd);

    public static boolean delete(File file) {
        try {
            deleteOrException(file);
            return true;
        } catch (JournalException e) {
            return false;
        }
    }

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

    public native static long getStdOutFd();

    public static long length(LPSZ lpsz) {
        return length(lpsz.address());
    }

    public static File makeTempDir() {
        File result;
        try {
            result = File.createTempFile("questdb", "");
            deleteOrException(result);
            mkDirsOrException(result);
        } catch (Exception e) {
            throw new JournalRuntimeException("Exception when creating temp dir", e);
        }
        return result;
    }

    public static File makeTempFile() {
        try {
            return File.createTempFile("questdb", "");
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
                byte buffer[]
                        = new byte[(int) fis.getChannel().size()];
                int totalRead = 0;
                int read;
                while (totalRead < buffer.length
                        && (read = fis.read(buffer, totalRead, buffer.length - totalRead)) > 0) {
                    totalRead += read;
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
        if (result) {
            close(fd);
        }
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
            throw new IllegalArgumentException("File must not be null");
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
