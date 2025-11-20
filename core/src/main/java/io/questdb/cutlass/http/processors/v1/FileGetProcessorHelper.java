/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.http.processors.v1;

import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.std.Files;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.http.HttpConstants.*;

/**
 * Helper class for file operations used by file processors.
 * Contains utility methods for extracting file paths, validating paths, and determining content types.
 */
public class FileGetProcessorHelper {

    /**
     * Extracts the file path from the URL.
     * Returns null if no file is specified (directory listing).
     *
     * URL format: /api/v1/imports or /api/v1/imports/file1.parquet
     * Returns the file path if it exists after /imports or /exports.
     */
    public static DirectUtf8Sequence extractFilePathFromUrl(HttpRequestHeader request, FilesRootDir filesRoot) {
        DirectUtf8String url = request.getUrl();
        if (url == null || url.size() == 0) {
            return null;
        }

        final long urlPtr = url.ptr();
        final int urlSize = url.size();

        // Look for "/imports" or "/exports" (without trailing slash first)
        String searchString = filesRoot == FilesRootDir.IMPORTS ? "/imports" : "/exports";
        int searchLen = searchString.length();

        // Find the route segment
        int routeEnd = -1;
        for (int i = 0; i <= urlSize - searchLen; i++) {
            boolean match = true;
            for (int j = 0; j < searchLen; j++) {
                if (url.byteAt(i + j) != searchString.charAt(j)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                routeEnd = i + searchLen;
                break;
            }
        }

        if (routeEnd == -1) {
            return null;
        }

        // Check what comes after the route
        // If we're at the end of URL or next char is '?', there's no file parameter
        if (routeEnd >= urlSize) {
            return null;
        }

        // Next char must be '/' for a file path, or '?' for query params, or end of string
        byte nextChar = url.byteAt(routeEnd);
        if (nextChar == '?') {
            // Query string, no file in path
            return null;
        }

        // Must be a slash to indicate a file path follows
        if (nextChar != '/') {
            // Unexpected character, no file path
            return null;
        }

        // Skip the '/' and extract the file path
        int fileStart = routeEnd + 1;

        // Extract the file path from fileStart to the end or query string
        int endPos = urlSize;
        for (int i = fileStart; i < urlSize; i++) {
            if (url.byteAt(i) == '?') {
                endPos = i;
                break;
            }
        }

        // If nothing after the slash, return null (empty file path)
        if (endPos <= fileStart) {
            return null;
        }

        // Return the extracted file path as a DirectUtf8String with adjusted boundaries
        DirectUtf8String result = new DirectUtf8String();
        result.of(urlPtr + fileStart, urlPtr + endPos);
        return result;
    }

    /**
     * Determines the content type based on file extension.
     */
    public static String getContentType(DirectUtf8Sequence filename) {
        if (Utf8s.endsWithAscii(filename, ".parquet")) {
            return CONTENT_TYPE_PARQUET;
        } else if (Utf8s.endsWithAscii(filename, ".csv")) {
            return CONTENT_TYPE_CSV;
        } else if (Utf8s.endsWithAscii(filename, ".json")) {
            return CONTENT_TYPE_JSON;
        } else if (Utf8s.endsWithAscii(filename, ".txt")) {
            return CONTENT_TYPE_TEXT;
        }
        return CONTENT_TYPE_OCTET_STREAM;
    }

    /**
     * Validates that the filename doesn't contain absolute or relative path traversal.
     * Returns true if path traversal is detected (should be rejected).
     */
    public static boolean containsAbsOrRelativePath(DirectUtf8Sequence filename) {
        if (filename.byteAt(0) == Files.SEPARATOR) {
            return true;
        }
        // check for windows
        if (filename.size() >= 2 && filename.byteAt(1) == ':') {
            return true;
        }

        return Utf8s.containsAscii(filename, "../") || Utf8s.containsAscii(filename, "..\\");
    }
}
