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

package io.questdb;

// Constants that enable various diagnostics to catch leaks, double closes, etc.
public class ParanoiaState {
    /**
     * <pre>
     * BASIC -> validates UTF-8 in log records (throws a LogError if invalid),
     *          throws a LogError on abandoned log records (missing .$() at the end of log statement),
     *          detects closed stdout in LogConsoleWriter.
     *          This introduces a low overhead to logging.
     * AGGRESSIVE -> BASIC + holds recent history of log lines to help diagnose closed stdout,
     *               holds the stack trace of abandoned log record.
     *               This introduces a significant overhead to logging.
     *
     * When running inside JUnit/Surefire, BASIC log paranoia mode gets activated automatically.
     * You can manually edit the code in the static { } block below to activate AGGRESSIVE instead.
     *
     * Logs may go silent when Maven Surefire plugin closes stdout due to broken text encoding.
     * In BASIC mode, the log writer will detect this and print errors through System.out, which
     * under Surefire uses an alternate channel and not stdout.
     * In AGGRESSIVE mode, it will additionally remember the most recent log lines and print them.
     * This will help you find the offending log line with broken encoding.
     *
     * The logging framework detects a common coding error where you forget to end a log statement
     * with .$(), causing the statement not to be logged. This problem can only be detected after
     * the fact, when you start a new log record and the previous one wasn't completed.
     *
     * With Log Paranoia off (LOG_PARANOIA_MODE_NONE), we only detect this problem and print an
     * error message.
     * In BASIC mode, we throw a LogError without a stack trace.
     * In AGGRESSIVE mode, we capture the stack trace at every start of a log statement, so when
     * we throw the LogError, it points to the code that created and then abandoned the log record.
     * </pre>
     */
    public static final int LOG_PARANOIA_MODE;
    public static final int LOG_PARANOIA_MODE_AGGRESSIVE = 2;
    public static final int LOG_PARANOIA_MODE_BASIC = 1;
    public static final int LOG_PARANOIA_MODE_NONE = 0;
    // Set to true to enable Thread Local path instances created/closed stack trace logs.
    public static final boolean THREAD_LOCAL_PATH_PARANOIA_MODE = false;
    // Set to true to enable stricter boundary checks on Vm memories implementations.
    public static final boolean VM_PARANOIA_MODE = false;
    // Set to true to enable stricter File Descriptor double close checks, trace closed usages.
    public static boolean FD_PARANOIA_MODE = false;

    public static boolean isInsideJUnitTest() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : stackTrace) {
            String className = element.getClassName();
            if (className.startsWith("org.apache.maven.surefire") || className.startsWith("org.junit.")) {
                return true;
            }
        }
        return false;
    }

    static {
        LOG_PARANOIA_MODE = isInsideJUnitTest() ? LOG_PARANOIA_MODE_BASIC : LOG_PARANOIA_MODE_NONE;
    }
}
