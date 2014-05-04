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

package com.nfsdb.journal.logging;


import java.util.logging.Level;

/**
 * Logger class with lazy evaluation.
 * <p/>
 * Create messages with the %s string converter as follows:
 * String a = "example";
 * String b = "message";
 * log.debug("This is an %s of a %s to log.", a, b);
 */
public class Logger {
    private final java.util.logging.Logger logger;

    /////////////////////////////////////////////////////////////////

    public static Logger getLogger(Class<?> clazz) {
        return new Logger(clazz);
    }

    /////////////////////////////////////////////////////////////////

    public void trace(java.lang.Object message) {
        logger.finest(message.toString());
    }


    public void trace(String format, Object... args) {
        if (isTraceEnabled()) {
            logger.finest(String.format(format, args));
        }
    }

    public boolean isTraceEnabled() {
        return logger.isLoggable(Level.FINEST);
    }


    /////////////////////////////////////////////////////////////////

    public void debug(java.lang.Object message) {
        logger.fine(message.toString());
    }

    public void debug(java.lang.Object message, java.lang.Throwable throwable) {
        logger.log(Level.FINE, message.toString(), throwable);
    }

    public void debug(String format, Object... args) {
        if (isDebugEnabled()) {
            logger.fine(String.format(format, args));
        }
    }

    public boolean isDebugEnabled() {
        return logger.isLoggable(Level.FINE);
    }

    public void debug(String format, java.lang.Throwable throwable, Object... args) {
        if (isDebugEnabled()) {
            logger.log(Level.FINE, String.format(format, args), throwable);
        }
    }

    /////////////////////////////////////////////////////////////////

    public void info(java.lang.Object message) {
        logger.info(message.toString());
    }

    public void info(java.lang.Object message, java.lang.Throwable throwable) {
        logger.log(Level.INFO, message.toString(), throwable);
    }

    public void info(String format, Object... args) {
        if (isInfoEnabled()) {
            logger.info(String.format(format, args));
        }
    }

    public boolean isInfoEnabled() {
        return logger.isLoggable(Level.INFO);
    }

    public void info(String format, java.lang.Throwable throwable, Object... args) {
        if (isInfoEnabled()) {
            logger.log(Level.INFO, String.format(format, args), throwable);
        }
    }

    /////////////////////////////////////////////////////////////////

    public void warn(java.lang.Object message) {
        logger.warning(message.toString());
    }

    /////////////////////////////////////////////////////////////////

    public void error(java.lang.Object message) {
        logger.severe(message.toString());
    }

    public void error(java.lang.Object message, java.lang.Throwable throwable) {
        logger.log(Level.SEVERE, message.toString(), throwable);
    }

    public void error(String format, Object... args) {
        if (isErrorEnabled()) {
            logger.severe(String.format(format, args));
        }
    }

    public boolean isErrorEnabled() {
        return logger.isLoggable(Level.SEVERE);
    }

    public void error(String format, java.lang.Throwable throwable, Object... args) {
        if (isErrorEnabled()) {
            logger.log(Level.SEVERE, String.format(format, args), throwable);
        }
    }

    /////////////////////////////////////////////////////////////////

    private Logger(Class<?> aClass) {
        logger = java.util.logging.Logger.getLogger(aClass.getName());
    }
}
