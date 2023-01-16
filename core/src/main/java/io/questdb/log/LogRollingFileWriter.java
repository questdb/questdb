/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.log;

import io.questdb.mp.QueueConsumer;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class LogRollingFileWriter extends SynchronizedJob implements Closeable, LogWriter {

    public static final long DEFAULT_SPIN_BEFORE_FLUSH = 100_000;
    private static final int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;
    private final MicrosecondClock clock;
    private final FilesFacade ff;
    private final int level;
    private final TemplateParser locationParser = new TemplateParser();
    private final Path path = new Path();
    private final Path renameToPath = new Path();
    private final RingQueue<LogRecordSink> ring;
    private final SCSequence subSeq;
    private long _wptr;
    private long buf;
    private String bufferSize;
    private long currentLogSizeSum;
    private long currentSize;
    private int fd = -1;
    private long idleSpinCount = 0;
    private String lifeDuration;
    private long lim;
    // can be set via reflection
    private String location;
    private String logDir;
    private final NativeLPSZ logFileName = new NativeLPSZ();
    private String logFileTemplate;
    private int nBufferSize;
    private long nLifeDuration;
    private final FindVisitor removeExpiredLogsVisitor = this::removeExpiredLogsVisitor;
    private long nRollSize;
    private long nSizeLimit;
    private final FindVisitor removeExcessiveLogsVisitor = this::removeExcessiveLogsVisitor;
    private long nSpinBeforeFlush;
    private long rollDeadline;
    private NextDeadline rollDeadlineFunction;
    private String rollEvery;
    private String rollSize;
    private String sizeLimit;
    private final QueueConsumer<LogRecordSink> myConsumer = this::copyToBuffer;
    private String spinBeforeFlush;

    public LogRollingFileWriter(RingQueue<LogRecordSink> ring, SCSequence subSeq, int level) {
        this(FilesFacadeImpl.INSTANCE, MicrosecondClockImpl.INSTANCE, ring, subSeq, level);
    }

    public LogRollingFileWriter(
            FilesFacade ff,
            MicrosecondClock clock,
            RingQueue<LogRecordSink> ring,
            SCSequence subSeq,
            int level
    ) {
        this.ff = ff;
        this.clock = clock;
        this.ring = ring;
        this.subSeq = subSeq;
        this.level = level;
    }

    @Override
    public void bindProperties(LogFactory factory) {
        locationParser.parseEnv(location, clock.getTicks());
        if (this.bufferSize != null) {
            try {
                nBufferSize = Numbers.parseIntSize(this.bufferSize);
            } catch (NumericException e) {
                throw new LogError("Invalid value for bufferSize");
            }
        } else {
            nBufferSize = DEFAULT_BUFFER_SIZE;
        }

        if (this.rollSize != null) {
            try {
                nRollSize = Numbers.parseLongSize(this.rollSize);
            } catch (NumericException e) {
                throw new LogError("Invalid value for rollSize");
            }
        } else {
            nRollSize = Long.MAX_VALUE;
        }

        if (this.sizeLimit != null) {
            try {
                nSizeLimit = Numbers.parseLongSize(this.sizeLimit);
                if(nSizeLimit < nRollSize) {
                    throw new LogError("sizeLimit must be larger than rollSize");
                }
            } catch (NumericException e) {
                throw new LogError("Invalid value for sizeLimit");
            }
        }

        if (this.lifeDuration != null) {
            try {
                nLifeDuration = Numbers.parseLongDuration(lifeDuration);
            } catch (NumericException e) {
                throw new LogError("Invalid value for lifeDuration");
            }
        }

        if (spinBeforeFlush != null) {
            try {
                nSpinBeforeFlush = Numbers.parseLong(spinBeforeFlush);
            } catch (NumericException e) {
                throw new LogError("Invalid value for spinBeforeFlush");
            }
        } else {
            nSpinBeforeFlush = DEFAULT_SPIN_BEFORE_FLUSH;
        }

        if (rollEvery != null) {
            switch (rollEvery.toUpperCase()) {
                case "DAY":
                    rollDeadlineFunction = this::getNextDayDeadline;
                    break;
                case "MONTH":
                    rollDeadlineFunction = this::getNextMonthDeadline;
                    break;
                case "YEAR":
                    rollDeadlineFunction = this::getNextYearDeadline;
                    break;
                case "HOUR":
                    rollDeadlineFunction = this::getNextHourDeadline;
                    break;
                case "MINUTE":
                    rollDeadlineFunction = this::getNextMinuteDeadline;
                    break;
                default:
                    rollDeadlineFunction = this::getInfiniteDeadline;
                    break;
            }
        } else {
            rollDeadlineFunction = this::getInfiniteDeadline;
        }

        this.rollDeadline = rollDeadlineFunction.getDeadline();
        this.buf = _wptr = Unsafe.malloc(nBufferSize, MemoryTag.NATIVE_LOGGER);
        this.lim = buf + nBufferSize;
        openFile();
        logFileTemplate = location.substring(path.toString().lastIndexOf(Files.SEPARATOR) + 1, location.indexOf('$'));
        logDir = location.substring(0, location.indexOf(logFileTemplate) - 1);
    }

    @Override
    public void close() {
        if (buf != 0) {
            if (_wptr > buf) {
                flush();
            }
            Unsafe.free(buf, nBufferSize, MemoryTag.NATIVE_LOGGER);
            buf = 0;
        }
        if (ff.closeChecked(this.fd)) {
            this.fd = -1;
        }
        Misc.free(path);
        Misc.free(renameToPath);
    }

    @Override
    public boolean runSerially() {
        if (subSeq.consumeAll(ring, myConsumer)) {
            return true;
        }

        if (++idleSpinCount > nSpinBeforeFlush && _wptr > buf) {
            flush();
            idleSpinCount = 0;
            return true;
        }
        return false;
    }

    public void setBufferSize(String bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setLifeDuration(String lifeDuration) {
        this.lifeDuration = lifeDuration;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public void setRollEvery(String rollEvery) {
        this.rollEvery = rollEvery;
    }

    public void setRollSize(String rollSize) {
        this.rollSize = rollSize;
    }

    public void setSizeLimit(String sizeLimit) {
        this.sizeLimit = sizeLimit;
    }

    public void setSpinBeforeFlush(String spinBeforeFlush) {
        this.spinBeforeFlush = spinBeforeFlush;
    }

    private void buildFilePath(Path path) {
        path.of("");
        locationParser.toSink(path);
    }

    private void buildUniquePath() {
        buildFilePath(path);
        while (ff.exists(path.$())) {
            pushFileStackUp();
            buildFilePath(path);
        }
    }

    private void copyToBuffer(LogRecordSink sink) {
        final int l = sink.length();
        if ((sink.getLevel() & this.level) != 0 && l > 0) {

            if (_wptr + l >= lim) {
                flush();
            }

            Vect.memcpy(_wptr, sink.getAddress(), l);
            _wptr += l;
        }
    }

    private void flush() {
        long ticks = Long.MIN_VALUE;
        if (currentSize > nRollSize || (ticks = clock.getTicks()) > rollDeadline) {
            ff.close(fd);
            removeOldLogs();
            if (ticks > rollDeadline) {
                rollDeadline = rollDeadlineFunction.getDeadline();
                locationParser.setDateValue(ticks);
            }
            openFile();
        }

        int len = (int) (_wptr - buf);
        if (ff.append(fd, buf, len) != len) {
            throw new LogError("Could not append log [fd=" + fd + "]");
        }
        currentSize += len;
        _wptr = buf;
    }

    private long getInfiniteDeadline() {
        return Long.MAX_VALUE;
    }

    private long getNextDayDeadline() {
        return Timestamps.addDays(Timestamps.floorDD(clock.getTicks()), 1);
    }

    private long getNextHourDeadline() {
        return Timestamps.addHours(Timestamps.floorHH(clock.getTicks()), 1);
    }

    private long getNextMinuteDeadline() {
        return Timestamps.floorMI(clock.getTicks()) + Timestamps.MINUTE_MICROS;
    }

    private long getNextMonthDeadline() {
        return Timestamps.addMonths(Timestamps.floorMM(clock.getTicks()), 1);
    }

    private long getNextYearDeadline() {
        return Timestamps.addYear(Timestamps.floorYYYY(clock.getTicks()), 1);
    }

    private void openFile() {
        buildUniquePath();
        this.fd = ff.openAppend(path.$());
        if (this.fd == -1) {
            throw new LogError("[" + ff.errno() + "] Cannot open file for append: " + path);
        }
        this.currentSize = ff.length(fd);
    }

    private void pushFileStackUp() {
        // find max file index that has space above it
        // for files like:
        // myapp.6.log
        // myapp.5.log
        // myapp.3.log
        // myapp.2.log
        // myapp.1.log
        // myapp.log
        //
        // max file will be .3 because it can be renamed to .4
        // without needing to shift anything

        int index = 1;
        while (true) {
            buildFilePath(path);

            path.put('.').put(index);
            if (ff.exists(path.$())) {
                index++;
            } else {
                break;
            }
        }

        // rename files
        while (index > 1) {
            buildFilePath(path);
            buildFilePath(renameToPath);

            path.put('.').put(index - 1);
            renameToPath.put('.').put(index);
            if (ff.rename(path.$(), renameToPath.$()) != Files.FILES_RENAME_OK) {
                throw new LogError("Could not rename " + path + " to " + renameToPath);
            }
            index--;
        }

        // finally move original file to .1
        buildFilePath(path);
        buildFilePath(renameToPath);
        renameToPath.put(".1");
        if (ff.rename(path.$(), renameToPath.$()) != Files.FILES_RENAME_OK) {
            throw new LogError("Could not rename " + path + " to " + renameToPath);
        }
    }

    private void removeExcessiveLogsVisitor(long filePointer, int type) {
        path.trimTo(logDir.length());
        path.concat(filePointer).$();
        logFileName.of(filePointer);
        if (Files.notDots(filePointer) && type == Files.DT_FILE && Chars.contains(logFileName, logFileTemplate)
                && (currentLogSizeSum += Files.length(path)) > nSizeLimit) {
            if (!ff.remove(path)) {
                throw new LogError("cannot remove: " + path.$());
            }
        }
    }

    private void removeOldLogs() {
        if (this.lifeDuration != null) {
            ff.iterateDir(path.of(logDir).$(), removeExpiredLogsVisitor);
        }
        if (this.sizeLimit != null) {
            currentLogSizeSum = 0;
            ff.iterateDir(path.of(logDir).$(), removeExcessiveLogsVisitor);
        }
    }

    private void removeExpiredLogsVisitor(long filePointer, int type) {
        path.trimTo(logDir.length());
        path.concat(filePointer).$();
        logFileName.of(filePointer);
        if (Files.notDots(filePointer) && type == Files.DT_FILE && Chars.contains(logFileName, logFileTemplate)
                && clock.getTicks() - ff.getLastModified(path.$()) * Timestamps.MILLI_MICROS > nLifeDuration) {
            if (!ff.remove(path)) {
                throw new LogError("cannot remove: " + path.$());
            }
        }
    }

    @FunctionalInterface
    private interface NextDeadline {
        long getDeadline();
    }
}
