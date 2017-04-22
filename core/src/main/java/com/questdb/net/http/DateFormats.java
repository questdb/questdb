package com.questdb.net.http;

import com.questdb.misc.ByteBuffers;
import com.questdb.misc.Unsafe;
import com.questdb.std.ObjList;
import com.questdb.std.str.DirectByteCharSequence;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DateFormats extends ObjList<String> {
    public DateFormats(File dateFormats) throws IOException {

        final DirectByteCharSequence dbcs = new DirectByteCharSequence();

        try (FileInputStream fis = new FileInputStream(dateFormats)) {
            int sz;
            ByteBuffer buf = ByteBuffer.allocateDirect(sz = fis.available());
            try {

                fis.getChannel().read(buf);

                long p = ByteBuffers.getAddress(buf);
                long hi = p + sz;
                long _lo = p;

                boolean newline = true;
                boolean comment = false;

                while (p < hi) {
                    char b = (char) Unsafe.getUnsafe().getByte(p++);

                    switch (b) {
                        case '#':
                            comment = newline;
                            break;
                        case '\n':
                        case '\r':
                            if (!comment && _lo < p - 1) {
                                add(dbcs.of(_lo, p - 1).toString());
                            }
                            newline = true;
                            comment = false;
                            _lo = p;
                            break;
                        default:
                            if (newline) {
                                newline = false;
                            }
                            break;
                    }

                }
            } finally {
                ByteBuffers.release(buf);
            }
        }
    }
}
