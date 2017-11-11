package com.questdb.parser.typeprobe;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Files;
import com.questdb.misc.Os;
import com.questdb.misc.Unsafe;
import com.questdb.std.ObjList;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.std.time.DateLocale;
import com.questdb.std.time.DateLocaleFactory;

import java.io.IOException;

public class TypeProbeCollection {
    private static final Log LOG = LogFactory.getLog(TypeProbeCollection.class);

    private static final ObjList<String> DEFAULT_DATE_FORMATS = new ObjList<>();
    private final ObjList<TypeProbe> probes = new ObjList<>();
    private final int probeCount;

    public TypeProbeCollection() {
        addDefaultProbes();
        DateFormatFactory dateFormatFactory = new DateFormatFactory();
        DateLocale dateLocale = DateLocaleFactory.INSTANCE.getDefaultDateLocale();
        for (int i = 0, n = DEFAULT_DATE_FORMATS.size(); i < n; i++) {
            probes.add(new DateProbe(dateFormatFactory, dateLocale, DEFAULT_DATE_FORMATS.getQuick(i)));
        }
        this.probeCount = probes.size();
    }

    public TypeProbeCollection(CharSequence file, DateFormatFactory dateFormatFactory, DateLocaleFactory dateLocaleFactory) throws IOException {
        addDefaultProbes();
        parseFile(file, dateFormatFactory, dateLocaleFactory);
        this.probeCount = probes.size();
    }

    public TypeProbe getProbe(int index) {
        return probes.getQuick(index);
    }

    public int getProbeCount() {
        return probeCount;
    }

    private void addDefaultProbes() {
        probes.add(new IntProbe());
        probes.add(new LongProbe());
        probes.add(new DoubleProbe());
        probes.add(new BooleanProbe());
    }

    private void parseFile(CharSequence fileName, DateFormatFactory dateFormatFactory, DateLocaleFactory dateLocaleFactory) throws IOException {
        final DirectByteCharSequence dbcs = new DirectByteCharSequence();
        try (CompositePath path = new CompositePath().of(fileName).$()) {
            long fd = Files.openRO(path);
            if (fd < 0) {
                throw new IOException("Cannot open " + fileName + " [errno=" + Os.errno() + ']');
            }


            long sz = Files.length(fd);
            long buf = Unsafe.malloc(sz);
            try {

                Files.read(fd, buf, sz, 0);

                long p = buf;
                long hi = p + sz;
                long _lo = p;

                boolean newline = true;
                boolean comment = false;
                boolean quote = false;
                boolean space = true;

                String pattern = null;

                while (p < hi) {
                    char b = (char) Unsafe.getUnsafe().getByte(p++);

                    switch (b) {
                        case '#':
                            comment = newline;
                            break;
                        case '\'':
                            // inside comment, ignore
                            if (comment) {
                                continue;
                            }

                            if (quote) {
                                // we were inside quote, close out and check which part to assign result to
                                if (pattern == null) {
                                    pattern = dbcs.of(_lo, p - 1).toString();
                                    _lo = p;
                                    space = true;
                                    quote = false;
                                } else {
                                    // pattern has been assigned, should never end up here
                                    LOG.error().$("Internal error").$();
                                }
                            } else if (newline) {
                                // only start quote if it is at beginning of line
                                _lo = p;
                                quote = true;
                            }
                            break;
                        case ' ':
                        case '\t':
                            if (comment || quote) {
                                continue;
                            }

                            if (space) {
                                _lo = p;
                                continue;
                            }

                            space = true;
                            newline = false;

                            String s = dbcs.of(_lo, p - 1).toString();
                            if (pattern == null) {
                                pattern = s;
                                _lo = p;
                                space = true;
                            } else {
                                DateLocale locale = dateLocaleFactory.getDateLocale(s);
                                if (locale == null) {
                                    LOG.error().$("Unknown date locale: ").$(s).$();
                                    // skip rest of line
                                    comment = true;
                                    continue;
                                }
                                probes.add(new DateProbe(dateFormatFactory, locale, pattern));
                            }
                            break;
                        case '\n':
                        case '\r':
                            if (!comment) {
                                if (_lo < p - 1) {
                                    s = dbcs.of(_lo, p - 1).toString();
                                    if (pattern == null) {
                                        // no date locale, use default
                                        probes.add(new DateProbe(dateFormatFactory, dateLocaleFactory.getDefaultDateLocale(), s));
                                    } else {
                                        DateLocale locale = dateLocaleFactory.getDateLocale(s);
                                        if (locale == null) {
                                            LOG.error().$("Unknown date locale: ").$(s).$();
                                        } else {
                                            probes.add(new DateProbe(dateFormatFactory, locale, pattern));
                                        }
                                    }
                                } else if (pattern != null) {
                                    probes.add(new DateProbe(dateFormatFactory, dateLocaleFactory.getDefaultDateLocale(), pattern));
                                }
                            }

                            newline = true;
                            comment = false;
                            quote = false;
                            pattern = null;
                            space = false;
                            _lo = p;
                            break;
                        default:
                            if (newline) {
                                newline = false;
                            }

                            if (space) {
                                space = false;
                            }
                            break;
                    }
                }
            } finally {
                Unsafe.free(buf, sz);
            }
        }
    }

    static {
        DEFAULT_DATE_FORMATS.add(DateFormatUtils.UTC_PATTERN);
        DEFAULT_DATE_FORMATS.add("yyyy-MM-dd HH:mm:ss");
        DEFAULT_DATE_FORMATS.add("dd/MM/y");
        DEFAULT_DATE_FORMATS.add("MM/dd/y");
    }
}
