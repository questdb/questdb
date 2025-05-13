package io.questdb.preferences;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cutlass.json.JsonException;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.PropServerConfiguration.JsonPropertyValueFormatter.str;
import static io.questdb.cutlass.http.HttpConstants.METHOD_POST;
import static io.questdb.cutlass.http.HttpConstants.METHOD_PUT;

public class PreferencesStore implements Closeable {
    private static final String PREFERENCES_FILE_NAME = "_preferences~store";
    private final BlockFileReader blockFileReader;
    private final BlockFileWriter blockFileWriter;
    private final CairoConfiguration configuration;
    private final CharSequenceObjHashMap<CharSequence> parserMap;
    private final Path path = new Path();
    private final PreferencesMap preferencesMap;
    private final PreferencesParser preferencesParser;
    private final int rootLen;
    private long version = 0L;

    public PreferencesStore(CairoConfiguration configuration) {
        this.configuration = configuration;

        preferencesMap = new PreferencesMap(configuration);
        parserMap = new CharSequenceObjHashMap<>();
        preferencesParser = new PreferencesParser(configuration, parserMap);

        path.of(configuration.getDbRoot());
        rootLen = path.size();
        blockFileWriter = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
        blockFileReader = new BlockFileReader(configuration);
    }

    public synchronized void appendToSettingsSink(Utf8StringSink settings) {
        settings.putAscii("\"preferences\":{");
        final ObjList<CharSequence> keys = preferencesMap.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final CharSequence key = keys.getQuick(i);
            final CharSequence value = preferencesMap.get(key);
            str(key, value, settings);
        }
        if (keys.size() > 0) {
            settings.clear(settings.size() - 1);
        }
        settings.putAscii('}');
    }

    @Override
    public void close() throws IOException {
        Misc.free(preferencesParser);
        Misc.free(blockFileReader);
        Misc.free(blockFileWriter);
        Misc.free(path);
    }

    public long getVersion() {
        return version;
    }

    public synchronized void init() {
        final LPSZ configPath = configFilePath();
        if (configuration.getFilesFacade().exists(configPath)) {
            load(configPath, preferencesMap);
        }
    }

    public synchronized void save(DirectUtf8Sink sink, Mode mode, long expectedVersion) {
        if (version != expectedVersion) {
            throw CairoException.preferencesOutOfDate(version, expectedVersion);
        }

        try {
            preferencesParser.clear();
            preferencesParser.parse(sink);
        } catch (JsonException e) {
            throw CairoException.nonCritical()
                    .put("Malformed preferences message [error=").put(e.getFlyweightMessage())
                    .put(", preferences=").put(sink)
                    .put(']');
        }

        switch (mode) {
            case OVERWRITE:
                overwrite();
                break;
            case MERGE:
                merge();
                break;
            default:
                throw CairoException.nonCritical().put("Invalid mode [mode=").put(mode.name()).put(']');
        }
        version++;
    }

    private LPSZ configFilePath() {
        return path.trimTo(rootLen).concat(PREFERENCES_FILE_NAME).$();
    }

    private void load(LPSZ configPath, PreferencesMap map) {
        map.clear();
        try (BlockFileReader reader = blockFileReader) {
            reader.of(configPath);

            final BlockFileReader.BlockCursor cursor = reader.getCursor();
            map.readFromBlock(cursor);
        }
    }

    private void merge() {
        preferencesMap.putAll(parserMap);
        persist(preferencesMap);
    }

    private void overwrite() {
        preferencesMap.clear();
        merge();
    }

    private void persist(PreferencesMap map) {
        try (BlockFileWriter writer = blockFileWriter) {
            writer.of(configFilePath());
            final AppendableBlock block = writer.append();
            map.writeToBlock(block);
            writer.commit();
        }
    }

    public enum Mode {
        MERGE, OVERWRITE;

        public static Mode of(Utf8Sequence method) {
            if (Utf8s.equalsNcAscii(METHOD_PUT, method)) {
                return OVERWRITE;
            }
            if (Utf8s.equalsNcAscii(METHOD_POST, method)) {
                return MERGE;
            }
            throw CairoException.nonCritical().put("Unsupported HTTP method [method=").put(method).put(']');
        }
    }
}
