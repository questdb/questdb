package io.questdb.preferences;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cutlass.http.HttpKeywords;
import io.questdb.cutlass.json.JsonException;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.PropServerConfiguration.JsonPropertyValueFormatter.str;

public class SettingsStore implements Closeable {
    public static final String PREFERENCES_FILE_NAME = "_preferences~store";
    private final BlockFileReader blockFileReader;
    private final BlockFileWriter blockFileWriter;
    private final CairoConfiguration configuration;
    private final CharSequenceObjHashMap<CharSequence> parserMap;
    private final Path path = new Path();
    private final PreferencesMap preferencesMap;
    private final PreferencesParser preferencesParser;
    private final int rootLen;
    private PreferencesUpdateListener listener;
    private long version = 0L;

    public SettingsStore(CairoConfiguration configuration) {
        this.configuration = configuration;

        preferencesMap = new PreferencesMap(configuration);
        parserMap = new CharSequenceObjHashMap<>();
        preferencesParser = new PreferencesParser(configuration, parserMap);

        path.of(configuration.getDbRoot());
        rootLen = path.size();
        blockFileWriter = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
        blockFileReader = new BlockFileReader(configuration);
    }

    @Override
    public void close() throws IOException {
        Misc.free(preferencesParser);
        Misc.free(blockFileReader);
        Misc.free(blockFileWriter);
        Misc.free(path);
    }

    public synchronized void exportPreferences(Utf8StringSink settings) {
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

    public long getVersion() {
        return version;
    }

    public synchronized void init() {
        final LPSZ preferencesPath = preferencesFilePath();
        if (configuration.getFilesFacade().exists(preferencesPath)) {
            load(preferencesPath, preferencesMap);
        }
    }

    /**
     * Calls the listener with the current preferences state without registering it.
     * For testing purposes only.
     */
    @TestOnly
    public void observe(@NotNull PreferencesUpdateListener listener) {
        listener.update(preferencesMap);
    }

    /**
     * Writes the current preferences to a specified destination path.
     * Used for checkpoint creation to capture preferences in a consistent state.
     */
    public synchronized void persistTo(LPSZ destinationPath, FilesFacade ff, int commitMode) {
        try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
            writer.of(destinationPath);
            final AppendableBlock block = writer.append();
            preferencesMap.writeToBlock(block, version);
            writer.commit();
        }
    }

    public void registerListener(@NotNull PreferencesUpdateListener listener) {
        this.listener = listener;

        // call the listener with current state,
        // it will be called on subsequent updates too
        listener.update(preferencesMap);
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

        if (listener != null) {
            listener.update(preferencesMap);
        }
    }

    private void load(LPSZ preferencesPath, PreferencesMap map) {
        map.clear();
        try (BlockFileReader reader = blockFileReader) {
            reader.of(preferencesPath);
            version = map.readFromBlock(reader.getCursor());
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
            writer.of(preferencesFilePath());
            final AppendableBlock block = writer.append();
            map.writeToBlock(block, ++version);
            writer.commit();
        }
    }

    private LPSZ preferencesFilePath() {
        return path.trimTo(rootLen).concat(PREFERENCES_FILE_NAME).$();
    }

    public enum Mode {
        MERGE, OVERWRITE;

        public static Mode of(Utf8Sequence method) {
            if (HttpKeywords.isPUT(method)) {
                return OVERWRITE;
            }
            if (HttpKeywords.isPOST(method)) {
                return MERGE;
            }
            throw CairoException.nonCritical().put("Unsupported HTTP method [method=").put(method).put(']');
        }
    }
}
