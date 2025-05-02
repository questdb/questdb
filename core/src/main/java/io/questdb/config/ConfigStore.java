package io.questdb.config;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.vm.Vm;
import io.questdb.cutlass.json.JsonException;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.PropServerConfiguration.JsonPropertyValueFormatter.str;

public class ConfigStore implements Closeable {
    private static final String CONFIG_FILE_NAME = "_config~store";
    private static final int CONFIG_FORMAT_MSG_TYPE = 0;
    private static final Utf8String MERGE_STR = new Utf8String("merge");
    private static final Utf8String OVERWRITE_STR = new Utf8String("overwrite");
    private final BlockFileWriter blockFileWriter;
    private final CharSequenceObjHashMap<CharSequence> configMap = new CharSequenceObjHashMap<>();
    // TODO: add versioning
    private final ConfigParser configParser;
    private final CairoConfiguration configuration;
    private final CharSequenceObjHashMap<CharSequence> parserMap = new CharSequenceObjHashMap<>();
    private final Path path = new Path();
    private final int rootLen;

    public ConfigStore(CairoConfiguration configuration) {
        this.configuration = configuration;

        configParser = new ConfigParser(configuration, parserMap);

        path.of(configuration.getDbRoot());
        rootLen = path.size();
        blockFileWriter = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
    }

    @Override
    public void close() throws IOException {
        Misc.free(configParser);
        Misc.free(blockFileWriter);
        Misc.free(path);
    }

    public synchronized void init() {
        final LPSZ configPath = configFilePath();
        if (configuration.getFilesFacade().exists(configPath)) {
            read(configPath);
        }
    }

    public synchronized void populateSettings(Utf8StringSink sink) {
        final ObjList<CharSequence> keys = configMap.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final CharSequence key = keys.getQuick(i);
            final CharSequence value = configMap.get(key);
            str(key, value, sink);
        }
    }

    public synchronized void save(DirectUtf8Sink sink, Mode mode) throws JsonException {
        configParser.clear();
        configParser.parse(sink);

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
    }

    private LPSZ configFilePath() {
        return path.trimTo(rootLen).concat(CONFIG_FILE_NAME).$();
    }

    private void merge() {
        // TODO: Implement MERGE mode
        throw CairoException.nonCritical().put("Unsupported mode [mode=").put(Mode.MERGE.name()).put(']');
    }

    private void overwrite() {
        // save file
        try (BlockFileWriter writer = blockFileWriter) {
            writer.of(configFilePath());
            final AppendableBlock block = writer.append();

            // process map
            block.putInt(parserMap.size());
            final ObjList<CharSequence> keys = parserMap.keys();
            for (int i = 0, n = keys.size(); i < n; i++) {
                final CharSequence key = keys.getQuick(i);
                final CharSequence value = parserMap.get(key);
                block.putStr(key);
                block.putStr(value);
            }

            block.commit(CONFIG_FORMAT_MSG_TYPE);
            writer.commit();
        }

        configMap.clear();
        configMap.putAll(parserMap);
    }

    private void read(LPSZ configPath) {
        configMap.clear();
        try (BlockFileReader reader = new BlockFileReader(configuration)) {
            reader.of(configPath);

            long offset = 0;

            final BlockFileReader.BlockCursor cursor = reader.getCursor();
            while (cursor.hasNext()) {
                final ReadableBlock block = cursor.next();
                if (block.type() != CONFIG_FORMAT_MSG_TYPE) {
                    // ignore unknown block
                    continue;
                }

                final int size = block.getInt(offset);
                offset += Integer.BYTES;
                for (int i = 0; i < size; i++) {
                    // TODO: ok to use toString() on initial load?
                    final CharSequence key = block.getStr(offset).toString();
                    offset += Vm.getStorageLength(key);
                    final CharSequence value = block.getStr(offset).toString();
                    offset += Vm.getStorageLength(value);
                    configMap.put(key, value);
                }
            }
        }
    }

    public enum Mode {
        MERGE, OVERWRITE;

        public static Mode of(DirectUtf8Sequence mode) {
            if (mode == null || Utf8s.equals(mode, MERGE_STR)) {
                return MERGE;
            }
            if (Utf8s.equals(mode, OVERWRITE_STR)) {
                return OVERWRITE;
            }
            throw CairoException.nonCritical().put("Unsupported mode [mode=").put(mode).put(']');
        }
    }
}
