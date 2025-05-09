package io.questdb.preferences;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cutlass.json.AbstractJsonParser;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.std.CharSequenceObjHashMap;

public final class PreferencesParser extends AbstractJsonParser {

    private static final int STATE_ROOT = 1;
    private static final int STATE_START = 0;
    private final CharSequenceObjHashMap<CharSequence> parserMap;
    private CharSequence key;
    private int state = STATE_START;

    public PreferencesParser(CairoConfiguration configuration, CharSequenceObjHashMap<CharSequence> parserMap) {
        super(16384, configuration.getPreferencesParserCacheSizeLimit(), configuration.getPreferencesStringPoolCapacity());
        this.parserMap = parserMap;
    }

    @Override
    public void clear() {
        super.clear();
        parserMap.clear();
        state = STATE_START;
    }

    @Override
    public void onEvent(int code, CharSequence tag, int position) {
        switch (code) {
            case JsonLexer.EVT_OBJ_START:
                if (state == STATE_START) { // expecting root object
                    state = STATE_ROOT;
                } else {
                    throw CairoException.critical(0).put("unexpected input format [code=").put(code)
                            .put(", state=").put(state).put(']');
                }
                break;
            case JsonLexer.EVT_OBJ_END:
                if (state == STATE_ROOT) { // the end
                    state = STATE_START;
                } else {
                    throw CairoException.critical(0).put("unexpected input format [code=").put(code)
                            .put(", state=").put(state).put(']');
                }
                break;
            case JsonLexer.EVT_NAME:
                if (state == STATE_ROOT) {
                    key = copy(tag);
                } else {
                    throw CairoException.critical(0).put("unexpected input format [code=").put(code)
                            .put(", tag=").put(tag).put(", state=").put(state).put(']');
                }
                break;
            case JsonLexer.EVT_VALUE:
                parserMap.put(key, copy(tag));
        }
    }
}
