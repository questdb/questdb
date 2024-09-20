package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;
import io.questdb.std.CharSequenceObjHashMap;

public class MatViewGraph {
    private final CharSequenceObjHashMap<MaterializedViewDefinition> matViewDefs = new CharSequenceObjHashMap<>();

    public void clear() {
        matViewDefs.clear();
    }

    public MaterializedViewDefinition getView(CharSequence name) {
        return matViewDefs.get(name);
    }

    public void upsertView(TableToken baseTableToken, MaterializedViewDefinition matViewDefinition) {
        matViewDefs.put(matViewDefinition.getMatViewToken().getTableName(), matViewDefinition);
    }
}
