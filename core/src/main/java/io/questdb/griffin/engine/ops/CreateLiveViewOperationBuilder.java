package io.questdb.griffin.engine.ops;

import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Mutable;

public class CreateLiveViewOperationBuilder implements ExecutionModel, Mutable {
    private String baseTableName;
    private boolean ignoreIfExists;
    private long lagMicros;
    private long retentionMicros;
    private QueryModel selectModel;
    private String selectSql;
    private String viewName;
    private int viewNamePosition;

    public CreateLiveViewOperation build(CharSequence sqlText) {
        return new CreateLiveViewOperation(
                viewName,
                viewNamePosition,
                baseTableName,
                selectSql,
                lagMicros,
                retentionMicros,
                ignoreIfExists
        );
    }

    @Override
    public void clear() {
        baseTableName = null;
        ignoreIfExists = false;
        lagMicros = 0;
        retentionMicros = 0;
        selectModel = null;
        selectSql = null;
        viewName = null;
        viewNamePosition = 0;
    }

    @Override
    public int getModelType() {
        return CREATE_LIVE_VIEW;
    }

    @Override
    public QueryModel getQueryModel() {
        return selectModel;
    }

    public String getViewName() {
        return viewName;
    }

    public void setBaseTableName(String baseTableName) {
        this.baseTableName = baseTableName;
    }

    public void setIgnoreIfExists(boolean ignoreIfExists) {
        this.ignoreIfExists = ignoreIfExists;
    }

    public void setLagMicros(long lagMicros) {
        this.lagMicros = lagMicros;
    }

    public void setRetentionMicros(long retentionMicros) {
        this.retentionMicros = retentionMicros;
    }

    public void setSelectModel(QueryModel selectModel) {
        this.selectModel = selectModel;
    }

    public void setSelectSql(String selectSql) {
        this.selectSql = selectSql;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    public void setViewNamePosition(int viewNamePosition) {
        this.viewNamePosition = viewNamePosition;
    }
}
