package com.etisalat.log.cache;

import com.etisalat.log.query.QueryBatch;

public class CacheQueryResTaskInfo {
    boolean finish = false;
    private QueryBatch queryBatch;
    private int retryCnt = 0;

    public CacheQueryResTaskInfo(QueryBatch queryBatch) {
        this.queryBatch = queryBatch;
    }

    public QueryBatch getQueryBatch() {
        return queryBatch;
    }

    public void setQueryBatch(QueryBatch queryBatch) {
        this.queryBatch = queryBatch;
    }

    public int getRetryCnt() {
        return retryCnt;
    }

    public void setRetryCnt(int retryCnt) {
        this.retryCnt = retryCnt;
    }

    public boolean isFinish() {
        return finish;
    }

    public void setFinish(boolean finish) {
        this.finish = finish;
    }
}
