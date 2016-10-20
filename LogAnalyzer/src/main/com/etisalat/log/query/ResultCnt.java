package com.etisalat.log.query;

public class ResultCnt {
    private String collWithShardId;
    private long fetchNum;
    private long totalNum;

    public ResultCnt(String collWithShardId, long totalNum) {
        this.collWithShardId = collWithShardId;
        this.totalNum = totalNum;
    }

    public ResultCnt(long fetchNum, long totalNum) {
        this.fetchNum = fetchNum;
        this.totalNum = totalNum;
    }

    public long getFetchNum() {
        return fetchNum;
    }

    public void setFetchNum(long fetchNum) {
        this.fetchNum = fetchNum;
    }

    public long getTotalNum() {
        return totalNum;
    }

    public void setTotalNum(long totalNum) {
        this.totalNum = totalNum;
    }

    public String getCollWithShardId() {
        return collWithShardId;
    }

    public void setCollWithShardId(String collWithShardId) {
        this.collWithShardId = collWithShardId;
    }
}
