package com.etisalat.log.query;

import com.etisalat.log.common.LogQueryException;

import java.util.Set;

public class ResultCnt {
    private String collWithShardId;
    private long fetchNum;
    private long totalNum;
    
    private Set<String> maxCollWithShardSets = null;

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

    public Set<String> getMaxCollWithShardSets() {
        return maxCollWithShardSets;
    }

    public void setMaxCollWithShardSets(Set<String> maxCollWithShardSets) {
        this.maxCollWithShardSets = maxCollWithShardSets;
    }


    @Override
    public String toString() {
        return "ResultCnt{" +
                "collWithShardId='" + collWithShardId + '\'' +
                ", totalNum=" + totalNum +
                '}';
    }

    public int compareTo(ResultCnt resultCnt) {
        if(resultCnt == null) {
            return -1;
        }

        String firstColl = SolrUtils.getCollection(this.getCollWithShardId());
        String secondColl = SolrUtils.getCollection(resultCnt.getCollWithShardId());

        int res = firstColl.compareTo(secondColl);
        if(res != 0) {
            return res;
        }

        res = (this.totalNum < resultCnt.getTotalNum()) ? -1 : ((this.totalNum == resultCnt.getTotalNum()) ? 0 : 1);
        if(res != 0) {
            return res;
        }

        String secondShardId = SolrUtils.getShardId(resultCnt.getCollWithShardId());
        String shardId =  SolrUtils.getShardId(this.getCollWithShardId());

        try{
            return  SolrUtils.compareShardId(shardId, secondShardId);
        }catch (LogQueryException e) {
            return 0;
        }
    }
}
