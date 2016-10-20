package com.etisalat.log.query;

import org.apache.solr.client.solrj.response.QueryResponse;

public class SolrQueryRsp {
    private long timeCost;
    private String collWithShardId;
    private String reqUrl;
    private QueryResponse queryResponse;

    public SolrQueryRsp(QueryResponse queryResponse, String reqUrl, String collWithShardId, long timeCost) {
        this.queryResponse = queryResponse;
        this.reqUrl = reqUrl;
        this.collWithShardId = collWithShardId;
        this.timeCost = timeCost;
    }

    public SolrQueryRsp(String reqUrl, QueryResponse queryResponse) {
        this.queryResponse = queryResponse;
        this.reqUrl = reqUrl;
    }

    public SolrQueryRsp(QueryResponse queryResponse, String reqUrl, long timeCost) {
        this.queryResponse = queryResponse;
        this.reqUrl = reqUrl;
        this.timeCost = timeCost;
    }

    public QueryResponse getQueryResponse() {
        return queryResponse;
    }

    public void setQueryResponse(QueryResponse queryResponse) {
        this.queryResponse = queryResponse;
    }

    public String getReqUrl() {
        return reqUrl;
    }

    public void setReqUrl(String reqUrl) {
        this.reqUrl = reqUrl;
    }

    public long getTimeCost() {
        return timeCost;
    }

    public void setTimeCost(long timeCost) {
        this.timeCost = timeCost;
    }

    public String getCollWithShardId() {
        return collWithShardId;
    }

    public void setCollWithShardId(String collWithShardId) {
        this.collWithShardId = collWithShardId;
    }
}
