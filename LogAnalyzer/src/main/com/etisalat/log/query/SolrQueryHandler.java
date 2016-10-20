package com.etisalat.log.query;

import com.etisalat.log.config.LogConfFactory;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;

public class SolrQueryHandler {
    protected static Logger logger = LoggerFactory.getLogger(SolrQueryHandler.class);
    private String reqUrl;
    private String qString;
    private String rows;

    private HttpClient httpClient;

    private String collWithShardId;

    private CompletionService<SolrQueryRsp> completionService;
    private Set<Future<SolrQueryRsp>> pending;

    public SolrQueryHandler(String qString, String reqUrl) {
        this.qString = qString;
        this.reqUrl = reqUrl;
    }

    public CompletionService<SolrQueryRsp> getCompletionService() {
        return completionService;
    }

    public void setCompletionService(CompletionService<SolrQueryRsp> completionService) {
        this.completionService = completionService;
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }

    public void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public Set<Future<SolrQueryRsp>> getPending() {
        return pending;
    }

    public void setPending(Set<Future<SolrQueryRsp>> pending) {
        this.pending = pending;
    }

    public String getCollWithShardId() {
        return collWithShardId;
    }

    public void setCollWithShardId(String collWithShardId) {
        this.collWithShardId = collWithShardId;
    }

    public String getqString() {
        return qString;
    }

    public void setqString(String qString) {
        this.qString = qString;
    }

    public String getReqUrl() {
        return reqUrl;
    }

    public void setReqUrl(String reqUrl) {
        this.reqUrl = reqUrl;
    }

    public String getRows() {
        return rows;
    }

    public void setRows(String rows) {
        this.rows = rows;
    }

    public void submit() {
        Callable<SolrQueryRsp> task = new Callable<SolrQueryRsp>() {
            @Override
            public SolrQueryRsp call() {
                long start = System.currentTimeMillis();
                final SolrQuery myParameters = new SolrQuery();
                myParameters.add("q", qString);
                myParameters.add("rows", rows);
                myParameters.add("distrib", "false");
                myParameters.add("fl", LogConfFactory.rowkeyName);

                final HttpSolrClient httpSolrClient = new HttpSolrClient(reqUrl, httpClient);
                try {
                    logger.debug("request url is {} in call.", httpSolrClient.getBaseURL());
                    logger.debug("myParameters is {} in call.", myParameters.toQueryString());
                    return new SolrQueryRsp(httpSolrClient.query(myParameters), reqUrl, collWithShardId,
                            (System.currentTimeMillis() - start));
                } catch (Exception e) {
                    logger.error("Error(SolrServerException) sending live Query command, url is {}",
                            httpSolrClient.getBaseURL(), e);
                    return null;
                }
            }
        };

        pending.add(completionService.submit(task));
    }
}
