package com.etisalat.log.query;

import com.etisalat.log.config.LogConfFactory;
import com.etisalat.log.parser.QueryCondition;
import com.etisalat.log.sort.SortUtils;
import com.google.gson.JsonObject;
import org.apache.hadoop.hbase.client.Get;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;

public class SolrQueryTask implements Runnable {
    protected static Logger logger = LoggerFactory.getLogger(SolrQueryTask.class);
    private String cacheKey;
    private String shardId;
    private String reqUrl;
    private String qString;
    private String collection;
    private String rows;
    private HttpClient httpClient;
    private QueryCondition qCondition;

    private HBaseQueryHandlerFactory queryHBaseHandlerFactory = null;

    public SolrQueryTask(String qString, String reqUrl) {
        this.qString = qString;
        this.reqUrl = reqUrl;
    }

    public String getCacheKey() {
        return cacheKey;
    }

    public void setCacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }

    public void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public QueryCondition getqCondition() {
        return qCondition;
    }

    public void setqCondition(QueryCondition qCondition) {
        this.qCondition = qCondition;
    }

    public String getqString() {
        return qString;
    }

    public void setqString(String qString) {
        this.qString = qString;
    }

    public HBaseQueryHandlerFactory getQueryHBaseHandlerFactory() {
        return queryHBaseHandlerFactory;
    }

    public void setQueryHBaseHandlerFactory(HBaseQueryHandlerFactory queryHBaseHandlerFactory) {
        this.queryHBaseHandlerFactory = queryHBaseHandlerFactory;
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

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();
        final SolrQuery myParameters = new SolrQuery();
        myParameters.add("q", qString);
        myParameters.add("rows", rows);
        myParameters.add("distrib", "false");
        myParameters.add("fl", LogConfFactory.rowkeyName);
        myParameters.set("collection", collection);

        final HttpSolrClient httpSolrClient = new HttpSolrClient(reqUrl, httpClient);
        try {
            logger.debug("QUERY request url is {}.", httpSolrClient.getBaseURL());
            logger.debug("QUERY myParameters is {}.", myParameters.toQueryString());

            QueryResponse response = httpSolrClient.query(myParameters);

            if (response.getStatus() != 0) {
                logger.error(
                        "Error(SolrServerException) sending live Query command, url is {}, query is {} with status return code {}",
                        reqUrl, qString, response.getStatus());

                return;
            }

            logger.info("query {}, numFound {}  QTime {}, cost {} ms", reqUrl, response.getResults().getNumFound(),
                    response.getQTime(), (System.currentTimeMillis() - start));
            fetchResultFromHBase(queryHBaseHandlerFactory, response);

        } catch (Exception e) {
            logger.error("Error(SolrServerException) sending live Query command, url is {}",
                    httpSolrClient.getBaseURL(), e);
        }
    }

    private void fetchResultFromHBase(HBaseQueryHandlerFactory queryHBaseHandlerFactory, QueryResponse queryResponse) {
        SolrDocumentList results = queryResponse.getResults();

        int hbaseSize = (results.size() < LogConfFactory.hbaseBatchSize * 10 ?
                LogConfFactory.hbaseBatchMinSize :
                LogConfFactory.hbaseBatchSize);

        logger.info("get hbase row key with hbase size {}", hbaseSize);
        logger.debug("get hbase row key with result size {}", results.size());

        CompletionService<HBaseQueryCursorRsp> completionService = queryHBaseHandlerFactory.newCompletionService();
        Set<Future<HBaseQueryCursorRsp>> pending = new HashSet<Future<HBaseQueryCursorRsp>>();

        List<Get> gets = new ArrayList<Get>();
        long start = System.currentTimeMillis();
        logger.debug("Query DEBUG results size {} ", results.size());
        for (int i = 0; i < results.size(); i++) {
            String solrKey = ((SolrDocument) results.get(i)).getFieldValue("rowkey").toString();
            byte[] rowkey = SolrUtils.exchangeKey(solrKey);
            Get get = new Get(rowkey);
            gets.add(get);

            if (gets.size() == hbaseSize) {
                HBaseQueryCursorHandler hBaseQueryCursorHandler = new HBaseQueryCursorHandler(collection);
                hBaseQueryCursorHandler.setGets(gets);
                hBaseQueryCursorHandler.setCompletionService(completionService);
                hBaseQueryCursorHandler.setPending(pending);
                hBaseQueryCursorHandler.submit();
                gets = new ArrayList<Get>();
            }
        }

        if (gets.size() != 0) {
            HBaseQueryCursorHandler hBaseQueryCursorHandler = new HBaseQueryCursorHandler(collection);
            hBaseQueryCursorHandler.setGets(gets);
            hBaseQueryCursorHandler.setCompletionService(completionService);
            hBaseQueryCursorHandler.setPending(pending);
            hBaseQueryCursorHandler.submit();
        }

        List<JsonObject> jsonObjects = new ArrayList<JsonObject>();
        while (pending.size() > 0) {
            try {
                Future<HBaseQueryCursorRsp> future = completionService.take();
                pending.remove(future);
                HBaseQueryCursorRsp rsp = future.get();
                if (rsp != null && rsp.getResultJsonObjList() != null && rsp.getResultJsonObjList().size() > 0) {
                    jsonObjects.addAll(rsp.getResultJsonObjList());
                }
            } catch (Exception e) {
                logger.error("hbase data fetch task failed and IOException arised", e);
                return;
            }
        }

        addResultToCache(jsonObjects);
        logger.debug("Query hbase table {}", collection);
        logger.debug("Query hbase cost:{} ms", (System.currentTimeMillis() - start));
    }

    private void addResultToCache(List<JsonObject> jsonObjects) {
        if (jsonObjects.size() == 0) {
            return;
        }

        synchronized (QueryBatch.RESULTS_FOR_SHARDS) {
            ResultCnt resultCnt = QueryBatch.RESULTS_CNT_FOR_SHARDS.get(cacheKey);
            if (resultCnt == null) {
                return;
            }

            if (resultCnt.getFetchNum() >= resultCnt.getTotalNum()) {
                return;
            }

            Map<String, List<JsonObject>> map = null;
            map = QueryBatch.RESULTS_FOR_SHARDS.get(cacheKey);
            if (map == null) {
                map = new HashMap<String, List<JsonObject>>();
                QueryBatch.RESULTS_FOR_SHARDS.put(cacheKey, map);
            }

            map.put(shardId, jsonObjects);

            if (qCondition.isSort() && qCondition.isExportOp() && jsonObjects.size() != 0) {
                SortUtils.sortSingleShardRsp(qCondition.getSortedFields(), jsonObjects);
            }

            resultCnt.setFetchNum(resultCnt.getFetchNum() + jsonObjects.size());
        }
    }
}
