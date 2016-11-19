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
import java.util.concurrent.ConcurrentHashMap;
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
            logger.debug("Query session {}, query {}, QUERY request url is {}.", cacheKey, shardId,
                    httpSolrClient.getBaseURL());
            logger.debug("Query session {}, query {}, QUERY myParameters is {}.", cacheKey, shardId,
                    myParameters.toQueryString());

            QueryResponse response = httpSolrClient.query(myParameters);

            if (response.getStatus() != 0) {
                logger.error(
                        "Query session {}, Error(SolrServerException) sending live Query command, url is {}, query is {} with status return code {}",
                        cacheKey, reqUrl, qString, response.getStatus());
                
                checkAndAddResultToCache();
                return;
            }

            logger.info("Query session {}, {} with shardId {}, rows {}, QTime {}, solr cost {} ms", cacheKey, reqUrl,
                    shardId, rows, response.getQTime(),
                    (System.currentTimeMillis() - start));
            fetchResultFromHBase(queryHBaseHandlerFactory, response);

        } catch (Exception e) {
            logger.error("Query session {}, query {}, Error(SolrServerException) sending live Query command, url is {}",
                    cacheKey, shardId, httpSolrClient.getBaseURL(), e);
        }
        
        checkAndAddResultToCache();
    }

    private void fetchResultFromHBase(HBaseQueryHandlerFactory queryHBaseHandlerFactory, QueryResponse queryResponse) {
        SolrDocumentList results = queryResponse.getResults();

        int hbaseSize = (results.size() < LogConfFactory.hbaseBatchSize * 10 ?
                LogConfFactory.hbaseBatchMinSize :
                LogConfFactory.hbaseBatchSize);

        logger.debug("Query session {}, query {}, get hbase row key with hbase batch size {}, result size {}", cacheKey, shardId, hbaseSize, 
        		results.size());

        CompletionService<HBaseQueryCursorRsp> completionService = queryHBaseHandlerFactory.newCompletionService();
        Set<Future<HBaseQueryCursorRsp>> pending = new HashSet<Future<HBaseQueryCursorRsp>>();

        List<Get> gets = new ArrayList<Get>();
        long start = System.currentTimeMillis();
        logger.debug("Query session {}, query {}, query DEBUG results size {} ", cacheKey, shardId, results.size());
        List<String> rowKeyList = new ArrayList<String>();
        for (int i = 0; i < results.size(); i++) {
            String solrKey = ((SolrDocument) results.get(i)).getFieldValue(LogConfFactory.rowkeyName).toString();
            rowKeyList.add(solrKey);
            byte[] rowkey = SolrUtils.exchangeKey(solrKey);
            Get get = new Get(rowkey);
            gets.add(get);

            if (gets.size() == hbaseSize) {
                HBaseQueryCursorHandler hBaseQueryCursorHandler = new HBaseQueryCursorHandler(collection, cacheKey,
                        shardId);
                hBaseQueryCursorHandler.setGets(gets);
                hBaseQueryCursorHandler.setCompletionService(completionService);
                hBaseQueryCursorHandler.setPending(pending);
                hBaseQueryCursorHandler.submit();
                gets = new ArrayList<Get>();
            }
        }

        if (gets.size() != 0) {
            HBaseQueryCursorHandler hBaseQueryCursorHandler = new HBaseQueryCursorHandler(collection, cacheKey,
                    shardId);
            hBaseQueryCursorHandler.setGets(gets);
            hBaseQueryCursorHandler.setCompletionService(completionService);
            hBaseQueryCursorHandler.setPending(pending);
            hBaseQueryCursorHandler.submit();
        }

        Map<String, String> jsonObjectsMap = new HashMap<String, String>();
        while (pending.size() > 0) {
            try {
                Future<HBaseQueryCursorRsp> future = completionService.take();
                HBaseQueryCursorRsp rsp = future.get();
                pending.remove(future);

                if (rsp != null && rsp.getResultJsonObjMap() != null && rsp.getResultJsonObjMap().size() > 0) {
                    jsonObjectsMap.putAll(rsp.getResultJsonObjMap());
                }
            } catch (Exception e) {
                logger.error("Query session {}, query {}, hbase data fetch task failed and IOException arised",
                        cacheKey, shardId, e);
                continue;
            }
        }

        addResultToCache(jsonObjectsMap, rowKeyList);
        logger.info("Query session {}, query {}, hbase cost: {} ms", cacheKey, shardId, (System.currentTimeMillis() - start));
    }

    private void addResultToCache(Map<String, String> jsonObjectsMap, List<String> rowKeyList) {
        ConcurrentHashMap<String, List<String>> map = QueryBatch.RESULTS_FOR_SHARDS.get(cacheKey);
        if (map == null) {
            map = new ConcurrentHashMap<String, List<String>>();
            QueryBatch.RESULTS_FOR_SHARDS.put(cacheKey, map);
        }

        List<String> jsonObjects = new ArrayList<String>();
        String jsonStr = null;
        
        if(jsonObjectsMap.size() != 0) {
        for (String rowKey : rowKeyList) {
            jsonStr = jsonObjectsMap.get(rowKey);
            if (jsonStr == null) {
                logger.debug("Query session {}, query {}, rowKey: {} does not have the related record.", cacheKey,
                        shardId, rowKey);
                continue;
            }
            jsonObjects.add(jsonStr);
        }
        }

        map.put(shardId, jsonObjects);

        if (qCondition.isSort() && qCondition.isExportOp() && jsonObjects.size() != 0) {
            SortUtils.sortSingleShardRsp(qCondition.getSortedFields(), jsonObjects);
        }

        if (Integer.valueOf(rows) != jsonObjects.size()) {
        	logger.warn("Query session {}, solr result size {}, but hbase result size is {} ", cacheKey, shardId, rows, jsonObjects.size());
        }
        logger.info("Query session {}, query {}, add {} record to cache.", cacheKey, shardId, jsonObjects.size() );

    }
    
    private void checkAndAddResultToCache() {
    	ConcurrentHashMap<String, List<String>> map = QueryBatch.RESULTS_FOR_SHARDS.get(cacheKey);
    	if (map == null) {
    		map = new ConcurrentHashMap<String, List<String>>();
    		QueryBatch.RESULTS_FOR_SHARDS.put(cacheKey, map);
    	}
    	
    	if (!map.containsKey(shardId)) {
    		logger.warn("Query session {}, query {} failed or result is empty.", cacheKey, shardId);
    		map.put(shardId, new ArrayList<String>());
    	}
    	
    }
    
}
