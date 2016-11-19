package com.etisalat.log.query;

import com.etisalat.log.common.AssertUtil;
import com.etisalat.log.common.JsonUtil;
import com.etisalat.log.config.LogConfFactory;
import com.google.gson.JsonObject;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;

public class HBaseQueryCursorHandler {
    private static Logger logger = LoggerFactory.getLogger(HBaseQueryCursorHandler.class);
  
    private String tableName;

    private String cacheKey;
    private String collWithShard;

    private List<Get> gets;

    private CompletionService<HBaseQueryCursorRsp> completionService;
    private Set<Future<HBaseQueryCursorRsp>> pending;

    public HBaseQueryCursorHandler(String tableName, String cacheKey, String collWithShard) {
        AssertUtil.noneNull("HBaseQueryCursorHandler construct should not null", tableName);

        this.tableName = tableName;
        this.cacheKey = cacheKey;
        this.collWithShard = collWithShard;
    }

    public CompletionService<HBaseQueryCursorRsp> getCompletionService() {
        return completionService;
    }

    public void setCompletionService(CompletionService<HBaseQueryCursorRsp> completionService) {
        this.completionService = completionService;
    }

    public Set<Future<HBaseQueryCursorRsp>> getPending() {
        return pending;
    }

    public void setPending(Set<Future<HBaseQueryCursorRsp>> pending) {
        this.pending = pending;
    }

    public List<Get> getGets() {
        return gets;
    }

    public void setGets(List<Get> gets) {
        this.gets = gets;
    }

    public void submit() {
        if (gets == null || gets.isEmpty()) {
            logger.info("Query session {}, query {}, gets is empty and end.", cacheKey, collWithShard);
            return;
        }

        Callable<HBaseQueryCursorRsp> task = new Callable<HBaseQueryCursorRsp>() {
            @Override
            public HBaseQueryCursorRsp call()  {
                logger.info("Query session {}, query {}, hbase data fetch {} gets task start", cacheKey, collWithShard, gets.size());

                long start = System.currentTimeMillis();
                Table table = null;
                try {
                    table = HBaseQueryHandlerFactory.connection.getTable(TableName.valueOf(tableName));

                    Result[] results = table.get(gets);

                    Map<String, String> resultJsonObjMap = new HashMap<String, String>();

                    if (results == null) {
                        return new HBaseQueryCursorRsp(resultJsonObjMap);
                    }

                    HBaseQueryCursorRsp hbaseQueryRsp = new HBaseQueryCursorRsp(resultJsonObjMap);
                    addResultToJsonMap(resultJsonObjMap, results);
                    logger.info("Query session {}, query {}, with batch size {}, cost {} ms", cacheKey, collWithShard, gets.size(),
                            (System.currentTimeMillis() - start));
                    return hbaseQueryRsp;
                } catch (Exception e) {
                    logger.error("Query session {}, query {}, hbase data fetch task failed and IOException arised", cacheKey, collWithShard, e);
                    return new HBaseQueryCursorRsp(new HashMap<String, String>());
                } finally {
                    try {
                        if(table != null) {
                            table.close();
                        }
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                        logger.warn("Query session {}, query {}, table close and IOException arised", cacheKey, collWithShard);
                    }
                }
            }
        };

        pending.add(completionService.submit(task));
    }

    private void addResultToJsonMap(Map<String, String> jsonObjectMap, Result[] results) {
        JsonObject jsonObj = null;
        for (int i = 0; i < results.length; ++i) {
            Result result = results[i];
            if (null == result.getRow()) {
                logger.debug("Query session {}, query {}, result.getRow() is null, this can ignore.", cacheKey, collWithShard);
                continue;
            }

            String rowKey = SolrUtils.getSolrKey(result.getRow());
            if (null == rowKey) {
                logger.debug("Query session {}, query {}, rowkey is null!", cacheKey, collWithShard);
                continue;
            }

            jsonObj = new JsonObject();
            jsonObj.addProperty(LogConfFactory.rowkeyName, rowKey);

            for (String qualifier : LogConfFactory.columnQualifiersBytesMap.keySet()) {
                SolrUtils.addJsonElement(jsonObj, qualifier, result.getValue(LogConfFactory.columnFamilyBytes,
                        LogConfFactory.columnQualifiersBytesMap.get(qualifier)));
            }

            jsonObjectMap.put(rowKey, JsonUtil.toJson(jsonObj));
        }
    }
}
