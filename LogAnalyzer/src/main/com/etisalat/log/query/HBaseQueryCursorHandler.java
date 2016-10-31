package com.etisalat.log.query;

import com.etisalat.log.common.AssertUtil;
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
    private Table table;

    private List<Get> gets;

    private CompletionService<HBaseQueryCursorRsp> completionService;
    private Set<Future<HBaseQueryCursorRsp>> pending;

    public HBaseQueryCursorHandler(String tableName) {
        AssertUtil.noneNull("HBaseQueryCursorHandler construct should not null", tableName);

        this.tableName = tableName;
        try {
            this.table = HBaseQueryHandlerFactory.connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            logger.error("Get Table and IOException arised.", e);
            return;
        }
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

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void submit() {
        if (gets == null || gets.isEmpty()) {
            logger.info("Gets is Empty and end.");
            return;
        }

        Callable<HBaseQueryCursorRsp> task = new Callable<HBaseQueryCursorRsp>() {
            @Override
            public HBaseQueryCursorRsp call() throws Exception {
                logger.debug("HBase data fetch task start");

                long start = System.currentTimeMillis();

                try {
                    Result[] results = table.get(gets);

                    Map<String, JsonObject> resultJsonObjMap = new HashMap<String, JsonObject>();

                    if (results == null) {
                        return new HBaseQueryCursorRsp(resultJsonObjMap);
                    }

                    HBaseQueryCursorRsp hbaseQueryRsp = new HBaseQueryCursorRsp(resultJsonObjMap);
                    addResultToJsonMap(resultJsonObjMap, results);
                    logger.info("query hbase table {}, with batch size {}, cost {} ms", tableName, gets.size(),
                            (System.currentTimeMillis() - start));
                    return hbaseQueryRsp;
                } catch (IOException e) {
                    logger.error("hbase data fetch task failed and IOException arised", e);
                    return new HBaseQueryCursorRsp(new HashMap<String, JsonObject>());
                } finally {
                    try {
                        table.close();
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                        logger.warn("Table close and IOException arised");
                    }
                }
            }
        };

        pending.add(completionService.submit(task));
    }

    private void addResultToJsonMap(Map<String, JsonObject> jsonObjectMap, Result[] results) {
        JsonObject jsonObj = null;
        for (int i = 0; i < results.length; ++i) {
            Result result = results[i];
            if (null == result.getRow()) {
                logger.debug("result.getRow() is null, this can ignore.");
                continue;
            }

            String rowKey = SolrUtils.getSolrKey(result.getRow());
            if (null == rowKey) {
                logger.error("rowkey is null!");
                continue;
            }

            jsonObj = new JsonObject();
            jsonObj.addProperty(LogConfFactory.rowkeyName, rowKey);

            for (String qualifier : LogConfFactory.columnQualifiersBytesMap.keySet()) {
                SolrUtils.addJsonElement(jsonObj, qualifier, result.getValue(LogConfFactory.columnFamilyBytes,
                        LogConfFactory.columnQualifiersBytesMap.get(qualifier)));
            }

            jsonObjectMap.put(rowKey, jsonObj);
        }
    }
}
