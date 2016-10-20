package com.etisalat.log.query;

import com.etisalat.log.config.LogConfFactory;
import com.etisalat.log.parser.QueryCondition;
import com.google.gson.JsonObject;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HBaseQueryRsp {
    protected static final Logger logger = LoggerFactory.getLogger(HBaseQueryRsp.class);
    private long timeCost;
    private String tableName;
    private QueryCondition condition;
    private Map<String, JsonObject> rspResults = null;

    public HBaseQueryRsp(String tableName, QueryCondition condition) {
        this.condition = condition;
        this.tableName = tableName;
    }

    public HBaseQueryRsp() {
    }

    public QueryCondition getCondition() {
        return condition;
    }

    public void setCondition(QueryCondition condition) {
        this.condition = condition;
    }

    public Map<String, JsonObject> getRspResults() {
        return rspResults;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getTimeCost() {
        return timeCost;
    }

    public void setTimeCost(long timeCost) {
        this.timeCost = timeCost;
    }

    public void process(Result[] results) throws IOException {
        rspResults = new HashMap<String, JsonObject>();
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

            JsonObject jsonObj = new JsonObject();
            jsonObj.addProperty(LogConfFactory.rowkeyName, rowKey);
            for (String qualifier : LogConfFactory.columnQualifiersBytesMap.keySet()) {
                jsonObj.addProperty(qualifier, Bytes.toString(result.getValue(LogConfFactory.columnFamilyBytes,
                        LogConfFactory.columnQualifiersBytesMap.get(qualifier))));
            }

            rspResults.put(rowKey, jsonObj);

        }
    }
}
