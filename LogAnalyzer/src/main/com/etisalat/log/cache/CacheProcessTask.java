package com.etisalat.log.cache;

import com.etisalat.log.common.LogQueryException;
import com.etisalat.log.query.HBaseQueryHandlerFactory;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheProcessTask implements Runnable {
    protected static final Logger logger = LoggerFactory.getLogger(QueryAndCacheThread.class);
    private CacheQueryResTaskInfo cacheQueryResTaskInfo;
    private HBaseQueryHandlerFactory queryHBaseHandlerFactory;
    private LRUCache<String, String> lruCache;
    private String cursor;

    public CacheProcessTask(HBaseQueryHandlerFactory queryHBaseHandlerFactory,
            CacheQueryResTaskInfo cacheQueryResTaskInfo, LRUCache<String, String> lruCache) {
        this.queryHBaseHandlerFactory = queryHBaseHandlerFactory;
        this.cacheQueryResTaskInfo = cacheQueryResTaskInfo;
        this.lruCache = lruCache;
        this.cursor = cacheQueryResTaskInfo.getQueryBatch().getNextCursorMark();
    }

    private static String getSelectReqExceptionRsp(String msg, int msgCode) {
        JsonObject rspJson = new JsonObject();
        JsonObject errorJson = new JsonObject();
        JsonObject responseJson = new JsonObject();

        responseJson.addProperty("numFound", 0);
        rspJson.add("response", responseJson);

        errorJson.addProperty("msg", msg);
        errorJson.addProperty("code", msgCode);
        rspJson.add("error", errorJson);

        return rspJson.toString();
    }

    @Override
    public void run() {
        try {
            String rsp = cacheQueryResTaskInfo.getQueryBatch().startQueryBySolrj(queryHBaseHandlerFactory, null);
            cacheQueryResTaskInfo.setFinish(true);
            lruCache.put(cursor, rsp);
        } catch (LogQueryException e) {
            logger.error("failed to query for cache", e);
            cacheQueryResTaskInfo.setRetryCnt(cacheQueryResTaskInfo.getRetryCnt() + 1);
            if (cacheQueryResTaskInfo.getRetryCnt() >= QueryAndCacheThread.maxRetryCnt) {
                lruCache.put(cursor, getSelectReqExceptionRsp(e.getMessage(), e.getMsgCode()));
                cacheQueryResTaskInfo.setFinish(true);
            }
        }
    }
}
