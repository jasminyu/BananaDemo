package com.etisalat.log.query;

import com.etisalat.log.common.JsonUtil;
import com.etisalat.log.common.LogQueryException;
import com.etisalat.log.common.WTType;
import com.etisalat.log.config.LogConfFactory;
import com.etisalat.log.parser.Cursor;
import com.etisalat.log.parser.QueryCondition;
import com.etisalat.log.sort.SortField;
import com.etisalat.log.sort.SortUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CursorRspProcessor implements RspProcess {
    protected static final Logger logger = LoggerFactory.getLogger(CursorRspProcessor.class);

    private QueryCondition queryCondition = null;
    private Cursor cursor = null;
    private long realReturnNum = 0;
    private long singleFileSize = 0;
    private String maxCollection = null;
    private String maxCollShard = null;
    private Set<String> maxCollShardSet;

    private SolrQueryHandlerFactory solrHandlerFactory = null;

    public CursorRspProcessor(QueryCondition queryCondition, Cursor cursor, long realReturnNum) {
        this.cursor = cursor;
        this.queryCondition = queryCondition;
        this.realReturnNum = realReturnNum;
        this.singleFileSize = SolrUtils.getSingleFileRecordNum(realReturnNum);
    }

    public CursorRspProcessor(QueryCondition queryCondition, Cursor cursor, long realReturnNum,
            Set<String> maxCollShardSet) {
        this(queryCondition, cursor, realReturnNum);
        this.maxCollShardSet = maxCollShardSet;
        setMaxCollection();
        setMaxCollShardId();
    }

    public SolrQueryHandlerFactory getSolrHandlerFactory() {
        return solrHandlerFactory;
    }

    public void setSolrHandlerFactory(SolrQueryHandlerFactory solrHandlerFactory) {
        this.solrHandlerFactory = solrHandlerFactory;
    }

    private void setMaxCollection(){
        List<String> collections = new ArrayList<String>();
        for(String collWithShard : maxCollShardSet) {
           collections.add(SolrUtils.getCollection(collWithShard));
        }

        Collections.sort(collections);
        this. maxCollection = collections.get(collections.size()-1);
    }

    private void setMaxCollShardId() {
        List<Integer> maxShardIds = new ArrayList<Integer>();
        for (String collWithShard : maxCollShardSet) {
            if (collWithShard.startsWith(maxCollection)) {
                try {
                    maxShardIds.add(SolrUtils.getIdOfShardId(SolrUtils.getShardId(collWithShard)));
                } catch (LogQueryException e) {
                    logger.warn("Query session {}", cursor.getCacheKey(), e.getMessage());
                }
            }
        }

        Collections.sort(maxShardIds);
        maxCollShard = SolrUtils.getCollWithShardId(maxCollection, "_shard" + maxShardIds.get(maxShardIds.size() - 1));
    }

    @Override
    public String process() throws LogQueryException {
        if (queryCondition.isExportOp()) {
            return processExport();
        } else {
            return processDisplay();
        }
    }

    @Override
    public String processExport() throws LogQueryException {
        if(realReturnNum == 0) {
        	return null;
        }
        
    	String cacheKey = cursor.getCacheKey();
        logger.info("Start process download, query session {}", cacheKey);
        long waitStart = System.currentTimeMillis();

        try {
            Thread.sleep(LogConfFactory.queryFirstWaitTime);
        } catch (InterruptedException e) {
        }

        String collection = null;
        String collWithShardId = null;
        int idx = 0;
        int cnt = 0;
        List<String> jsonObjectList = null;
        int jsonListSize = 0;
        boolean walkingFinished = false;
        while (true) {
            checkTime(waitStart);
            
            if(cnt >= realReturnNum) {
            	break;
            }
            
            ConcurrentHashMap<String,List<String>> resultMap = QueryBatch.RESULTS_FOR_SHARDS.get(cacheKey);
            if (resultMap == null) {
            try {
            	Thread.sleep(LogConfFactory.queryResultCheckInterval);
                } catch (InterruptedException e) {
            	logger.warn(e.getMessage());
            }
          continue;
         }
        
            collection = cursor.getCollection();
            collWithShardId =  SolrUtils.getCollWithShardId(collection, cursor.getShardId());
            idx = cursor.getFetchIdx();
            
            logger.debug("Query session {}, getres {}, {}, {}",cacheKey,collection,collWithShardId,idx);
            jsonObjectList = resultMap.get(collWithShardId);
            
            if (jsonObjectList == null) {
            	if (collection.equals(maxCollection) && maxCollShardSet.contains(LogConfFactory.solrMaxShardId)){
                break;
            }
            
            try {
                Thread.sleep(LogConfFactory.queryResultCheckInterval);
            } catch (InterruptedException e) {
            	logger.warn(e.getMessage());
            }
            continue;
        }

        jsonListSize = jsonObjectList.size();
        cnt = cnt + jsonListSize;
        
            walkingFinished = isWalkingFinished(cursor,collection,collWithShardId,jsonListSize,jsonListSize);
        if (walkingFinished) {
        	break;
        }
        
        updateCursor(cursor,collection,collWithShardId,jsonListSize,jsonListSize);
    }
        
        ConcurrentHashMap<String, List<String>> resultMap = QueryBatch.RESULTS_FOR_SHARDS.get(cacheKey);
        if (resultMap == null) {
            String errMsg = "failed to query for null results.";
            logger.error("Query session " + cacheKey + ", error: " + errMsg);
            throw new LogQueryException(errMsg);
        }

        logger.warn("Query remove session {}", cacheKey);
        QueryBatch.RESULTS_FOR_SHARDS.remove(cacheKey);
        QueryBatch.RESULTS_CNT_FOR_SHARDS.remove(cacheKey);
        
        if (realReturnNum !=0 && cnt == 0) {
        	resultMap.clear();
        	logger.error("Query session {}, failed to get data, query {}, but cannot fetch the query data.",cacheKey,realReturnNum);
        	return JsonUtil.toJson(SolrUtils.getErrJsonObj("failed to download data.", 500));
        }
        
       return export(resultMap);
    }

    private String export(ConcurrentHashMap<String, List<String>> resultMap) throws LogQueryException {
        List<SortField> fields = queryCondition.getSortedFields();
        if (fields == null || fields.size() == 0) {
            return exportWithoutSort(resultMap);
        }

        List<JsonObject> results = SortUtils.sort(resultMap, fields, (int) realReturnNum, LogConfFactory.uniqueKey);
        resultMap.clear();
        resultMap = null;
        
        logger.info("Query session {}, fetch total {} docs.", cursor.getCacheKey(),results.size());
        String res = null;
        try {
            if (WTType.JSON == queryCondition.getWtType()) {
                res = writeJsonFiles(results);
            } else if (WTType.CSV == queryCondition.getWtType()) {
                res = writeCSVFiles(results);
            } else if (WTType.XML == queryCondition.getWtType()) {
                res = writeXmlFiles(results);
            } else {
                throw new LogQueryException("Unknown wtType.");
            }
            
            results.clear();
            results =null;
            return res;
        } catch (IOException e) {
            throw new LogQueryException("failed to download data, " + e.getMessage());
        }
    }

    private String exportWithoutSort(Map<String, List<String>> resultMap) throws LogQueryException {
        String res = null;
    	try {
            if (WTType.JSON == queryCondition.getWtType()) {
                res = writeJsonFiles(resultMap.entrySet());
            } else if (WTType.CSV == queryCondition.getWtType()) {
                res = writeCSVFiles(resultMap.entrySet());
            } else if (WTType.XML == queryCondition.getWtType()) {
                res = writeXmlFiles(resultMap.entrySet());
            } else {
                throw new LogQueryException("Unknown wtType.");
            }
            resultMap.clear();
            resultMap =null;
            
            return res;
        } catch (IOException e) {
            throw new LogQueryException("failed to download data, " + e.getMessage());
        }
    }

    private String getFileName(int fileIdx) {
        return queryCondition.getLocalPath() + File.separator + "data-" + fileIdx + "." + queryCondition.getWtType()
                .name().toLowerCase();
    }

    private void write(String filePath, String content) throws IOException {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(new BufferedWriter(new FileWriter(filePath)));
            writer.write(content);
            writer.flush();
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    private String writeJsonFiles(Set<Map.Entry<String, List<String>>> entrySet) throws IOException {
        JsonObject resultObj = SolrUtils.deepCopyJsonObj(LogConfFactory.solrQueryRspJsonObj);
        JsonObject rspJson = resultObj.getAsJsonObject("response");
        if (null == rspJson) {
            logger.error("Query session {}, rspJson is null and return null!", cursor.getCacheKey());
            return null;
        }

        JsonArray rspJsonList = new JsonArray();
        rspJson.addProperty("nums", realReturnNum);
        List<String> jsonObjects = null;
        int docNum = 0;
        boolean finished = false;
        int fileIdx = 0;
        for (Map.Entry<String, List<String>> entry : entrySet) {
            if (finished) {
                break;
            }

            jsonObjects = entry.getValue();
            for (String jsonObjStr : jsonObjects) {
                if (docNum >= realReturnNum) {
                    finished = true;
                    break;
                }

                docNum++;

                rspJsonList.add(JsonUtil.fromJson(jsonObjStr, JsonObject.class));
                if (docNum % singleFileSize == 0) {
                    rspJson.add("docs", rspJsonList);
                    write(getFileName(fileIdx++), JsonUtil.toJson(resultObj));
                    rspJsonList = new JsonArray();
                }
            }
        }

        if (!finished && rspJsonList.size() != 0) {
            rspJson.add("docs", rspJsonList);
            write(getFileName(fileIdx++), JsonUtil.toJson(resultObj));
        }

        logger.info("Query session {}, fetch total {} docs.", cursor.getCacheKey(),docNum);
        return queryCondition.getLocalPath();
    }

    private String writeXmlFiles(Set<Map.Entry<String, List<String>>> entrySet) throws IOException {
        List<String> jsonObjects = null;
        int docNum = 0;
        XmlWriter xmlWriter = null;
        boolean finished = false;
        int fileIdx = 0;
        try {
            xmlWriter = XmlWriter.getWriter(getFileName(fileIdx++), realReturnNum);
            xmlWriter.writeStart();
            for (Map.Entry<String, List<String>> entry : entrySet) {
                if (finished) {
                    break;
                }

                jsonObjects = entry.getValue();
                for (String jsonObjStr : jsonObjects) {
                    if (docNum >= realReturnNum) {
                        finished = true;
                        break;
                    }

                    docNum++;
                    xmlWriter.writeXmlJson(JsonUtil.fromJson(jsonObjStr, JsonObject.class));
                    if (docNum % singleFileSize == 0) {
                        xmlWriter.writeEnd();
                        xmlWriter.flush();
                        xmlWriter.close();

                        if (docNum >= realReturnNum) {
                            finished = true;
                            break;
                        } else {
                            xmlWriter = XmlWriter.getWriter(getFileName(fileIdx++), realReturnNum);
                            xmlWriter.writeStart();
                        }
                    }
                }
            }
            if (!finished && docNum % singleFileSize != 0) {
                xmlWriter.writeEnd();
                xmlWriter.flush();
                xmlWriter.close();
            }
        } finally {
            if (xmlWriter != null && !xmlWriter.isClosed()) {
                xmlWriter.close();
            }
        }

        logger.info("Query session {}, fetch total {} docs.", cursor.getCacheKey(),docNum);
        return queryCondition.getLocalPath();
    }

    private String writeCSVFiles(Set<Map.Entry<String, List<String>>> entrySet) throws IOException {
        String csvHeader = null;
        StringBuilder builder = new StringBuilder();
        List<String> jsonObjects = null;
        JsonObject jsonObj = null;
        boolean first = true;
        int docNum = 0;
        boolean finished = false;
        int fileIdx = 0;
        for (Map.Entry<String, List<String>> entry : entrySet) {
            if (finished) {
                break;
            }

            jsonObjects = entry.getValue();
            for (String jsonObjStr : jsonObjects) {
                if (docNum >= realReturnNum) {
                    finished = true;
                    break;
                }

                docNum++;

                jsonObj = JsonUtil.fromJson(jsonObjStr, JsonObject.class);
                if (LogConfFactory.keepCsvHeader && csvHeader == null) {
                    csvHeader = SolrUtils.getCsvHeader(jsonObj);
                    builder.append(csvHeader).append("\n");
                }

                Set<Map.Entry<String, JsonElement>> set = jsonObj.getAsJsonObject().entrySet();
                first = true;
                for (Map.Entry<String, JsonElement> entryElement : set) {
                    if (first) {
                        builder.append(entryElement.getValue().getAsString());
                        first = false;
                        continue;
                    }
                    builder.append(",").append(entryElement.getValue().getAsString());
                }

                builder.append("\n");
                if (docNum % singleFileSize == 0) {
                    write(getFileName(fileIdx++), builder.toString());
                    builder.delete(0, builder.length());
                    if (LogConfFactory.keepCsvHeader) {
                        builder.append(csvHeader).append("\n");
                    }

                }
            }
        }

        if (!finished && builder.length() != 0) {
            write(getFileName(fileIdx++), builder.toString());
        }

        logger.info("Query session {}, fetch total {} docs.", cursor.getCacheKey(),docNum);
        return queryCondition.getLocalPath();
    }

    private String writeJsonFiles(List<JsonObject> jsonObjects) throws IOException {
        JsonObject resultObj = SolrUtils.deepCopyJsonObj(LogConfFactory.solrQueryRspJsonObj);
        JsonObject rspJson = resultObj.getAsJsonObject("response");
        if (null == rspJson) {
            logger.error("Query session {}, rspJson is null and return null!", cursor.getCacheKey());
            return null;
        }

        JsonArray rspJsonList = new JsonArray();
        rspJson.addProperty("nums", realReturnNum);
        int docNum = 0;
        boolean finished = false;
        int fileIdx = 0;
        for (JsonObject jsonObj : jsonObjects) {
            if (docNum >= realReturnNum) {
                finished = true;
                break;
            }

            docNum++;

            rspJsonList.add(jsonObj);
            if (docNum % singleFileSize == 0) {
                rspJson.add("docs", rspJsonList);
                write(getFileName(fileIdx++), JsonUtil.toJson(resultObj));
                rspJsonList = new JsonArray();
            }
        }

        if (!finished && rspJsonList.size() != 0) {
            rspJson.add("docs", rspJsonList);
            write(getFileName(fileIdx++), JsonUtil.toJson(resultObj));
        }

        return queryCondition.getLocalPath();
    }

    private String writeXmlFiles(List<JsonObject> jsonObjects) throws IOException {
        XmlWriter xmlWriter = null;
        int docNum = 0;
        boolean finished = false;
        int fileIdx = 0;
        try {
            xmlWriter = XmlWriter.getWriter(getFileName(fileIdx++), realReturnNum);
            xmlWriter.writeStart();

            for (JsonObject jsonObj : jsonObjects) {
                if (docNum >= realReturnNum) {
                    finished = true;
                    break;
                }

                docNum++;
                xmlWriter.writeXmlJson(jsonObj);
                if (docNum % singleFileSize == 0) {
                    xmlWriter.writeEnd();
                    xmlWriter.flush();
                    xmlWriter.close();

                    if (docNum >= realReturnNum) {
                        finished = true;
                        break;
                    } else {
                        xmlWriter = XmlWriter.getWriter(getFileName(fileIdx++), realReturnNum);
                        xmlWriter.writeStart();
                    }
                }
            }

            if (!finished && docNum % singleFileSize != 0) {
                xmlWriter.writeEnd();
                xmlWriter.flush();
                xmlWriter.close();
            }
        } finally {
            if (xmlWriter != null && !xmlWriter.isClosed()) {
                xmlWriter.close();
            }
        }
        return queryCondition.getLocalPath();
    }

    private String writeCSVFiles(List<JsonObject> jsonObjectList) throws IOException {
        String csvHeader = null;
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        int docNum = 0;
        boolean finished = false;
        int fileIdx = 0;
        for (JsonObject jsonObj : jsonObjectList) {
            if (docNum >= realReturnNum) {
                finished = true;
                break;
            }

            docNum++;

            if (LogConfFactory.keepCsvHeader && csvHeader == null) {
                csvHeader = SolrUtils.getCsvHeader(jsonObj);
                builder.append(csvHeader).append("\n");
            }
            Set<Map.Entry<String, JsonElement>> set = jsonObj.getAsJsonObject().entrySet();
            first = true;
            for (Map.Entry<String, JsonElement> entryElement : set) {
                if (first) {
                    builder.append(entryElement.getValue().getAsString());
                    first = false;
                    continue;
                }
                builder.append(",").append(entryElement.getValue().getAsString());
            }

            builder.append("\n");

            if (docNum % singleFileSize == 0) {
                write(getFileName(fileIdx++), builder.toString());
                builder.delete(0, builder.length());
                if (LogConfFactory.keepCsvHeader) {
                    builder.append(csvHeader).append("\n");
                }
            }
        }

        if (!finished && builder.length() != 0) {
            write(getFileName(fileIdx++), builder.toString());
        }

        return queryCondition.getLocalPath();
    }

    @Override
    public String processDisplay() throws LogQueryException {
        String cacheKey = cursor.getCacheKey();
        logger.info("Start process display, query session {}", cacheKey);
        String collection = null;
        String collWithShardId = null;
        int idx = 0;

        if (queryCondition.getNextCursor() == null) {
            try {
                Thread.sleep(LogConfFactory.queryFirstWaitTime);
            } catch (InterruptedException e) {
            }
        }

        long rows = queryCondition.getTotalNum();
        List<String> jsonObjectList = null;
        int cnt = 0;
        long waitStart = System.currentTimeMillis();
        JsonObject resultJsonObj = SolrUtils.deepCopyJsonObj(LogConfFactory.solrQueryRspJsonObj);
        JsonObject rspJsonObj = resultJsonObj.getAsJsonObject("response");
        JsonArray resultJsonObjArray = new JsonArray();
        int jsonListSize = 0;
        boolean walkingFinished = false;
        while (true) {
            checkTime(waitStart);
            ConcurrentHashMap<String, List<String>> resultMap = QueryBatch.RESULTS_FOR_SHARDS.get(cacheKey);
            if (cnt >= rows || cnt >= realReturnNum) {
                break;
            }

            if (resultMap == null) {
                try {
                    Thread.sleep(LogConfFactory.queryResultCheckInterval);
                } catch (InterruptedException e) {
                    logger.warn(e.getMessage());
                }
                continue;
            }

            collection = cursor.getCollection();
            collWithShardId = SolrUtils.getCollWithShardId(collection, cursor.getShardId());
            idx = cursor.getFetchIdx();

            logger.debug("Query session {}, getres {}, {}, {}", cacheKey, collection, collWithShardId, idx);
            jsonObjectList = resultMap.get(collWithShardId);

            if (jsonObjectList == null) {
                if (collection.equals(maxCollection) && maxCollShardSet.contains(LogConfFactory.solrMaxShardId)) {
                    break;
                }

                try {
                    Thread.sleep(LogConfFactory.queryResultCheckInterval);
                } catch (InterruptedException e) {
                    logger.warn(e.getMessage());
                }

                continue;
            }

            jsonListSize = jsonObjectList.size();
            for (; idx < jsonListSize; idx++) {
                if (cnt >= rows) {
                    break;
                }
                resultJsonObjArray.add(JsonUtil.fromJson(jsonObjectList.get(idx),  JsonObject.class));
                cnt++;
            }

            walkingFinished = isWalkingFinished(cursor, collection, collWithShardId, idx, jsonListSize);
            if (walkingFinished) {
                break;
            }

            updateCursorAndUpdateCache(cursor, collection, collWithShardId, idx, jsonListSize);
        }

        if (queryCondition.isSort()) {
            resultJsonObjArray = SortUtils.sortSingleShardRsp(queryCondition.getSortedFields(), resultJsonObjArray);
        }

        rspJsonObj.add("docs", resultJsonObjArray);
        rspJsonObj.addProperty("nums", realReturnNum);
        
        if (realReturnNum != 0 && resultJsonObjArray.size() == 0 ) {
            logger.warn("Query remove session {} for no results.", cacheKey);
        	QueryBatch.RESULTS_FOR_SHARDS.remove(cacheKey);
        	QueryBatch.RESULTS_CNT_FOR_SHARDS.remove(cacheKey);
        	logger.error("Query session {}, failed to get data, query {}, but cannot fetch the query data.", cacheKey,realReturnNum);
        	return JsonUtil.toJson(SolrUtils.getErrJsonObj("failed to query data.", 500));
        }
        if(rows > realReturnNum || walkingFinished) {
            logger.warn("Query remove session {} for walk finished.", cacheKey);
            rspJsonObj.addProperty("nextCursorMark", "");
            QueryBatch.RESULTS_FOR_SHARDS.remove(cacheKey);
            QueryBatch.RESULTS_CNT_FOR_SHARDS.remove(cacheKey);
        }else {
            rspJsonObj.addProperty("nextCursorMark", cursor.toString());
            submitNextTask(cacheKey);
        }

        logger.info("Query session {} for rows {}, end process display.", cacheKey,
        		queryCondition.getTotalReturnNum() !=0 ? queryCondition.getTotalReturnNum() : queryCondition.getTotalNum());
        return JsonUtil.toJson(resultJsonObj);
    }

    private void submitNextTask(String cacheKey) throws LogQueryException {
        int left = queryCondition.getTotalNum();
        int start = cursor.getFetchIdx();
        String shardId = SolrUtils.getCollWithShardId(cursor.getCollection(), cursor.getShardId());

        ResultCnt resultCnt = QueryBatch.RESULTS_CNT_FOR_SHARDS.get(cacheKey);
        Map<String, SolrQueryTask> taskMap = resultCnt.getTaskMap();
        SolrQueryTask solrQueryTask = null;
        int numFound = 0;
        int cnt = 0;
        while (left > 0) {
            solrQueryTask = taskMap.get(shardId);
            if (solrQueryTask == null) {
                logger.info("Query session {}, shardId {}, not solr task and task size is {}", cursor.getCacheKey(), shardId, taskMap.size());
                shardId = getNextCollWithShard(shardId);
                continue;
            }
            numFound = Integer.valueOf(solrQueryTask.getRows());
            logger.info("Query session {}, shardId {}, left={}, numFound={}, numFound-start={}", cursor.getCacheKey(), shardId, left, numFound, numFound - start);
            if ((numFound - start) >= left) {
                solrQueryTask.setStartRows(start);
                solrQueryTask.setFetchRows(left);
                cnt++;
                solrHandlerFactory.submitQuerySolrTask(solrQueryTask);
                break;
            } else {
                solrQueryTask.setStartRows(start);
                solrQueryTask.setFetchRows(numFound - start);
                cnt++;
                solrHandlerFactory.submitQuerySolrTask(solrQueryTask);

                left = left - (numFound - start);
                start = 0;
                shardId = getNextCollWithShard(shardId);
                logger.info("Query session {}, shardId {}, left={}, numFound={}, numFound-start={}", cursor.getCacheKey(), shardId, left, numFound, numFound - start);
            }

            if (taskMap.size() == 0) {
                logger.info("taskMap size == 0");
                break;
            }
        }
        logger.info("Query session {}, total submit task size {}.", cursor.getCacheKey(), cnt);
    }

    private void checkTime(long waitStart) throws LogQueryException {
        long cost = System.currentTimeMillis() - waitStart;
        if (cost > LogConfFactory.queryTimeout) {
            String errMsg =
                    "query cost " + cost + "ms greater than " + LogConfFactory.queryTimeout + "ms, it is timeout";
            logger.error(errMsg);
            throw new LogQueryException(errMsg);
        }
    }

    private boolean isWalkingFinished(Cursor cursor, String collection, String collWithShardId, int idx, int jsonListSize)
            throws LogQueryException {
        logger.debug("cursor {}, {}, {}", cursor.toString(), collection, collWithShardId);
        if (idx >= jsonListSize && maxCollShard.equals(collWithShardId) && collection.equals(maxCollection)) {
            return true;
        }
        return false;
    }

    private void updateCursorAndUpdateCache(Cursor cursor, String collection, String collWithShardId, int idx, int jsonListSize)
            throws LogQueryException {
        if (idx >= jsonListSize) {
            String removeCollWithShard = SolrUtils.getPreCollWithShardId(collWithShardId);
            logger.warn("Query remove session {}, {}", cursor.getCacheKey(), removeCollWithShard);
            QueryBatch.RESULTS_FOR_SHARDS.get(cursor.getCacheKey()).remove(removeCollWithShard);
            QueryBatch.RESULTS_CNT_FOR_SHARDS.get(cursor.getCacheKey()).getTaskMap().remove(removeCollWithShard);
            cursor.setFetchIdx(0);
            if (maxCollShardSet.contains(collWithShardId)) {
                collection = SolrUtils.getNextCollection(collection, maxCollection);
                cursor.setCollection(collection);
                cursor.setShardId(LogConfFactory.solrMinShardId);
            } else {
                 cursor.setShardId(SolrUtils.getNextShardId(SolrUtils.getShardId(collWithShardId)));
            }
        } else {
            cursor.setFetchIdx(idx);
            cursor.setShardId(SolrUtils.getShardId(collWithShardId));
        }
    }
    
    private void updateCursor(Cursor cursor, String collection, String collWithShardId, int idx, int jsonListSize)
            throws LogQueryException {
    	if (idx >= jsonListSize) {
    	  cursor.setFetchIdx(0);
    	  if(maxCollShardSet.contains(collWithShardId)) {
    		  collection = SolrUtils.getNextCollection(collection, maxCollection);
    		  cursor.setCollection(collection);
    		  cursor.setShardId(LogConfFactory.solrMinShardId);
    	  } else {
    		  cursor.setShardId(SolrUtils.getNextShardId(SolrUtils.getShardId(collWithShardId)));
    	  }
    	} else {
    		cursor.setFetchIdx(idx);
    		cursor.setShardId(SolrUtils.getShardId(collWithShardId));
            logger.debug("Query session {} update cursor.", cursor.toString());
    	}
    }

    private String getNextCollWithShard(String collWithShardId) throws LogQueryException {
        String collection = SolrUtils.getCollection(collWithShardId);
        if (maxCollShardSet.contains(collWithShardId)) {
            collection = SolrUtils.getNextCollection(collection, maxCollection);
            return SolrUtils.getCollWithShardId(collection, LogConfFactory.solrMinShardId);
        } else {
            return SolrUtils.getCollWithShardId(collection, SolrUtils.getNextShardId(SolrUtils.getShardId(collWithShardId)));
        }
    }
}




