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
import java.util.concurrent.ThreadFactory;

public class CursorRspProcessor implements RspProcess {
    protected static final Logger logger = LoggerFactory.getLogger(CursorRspProcessor.class);

    private QueryCondition queryCondition = null;
    private Cursor cursor = null;
    private long realReturnNum = 0;
    private long singleFileSize = 0;
    private String maxCollection = null;
    private Set<String> maxCollShardSet;

    public CursorRspProcessor(QueryCondition queryCondition, Cursor cursor, long realReturnNum) {
        this.cursor = cursor;
        this.queryCondition = queryCondition;
        this.realReturnNum = realReturnNum;
        this.singleFileSize = SolrUtils.getSingleFileRecordNum(realReturnNum);;
    }

    public CursorRspProcessor(QueryCondition queryCondition, Cursor cursor, long realReturnNum, Set<String> maxCollShardSet) {
        this(queryCondition, cursor, realReturnNum);
        this.maxCollShardSet = maxCollShardSet;
        setMaxCollection();
    }

    private void setMaxCollection(){
        List<String> collections = new ArrayList<String>();
        for(String collWithShard : maxCollShardSet) {
            collections.add(collWithShard);
        }
        Collections.sort(collections);
        this.maxCollection = collections.get(collections.size()-1);
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
        String cacheKey = cursor.getCacheKey();
        long waitStart = System.currentTimeMillis();
        ResultCnt resultCnt = null;
        try{
            Thread.sleep(LogConfFactory.queryFirstWaitTime);
        }catch(InterruptedException e){
        }

        while (true) {
            checkTime(waitStart);
            resultCnt = QueryBatch.RESULTS_CNT_FOR_SHARDS.get(cacheKey);
            if (resultCnt == null) {
                throw new LogQueryException("no data to be downloaded");
            }
            if (resultCnt.getFetchNum() >= resultCnt.getTotalNum()) {
                break;
            }

            try{
                Thread.sleep(LogConfFactory.queryResultCheckInterval);
            }catch(InterruptedException e){
            }
        }
        
		Map<String, List<JsonObject>> resultMap = QueryBatch.RESULTS_FOR_SHARDS
				.get(cacheKey);
		if (resultMap == null) {
			String errMsg = "failed to query for null results.";
			logger.error(errMsg);
			throw new LogQueryException("errMsg");
		}
		
		return export(resultMap);

    }
    
    private String export(Map<String, List<JsonObject>> resultMap) throws LogQueryException {
		List<SortField> fields = queryCondition.getSortedFields();
		if (fields == null || fields.size() == 0) {
			return exportWithoutSort(resultMap);
		}

		List<JsonObject> results = SortUtils.sort(resultMap, fields,
				(int) realReturnNum, LogConfFactory.uniqueKey);
		try {
			if (WTType.JSON == queryCondition.getWtType()) {
				return writeJsonFiles(results);
			} else if (WTType.CSV == queryCondition.getWtType()) {
				return writeCSVFiles(results);
			} else if (WTType.XML == queryCondition.getWtType()) {
				return writeXmlFiles(results);
			} else {
				throw new LogQueryException("Unknown wtType.");
			}
		} catch (IOException e) {
			throw new LogQueryException("failed to download data, "
					+ e.getMessage());
		}
	}

    private String exportWithoutSort(Map<String, List<JsonObject>> resultMap) throws LogQueryException {
    	try {
    		if (WTType.JSON == queryCondition.getWtType()) {
    			return writeJsonFiles(resultMap.entrySet());
    			} else if (WTType.CSV == queryCondition.getWtType()) {
    				return writeCSVFiles(resultMap.entrySet());
    				} else if (WTType.XML == queryCondition.getWtType()) {
    			return writeXmlFiles(resultMap.entrySet());                   
    			} else {
                   throw new LogQueryException("Unknown wtType.");
               }
           } catch (IOException e) {
               throw new LogQueryException("failed to download data, " + e.getMessage());
           }
    }
 
    private String getFileName(int fileIdx) {
        return queryCondition.getLocalPath() + File.separator + "data-" + fileIdx + "." + queryCondition
                .getWtType().name().toLowerCase();
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

    private String writeJsonFiles(Set<Map.Entry<String, List<JsonObject>>> entrySet) throws IOException {
        JsonObject resultObj = SolrUtils.deepCopyJsonObj(LogConfFactory.solrQueryRspJsonObj);
        JsonObject rspJson = resultObj.getAsJsonObject("response");
        if (null == rspJson) {
            logger.error("rspJson is null and return null!");
            return null;
        }

        JsonArray rspJsonList = new JsonArray();
        rspJson.addProperty("nums", realReturnNum);
        List<JsonObject> jsonObjects = null;
        int docNum = 0;
        boolean finished = false;
        int fileIdx = 0;
        for (Map.Entry<String, List<JsonObject>> entry : entrySet) {
            if (finished) {
                break;
            }

            jsonObjects = entry.getValue();
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
        }

        if (!finished && rspJsonList.size() != 0) {
            rspJson.add("docs", rspJsonList);
            write(getFileName(fileIdx++), JsonUtil.toJson(resultObj));
        }

        return queryCondition.getLocalPath();
    }

    private String writeXmlFiles(Set<Map.Entry<String, List<JsonObject>>> entrySet) throws IOException {
        List<JsonObject> jsonObjects = null;
        int docNum = 0;
        XmlWriter xmlWriter = null;
        boolean finished = false;
        int fileIdx = 0;
        try {
            xmlWriter = XmlWriter.getWriter(getFileName(fileIdx++), realReturnNum);
            xmlWriter.writeStart();
            for (Map.Entry<String, List<JsonObject>> entry : entrySet) {
                if (finished) {
                    break;
                }

                jsonObjects = entry.getValue();
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

    private String writeCSVFiles(Set<Map.Entry<String, List<JsonObject>>> entrySet) throws IOException {
        String csvHeader = null;
        StringBuilder builder = new StringBuilder();
        List<JsonObject> jsonObjects = null;
        boolean first = true;
        int docNum = 0;
        boolean finished = false;
        int fileIdx = 0;
        for (Map.Entry<String, List<JsonObject>> entry : entrySet) {
            if (finished) {
                break;
            }

            jsonObjects = entry.getValue();
            for (JsonObject jsonObj : jsonObjects) {
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

        return queryCondition.getLocalPath();
    }

    private String writeJsonFiles(List<JsonObject> jsonObjects) throws IOException {
        JsonObject resultObj = SolrUtils.deepCopyJsonObj(LogConfFactory.solrQueryRspJsonObj);
        JsonObject rspJson = resultObj.getAsJsonObject("response");
        if (null == rspJson) {
            logger.error("rspJson is null and return null!");
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
        String collection = null;
        String collWithShardId = null;
        int idx = 0;

        if(queryCondition.getNextCursor() == null) {
            try{
                Thread.sleep(LogConfFactory.queryFirstWaitTime);
            }catch(InterruptedException e) {
            }
        }

        long rows = queryCondition.getTotalNum();
        List<JsonObject> jsonObjectList = null;
        int cnt = 0;
        long waitStart = System.currentTimeMillis();
        JsonObject resultJsonObj = SolrUtils.deepCopyJsonObj(LogConfFactory.solrQueryRspJsonObj);
        JsonObject rspJsonObj = resultJsonObj.getAsJsonObject("response");
        JsonArray resultJsonObjArray = new JsonArray();
        int jsonListSize = 0;
        while (true) {
            checkTime(waitStart);
            Map<String, List<JsonObject>> resultMap = QueryBatch.RESULTS_FOR_SHARDS.get(cacheKey);
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

            logger.debug("getres {}. {}, {}",collection, collWithShardId, idx);
            jsonObjectList = resultMap.get(collWithShardId);

            if (jsonObjectList == null) {
                if (collection.equals(maxCollection)) {
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
                resultJsonObjArray.add(jsonObjectList.get(idx));
                cnt++;
            }

            updateCursor(cursor, collection, collWithShardId, idx, jsonListSize);
        }

        if (queryCondition.isSort()) {
            resultJsonObjArray = SortUtils.sortSingleShardRsp(queryCondition.getSortedFields(), resultJsonObjArray);
        }

        rspJsonObj.add("docs", resultJsonObjArray);
        rspJsonObj.addProperty("nums", realReturnNum);
        rspJsonObj.addProperty("nextCursorMark", (rows > realReturnNum ? "" : cursor.toString()));

        return JsonUtil.toJson(resultJsonObj);
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

    private void updateCursor(Cursor cursor, String collection, String collWithShardId, int idx, int jsonListSize)
            throws LogQueryException {
        logger.debug("cursor {}. {},{}",cursor.toString(), collection, collWithShardId);
        if (idx >= jsonListSize) {
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
}
