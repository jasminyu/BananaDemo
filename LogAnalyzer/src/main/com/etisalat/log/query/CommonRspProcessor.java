package com.etisalat.log.query;

import com.etisalat.log.common.JsonUtil;
import com.etisalat.log.common.LogQueryException;
import com.etisalat.log.common.WTType;
import com.etisalat.log.config.LogConfFactory;
import com.etisalat.log.parser.QueryCondition;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;

public class CommonRspProcessor implements RspProcess {
    protected static final Logger logger = LoggerFactory.getLogger(CommonRspProcessor.class);

    private QueryCondition queryCondition = null;
    private long realReturnNum = 0;
    private long singleFileSize = 0;
    private CompletionService<HBaseQueryRsp> completionService = null;
    private Set<Future<HBaseQueryRsp>> pending = null;
    private List<String> rowKeyList = null;

    public CommonRspProcessor(List<String> rowKeyList, CompletionService<HBaseQueryRsp> completionService,
            Set<Future<HBaseQueryRsp>> pending, QueryCondition queryCondition, long realReturnNum)
            throws LogQueryException {
        if (rowKeyList == null || rowKeyList.size() == 0 || realReturnNum == 0) {
            logger.error("There's no data to be displayed or downloaded");
            throw new LogQueryException("There's no data to be displayed or downloaded");
        }

        if (pending == null || pending.size() == 0) {
            logger.error("No query hbase task was submitted.");
            throw new LogQueryException("No query hbase task was submitted.");
        }

        this.rowKeyList = rowKeyList;
        this.completionService = completionService;
        this.pending = pending;
        this.queryCondition = queryCondition;
        this.realReturnNum = realReturnNum;
        this.singleFileSize = SolrUtils.getSingleFileRecordNum(realReturnNum);
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
    public String processDisplay() throws LogQueryException {
        JsonObject resultObj = SolrUtils.deepCopyJsonObj(LogConfFactory.solrQueryRspJsonObj);
        JsonObject rspJson = resultObj.getAsJsonObject("response");
        if (null == rspJson) {
            logger.error("rspJson is null and return null!");
            return null;
        }

        JsonArray rspJsonList = new JsonArray();
        rspJson.add("docs", rspJsonList);
        rspJson.addProperty("nums", realReturnNum);
        Map<String, JsonObject> rspResults = collectHBaseQueryRsp();
        int docNum = 0;
        JsonObject jsonObj = null;
        for (String rowKey : rowKeyList) {
            jsonObj = rspResults.get(rowKey);
            if (jsonObj == null) {
                logger.debug("rowKey: {} does not have the related record.", rowKey);
                continue;
            }
            rspJsonList.add(jsonObj);

            ++docNum;
            if (docNum == realReturnNum) {
                return JsonUtil.toJson(resultObj);
            }
        }
        return JsonUtil.toJson(resultObj);
    }

    private void printCost(long maxTimeCost, long minTimeCost, long cost) {
        logger.warn("HBase took min {} ms", minTimeCost);
        logger.warn("HBase took max {} ms", maxTimeCost);
        logger.warn("HBase took {} ms", cost);
    }

    @Override
    public String processExport() throws LogQueryException {
        Map<String, JsonObject> rspResults = collectHBaseQueryRsp();
        try {
            if (WTType.JSON == queryCondition.getWtType()) {
                return writeJsonFiles(rspResults);
            } else if (WTType.CSV == queryCondition.getWtType()) {
                return writeCSVFiles(rspResults);
            } else if (WTType.XML == queryCondition.getWtType()) {
                return writeXmlFiles(rspResults);
            } else {
                throw new LogQueryException("Unknown wtType.");
            }
        } catch (IOException e) {
            throw new LogQueryException("failed to download data, " + e.getMessage());
        }
    }

    private String writeJsonFiles(Map<String, JsonObject> rspResults) throws IOException {
        JsonObject resultObj = SolrUtils.deepCopyJsonObj(LogConfFactory.solrQueryRspJsonObj);
        JsonObject rspJson = resultObj.getAsJsonObject("response");
        if (null == rspJson) {
            logger.error("rspJson is null and return null!");
            return null;
        }

        JsonArray rspJsonList = new JsonArray();
        rspJson.addProperty("nums", realReturnNum);
        JsonObject jsonObj = null;
        int docNum = 0;
        int fileIdx = 0;
        for (String rowKey : rowKeyList) {
            jsonObj = rspResults.get(rowKey);
            if (jsonObj == null) {
                logger.debug("rowKey: {} does not have the related record.", rowKey);
                continue;
            }
            docNum++;
            rspJsonList.add(jsonObj);
            if (docNum % singleFileSize == 0) {
                rspJson.add("docs", rspJsonList);
                write(getFileName(fileIdx++), JsonUtil.toJson(resultObj));
                rspJsonList = new JsonArray();
            }

            if (docNum >= realReturnNum) {
                break;
            }
        }

        if (rspJsonList.size() == 0) {
            return queryCondition.getLocalPath();
        }

        rspJson.add("docs", rspJsonList);
        write(getFileName(fileIdx++), JsonUtil.toJson(resultObj));
        return queryCondition.getLocalPath();
    }

    private String writeXmlFiles(Map<String, JsonObject> rspResults) throws IOException {
        XmlWriter xmlWriter = null;
        int docNum = 0;
        boolean finished = false;
        JsonObject jsonObj = null;
        int fileIdx = 0;
        try {
            xmlWriter = XmlWriter.getWriter(getFileName(fileIdx++), realReturnNum);
            xmlWriter.writeStart();
            for (String rowKey : rowKeyList) {
                jsonObj = rspResults.get(rowKey);
                if (jsonObj == null) {
                    logger.debug("rowKey: {} does not have the related record.", rowKey);
                    continue;
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

            if (!finished) {
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

    private String writeCSVFiles(Map<String, JsonObject> rspResults) throws IOException {
        int docNum = 0;
        JsonArray rspJsonList = new JsonArray();
        JsonObject jsonObj = null;
        int fileIdx = 0;
        for (String rowKey : rowKeyList) {
            jsonObj = rspResults.get(rowKey);
            if (jsonObj == null) {
                logger.debug("rowKey: {} does not have the related record.", rowKey);
                continue;
            }
            docNum++;
            rspJsonList.add(jsonObj);
            if (docNum % singleFileSize == 0) {
                write(getFileName(fileIdx++), SolrUtils.getCsvString(rspJsonList));
                rspJsonList = new JsonArray();
            }

            if (docNum >= realReturnNum) {
                break;
            }
        }

        if (rspJsonList.size() != 0) {
        	 write(getFileName(fileIdx++), SolrUtils.getCsvString(rspJsonList));
        }

        return queryCondition.getLocalPath();
    }

    private Map<String, JsonObject> collectHBaseQueryRsp() throws LogQueryException {
        long maxTimeCost = 0L;
        long minTimeCost = Long.MAX_VALUE;
        Map<String, JsonObject> rspResults = new HashMap<String, JsonObject>();
        long start = System.currentTimeMillis();
        while (pending.size() > 0) {

            if (rspResults.size() >= realReturnNum) {
                break;
            }

            try {
                Future<HBaseQueryRsp> future = completionService.take();
                pending.remove(future);
                HBaseQueryRsp rsp = future.get();
                if (rsp.getRspResults() != null && rsp.getRspResults().size() > 0) {
                    if (maxTimeCost < rsp.getTimeCost()) {
                        maxTimeCost = rsp.getTimeCost();
                    }
                    if (minTimeCost > rsp.getTimeCost()) {
                        minTimeCost = rsp.getTimeCost();
                    }
                    if (rsp.getRspResults().size() > 0) {
                        rspResults.putAll(rsp.getRspResults());
                    }
                } else {
                    continue;
                }
            } catch (Exception e) {
                throw new LogQueryException(e.getMessage());
            }
        }

        printCost(maxTimeCost, minTimeCost, (System.currentTimeMillis() - start));
        return rspResults;
    }

    private String getFileName(int idx) {
        return queryCondition.getLocalPath() + File.separator + "data-" + idx + "." + queryCondition.getWtType().name()
                .toLowerCase();
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
}
