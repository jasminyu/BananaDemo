package com.etisalat.log.query;

import com.etisalat.log.cache.CacheQueryResTaskInfo;
import com.etisalat.log.cache.LRUCache;
import com.etisalat.log.common.DateMathParser;
import com.etisalat.log.common.JsonUtil;
import com.etisalat.log.common.LogQueryException;
import com.etisalat.log.common.WTType;
import com.etisalat.log.config.LogConfFactory;
import com.etisalat.log.parser.Cursor;
import com.etisalat.log.parser.QueryCondition;
import com.google.gson.JsonObject;
import org.apache.hadoop.hbase.client.Get;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CursorMarkParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;

public class QueryBatch {
    public static final LRUCache<String, Map<String, List<JsonObject>>> RESULTS_FOR_SHARDS = new LRUCache<String, Map<String, List<JsonObject>>>();
    public static final LRUCache<String, ResultCnt> RESULTS_CNT_FOR_SHARDS = new LRUCache<String, ResultCnt>();
    public static final LRUCache<String, String> LRU_CACHE = new LRUCache<String, String>();
    public static final LRUCache<String, CacheQueryResTaskInfo> LRU_TASK_CACHE = new LRUCache<String, CacheQueryResTaskInfo>();
    protected static final Logger logger = LoggerFactory.getLogger(QueryBatch.class);
    private static CloudSolrClient solrClient;
    private String reqUrl;
    private WTType wtType;
    private String queryString = null;
    private SolrQuery parameters = new SolrQuery();
    private QueryCondition queryCondition;
    private List<String> collections = new ArrayList<String>();
    private int totalNum;
    private long searchNum = -1;
    private long realReturnNum = 0;
    private String nextCursorMark;

    public QueryBatch(String reqURL, String queryString) {
        this.reqUrl = reqURL;
        this.queryString = queryString;
    }

    public QueryBatch() {
    }

    public static boolean init() {
        logger.info("QueryBatch begin to init.....");
        try {
            solrClient = SolrUtils.getSolrClient();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("QueryBatch init failed!!", e);
            return false;
        }

        RESULTS_FOR_SHARDS.init();
        RESULTS_CNT_FOR_SHARDS.init();
        LRU_CACHE.init();
        LRU_TASK_CACHE.init();

        return true;
    }

    private static synchronized String getNextEndTime(String startTime, String endTime, int queryWindows)
            throws LogQueryException {
        String newEndTime = endTime;
        Date startDate = null;
        Date endDate = null;
        try {
            startDate = DateMathParser.LOCAL_TIME_FORMATER.parse(startTime);
            endDate = DateMathParser.LOCAL_TIME_FORMATER.parse(endTime);
        } catch (ParseException e) {
            throw new LogQueryException(e.getMessage(), 0);
        }

        Calendar calTime1 = Calendar.getInstance();
        calTime1.setTime(startDate);
        calTime1.add(Calendar.MILLISECOND, queryWindows);

        Calendar calTime2 = Calendar.getInstance();
        calTime2.setTime(endDate);
        long difference = calTime1.getTimeInMillis() - calTime2.getTimeInMillis();

        logger.debug("calTime1 is {}", DateMathParser.LOCAL_TIME_FORMATER.format(calTime1.getTime()));
        logger.debug("calTime2 is {}", DateMathParser.LOCAL_TIME_FORMATER.format(calTime2.getTime()));
        if (difference < 0) {
            newEndTime = DateMathParser.LOCAL_TIME_FORMATER.format(calTime1.getTime());
        }

        logger.info("newEndTime is {}", newEndTime);
        return newEndTime;
    }

    public void genParameters() throws UnsupportedEncodingException {
        List<String> paramList = Arrays.asList(queryString.split("&"));

        for (String param : paramList) {
            String[] paramPair = param.split("=");
            logger.debug("param : {}.", param);
            if ("wt".equals(paramPair[0])) {
                continue;
            }

            String paramString = URLDecoder.decode(paramPair[1], "UTF-8");
            logger.debug("{}={}.", paramPair[0], paramString);
            this.parameters.add(paramPair[0], paramString);
        }
    }

    public void setParameters(SolrQuery parameters) {
        this.parameters = parameters;
    }

    public QueryCondition getQueryCondition() {
        return queryCondition;
    }

    public void setQueryCondition(QueryCondition queryCondition) {
        this.queryCondition = queryCondition;
    }

    public String getQueryString() {
        return queryString;
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    public long getRealReturnNum() {
        return realReturnNum;
    }

    public void setRealReturnNum(long realReturnNum) {
        this.realReturnNum = realReturnNum;
    }

    public String getReqUrl() {
        return reqUrl;
    }

    public void setReqUrl(String reqUrl) {
        this.reqUrl = reqUrl;
    }

    public long getSearchNum() {
        return searchNum;
    }

    public void setSearchNum(long searchNum) {
        this.searchNum = searchNum;
    }

    public int getTotalNum() {
        return totalNum;
    }

    public void setTotalNum(int totalNum) {
        this.totalNum = totalNum;
    }

    public WTType getWtType() {
        return wtType;
    }

    public void setWtType(WTType wtType) {
        this.wtType = wtType;
    }

    public List<String> getCollections() {
        return collections;
    }

    public void setCollections(List<String> collections) {
        this.collections = collections;
    }

    public String getNextCursorMark() {
        return nextCursorMark;
    }

    public void setNextCursorMark(String nextCursorMark) {
        this.nextCursorMark = nextCursorMark;
    }

    private boolean ifNeedGetNextEndTime() throws LogQueryException {
        boolean bNeed = false;
        QueryResponse response = null;
        SolrQuery tmpParameters = new SolrQuery();
        long numFound = 0;
        String origQString = this.queryCondition.getOriQString();
        String startTime = this.queryCondition.getStartTime();
        String endTime = this.queryCondition.getEndTime();

        tmpParameters.add("collection", this.parameters.getParams("collection"));
        tmpParameters.add("rows", "0");
        tmpParameters.add("shards.tolerant", "true");

        StringBuilder tmpQStringSB = new StringBuilder();
        tmpQStringSB.setLength(0);
        try {
            tmpQStringSB.append("(" + URLDecoder.decode(origQString, "UTF-8") + ")");
        } catch (UnsupportedEncodingException e) {
            String errMsg = "failed to decode query string: " + origQString;
            logger.error(errMsg);
            throw new LogQueryException(errMsg, e);
        }

        tmpQStringSB.append(" AND " + LogConfFactory.fieldTimestamp + ":[" + startTime + " TO " + endTime + "]");

        logger.info("qString is {}", tmpQStringSB.toString());
        tmpParameters.add("q", tmpQStringSB.toString());

        try {
            response = solrClient.query(tmpParameters);
        } catch (Exception e) {
            logger.error("Get real time range met an error!! Message :{}.", e);
            throw new LogQueryException(e.getMessage(), 509);
        }

        numFound = response.getResults().getNumFound();
        searchNum = numFound;
        logger.info("numFound is {} in Function needGetNextEndTime.", numFound);
        if (numFound > LogConfFactory.solrUseWindowMinDoc) {
            bNeed = true;
        }

        return bNeed;
    }

    private void getRealEndTime() throws LogQueryException {
        if (LogConfFactory.queryPerShard) {
            return;
        }

        QueryResponse response = null;
        SolrQuery tmpParameters = new SolrQuery();
        long numFound = 0;
        String origQString = this.queryCondition.getOriQString();
        String startTime = this.queryCondition.getStartTime();
        String endTime = this.queryCondition.getEndTime();
        long start = System.currentTimeMillis();

        if (!ifNeedGetNextEndTime()) {
            logger.warn("It doesn't need to use Query Windows!");
            long secs = System.currentTimeMillis() - start;
            logger.warn("Get Real EndTime and took " + secs + " secs.");
            return;
        }

        tmpParameters.add("collection", this.parameters.getParams("collection"));
        tmpParameters.add("rows", "0");
        tmpParameters.add("shards.tolerant", "true");

        String newStartTime = startTime;
        String newEndTime = getNextEndTime(startTime, endTime, LogConfFactory.solrQueryWindow);
        boolean bFirst = true;
        int queryWinDep = 0;

        while (true) {
            StringBuilder tmpQStringSB = new StringBuilder();
            tmpQStringSB.setLength(0);
            try {
                tmpQStringSB.append("(" + URLDecoder.decode(origQString, "UTF-8") + ")");
            } catch (UnsupportedEncodingException e) {
                String errMsg = "failed to decode query string: " + origQString;
                logger.error(errMsg);
                throw new LogQueryException(errMsg, e);
            }

            if (bFirst) {
                tmpQStringSB.append(" AND " + LogConfFactory.fieldTimestamp + ":[" + newStartTime + " TO " + newEndTime
                        + "]");
                bFirst = false;
            } else {
                tmpQStringSB.append(" AND " + LogConfFactory.fieldTimestamp + ":{" + newStartTime + " TO " + newEndTime
                        + "]");
            }

            logger.info("qString is {}", tmpQStringSB.toString());
            tmpParameters.remove("q");
            tmpParameters.add("q", tmpQStringSB.toString());

            try {
                response = solrClient.query(tmpParameters);
            } catch (Exception e) {
                logger.error("Get real time range met an error!! Message :{}.", e);
                e.printStackTrace();
                throw new LogQueryException(e.getMessage(), 509);
            }

            numFound += response.getResults().getNumFound();
            ++queryWinDep;

            logger.info("numFound is {}.", numFound);
            logger.info("queryWinDep is {}.", queryWinDep);
            if (numFound >= this.totalNum || queryWinDep > LogConfFactory.solrQueryWinMaxDep) {
                break;
            }

            newStartTime = newEndTime;
            newEndTime = getNextEndTime(newStartTime, endTime, LogConfFactory.solrQueryWindow);
            if (endTime.equals(newEndTime)) {
                break;
            }
        }

        logger.info("startTime is {}", startTime);
        logger.info("newEndTime is {}", newEndTime);

        logger.warn("Get Real EndTime and took {} ms", System.currentTimeMillis() - start);
        return;
    }

    private String getRealQueryString() throws LogQueryException {
        getRealEndTime();
        String origQString = this.queryCondition.getOriQString();
        StringBuilder tmpQStringSB = null;
        try {
            tmpQStringSB = new StringBuilder("(" + URLDecoder.decode(origQString, "UTF-8") + ")");
        } catch (UnsupportedEncodingException e) {
            String errMsg = "failed to decode query string: " + origQString;
            logger.error(errMsg);
            throw new LogQueryException(errMsg, e);
        }

        tmpQStringSB.append(" AND " + LogConfFactory.fieldTimestamp + ":[" + this.queryCondition.getStartTime() + " TO "
                + this.queryCondition.getEndTime() + "]");
        return tmpQStringSB.toString();
    }

    public QueryBatch deepCopy(String nextCursorMark) {
        QueryBatch queryBatch = new QueryBatch();
        queryBatch.reqUrl = this.reqUrl;
        queryBatch.queryCondition = this.queryCondition;
        queryBatch.queryCondition.setNextCursorMark(nextCursorMark);
        queryBatch.setWtType(this.wtType);
        queryBatch.setTotalNum(this.totalNum);
        queryBatch.nextCursorMark = nextCursorMark;
        queryBatch.queryString = this.queryString;
        queryBatch.setCollections(this.collections);
        return queryBatch;
    }

    public String startQueryBySolrj(HBaseQueryHandlerFactory queryHBaseHandlerFactory,
            SolrQueryHandlerFactory solrQueryHandlerFactory) throws LogQueryException {
        logger.info("Start to query batch by solrj.");
        if (!LogConfFactory.queryPerShard || queryCondition.queryInCloudMode()) {
            return startQuery(queryHBaseHandlerFactory);
        } else {
            return queryPerShards(solrQueryHandlerFactory, queryHBaseHandlerFactory);
        }

    }

    public String startQuery(HBaseQueryHandlerFactory queryHBaseHandlerFactory) throws LogQueryException {
        logger.info("startQuery Start.");

        CompletionService<HBaseQueryRsp> completionService = queryHBaseHandlerFactory.newCompletionService();
        Set<Future<HBaseQueryRsp>> pending = new HashSet<Future<HBaseQueryRsp>>();

        String qRsp = null;
        CacheQueryResTaskInfo cacheQueryResTaskInfo = null;
        List<String> rspRowKeyList = new ArrayList<String>();

        long start = System.currentTimeMillis();
        try {
            genParameters();
            if (LogConfFactory.enablePaging) {
                qRsp = queryCondition.getNextCursorMark() == null ?
                        null :
                        LRU_CACHE.get(queryCondition.getNextCursorMark());
                cacheQueryResTaskInfo = queryCondition.getNextCursorMark() == null ?
                        null :
                        LRU_TASK_CACHE.get(queryCondition.getNextCursorMark());
                if (qRsp != null) {
                    this.nextCursorMark = cacheQueryResTaskInfo.getQueryBatch().getNextCursorMark();
                    LRU_TASK_CACHE.remove(queryCondition.getNextCursorMark());
                    return qRsp;
                } else {
                    queryWithCursor(completionService, pending, rspRowKeyList);
                }
            } else {
                if (queryCondition.isNeedUseCursor()) {
                    queryWithCursorMulti(completionService, pending, rspRowKeyList);
                } else {
                    queryWithoutCursor(completionService, pending, rspRowKeyList);
                }
            }
        } catch (IOException e) {
            logger.error("startQuery and IOException arised!", e);
            throw new LogQueryException(e.getMessage());
        }

        long start2 = System.currentTimeMillis();
        logger.warn("start to process results");
        CommonRspProcessor commonRspProcessor = new CommonRspProcessor(rspRowKeyList, completionService, pending,
                queryCondition, getRealReturnNum());

        qRsp = commonRspProcessor.process();
        logger.warn("end to process results cost {}ms", System.currentTimeMillis() - start2);
        logger.warn("Solr with HBase query {}ms", System.currentTimeMillis() - start);
        logger.info("startQuery End.");
        return qRsp;
    }

    private void queryWithCursor(CompletionService<HBaseQueryRsp> completionService, Set<Future<HBaseQueryRsp>> pending,
            List<String> rspRowKeyList) throws IOException, LogQueryException {
        logger.info("queryWithCursor Start.");

        QueryResponse response = null;

        long start = System.currentTimeMillis();

        String qString = getRealQueryString();
        logger.info("qString is {}", qString);

        try {
            this.parameters.add("q", qString);
            this.parameters.setRows(queryCondition.getTotalNum());
            this.parameters.set(CursorMarkParams.CURSOR_MARK_PARAM, queryCondition.getNextCursorMark() == null ?
                    CursorMarkParams.CURSOR_MARK_START :
                    queryCondition.getNextCursorMark());

            response = solrClient.query(this.parameters);
        } catch (Exception e) {
            logger.error("query solr \"{}\" get an error!!.", this.parameters.toString(), e);
            throw new LogQueryException(e.getMessage(), 509);
        }

        logger.info("query {}, with cursor {}, QTime {}, cost {} ms", qString, queryCondition.getNextCursorMark(),
                response.getQTime(), System.currentTimeMillis() - start);

        fetchResultsFromHBase(response, completionService, pending, rspRowKeyList);

        String newCursor = response.getNextCursorMark();
        if (response.getResults().getNumFound() < queryCondition.getTotalNum() || newCursor
                .equals(queryCondition.getNextCursorMark())) {
            queryCondition.setNextCursorMark(null);
        } else {
            queryCondition.setNextCursorMark(newCursor);
        }

        this.nextCursorMark = queryCondition.getNextCursorMark();
        this.realReturnNum =
                queryCondition.getTotalReturnNum() < searchNum ? queryCondition.getTotalReturnNum() : searchNum;

        logger.warn("Solr(javabin) took {} ms", System.currentTimeMillis() - start);
        logger.info("queryWithCursor End.");
    }

    private void queryWithCursorMulti(CompletionService<HBaseQueryRsp> completionService,
            Set<Future<HBaseQueryRsp>> pending, List<String> rspRowKeyList) throws IOException, LogQueryException {
        logger.info("queryWithCursorMulti Start.");
        int batchSize = getSolrBatchSize(this.totalNum);
        int left = this.totalNum;

        String realQueryString = getRealQueryString();
        logger.debug(queryString);

        long start = System.currentTimeMillis();
        int queryCounter = 0;

        float minTimeCost = Float.MAX_VALUE;
        float maxTimeCost = 0.0f;

        QueryResponse response = null;
        String nextCursorMark = "*";

        logger.info("query solr start");

        while (left > 0) {
            if (left < batchSize) {
                batchSize = left;
            }

            logger.info("query solr start");
            long start1 = System.currentTimeMillis();
            try {
                this.parameters.add("q", realQueryString);
                this.parameters.setRows(batchSize);
                this.parameters.set(CursorMarkParams.CURSOR_MARK_PARAM, nextCursorMark);

                response = solrClient.query(this.parameters);
            } catch (Exception e) {
                logger.error("query solr get an error!! Message :{}.", e);
                throw new LogQueryException(e.getMessage(), 509);
            }

            logger.info("query {}, with cursor {}, QTime {}, cost {}ms", realQueryString,
                    queryCondition.getNextCursorMark(), response.getQTime(), System.currentTimeMillis() - start1);

            if (response != null) {
                fetchResultsFromHBase(response, completionService, pending, rspRowKeyList);

                left -= response.getResults().size();
                nextCursorMark = response.getNextCursorMark();
            }

            long solrInMS = System.currentTimeMillis() - start1;
            logger.warn("Solr(javabin)-{} took {} ms.", ++queryCounter, solrInMS);

            if (solrInMS > maxTimeCost) {
                maxTimeCost = solrInMS;
            }

            if (solrInMS < minTimeCost) {
                minTimeCost = solrInMS;
            }
        }

        logger.warn("Solr(javabin) took min {}ms", minTimeCost);
        logger.warn("Solr(javabin) took max {}ms", maxTimeCost);
        logger.warn("Solr(javabin) took {}ms", System.currentTimeMillis() - start);
        logger.info("queryWithCursorMulti End.");
    }

    private void queryWithoutCursor(CompletionService<HBaseQueryRsp> completionService,
            Set<Future<HBaseQueryRsp>> pending, List<String> rspRowKeyList) throws IOException, LogQueryException {
        logger.info("queryWithoutCursor start.");

        String realQueryString = getRealQueryString();
        logger.debug(queryString);

        QueryResponse response = null;
        long start = System.currentTimeMillis();

        try {
            this.parameters.add("q", realQueryString);
            this.parameters.setRows(queryCondition.getTotalReturnNum());
            response = solrClient.query(this.parameters);
        } catch (Exception e) {
            logger.error("query solr get an error!! Message :{}.", e);
            throw new LogQueryException(e.getMessage(), 509);
        }

        this.realReturnNum =
                queryCondition.getTotalReturnNum() < searchNum ? queryCondition.getTotalReturnNum() : searchNum;
        logger.info("query string {}", queryString);
        logger.info("Solr(Javabin) without cursor elapsedTime {}ms, cost {}ms ", response.getElapsedTime(),
                System.currentTimeMillis() - start);

        fetchResultsFromHBase(response, completionService, pending, rspRowKeyList);

        logger.info("queryWithoutCursor End.");
    }

    protected int getSolrBatchSize(int count) {
        if (count > searchNum && searchNum != -1) {
            count = (int) searchNum;
            this.totalNum = count;
            this.queryCondition.setTotalNum(totalNum);
        }

        int tmpSize = count / LogConfFactory.solrBatchTime;

        if (tmpSize > LogConfFactory.solrMaxBatchSize) {
            tmpSize = LogConfFactory.solrMaxBatchSize;
        } else if (tmpSize < LogConfFactory.solrMinBatchSize) {
            tmpSize = LogConfFactory.solrMinBatchSize;
        }

        logger.info("solr batch size {} and actual query result num {}, and count {}", tmpSize, totalNum, count);
        return tmpSize;
    }

    private void fetchResultsFromHBase(QueryResponse response, CompletionService<HBaseQueryRsp> completionService,
            Set<Future<HBaseQueryRsp>> pending, List<String> rspRowKeyList) {
        if (response == null) {
            return;
        }

        SolrDocumentList results = response.getResults();
        logger.debug("Query DEBUG results size {} ", results.size());
        int hbaseSize = (results.size() < LogConfFactory.hbaseBatchMinSize * 10 ?
                LogConfFactory.hbaseBatchMinSize :
                LogConfFactory.hbaseBatchSize);

        List<Get> gets = new ArrayList<Get>();
        List<String> collections = queryCondition.getCollections();
        long start = System.currentTimeMillis();
        for (int i = 0; i < results.size(); i++) {
            String solrKey = ((SolrDocument) results.get(i)).getFieldValue(LogConfFactory.rowkeyName).toString();
            rspRowKeyList.add(solrKey);
            byte[] rowkey = SolrUtils.exchangeKey(solrKey);
            Get get = new Get(rowkey);
            gets.add(get);

            if (gets.size() == hbaseSize) {
                for (String collection : collections) {
                    submitQueryHBaseTask(completionService, pending, collection, gets);
                    gets = new ArrayList<Get>();
                }
            }
        }

        if (gets.size() != 0) {
            for (String collection : collections) {
                submitQueryHBaseTask(completionService, pending, collection, gets);
            }
        }

        logger.info("submit query hbase tasks {}ms", System.currentTimeMillis() - start);
    }

    private void submitQueryHBaseTask(CompletionService<HBaseQueryRsp> completionService,
            Set<Future<HBaseQueryRsp>> pending, String collection, List<Get> gets) {
        HBaseQueryHandler handler = HBaseQueryHandlerFactory.getQueryHBaseHandler(collection, queryCondition);
        handler.setGets(gets);
        handler.setCompletionService(completionService);
        handler.setPending(pending);
        handler.submit();
    }

    private String queryPerShards(SolrQueryHandlerFactory solrHandlerFactory,
            HBaseQueryHandlerFactory hbaseQueryHandlerFactory) throws LogQueryException {
        Cursor cursor = queryCondition.getNextCursor();
        if (cursor == null) {
            cursor = new Cursor(SolrUtils.generateCacheKey(), queryCondition.getCollections().get(0),
                    //                    SolrUtils.getCollWithShardId(queryCondition.getCollections().get(0), LogConfFactory.solrMinShardId),
                    LogConfFactory.solrMinShardId, 0);
            queryPerShards(cursor, solrHandlerFactory, hbaseQueryHandlerFactory);
            if (!queryCondition.isExportOp() && getRealReturnNum() == 0) {
                return JsonUtil.toJson(LogConfFactory.solrQueryRspJsonObj);
            }
        }

        ResultCnt resultCnt = RESULTS_CNT_FOR_SHARDS.get(cursor.getCacheKey());
        CursorRspProcessor rspProcessor = new CursorRspProcessor(queryCondition, cursor,
                resultCnt == null ? this.realReturnNum : resultCnt.getTotalNum());
        return rspProcessor.process();
    }

    private void queryPerShards(Cursor cursor, SolrQueryHandlerFactory solrHandlerFactory,
            HBaseQueryHandlerFactory queryHBaseHandlerFactory) throws LogQueryException {
        logger.info("Start to queryPerShards");

        long start = System.currentTimeMillis();

        String qString = getRealQueryString();

        Map<String, ResultCnt> reqUrlMap = new HashMap<String, ResultCnt>();
        long totalNumFound = 0;
        StringBuilder builder = new StringBuilder();
        for (String collection : collections) {
            totalNumFound = totalNumFound + getTotalNumFound(solrHandlerFactory, qString, collection, reqUrlMap);
            builder.append(",").append(collection);
            if (totalNumFound >= queryCondition.getTotalReturnNum()) {
                break;
            }
        }

        this.realReturnNum =
                queryCondition.getTotalReturnNum() < totalNumFound ? queryCondition.getTotalReturnNum() : totalNumFound;

        if (totalNumFound == 0) {
            return;
        }

        logger.info("Solr query string {} on collection {}", qString, builder.delete(0, 1).toString());
        logger.info("Actual total return num {}", realReturnNum);

        RESULTS_CNT_FOR_SHARDS.put(cursor.getCacheKey(), new ResultCnt(0, this.realReturnNum));

        Set<Map.Entry<String, ResultCnt>> entrySet = reqUrlMap.entrySet();
        ResultCnt resultCnt = null;
        SolrQueryTask querySolrTask = null;
        for (Map.Entry<String, ResultCnt> entry : entrySet) {
            resultCnt = entry.getValue();
            querySolrTask = new SolrQueryTask(qString, entry.getKey());
            querySolrTask.setCacheKey(cursor.getCacheKey());
            querySolrTask.setShardId(resultCnt.getCollWithShardId());
            querySolrTask.setCollection(SolrUtils.getCollection(resultCnt.getCollWithShardId()));
            querySolrTask.setqCondition(queryCondition);
            querySolrTask.setHttpClient(solrClient.getLbClient().getHttpClient());
            querySolrTask.setQueryHBaseHandlerFactory(queryHBaseHandlerFactory);
            querySolrTask.setRows(Long.valueOf((long) Math
                    .ceil((float) resultCnt.getTotalNum() / totalNumFound * queryCondition.getTotalReturnNum())
                    + LogConfFactory.solrQueryThreshold).toString());
            solrHandlerFactory.submitQuerySolrTask(querySolrTask);
        }

        logger.warn("Solr(javabin) took {} ms", System.currentTimeMillis() - start);
        logger.info("End to queryPerShards");
    }

    private long getTotalNumFound(SolrQueryHandlerFactory solrHandlerFactory, String qString, String collection,
            Map<String, ResultCnt> reqUrlMap) throws LogQueryException {
        long totalNumFound = 0;
        long maxTimeCost = 0l;
        long minTimeCost = Long.MAX_VALUE;
        long start = System.currentTimeMillis();
        try {
            DocCollection docCollection = getDocCollection(collection);
            CompletionService<SolrQueryRsp> solrCompletionService = solrHandlerFactory.newCompletionService();
            Set<Future<SolrQueryRsp>> solrGetNumFoundPending = new HashSet<Future<SolrQueryRsp>>();

            Collection<Slice> slices = docCollection.getSlices();
            String baseUrl = null;
            SolrQueryHandler solrHandler = null;
            for (Slice slice : slices) {
                baseUrl = SolrUtils.getRandomReplicaBaseUrl(slice);
                solrHandler = SolrQueryHandlerFactory.getSolrQueryHandler(qString, baseUrl);
                solrHandler.setCompletionService(solrCompletionService);
                solrHandler.setHttpClient(solrClient.getLbClient().getHttpClient());
                solrHandler.setPending(solrGetNumFoundPending);
                solrHandler.setRows("0");
                solrHandler.setCollWithShardId(SolrUtils.getCollWithShardId(collection, "_" + slice.getName()));
                solrHandler.submit();
            }

            long numFound = 0;

            while (solrGetNumFoundPending.size() > 0) {
                try {
                    Future<SolrQueryRsp> future = solrCompletionService.take();
                    solrGetNumFoundPending.remove(future);
                    try {
                        SolrQueryRsp solrQueryRsp = future.get();
                        numFound = SolrUtils.getNumFound(solrQueryRsp);
                        if (numFound <= 0) {
                            continue;
                        } else {
                            if (maxTimeCost < solrQueryRsp.getTimeCost()) {
                                maxTimeCost = solrQueryRsp.getTimeCost();
                            }
                            if (minTimeCost > solrQueryRsp.getTimeCost()) {
                                minTimeCost = solrQueryRsp.getTimeCost();
                            }

                            reqUrlMap.put(solrQueryRsp.getReqUrl(),
                                    new ResultCnt(solrQueryRsp.getCollWithShardId(), numFound));
                            totalNumFound = totalNumFound + numFound;
                            logger.info("Sent to {}, numFound {}, QTime {}, cost {} ms", solrQueryRsp.getReqUrl(),
                                    solrQueryRsp.getQueryResponse().getResults().getNumFound(),
                                    solrQueryRsp.getQueryResponse().getQTime(), solrQueryRsp.getTimeCost());
                        }

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.error("Live Query process interrupted");
                        throw new LogQueryException(e.getMessage(), 509);
                    }
                } catch (Exception e) {
                    logger.error("Live Query process Exception", e);
                    throw new LogQueryException(e.getMessage(), 509);
                }

            }
        } catch (Exception e) {
            logger.error("Live Query process Exception", e);
            throw new LogQueryException(e.getMessage(), 509);
        }

        logger.info("Solr(javabin) query total numFound {}, min cost {} ms, max cost {} ms, and total cost {} ms.",
                totalNumFound, minTimeCost, maxTimeCost, System.currentTimeMillis() - start);
        return totalNumFound;
    }

    private DocCollection getDocCollection(String collection) throws LogQueryException {
        ZkStateReader zkStateReader = solrClient.getZkStateReader();
        try {
            zkStateReader.updateAliases();
            zkStateReader.updateClusterState();
        } catch (Exception e) {
            logger.error("updateAliases failed", e);
            throw new LogQueryException(e.getMessage(), 509);
        }

        logger.debug("Aliases is {}", zkStateReader.getAliases());
        String collName = zkStateReader.getAliases().getCollectionAlias(collection);
        if (null == collName) {
            logger.error("Collection alias to {} doesn't exist!.", collection);
            throw new LogQueryException("Collection alias to " + collection + " doesn't exist.", 509);
        }

        DocCollection docCollection = zkStateReader.getClusterState().getCollection(collName);
        if (null == docCollection) {
            logger.error("Collection could not get DocCollection!!");
            throw new LogQueryException("Collection could not get DocCollection!!", 509);
        }
        return docCollection;
    }
}
