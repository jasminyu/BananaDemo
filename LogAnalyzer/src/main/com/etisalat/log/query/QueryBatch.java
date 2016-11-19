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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class QueryBatch {
    public static final LRUCache<String, ConcurrentHashMap<String, List<String>>> RESULTS_FOR_SHARDS = new LRUCache<String, ConcurrentHashMap<String, List<String>>>();
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

        if (LogConfFactory.queryPerShard) {
            RESULTS_FOR_SHARDS.init();
            RESULTS_CNT_FOR_SHARDS.init();
            CacheStatsThread cacheStatsThread = new CacheStatsThread();
            cacheStatsThread.setDaemon(true);
            cacheStatsThread.start();
        }
        if (!LogConfFactory.queryPerShard && LogConfFactory.enablePaging) {
            LRU_CACHE.init();
            LRU_TASK_CACHE.init();
        }

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
        if ((queryCondition.isExportOp() || LogConfFactory.queryPerShard) && !queryCondition.queryInCloudMode()) {
        	return queryPerShards(solrQueryHandlerFactory, queryHBaseHandlerFactory);
        } 

//        if (queryCondition.queryInCloudMode()) {
        	return startQuery(queryHBaseHandlerFactory);
//        }
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
            cursor = new Cursor(SolrUtils.generateCacheKey2(), queryCondition.getCollections().get(0),
                    LogConfFactory.solrMinShardId, 0);
            logger.info("Start to queryPerShards with query session {}", cursor.getCacheKey());
            queryPerShards(cursor, solrHandlerFactory, hbaseQueryHandlerFactory);
        }
        ResultCnt resultCnt = RESULTS_CNT_FOR_SHARDS.get(cursor.getCacheKey());
        
        if (resultCnt ==null) {
        	String errMsg = "No results for query session " + cursor.getCacheKey();
        	logger.error(errMsg);
        	throw new LogQueryException(errMsg);
        }
        
        if (resultCnt.getTotalNum() == 0)  {
        	this.realReturnNum = resultCnt.getTotalNum();
        	return JsonUtil.toJson(queryCondition.isExportOp() ? 
        			SolrUtils.getErrJsonObj("No query results for query session " + cursor.getCacheKey(), 500) :
        			LogConfFactory.solrQueryRspJsonObj);
        }
        
        CursorRspProcessor rspProcessor = new CursorRspProcessor(queryCondition, cursor,
               resultCnt.getTotalNum(), resultCnt.getMaxCollWithShardSets());
        if(!queryCondition.isExportOp()) {
            rspProcessor.setSolrHandlerFactory(solrHandlerFactory);
        }
        return rspProcessor.process();
    }

    private void queryPerShards(Cursor cursor, SolrQueryHandlerFactory solrHandlerFactory,
            HBaseQueryHandlerFactory queryHBaseHandlerFactory) throws LogQueryException {
        logger.info("Start to queryPerShards with query session {}", cursor.getCacheKey());

        long start = System.currentTimeMillis();

        String qString = getRealQueryString();

        Map<ResultCnt, String> reqUrlMap = new TreeMap<ResultCnt, String>(new Comparator<ResultCnt>() {
            @Override
            public int compare(ResultCnt o1, ResultCnt o2) {
                return o1.compareTo(o2);
            }
        });

        long totalNumFound = 0;
        StringBuilder builder = new StringBuilder();
        for (String collection : collections) {
            totalNumFound = totalNumFound + getTotalNumFound(solrHandlerFactory, qString, collection, reqUrlMap, cursor.getCacheKey());
            builder.append(",").append(collection);
            if (totalNumFound >= queryCondition.getTotalReturnNum()) {
                break;
            }
        }

        this.realReturnNum =
                queryCondition.getTotalReturnNum() < totalNumFound ? queryCondition.getTotalReturnNum() : totalNumFound;
        queryCondition.setTotalReturnNum((int) this.realReturnNum);

        if (totalNumFound == 0) {
            RESULTS_CNT_FOR_SHARDS.put(cursor.getCacheKey(), new ResultCnt(0));
            return;
        }

        logger.info("Query session {}, solr query string {} on collection {}", cursor.getCacheKey(), qString, builder.delete(0, 1).toString());
        logger.info("Query session {}, actual total return num {}", cursor.getCacheKey(), realReturnNum);

        RESULTS_CNT_FOR_SHARDS.put(cursor.getCacheKey(), new ResultCnt(this.realReturnNum));

        Set<String> maxCollWithShardSets = new HashSet<String>();

        Set<Map.Entry<ResultCnt, String>> entrySet = reqUrlMap.entrySet();
        Map<String, SolrQueryTask> taskMap = new HashMap<String, SolrQueryTask>();
        ResultCnt resultCnt = null;
        SolrQueryTask querySolrTask = null;
        int idx = 0;
        String lastColl = null;
        String lastShard = null;
        String coll = null;
        int numFound = 0;
        int left = queryCondition.getTotalNum();
        long actualFetchRows = 0;
        for (Map.Entry<ResultCnt, String> entry : entrySet) {
            resultCnt = entry.getKey();
            querySolrTask = new SolrQueryTask(qString, entry.getValue());
            querySolrTask.setCacheKey(cursor.getCacheKey());
            coll = SolrUtils.getCollection(resultCnt.getCollWithShardId());
            idx ++;
            if (!coll.equals(lastColl)) {
                idx = 1;
                querySolrTask.setShardId(SolrUtils.getCollWithShardId(coll, "_shard" + idx));
                querySolrTask.setCollection(coll);
                if (lastColl != null) {
                    maxCollWithShardSets.add(lastShard);
                }
                lastColl = coll;
            } else {
                querySolrTask.setShardId(SolrUtils.getCollWithShardId(coll, "_shard" + idx));
                querySolrTask.setCollection(coll);
            }

            lastShard = querySolrTask.getShardId();

            querySolrTask.setqCondition(queryCondition);
            querySolrTask.setHttpClient(solrClient.getLbClient().getHttpClient());
            querySolrTask.setQueryHBaseHandlerFactory(queryHBaseHandlerFactory);
            actualFetchRows = Long.valueOf((long) Math
                    .ceil((float) resultCnt.getTotalNum() / totalNumFound * queryCondition.getTotalReturnNum())) + LogConfFactory.solrQueryThreshold;

            querySolrTask.setRows(resultCnt.getTotalNum() <= actualFetchRows ? String.valueOf(resultCnt.getTotalNum()) :
                    String.valueOf(actualFetchRows));

            if (queryCondition.isExportOp()) {
                solrHandlerFactory.submitQuerySolrTask(querySolrTask);
                continue;
            }

            if (left > 0) {
                numFound = Integer.valueOf(querySolrTask.getRows());
                querySolrTask.setStartRows(0);
                querySolrTask.setFetchRows((numFound > left) ? left : numFound);
                querySolrTask.setLazyFetch(true);
                left = left - querySolrTask.getFetchRows();
                solrHandlerFactory.submitQuerySolrTask(querySolrTask);
                if(numFound > querySolrTask.getFetchRows()) {
                    logger.info("Query session {}, with shardId {}, rows {} add to task to lazy fetch.",
                            querySolrTask.getCacheKey(), querySolrTask.getShardId(), querySolrTask.getRows());
                    taskMap.put(querySolrTask.getShardId(), querySolrTask);
                }
            } else {
                logger.info("Query session {}, with shardId {}, rows {} add to task to lazy fetch.",
                        querySolrTask.getCacheKey(), querySolrTask.getShardId(), querySolrTask.getRows());
                taskMap.put(querySolrTask.getShardId(), querySolrTask);
            }
        }

        maxCollWithShardSets.add(querySolrTask.getShardId());
        ResultCnt taskResultCnt = RESULTS_CNT_FOR_SHARDS.get(cursor.getCacheKey());
        taskResultCnt.setMaxCollWithShardSets(maxCollWithShardSets);
        taskResultCnt.setTaskMap(taskMap);

        logger.warn("Query session {}, solr(javabin) took {} ms", cursor.getCacheKey(),
        		System.currentTimeMillis() - start);
        logger.info("End to queryPerShards with query session {}.", cursor.getCacheKey());
    }

    private long getTotalNumFound(SolrQueryHandlerFactory solrHandlerFactory, String qString, String collection,
            Map<ResultCnt, String> reqUrlMap, String cacheKey) throws LogQueryException {
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
                if (baseUrl == null) {
                    logger.warn("Query session {}, collection {} {} has no active replicas.", cacheKey, collection, slice.getName());
                    continue;
                }
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
                    if (future == null) {
                        continue;
                    }
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

                            reqUrlMap.put(new ResultCnt(solrQueryRsp.getCollWithShardId(), numFound), solrQueryRsp.getReqUrl());
                            totalNumFound = totalNumFound + numFound;
                            logger.info("Query session {}, sent to {}, numFound {}, QTime {}, cost {} ms", cacheKey,
                                    solrQueryRsp.getReqUrl(),
                                    solrQueryRsp.getQueryResponse().getResults().getNumFound(),
                                    solrQueryRsp.getQueryResponse().getQTime(), solrQueryRsp.getTimeCost());
                        }

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.error("Query session {}, live Query process interrupted.", cacheKey);
                        throw new LogQueryException(e.getMessage(), 509);
                    }
                } catch (Exception e) {
                    logger.error("Query session {}, live Query process Exception",cacheKey, e);
                    throw new LogQueryException(e.getMessage(), 509);
                }

            }
        } catch (Exception e) {
            logger.error("Query session {}, live Query process Exception", cacheKey, e);
            throw new LogQueryException(e.getMessage(), 509);
        }

        logger.info(
                "Query session {}, solr(javabin) query total numFound {}, min cost {} ms, max cost {} ms, and total cost {} ms.",
                cacheKey, totalNumFound, minTimeCost, maxTimeCost, System.currentTimeMillis() - start);
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
