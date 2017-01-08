package com.etisalat.log.parser;

import com.etisalat.log.common.DateMathParser;
import com.etisalat.log.common.LogQueryException;
import com.etisalat.log.common.WTType;
import com.etisalat.log.config.LogConfFactory;
import com.etisalat.log.query.SolrUtils;
import com.etisalat.log.sort.SortField;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.util.*;

public class QueryParser {
    private static final Logger logger = LoggerFactory.getLogger(QueryParser.class);

    public static QueryCondition preParseSelectQuery(String queryString, HttpServletResponse response) {
        logger.debug("queryString : {}.", queryString);

        QueryCondition spanTime = new QueryCondition();
        spanTime.setQueryInCloudMode(false);
        if (StringUtils.isBlank(queryString) || response == null) {
            spanTime.setErrorMsg("query string is blank or response is null");
            return spanTime;
        }

        List<String> paramList = Arrays.asList(queryString.split("&"));

        for (String param : paramList) {
            String[] paramPair = param.split("=");
            logger.debug("param : {}.", param);

            if (2 == paramPair.length && null != paramPair[0] && null != paramPair[1]) {
                if (param.contains("fq=" + LogConfFactory.fieldTimestamp + ":[")) {
                    try {
						spanTime = QueryParser.timePick(paramPair[1]);
					} catch (LogQueryException e) {
						SolrUtils.handSelectReqException(e.getMessage(), e.getMsgCode(), response);
					}
                    break;
                }
            }
        }

        String resultMsg = null;
        if (null == spanTime) {
            resultMsg = "Get Query Conditions Failed, Please try again!";
        } else if (null != spanTime.getErrorMsg()) {
            resultMsg = spanTime.getErrorMsg();
        } else {
            if (spanTime.getQuerySpan() >= LogConfFactory.solrQueryMaxSpan) {
                resultMsg = "Beyond Query Maximum Days : " + LogConfFactory.solrQueryMaxSpan
                        + ", Please adjust Start Time and End Time!";
            }
        }

        if (null == resultMsg) {
            spanTime.setMsgCode(HttpServletResponse.SC_OK);
            return spanTime;
        } else {
            SolrUtils.handSelectReqException(resultMsg, HttpServletResponse.SC_BAD_REQUEST, response);
            spanTime.setMsgCode(HttpServletResponse.SC_BAD_REQUEST);
            return spanTime;
        }
    }

    public static String parseQueryString(QueryCondition queryCondition) throws IOException {
        if (queryCondition == null) {
            throw new IOException("query condition is null.");
        }

        StringBuilder queryStringSB = new StringBuilder();

        String queryString = queryCondition.getQueryString();

        if (queryString.contains("wt=json")) {
            queryCondition.setWtType(WTType.JSON);
        } else if (queryString.contains("wt=xml")) {
            queryCondition.setWtType(WTType.XML);
        } else {
            queryCondition.setWtType(WTType.CSV);
        }

        List<String> paramList = Arrays.asList(queryString.split("&"));

        String qString = null;
        List<String> sortStrList = new ArrayList<String>();
        List<SortField> sortFields = new ArrayList<SortField>();
        String[] tmpArr = null;
        SortField.Type type = null;
        int uniqueKeySortStrIdx = -1;
        for (String param : paramList) {
            String[] paramPair = param.split("=");
            logger.debug("param : {}.", param);

            if (1 == paramPair.length && null != paramPair[0] && "q".equalsIgnoreCase(paramPair[0])) {
                qString = "*%3A*";
                continue;
            }

            if (2 == paramPair.length && null != paramPair[0] && null != paramPair[1]) {
                if ("q".equalsIgnoreCase(paramPair[0])) {
                    qString = paramPair[1];
                    continue;
                } else if ("rows".equalsIgnoreCase(paramPair[0])) {
                    queryCondition.setTotalNum(Integer.valueOf(paramPair[1]));
                    continue;
                } else if (param.contains("fq=" + LogConfFactory.fieldTimestamp + ":[")) {
                    continue;
                } else if (param.contains("facet.range.start") || param.contains("facet.range.end")) {
                    continue;
                } else if ("wt".equalsIgnoreCase(paramPair[0]) && "csv".equalsIgnoreCase(paramPair[1])) {
                    if (0 == queryStringSB.length()) {
                        queryStringSB.append(paramPair[0] + "=json");
                    } else {
                        queryStringSB.append("&" + paramPair[0] + "=json");
                    }
                } else if ("total".equalsIgnoreCase(paramPair[0])) {
                    queryCondition.setTotalReturnNum(Integer.valueOf(paramPair[1]));
                    continue;
                } else if ("sort".equalsIgnoreCase(paramPair[0])) {
                    queryCondition.setSort(true);
                    tmpArr = paramPair[1].split("%20");
                    if (tmpArr.length != 2) {
                        throw new IOException("query condition sort desc is invalid.");
                    }
                    String solrField = LogConfFactory.columnQualifiersMap.get(tmpArr[0]);
                    //add by xfx
                    if(solrField == null) {
                    	solrField = LogConfFactory.fieldTimestamp;
                    }
                    if (tmpArr[0].equals(LogConfFactory.uniqueKey)) {
                        type = LogConfFactory.fieldsMap.get(tmpArr[0]);
                    } else if(tmpArr[0].equals(LogConfFactory.fieldTimestamp)) {
                    	type = SortField.Type.tdate;
                    }
                    else {
                        type = LogConfFactory.fieldsMap.get(solrField);
                    }
                    //add by xfx

                    if (type == null) {
                        throw new IOException("sort fields " + tmpArr[0] + " mapped to solr field " + solrField
                                + " should be indexed.");
                    }

                    sortFields.add(new SortField(tmpArr[0], tmpArr[1].equalsIgnoreCase("desc"), type));
                    if (paramPair[1].contains(LogConfFactory.rowkeyName)) {
                        queryCondition.setUniqueKeySort(true);
                        sortStrList.add(paramPair[1]);
                        uniqueKeySortStrIdx = sortStrList.size() - 1;
                    } else {
                        sortStrList.add(solrField + "%20" + tmpArr[1]);
                    }

                    continue;
                } else if ("nextCursorMark".equalsIgnoreCase(paramPair[0])) {
                    if (LogConfFactory.queryPerShard) {
                        queryCondition.setNextCursor(Cursor.parse(paramPair[1]));
                    } else {
                        queryCondition.setNextCursorMark(URLDecoder.decode(paramPair[1], "UTF-8"));
                    }
                    continue;
                } else {
                    if (0 == queryStringSB.length()) {
                        queryStringSB.append(param);
                    } else {
                        queryStringSB.append("&" + param);
                    }
                }
            } else {
                logger.error("Wrong Query String : {}.", queryString);
            }
        }

        if (sortFields.size() != 0) {
            queryCondition.setSortedFields(sortFields);
        }

        if (!queryString.contains("rows=0")) {
            // Used later
            queryCondition.setOriQString(qString);
        } else {
            StringBuilder tmpQStringSB = new StringBuilder("q=(" + qString + ")");
            //            tmpQStringSB.append("%20AND%20" + LogConfFactory.fieldTimestamp
            //                    + ":[2016-09-09T00:00:00.000Z%20TO%202016-09-09T23:59:59.000Z]");
            tmpQStringSB.append("%20AND%20" + LogConfFactory.fieldTimestamp + ":[" + queryCondition.getStartTime()
                    + "%20TO%20" + queryCondition.getEndTime() + "]");
            if (0 == queryStringSB.length()) {
                queryStringSB.append(tmpQStringSB.toString());
            } else {
                queryStringSB.append("&" + tmpQStringSB.toString());
            }
        }

        if (0 == queryStringSB.length()) {
            queryStringSB.append("shards.tolerant=true");
        } else {
            queryStringSB.append("&shards.tolerant=true");
        }

        String uniqueKeySort = null;
        if ((queryCondition.getTotalNum()!= 0) && !LogConfFactory.queryPerShard &&
        		(queryCondition.getTotalNum()>= LogConfFactory.displaySizeUseUI || LogConfFactory.enablePaging)) {
            queryCondition.setNeedUseCursor(true);
            if (queryCondition.isUniqueKeySort()) {
                uniqueKeySort = sortStrList.get(uniqueKeySortStrIdx);
                sortStrList.remove(uniqueKeySort);
            } else {
                uniqueKeySort = LogConfFactory.rowkeyName + "%20asc";
            }

            queryStringSB.append(SolrUtils.getSortStr(sortStrList, uniqueKeySort));
        } else {
            logger.debug("{} does not need to use cursor", queryString);
            queryCondition.setNeedUseCursor(false);
            queryStringSB.append(SolrUtils.getSortStr(sortStrList));
        }

        queryStringSB.append("&collection=");
        for (int index = 0; index < queryCondition.getCollections().size(); index++) {
            if (index < (queryCondition.getCollections().size() - 1)) {
                queryStringSB.append(queryCondition.getCollections().get(index) + ",");
            } else {
                queryStringSB.append(queryCondition.getCollections().get(index));
            }
        }

        if (queryStringSB.indexOf("&facet=true") >= 0) {
            queryCondition.setQueryInCloudMode(true);
            queryStringSB.append("&rows=0");

            if (queryStringSB.indexOf("&facet.range=" + LogConfFactory.fieldTimestamp) >= 0) {
                queryStringSB.append("&facet.range.start=" + queryCondition.getStartTime())
                        .append("&facet.range.end=" + queryCondition.getEndTime());
            }
        }

        String queryStr = queryStringSB.toString();
        queryCondition.setExportOp(queryStr.contains("&op=exportfile"));

        if (!queryStr.contains("fl=")) {
            return queryStringSB.append("&fl=").append(LogConfFactory.rowkeyName).toString();
        } else {
            return queryStr;
        }
    }

    private static QueryCondition timePick(String fqString) throws LogQueryException {
        logger.debug("fqString : {}.", fqString);
        QueryCondition spanTime = new QueryCondition();
        if (StringUtils.isBlank(fqString)) {
            spanTime.setErrorMsg("time picker is blank");
            return spanTime;
        }

        if (fqString.contains("[NOW/")) {
            // Relative Mode
            spanTime = processRelativeMode(fqString);
        } else if (fqString.contains("*]")) {
            // Since Mode
            spanTime = processSinceMode(fqString);
        } else {
            // Absolute Mode
            spanTime = processAbsoluteMode(fqString);
        }

        if (null != spanTime.getErrorMsg()) {
            return spanTime;
        }

        List<String> collections = new ArrayList<String>();

        Date startDate = null;
        try {
            startDate = DateMathParser.LOCAL_TIME_FORMATER.parse(spanTime.getStartTime());
            collections.add(LogConfFactory.collectionPrefix + DateMathParser.LOCAL_DATE_FORMATER.format(startDate));

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(startDate);

            if (spanTime.getQuerySpan() > 0) {
                for (int day = 1; day <= spanTime.getQuerySpan(); day++) {
                    calendar.add(Calendar.DATE, day);
                    collections.add(LogConfFactory.collectionPrefix + DateMathParser.LOCAL_DATE_FORMATER
                            .format(calendar.getTime()));
                    calendar.setTime(startDate);
                }
            }

            logger.debug("collections : {}.", collections.toString());
            spanTime.setCollections(collections);
        } catch (ParseException e) {
            logger.error(e.getMessage());
            spanTime.setErrorMsg(e.getMessage());
        }

        return spanTime;
    }

    private static QueryCondition processRelativeMode(String fqString) throws LogQueryException {
        String strSpan = null;
        DateMathParser dateMathParser = new DateMathParser();

        QueryCondition spanTime = new QueryCondition();

        if (fqString.contains("[NOW/MINUTE")) {
            strSpan = fqString
                    .substring((LogConfFactory.fieldTimestamp + ":[NOW/MINUTE").length(), fqString.indexOf("%20TO"));
        } else if (fqString.contains("[NOW/HOUR")) {
            strSpan = fqString
                    .substring((LogConfFactory.fieldTimestamp + ":[NOW/HOUR").length(), fqString.indexOf("%20TO"));
        } else if (fqString.contains("[NOW/DAY")) {
            strSpan = fqString
                    .substring((LogConfFactory.fieldTimestamp + ":[NOW/DAY").length(), fqString.indexOf("%20TO"));
        } else {
            logger.error("Not Support Time Span Format!");
            spanTime.setErrorMsg("Not Support Time Span Format!");
            return spanTime;
        }

        String startTime, endTime;
        long querySpan;

        try {
            startTime = DateMathParser.LOCAL_FORMATER.format(dateMathParser.parseMath(strSpan));
            endTime = DateMathParser.LOCAL_FORMATER.format(dateMathParser.getNow());
            logger.info("Original query time range [{} TO {}].", startTime, endTime);

            querySpan = DateMathParser.getSpanDays(startTime, endTime);
            logger.info("Original query span {} days.", querySpan);

            spanTime.setStartTime(startTime);
            spanTime.setEndTime(endTime);
            spanTime.setQuerySpan(querySpan);
        } catch (ParseException e) {
            logger.error(e.getMessage());
            spanTime.setErrorMsg(e.getMessage());
        }

        return spanTime;
    }

    private static QueryCondition processSinceMode(String fqString) throws LogQueryException {
        DateMathParser dateMathParser = new DateMathParser();
        String startTime, endTime;
        long querySpan;
        QueryCondition spanTime = new QueryCondition();
        try {
            String tmpTime = fqString
                    .substring((LogConfFactory.fieldTimestamp + ":[").length(), fqString.indexOf("/SECOND"));
            logger.info("Original query start time: {} in UTC time.", tmpTime);

            startTime = DateMathParser
                    .utc2Local(tmpTime, DateMathParser.localTimePatten, DateMathParser.localTimePatten,
                            LogConfFactory.timeZone);
            logger.info("Original query start time: {}.", startTime);

            endTime = DateMathParser.LOCAL_FORMATER.format(dateMathParser.getNow());
            logger.info("Original query end time: {}.", endTime);

            querySpan = DateMathParser.getSpanDays(startTime, endTime);
            logger.info("Original query span {} days.", querySpan);

            spanTime.setStartTime(startTime);
            spanTime.setEndTime(endTime);
            spanTime.setQuerySpan(querySpan);
        } catch (ParseException e) {
            logger.error(e.getMessage());
            spanTime.setErrorMsg(e.getMessage());
        }

        return spanTime;
    }

    private static QueryCondition processAbsoluteMode(String fqString) throws LogQueryException {
        String startTime, endTime;
        long querySpan;

        QueryCondition spanTime = new QueryCondition();
        try {
            String tmpTime = fqString
                    .substring((LogConfFactory.fieldTimestamp + ":[").length(), fqString.indexOf("%20TO"));
            logger.debug("Original query start time: {} in UTC time.", tmpTime);

            startTime = DateMathParser
                    .utc2Local(tmpTime, DateMathParser.localTimePatten, DateMathParser.localTimePatten,
                            LogConfFactory.timeZone);
            logger.info("Original query start time: {}.", startTime);

            tmpTime = fqString.substring(fqString.indexOf("TO%20") + ("TO%20").length(), fqString.indexOf("]"));
            logger.debug("Original query end time: {} in UTC time.", tmpTime);

            endTime = DateMathParser.utc2Local(tmpTime, DateMathParser.localTimePatten, DateMathParser.localTimePatten,
                    LogConfFactory.timeZone);
            logger.info("Original query end time: {}.", endTime);

            querySpan = DateMathParser.getSpanDays(startTime, endTime);
            logger.info("Original query span {} days.", querySpan);

            spanTime.setStartTime(startTime);
            spanTime.setEndTime(endTime);
            spanTime.setQuerySpan(querySpan);
        } catch (ParseException e) {
            logger.error(e.getMessage());
            spanTime.setErrorMsg(e.getMessage());
        }

        return spanTime;
    }
}
