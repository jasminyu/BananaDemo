/*
 * Copyright Notice:
 *      Copyright  1998-2013, Huawei Technologies Co., Ltd.  ALL Rights Reserved.
 *
 *      Warning: This computer software sourcecode is protected by copyright law
 *      and international treaties. Unauthorized reproduction or distribution
 *      of this sourcecode, or any portion of it, may result in severe civil and
 *      criminal penalties, and will be prosecuted to the maximum extent
 *      possible under the law.
 */
package com.etisalat.log.filter;

import com.etisalat.download.CleanDownloadThread;
import com.etisalat.download.TarGZCompress;
import com.etisalat.log.cache.CacheQueryResTaskInfo;
import com.etisalat.log.common.JsonUtil;
import com.etisalat.log.common.LogQueryException;
import com.etisalat.log.common.LoginUtil;
import com.etisalat.log.config.LogConfFactory;
import com.etisalat.log.config.LogConstants;
import com.etisalat.log.parser.QueryCondition;
import com.etisalat.log.parser.QueryParser;
import com.etisalat.log.query.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.security.User;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.Map;

public class LogDispatchFilter implements Filter {
    protected static final Logger logger = LoggerFactory.getLogger(LogDispatchFilter.class);

    static {
        PropertyConfigurator
                .configureAndWatch(System.getProperty("log4j.configuration").substring(LogConstants.START_POSITION),
                        1000);
    }

    private long reqStartTime;
    private HBaseQueryHandlerFactory queryHBaseHandlerFactory;
    private SolrQueryHandlerFactory solrQueryHandlerFactory;

    public void init(FilterConfig filterConfig) throws ServletException {
        logger.info("LogDispatchFilter starting..........!");
        LogConfFactory.init();

        if (User.isHBaseSecurityEnabled(LogConfFactory.hbaseConf)) {
            try {
                LoginUtil.login();
            } catch (IOException e) {
            	logger.error("failed to login the cluster.", e);
                throw new ServletException("failed to login the cluster.");
            }
        }

        initDownloadDir();

        if (!QueryBatch.init()) {
            logger.error("QueryBatch init failed and LogDispatchFilter started failed!");
            return;
        }

        HBaseQueryHandlerFactory.init();
        queryHBaseHandlerFactory = new HBaseQueryHandlerFactory();

        solrQueryHandlerFactory = new SolrQueryHandlerFactory();

        logger.info("LogDispatchFilter started Successfully!");
    }

    private void initDownloadDir() throws ServletException {
        logger.info("Begin to init download dir .....");

        if (StringUtils.isBlank(LogConfFactory.exportFilePath)) {
            logger.error("Download dir\"{}\" is blank, init failed and LogDispatchFilter started failed!",
                    LogConstants.EXPORT_FILE_PATH);
            throw new ServletException("Download dir \"" + LogConstants.EXPORT_FILE_PATH
                    + "\" is blank, init failed and LogDispatchFilter started failed!");
        }

        File fPath = new File(LogConfFactory.exportFilePath);
        if (!fPath.exists()) {
            fPath.mkdirs();
            logger.info("Export file Path {} created successfully", LogConfFactory.exportFilePath);
        }

        if (!fPath.isDirectory()) {
            logger.error("{} is not a right export file Path, please modify", LogConfFactory.exportFilePath);
            throw new ServletException(
                    LogConfFactory.exportFilePath + " is not a right export file Path, please modify.");
        }

        if (LogConfFactory.enableCleanExported) {
            logger.info("startup clean exportedFile thread.");
            Thread daemonThread = new Thread(
                    new CleanDownloadThread(LogConfFactory.exportFilePath, LogConfFactory.exportedCleanTimeInterval,
                            LogConfFactory.exportedCleanSleepTime));
            daemonThread.setName("Clean-exportedFile");
            daemonThread.setDaemon(true);
            daemonThread.start();
        }

        logger.info("End to init download dir .....");
    }

    /**
     * doFilter
     *
     * @param request  ServletRequest
     * @param response ServletResponse
     * @param chain    FilterChain
     * @throws IOException      if has error
     * @throws ServletException if has error
     */
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        doFilter(request, response, chain, false);
    }

    /**
     * doFilter
     *
     * @param request  ServletRequest
     * @param response ServletResponse
     * @param chain    FilterChain
     * @param retry    boolean
     * @throws IOException      if has error
     * @throws ServletException if has error
     */
    @SuppressWarnings("unchecked")
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain, boolean retry)
            throws IOException, ServletException {
        if (request instanceof HttpServletRequest) {
            HttpServletRequest req = (HttpServletRequest) request;
            String path = req.getServletPath();

            if (req.getPathInfo() != null) {
                path = new StringBuilder().append(path).append(req.getPathInfo()).toString();
            }

            String queryString = null;

            if ("GET".equals(req.getMethod())) {
                queryString = req.getQueryString();
            } else if ("POST".equals(req.getMethod())) {
                Map<String, String[]> params = request.getParameterMap();
                for (String key : params.keySet()) {
                    String[] values = params.get(key);
                    for (int i = 0; i < values.length; i++) {
                        String encodedValue = URLEncoder.encode(values[i], "UTF-8").replaceAll("\\+", "%2B")
                                .replaceAll("\\%21", "!").replaceAll("\\%27", "'").replaceAll("\\%28", "(")
                                .replaceAll("\\%29", ")").replaceAll("\\%2B", "+").replaceAll("\\%2F", "/")
                                .replaceAll("\\%3A", ":").replaceAll("\\%5B", "[").replaceAll("\\%5D", "]")
                                .replaceAll("\\%7E", "~");
                        if (null == queryString) {
                            queryString = key + "=" + encodedValue;
                        } else {
                            queryString += "&" + key + "=" + encodedValue;
                        }
                    }
                }
            } else {
                SolrUtils.handSelectReqException("Unsupported Method in request!", 510, response);
            }

            if (null != queryString) {
                logger.debug("Query String : {}", queryString);
            }

            if (path.contains("/select") && null != queryString) {
                logger.info("Start to handle select query [{}]", queryString);
                reqStartTime = System.currentTimeMillis();
                QueryCondition spanTime = QueryParser.preParseSelectQuery(queryString, (HttpServletResponse) response);
                if (null == spanTime || HttpServletResponse.SC_OK != spanTime.getMsgCode()) {
                    return;
                }

                spanTime.setQueryString(queryString);
                if (queryString.contains("rows=0")) {
                    handleSelectReqWithoutRows(path, spanTime, response);
                } else {
                    handleSelectReqWithRows(path, spanTime, response);
                }

                long reqSelectSecs = (System.currentTimeMillis() - reqStartTime);
                logger.info("End to handle select query");
                logger.warn("Select request took {} ms", reqSelectSecs);
                return;
            }

            if (path.contains("/admin/cores") && null != queryString) {
                logger.info("Start to handle cores query [{}]", queryString);
                request.setAttribute("DoNotLog", "t");
                handleAdminCoresReq(response);
                logger.info("End to handle cores query [{}]", queryString);
                return;
            }

            if (path.contains("/admin/luke") && null != queryString) {
                logger.info("Start to handle luke query [{}]", queryString);
                request.setAttribute("DoNotLog", "t");
                handleAdminLukReq(path, queryString, response);
                logger.info("End to handle luke query [{}]", queryString);
                return;
            }

            logger.debug("path : {}" + path);

            // other request not logging
            request.setAttribute("DoNotLog", "t");
            // other request continue execute
            chain.doFilter(request, response);
        }
    }

    private String getCoresStatus(String result) {
        logger.debug(result);

        String defaultCollection = "log";
        String coresStatus = "{\"defaultCoreName\":\"" + defaultCollection + "\",\"initFailures\":{},\"status\":{}}";

        JsonObject collectionObj = JsonUtil.fromJson(result, JsonObject.class);
        JsonObject coresObj = JsonUtil.fromJson(coresStatus, JsonObject.class);
        JsonObject statusObj = coresObj.getAsJsonObject("status");

        JsonArray collectionArray = collectionObj.getAsJsonArray("collections");
        for (int i = 0; i < collectionArray.size(); i++) {
            String collectionName = collectionArray.get(i).getAsString();
            statusObj.add(collectionName, JsonUtil.fromJson("{}", JsonObject.class));
        }

        logger.debug(statusObj.toString());
        coresObj.add("status", statusObj);
        logger.debug(coresObj.toString());

        return coresObj.toString();
    }

    private void handleAdminCoresReq(ServletResponse response) {
        String path = "/admin/collections";
        String queryString = "action=LIST&wt=json&omitHeader=true";

        logger.info("Start to handleAdminCoresReq");
        SpnegoService spnegoService = new SpnegoService();

        BufferedReader br = null;
        try {
            br = spnegoService.request(LogConfFactory.solrServerUrl + path + "?" + queryString);
        } catch (LogQueryException e) {
            logger.error(e.getMessage() + "," + e.getMsgCode());
            SolrUtils.handSelectReqException(e.getMessage(), e.getMsgCode(), response);
            return;
        }

        if (null == br) {
            logger.error("BufferedReader is null!");
        } else {
            String line;
            StringBuilder result = new StringBuilder();

            try {
                while ((line = br.readLine()) != null) {
                    result.append(line);
                }

                PrintWriter out = response.getWriter();
                out.print(getCoresStatus(result.toString()));
                response.setContentType("text/plain; charset=UTF-8");
                response.getWriter().flush();
                out.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            } finally {
                if (null != br) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                    }
                }
            }
        }

        spnegoService.shutdown();
        logger.info("End to handleAdminCoresReq");
        return;
    }

    private void handleAdminLukReq(String path, String queryString, ServletResponse response) {
        logger.info("Start to handleAdminLukReq");
        logger.debug("path : {}.", path);
        logger.debug("queryString : {}.", queryString);

        SpnegoService spnegoService = new SpnegoService();

        BufferedReader br = null;
        try {
            br = spnegoService.request(LogConfFactory.solrServerUrl + path + "?" + queryString);
        } catch (LogQueryException e) {
            logger.error(e.getMessage() + "," + e.getMsgCode());
            SolrUtils.handSelectReqException(e.getMessage(), e.getMsgCode(), response);
            return;
        }

        if (null == br) {
            logger.error("BufferedReader is null!");
        } else {
            String line;
            StringBuilder result = new StringBuilder();

            try {
                while ((line = br.readLine()) != null) {
                    result.append(line);
                }

                logger.debug(result.toString());

                PrintWriter out = response.getWriter();
                out.print(result.toString());
                response.setContentType("text/plain; charset=UTF-8");
                response.getWriter().flush();
                out.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            } finally {
                if (null != br) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                    }
                }
            }
        }

        spnegoService.shutdown();
        logger.info("End to handleAdminLukReq");
        return;
    }

    private void handleSelectReqWithoutRows(String path, QueryCondition queryCondition, ServletResponse response)
            throws IOException {
        logger.info("Start to handleSelectReqWithoutRows");
        logger.debug("path : {}.", path);
        logger.debug("Original queryString : {}.", queryCondition.getQueryString());

        String newQueryString = QueryParser.parseQueryString(queryCondition) + "&rows=0";
        logger.debug("Hits queryString : {}", newQueryString);

        BufferedReader br = null;
        SpnegoService spnegoService = null;
        try {
            spnegoService = new SpnegoService();
            br = spnegoService.request(LogConfFactory.solrServerUrl + path + "?" + newQueryString);

            if (null == br) {
                logger.error("BufferedReader is null!");
            } else {
                String line;
                StringBuilder result = new StringBuilder();
                while ((line = br.readLine()) != null) {
                    result.append(line);
                }

                logger.debug(result.toString());
                PrintWriter out = response.getWriter();
                out.print(result.toString());
                response.setContentType("text/plain; charset=UTF-8");
                response.getWriter().flush();
                out.close();
            }
        } catch (LogQueryException e) {
            logger.error("Query collections:{}, errMsg:{}", queryCondition.getCollections(), e.getMessage(), e.getMsgCode());
            SolrUtils.handSelectReqException(e.getMessage(), e.getMsgCode(), response);
            return;
        } finally {
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }

            if (spnegoService != null) {
                spnegoService.shutdown();
            }
        }

        logger.info("End to handleSelectReqWithoutRows");
        return;
    }

    private void handleSelectReqWithRows(String path, QueryCondition queryCondition, ServletResponse response)
            throws IOException, ServletException {
        logger.info("Start to handleSelectReqWithRows");
        logger.debug("path : {}.", path);
        logger.debug("Original queryString : {}.", queryCondition.getQueryString());

        long queryStart = System.currentTimeMillis();

        String collectionName = null;
        if (path.contains("_shard")) {
            collectionName = path.substring(1, path.indexOf("_shard"));
        } else {
            collectionName = path.substring(1, path.indexOf("/select"));
        }

        logger.debug("collectionName : {}.", collectionName);

        String newQueryString = QueryParser.parseQueryString(queryCondition);

        logger.debug("The newQueryString : {}.", newQueryString);
        logger.debug("The Last Query Condition : {}.", queryCondition.toString());

        QueryBatch queryBatch = new QueryBatch(LogConfFactory.solrServerUrl + path, newQueryString);
        queryBatch.setQueryCondition(queryCondition);
        queryBatch.setCollections(queryCondition.getCollections());
        queryBatch.setTotalNum(queryCondition.getTotalNum());
        queryBatch.setWtType(queryCondition.getWtType());

        int actualReturnNum = queryCondition.getTotalReturnNum() > queryCondition.getTotalNum() ?
                queryCondition.getTotalReturnNum() :
                queryCondition.getTotalNum();

        if (queryCondition.isExportOp()) {
            if(actualReturnNum > LogConfFactory.exportSizeUseUI) {
                String errMsg =
                        "The download results size " + actualReturnNum + " is greater than the limit "
                                + LogConfFactory.exportSizeUseUI;
                logger.error(errMsg);
                SolrUtils.handSelectReqException(errMsg, 509, response);
                return;
            }

            queryCondition.setDir("export_" + System.currentTimeMillis());
            String localDir = LogConfFactory.exportFilePath + queryCondition.getDir() + File.separator;
            TarGZCompress.createDir(localDir);
            queryCondition.setLocalPath(localDir);
            queryCondition.setTotalReturnNum(queryCondition.getTotalNum());
            logger.info("create download tmp dir {}", localDir);
        } else {
            if(actualReturnNum > LogConfFactory.displaySizeUseUI) {
                String errMsg =
                        "The display results size " + actualReturnNum + " is greater than the limit "
                                + LogConfFactory.displaySizeUseUI;
                logger.error(errMsg);
                SolrUtils.handSelectReqException(errMsg, 509, response);
                return;
            }
        }

        String httpRsp = null;
        try {
            httpRsp = queryBatch.startQueryBySolrj(queryHBaseHandlerFactory, solrQueryHandlerFactory);

            String nextCursorMark = queryBatch.getNextCursorMark();
            if (nextCursorMark != null) {
                QueryBatch.LRU_TASK_CACHE
                        .put(nextCursorMark, new CacheQueryResTaskInfo(queryBatch.deepCopy(nextCursorMark)));
            }

        } catch (LogQueryException e) {
            logger.error("", e);
            if (queryCondition.isExportOp()) {
                throw new ServletException(e);
            }
            SolrUtils.handSelectReqException(e.getMessage(), e.getMsgCode(), response);
            return;
        }

        

        if (null == httpRsp) {
            logger.warn("Query's Result is null!");
            httpRsp = "Query's Result is null!";
        }

        if (queryCondition.isExportOp()) {
        	
        	if(queryBatch.getRealReturnNum() == 0) {
        		SolrUtils.handSelectReqException("No query results to be downloaded.", 500, response);
        		return;
        	}
            String dirName = queryCondition.getDir();
            String gzName = LogConfFactory.exportFilePath + dirName + ".tar.gz";
            TarGZCompress.createTarGZ(LogConfFactory.exportFilePath + dirName, gzName);
            TarGZCompress.deleteDirectory(queryCondition.getLocalPath());
            String resLoc = "/LogAnalyzer/download/" + dirName + ".tar.gz";
            logger.warn("download res:{}", resLoc);
            httpRsp = "{\"url\":\"" + resLoc + "\"}";
        }
        logger.debug(httpRsp);

        /************************************************************/
        PrintWriter out;
        out = response.getWriter();
        out.print(httpRsp);
        response.setContentType("text/plain; charset=UTF-8");
        out.flush();
        out.close();

        logger.info("End to handleSelectReqWithRows cost {}ms", System.currentTimeMillis() - queryStart);
    }

    @Override
    public void destroy() {
    }
}