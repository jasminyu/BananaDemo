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

package com.etisalat.log.config;

import com.etisalat.log.common.AssertUtil;
import com.etisalat.log.common.JsonParserImpl;
import com.etisalat.log.common.JsonUtil;
import com.etisalat.log.query.SolrUtils;
import com.etisalat.log.sort.Field;
import com.etisalat.log.sort.Schema;
import com.etisalat.log.sort.SortField;
import com.google.gson.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class LogConfFactory {
    public static final Map<String, SortField.Type> fieldsMap = new HashMap<>();
    public static final String uniqueKey;
    public static final Configuration hbaseConf;
    public static final boolean queryPerShard;
    public static final boolean enablePaging;
    public static final String timeZone;
    public static final String rowkeyName;
    public static final String defaultReurnFields;
    public static final String fieldTimestamp;
    public static final String columnFamily;
    public static final byte[] columnFamilyBytes;
    public static final Map<String, String> columnQualifiersMap;
    public static final Map<String, SortField.Type> columnQualifiersTypeMap;
    public static final Map<String, byte[]> columnQualifiersBytesMap;
    public static final String collectionPrefix;
    public static final int collectionPrefixLen;
    public static final String collectionSuffixDateFormatStr;
    public static final int collectionNameLen;
    public static final SimpleDateFormat collectionSuffixDateFormat;
    public static final int solrQueryWindow;
    public static final int solrQueryWinMaxDep;
    public static final int solrUseWindowMinDoc;
    public static final int solrBatchSize;
    public static final int downloadedSingleFileSize;
    public static final int solrMaxBatchSize;
    public static final int solrMinBatchSize;
    public static final int solrBatchTime;
    public static final int solrQueryThreshold;
    public static final boolean keepCsvHeader;
    public static final int solrPoolSize;
    public static final int solrPoolMaxSize;
    public static final int solrQueueSize;
    public static final int solrQueryMaxSpan;
    public static final String solrMaxShardId;
    public static final String solrMinShardId;
    public static final JsonObject solrQueryRspJsonObj;
    public static final long queryTimeout;
    public static final long queryFirstWaitTime;
    public static final long queryResultCheckInterval;
    public static final String solrServerUrl;
    public static final String zkHost;
    public static final String zkPrinciple;
    public static final String krb5Conf;
    public static final String keytabFile;
    public static final String userPrincipal;
    public static final int maxConnectionsPerHost;
    public static final int maxConnections;
    public static final int zkClientTimeout;
    public static final int zkConnectTimeout;
    public static final int hbasePoolSize;
    public static final int hbasePoolMaxSize;
    public static final int hbaseQueueSize;
    public static final int hbaseBatchSize;
    public static final int hbaseBatchMinSize;
    public static final String exportFilePath;
    public static final int exportSizeUseUI;
    public static final int displaySizeUseUI;
    public static final boolean enableCleanExported;
    public static final long exportedCleanTimeInterval;
    public static final long exportedCleanSleepTime;
    public static final String configFilePath;
    public static final String schemaPath;
    public static final String lineSeparator;
    public static final int initialCacheSize;
    public static final boolean cleanCache;
    private static final Logger logger = LoggerFactory.getLogger(LogConfFactory.class);
    private static final String FILE_PATH = "logconf.properties";
    private static final Properties properties;
    public static int cacheLimit;
    public static int minCacheLimit;
    public static int acceptableCacheLimit;

    static {
        JsonUtil.setJsonParser(new JsonParserImpl());

        // loading logconf.properties
      configFilePath = System.getProperty(LogConstants.LOG_CONF_PATH,
                System.getProperty("user.dir") + File.separator + "logconf" + File.separator);
        if (null == configFilePath) {
            logger.info("Get Solr Server Conf URL failed, please Check System's Configuration!");
            throw new RuntimeException("failed to get conf path:" + LogConstants.LOG_CONF_PATH);
        }
        logger.info("log conf path: {}", configFilePath);

        properties = new Properties();
        FileInputStream fileIs = null;
        String filePath = configFilePath + File.separator + FILE_PATH;

        try {
            logger.info("logconf.properties's path: {}", filePath);
            fileIs = new FileInputStream(filePath);
            properties.load(fileIs);
        } catch (IOException e) {
            logger.error("Occur a IOException. ", e);
            throw new RuntimeException("failed to init " + FILE_PATH, e);
        } finally {
            if (null != fileIs) {
                try {
                    fileIs.close();
                } catch (IOException e) {
                    logger.warn("Close file error . " + FILE_PATH, e.getMessage());
                }
            }
        }

        // generate hbase conf
        String OS = System.getProperty("os.name").toLowerCase();
        logger.info("OS is " + OS);
        if (OS.contains("windows")) {
            System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\hadoop");
            System.setProperty("HADOOP_HOME", System.getProperty("user.dir") + "\\hadoop");
        }

        hbaseConf = HBaseConfiguration.create();
        schemaPath = configFilePath + File.separator + "schema.xml";

        Schema schema = null;
        try {
            schema = SolrUtils.schemaFileToBean();
            for (Field field : schema.getFields()) {
                fieldsMap.put(field.getName(), field.getType());
            }

            uniqueKey = schema.getUniqueKey();

        } catch (IOException e) {
            logger.error("Occur a IOException. ", e);
            throw new RuntimeException("failed to init " + schemaPath, e);
        }

        queryPerShard = LogConfFactory.getBoolean(LogConstants.QUERY_PER_SHARD, true);
        enablePaging = LogConfFactory.getBoolean(LogConstants.ENABLE_PAGING, true);

        // init config constants
        timeZone = LogConfFactory.getString(LogConstants.SEARCH_TIME_ZONE, "GMT+4");
        rowkeyName = LogConfFactory.getString(LogConstants.COLUMN_ROWKEY);
        AssertUtil.notBlank(rowkeyName, getErrMsg(LogConstants.COLUMN_ROWKEY));
        defaultReurnFields = rowkeyName + "," + LogConstants.SOLR_IMPLICIT_COLLECTION_FIELD;

        fieldTimestamp = LogConfFactory.getString(LogConstants.FIELD_TIMESTAMP, "timestamp");
        columnFamily = LogConfFactory.getString(LogConstants.COLUMN_FAMILY, "f");
        columnFamilyBytes = Bytes.toBytes(columnFamily);

        String columnQualifiers = LogConfFactory.getString(LogConstants.COLUMN_QUALIFIERS);
        if (StringUtils.isBlank(columnQualifiers)) {
            String errMsg = LogConstants.COLUMN_QUALIFIERS + " config value is blank, it should like a:a1,b:b2";
            logger.error(errMsg);
            throw new RuntimeException(errMsg);
        }

        columnQualifiersMap = new LinkedHashMap<String, String>();
        columnQualifiersTypeMap = new LinkedHashMap<String, SortField.Type>();
        columnQualifiersBytesMap = new HashMap<String, byte[]>();

        String[] tmpArr = columnQualifiers.split(",");
        String errMsg = LogConstants.COLUMN_QUALIFIERS + " config value is \"" + columnQualifiers
                + "\", it should like a:a1,b:b2";
        String strArr[] = null;
        SortField.Type type = null;
        for (String tmp : tmpArr) {
            strArr = tmp.split(":");
            if (strArr.length < 1) {
                logger.error(errMsg);
                throw new RuntimeException(errMsg);
            }
            if (strArr.length == 2) {
                type = fieldsMap.get(strArr[1]);
                columnQualifiersMap.put(strArr[0], strArr[1]);
                columnQualifiersTypeMap.put(strArr[0], type == null ? SortField.Type.string : type);
            }
            columnQualifiersBytesMap.put(strArr[0], Bytes.toBytes(strArr[0]));
        }

        collectionPrefix = LogConfFactory.getString(LogConstants.COLLECTION_PREFIX, "tb_");
        collectionPrefixLen = collectionPrefix.length();
        collectionSuffixDateFormatStr = LogConfFactory
                .getString(LogConstants.COLLECTION_SUFFIX_DATA_FORMAT, "yyyyMMdd");
        collectionNameLen = (collectionPrefix + collectionSuffixDateFormatStr).length();
        collectionSuffixDateFormat = new SimpleDateFormat(collectionSuffixDateFormatStr);

        solrQueryWinMaxDep = LogConfFactory.getInt(LogConstants.SOLR_QUERY_WIN_MAX_DEP, 6);
        solrUseWindowMinDoc = LogConfFactory.getInt(LogConstants.SOLR_USE_WIN_MIN_DOC, 1000000);
        solrQueryWindow = LogConfFactory.getInt(LogConstants.SOLR_QUERY_WINDOWS, 3600 * 1000);

        solrBatchSize = LogConfFactory.getInt(LogConstants.SOLR_BATCH_SIZE, 2000);
        downloadedSingleFileSize = LogConfFactory.getInt(LogConstants.DOWNLOADED_SINGLE_FILE_SIZE, 20000);
        solrMaxBatchSize = LogConfFactory.getInt(LogConstants.SOLR_MAX_BATCH_SIZE, 20000);
        solrMinBatchSize = LogConfFactory.getInt(LogConstants.SOLR_MIN_BATCH_SIZE, 2000);

        solrBatchTime = LogConfFactory.getInt(LogConstants.SOLR_BATCH_TIME, 5);

        solrQueryThreshold = LogConfFactory.getInt(LogConstants.SOLR_QUERY_THRESHOLD, 200);
        keepCsvHeader = LogConfFactory.getBoolean(LogConstants.KEEP_CSV_HEADER, true);
        solrQueryMaxSpan = LogConfFactory.getInt(LogConstants.SOLR_QUERY_MAX_SPAN, 5);

        solrMaxShardId = LogConfFactory.getString(LogConstants.SOLR_MAX_SHARD_ID, "_shard93");
        solrMinShardId = LogConfFactory.getString(LogConstants.SOLR_MIN_SHARD_ID, "_shard1");

        queryTimeout = LogConfFactory.getLong(LogConstants.QUERY_TIMEOUT_SECOND, 4l * 60) * 1000;
        queryFirstWaitTime = LogConfFactory.getLong(LogConstants.QUERY_WAITTIME_SECOND, 5l) * 1000;
        queryResultCheckInterval = LogConfFactory.getLong(LogConstants.QUERY_RESULT_CHECK_INTERVAL_SECOND, 3l) * 1000;

        solrQueryRspJsonObj = JsonUtil.fromJson(LogConfFactory.getString(LogConstants.SOLR_QUERY_RSP_JSON,
                        "{\"responseHeader\":{\"status\":0,\"QTime\":0,\"params\":{\"q\":\"*:*\",\"rows\":\"0\",\"wt\":\"json\"}},\"response\":{\"numFound\":0,\"start\":0,\"docs\":[]}}"),
                JsonObject.class);

        solrPoolSize = LogConfFactory.getInt(LogConstants.SOLR_QUERY_POOL_SIZE, 1000);
        solrPoolMaxSize = LogConfFactory.getInt(LogConstants.SOLR_QUERY_POOL_MAX_SIZE, Integer.MAX_VALUE);
        solrQueueSize = LogConfFactory.getInt(LogConstants.SOLR_QUERY_POOL_QUEUE_SIZE, 1000);

        maxConnectionsPerHost = LogConfFactory.getInt(LogConstants.MAX_CONNECTIONS_PER_HOST, 100);
        maxConnections = LogConfFactory.getInt(LogConstants.MAX_CONNECTIONS, 2000);

        solrServerUrl = LogConfFactory.getString(LogConstants.SEARCH_SERVER_URL);

        zkHost = LogConfFactory.getString(LogConstants.ZK_HOST);
        zkPrinciple = LogConfFactory
                .getString(LogConstants.ZK_SERVER_PRINCIPAL, LogConstants.ZK_SERVER_PRINCIPAL_DEFAULT);
        krb5Conf = LogConfFactory.getString(LogConstants.KRB5_CONF, configFilePath + "krb5.conf");
        keytabFile = LogConfFactory.getString(LogConstants.KEYTAB_FILE, configFilePath + "solr.keytab");
        userPrincipal = LogConfFactory.getString(LogConstants.USER_PRINCIPLE, "solr/hadoop.hadoop.com@HADOOP.COM");

        zkClientTimeout = LogConfFactory.getInt(LogConstants.ZK_CLIENT_TIMEOUT, 120000);
        zkConnectTimeout = LogConfFactory.getInt(LogConstants.ZK_CONNECT_TIMEOUT, 120000);

        hbaseBatchSize = LogConfFactory.getInt(LogConstants.HBASE_BATCH_SIZE, 200);
        hbaseBatchMinSize = LogConfFactory.getInt(LogConstants.HBASE_BATCH_MIN_SIZE, 200);

        hbasePoolSize = LogConfFactory.getInt(LogConstants.HBASE_QUERY_POOL_SIZE, 1000);
        hbasePoolMaxSize = LogConfFactory.getInt(LogConstants.HBASE_QUERY_POOL_MAX_SIZE, Integer.MAX_VALUE);
        hbaseQueueSize = LogConfFactory.getInt(LogConstants.HBASE_QUERY_POOL_QUEUE_SIZE, 1000);

        displaySizeUseUI = LogConfFactory.getInt(LogConstants.DISPLAY_MAX_NUM_USE_UI, 200 * 1000);
        exportFilePath = System.getProperty(LogConstants.EXPORT_FILE_PATH) + File.separator;
        exportSizeUseUI = LogConfFactory.getInt(LogConstants.EXPORT_FILE_MAX_NUM_USE_UI, 300 * 10000);
        enableCleanExported = LogConfFactory.getBoolean(LogConstants.ENABLE_CLEAN_EXPORTED, false);
        exportedCleanTimeInterval = LogConfFactory
                .getLong(LogConstants.EXPORTED_FILE_CLEAN_INTERVAL_MS, 24 * 60 * 60 * 1000);
        exportedCleanSleepTime = LogConfFactory.getLong(LogConstants.EXPORTED_FILE_SLEEP_TIME_MS, 30 * 60 * 1000);

        lineSeparator = LogConfFactory.getString(LogConstants.LINE_SEPARATOR, System.getProperty("line.separator"));

        cacheLimit = LogConfFactory.getInt(LogConstants.CACHE_SIZE, 1024);
        minCacheLimit = LogConfFactory.getInt(LogConstants.CACHE_MIN_SIZE, (int) (cacheLimit * 0.9));
        minCacheLimit = (minCacheLimit == 0) ? 1 : minCacheLimit;
        cacheLimit = (cacheLimit <= minCacheLimit) ? minCacheLimit + 1 : cacheLimit;
        acceptableCacheLimit = LogConfFactory.getInt(LogConstants.CACHE_ACCEPTABLE_SIZE, (int) (cacheLimit * 0.95));
        acceptableCacheLimit = Math.max(minCacheLimit, acceptableCacheLimit);
        initialCacheSize = LogConfFactory.getInt(LogConstants.CACHE_INITIAL_SIZE, cacheLimit);
        cleanCache = LogConfFactory.getBoolean(LogConstants.CACHE_CLEAN_THREAD, false);

        logger.info("{}={}", LogConstants.QUERY_PER_SHARD, queryPerShard);
        logger.info("{}={}", LogConstants.ENABLE_PAGING, enablePaging);

        logger.info("{}={}", LogConstants.SEARCH_TIME_ZONE, timeZone);
        logger.info("{}={}", LogConstants.COLUMN_ROWKEY, rowkeyName);
        logger.info("{}={}", LogConstants.FIELD_TIMESTAMP, fieldTimestamp);
        logger.info("{}={}", LogConstants.COLUMN_FAMILY, columnFamily);
        logger.info("{}={}", LogConstants.COLUMN_QUALIFIERS, columnQualifiers);
        logger.info("{}={}", LogConstants.COLLECTION_PREFIX, collectionPrefix);
        logger.info("{}={}", LogConstants.COLLECTION_SUFFIX_DATA_FORMAT, collectionSuffixDateFormatStr);
        logger.info("{}={}", LogConstants.SOLR_QUERY_WIN_MAX_DEP, solrQueryWinMaxDep);
        logger.info("{}={}", LogConstants.SOLR_USE_WIN_MIN_DOC, solrUseWindowMinDoc);
        logger.info("{}={}", LogConstants.SOLR_QUERY_WINDOWS, solrQueryWindow);
        logger.info("{}={}", LogConstants.SOLR_BATCH_SIZE, solrBatchSize);
        logger.info("{}={}", LogConstants.DOWNLOADED_SINGLE_FILE_SIZE, downloadedSingleFileSize);
        logger.info("{}={}", LogConstants.SOLR_MAX_BATCH_SIZE, solrMaxBatchSize);
        logger.info("{}={}", LogConstants.SOLR_MIN_BATCH_SIZE, solrMinBatchSize);
        logger.info("{}={}", LogConstants.SOLR_BATCH_TIME, solrBatchTime);
        logger.info("{}={}", LogConstants.SOLR_QUERY_THRESHOLD, solrQueryThreshold);
        logger.info("{}={}", LogConstants.KEEP_CSV_HEADER, keepCsvHeader);
        logger.info("{}={}", LogConstants.SOLR_MAX_SHARD_ID, solrMaxShardId);
        logger.info("{}={}", LogConstants.SOLR_MIN_SHARD_ID, solrMinShardId);
        logger.info("{}={}", LogConstants.SOLR_QUERY_MAX_SPAN, solrQueryMaxSpan);
        logger.info("{}={}", LogConstants.QUERY_TIMEOUT_SECOND, queryTimeout / 1000);
        logger.info("{}={}", LogConstants.QUERY_WAITTIME_SECOND, queryFirstWaitTime / 1000);
        logger.info("{}={}", LogConstants.QUERY_RESULT_CHECK_INTERVAL_SECOND, queryResultCheckInterval / 1000);
        logger.info("{}={}", LogConstants.SOLR_QUERY_RSP_JSON, solrQueryRspJsonObj.toString());

        logger.info("{}={}", LogConstants.SOLR_QUERY_POOL_SIZE, solrPoolSize);
        logger.info("{}={}", LogConstants.SOLR_QUERY_POOL_MAX_SIZE, solrPoolMaxSize);
        logger.info("{}={}", LogConstants.SOLR_QUERY_POOL_QUEUE_SIZE, solrQueueSize);

        logger.info("{}={}", LogConstants.MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost);
        logger.info("{}={}", LogConstants.MAX_CONNECTIONS, maxConnections);

        logger.info("{}={}", LogConstants.ZK_HOST, zkHost);
        logger.info("{}={}", LogConstants.ZK_SERVER_PRINCIPAL, zkPrinciple);
        logger.info("{}={}", LogConstants.SEARCH_SERVER_URL, solrServerUrl);
        logger.info("{}={}", LogConstants.KRB5_CONF, krb5Conf);
        logger.info("{}={}", LogConstants.KEYTAB_FILE, keytabFile);
        logger.info("{}={}", LogConstants.USER_PRINCIPLE, userPrincipal);

        logger.info("{}={}", LogConstants.ZK_CLIENT_TIMEOUT, zkClientTimeout);
        logger.info("{}={}", LogConstants.ZK_CONNECT_TIMEOUT, zkConnectTimeout);

        logger.info("{}={}", LogConstants.HBASE_BATCH_SIZE, hbaseBatchSize);
        logger.info("{}={}", LogConstants.HBASE_BATCH_MIN_SIZE, hbaseBatchMinSize);
        logger.info("{}={}", LogConstants.HBASE_QUERY_POOL_SIZE, hbasePoolSize);
        logger.info("{}={}", LogConstants.HBASE_QUERY_POOL_MAX_SIZE, hbasePoolMaxSize);
        logger.info("{}={}", LogConstants.HBASE_QUERY_POOL_QUEUE_SIZE, hbaseQueueSize);

        logger.info("{}={}", LogConstants.DISPLAY_MAX_NUM_USE_UI, displaySizeUseUI);
        logger.info("{}={}", LogConstants.EXPORT_FILE_MAX_NUM_USE_UI, exportSizeUseUI);
        logger.info("{}={}", LogConstants.ENABLE_CLEAN_EXPORTED, enableCleanExported);
        logger.info("{}={}", LogConstants.EXPORTED_FILE_CLEAN_INTERVAL_MS, exportedCleanTimeInterval);
        logger.info("{}={}", LogConstants.EXPORTED_FILE_SLEEP_TIME_MS, exportedCleanSleepTime);

        logger.info("{}={}", LogConstants.CACHE_SIZE, cacheLimit);
        logger.info("{}={}", LogConstants.CACHE_MIN_SIZE, minCacheLimit);
        logger.info("{}={}", LogConstants.CACHE_ACCEPTABLE_SIZE, acceptableCacheLimit);
        logger.info("{}={}", LogConstants.CACHE_INITIAL_SIZE, initialCacheSize);
        logger.info("{}={}", LogConstants.CACHE_CLEAN_THREAD, cleanCache);

        logger.info("env config item {}={}", LogConstants.EXPORT_FILE_PATH, exportFilePath);
    }

    public static void init() {
        logger.info("start to load log analyzer configuration.");
    }

    public static String getString(String key) {
        return getTrimmed(properties.getProperty(key));
    }

    public static String getString(String key, String defaultValue) {
        String property = properties.getProperty(key);
        if (property == null) {
            return defaultValue;
        }

        return property.trim();
    }

    private static String getTrimmed(String value) {
        if (null == value) {
            return null;
        } else {
            return value.trim();
        }
    }

    public static int getInt(String key, int defaultValue) {
        String property = getTrimmed(properties.getProperty(key));
        int value;
        try {
            value = Integer.parseInt(property);
        } catch (NumberFormatException nfe) {
            value = defaultValue;
        }
        return value;
    }

    public static long getLong(String key, long defaultValue) {
        String property = getTrimmed(properties.getProperty(key));
        long value;
        try {
            value = Long.parseLong(property);
        } catch (NumberFormatException nfe) {
            value = defaultValue;
        }
        return value;
    }

    public static boolean getBoolean(String key, boolean defaultValue) {
        String property = getTrimmed(properties.getProperty(key));
        boolean value;
        try {
            value = Boolean.parseBoolean(property);
        } catch (NumberFormatException nfe) {
            value = defaultValue;
        }
        return value;
    }

    private static String getErrMsg(String item) {
        return "Config item " + item + " should not be blank";
    }
}
