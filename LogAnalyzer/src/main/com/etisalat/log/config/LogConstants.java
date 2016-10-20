/*
 * Copyright Notice:
 *      Copyright  1998-2009, Huawei Technologies Co., Ltd.  ALL Rights Reserved.
 *
 *      Warning: This computer software sourcecode is protected by copyright law
 *      and international treaties. Unauthorized reproduction or distribution
 *      of this sourcecode, or any portion of it, may result in severe civil and
 *      criminal penalties, and will be prosecuted to the maximum extent
 *      possible under the law.
 */
package com.etisalat.log.config;

public class LogConstants {
    public static final String QUERY_PER_SHARD = "query.per.shard";
    public static final String ENABLE_PAGING = "enable.paging";

    public static final int START_POSITION = 5;

    public static final String SOLR_IMPLICIT_COLLECTION_FIELD = "_collection_";
    public static final String SOLR_ECHO_COLLECTION_KEY = "echoCollection";

    public static final String COLUMN_FAMILY = "column.family";

    public static final String COLUMN_ROWKEY = "column.rowkey";

    public static final String COLUMN_QUALIFIERS = "column.qualifiers.mapping";

    public static final String SEARCH_SERVER_URL = "search.server.url";

    public static final String SEARCH_TIME_ZONE = "search.timezone";

    public static final String SOLR_QUERY_WIN_MAX_DEP = "solr.query.win.max.dep";

    public static final String SOLR_USE_WIN_MIN_DOC = "solr.use.win.min.doc";

    public static final String SOLR_QUERY_WINDOWS = "solr.query.win";

    public static final String SOLR_BATCH_SIZE = "solr.batch.size";
    public static final String DOWNLOADED_SINGLE_FILE_SIZE = "downloaded.single.file.size";
    public static final String SOLR_MAX_BATCH_SIZE = "solr.max.batch.size";

    public static final String SOLR_MIN_BATCH_SIZE = "solr.min.batch.size";

    public static final String SOLR_QUERY_THRESHOLD = "solr.query.threshold";

    public static final String SOLR_BATCH_TIME = "solr.batch.time";

    public static final String KEEP_CSV_HEADER = "keep.csv.header";

    public static final String HBASE_BATCH_SIZE = "hbase.batch.size";

    public static final String HBASE_BATCH_MIN_SIZE = "hbase.batch.min.size";

    public static final String FIELD_TIMESTAMP = "field.timestamp";

    public static final String SOLR_QUERY_MAX_SPAN = "query.max.span";

    public static final String SOLR_MAX_SHARD_ID = "solr.max.shard.id";
    public static final String SOLR_MIN_SHARD_ID = "solr.min.shard.id";

    public static final String QUERY_TIMEOUT_SECOND = "query.timeout.second";
    public static final String QUERY_RESULT_CHECK_INTERVAL_SECOND = "query.result.check.interval.second";

    public static final String SOLR_QUERY_RSP_JSON = "solr.query.response.json";

    public static final String COLLECTION_PREFIX = "collection.prefix";
    public static final String COLLECTION_SUFFIX_DATA_FORMAT = "collection.suffix";

    public static final String XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n<response>\r\n";

    public static final String ZK_HOST = "solr.zkHost";
    public static final String ZK_SERVER_PRINCIPAL = "zookeeper.server.principal";
    public static final String ZK_SERVER_PRINCIPAL_DEFAULT = "zookeeper/hadoop.hadoop.com";
    public static final String KRB5_CONF = "krb5.conf.path";
    public static final String KEYTAB_FILE = "keytab.file.path";
    public static final String USER_PRINCIPLE = "user.principal";

    public static final String MAX_CONNECTIONS_PER_HOST = "maxConnectionsPerHost";
    public static final String MAX_CONNECTIONS = "maxConnections";

    public static final String ZK_CLIENT_TIMEOUT = "zk.client.timeout";
    public static final String ZK_CONNECT_TIMEOUT = "zk.connect.timeout";

    public static final String SOLR_QUERY_POOL_SIZE = "solr.thread.pool.size";
    public static final String SOLR_QUERY_POOL_MAX_SIZE = "solr.thread.pool.max.size";
    public static final String SOLR_QUERY_POOL_QUEUE_SIZE = "solr.thread.pool.queue.size";

    public static final String HBASE_QUERY_POOL_SIZE = "hbase.thread.pool.size";
    public static final String HBASE_QUERY_POOL_MAX_SIZE = "hbase.thread.pool.max.size";
    public static final String HBASE_QUERY_POOL_QUEUE_SIZE = "hbase.thread.pool.queue.size";

    public static final String EXPORT_FILE_PATH = "export.file.path";
    public static final String EXPORT_FILE_MAX_NUM_USE_UI = "export.file.max.num.use.ui";
    public static final String ENABLE_CLEAN_EXPORTED = "clean.export.enable";
    public static final String EXPORTED_FILE_CLEAN_INTERVAL_MS = "exportedFile.clean.time.interval.ms";
    public static final String EXPORTED_FILE_SLEEP_TIME_MS = "exportedFile.sleep.time.ms";

    public static final String LINE_SEPARATOR = "line.separator";
    public static final String LOG_CONF_PATH = "log.conf.path";

    public static final String CACHE_SIZE = "cache.size";
    public static final String CACHE_MIN_SIZE = "cache.min.size";
    public static final String CACHE_ACCEPTABLE_SIZE = "cache.acceptable.size";
    public static final String CACHE_INITIAL_SIZE = "cache.initial.size";
    public static final String CACHE_CLEAN_THREAD = "cache.clean.thread";

    public static final char CACHE_KEY_SEPARATOR = '_';
}
