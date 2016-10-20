package com.etisalat.log.query;

import com.etisalat.log.config.LogConfFactory;
import com.etisalat.log.parser.QueryCondition;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class HBaseQueryHandlerFactory {
    public static Connection connection = null;
    protected static Logger logger = LoggerFactory.getLogger(HBaseQueryHandlerFactory.class);
    private ThreadPoolExecutor commExecutor = new ThreadPoolExecutor(LogConfFactory.hbasePoolSize,
            LogConfFactory.hbasePoolMaxSize, 5, TimeUnit.SECONDS,
            // terminate idle threads after 5 sec
            new ArrayBlockingQueue<Runnable>(LogConfFactory.hbaseQueueSize, false), // directly hand
            // off tasks
            new DefaultSolrThreadFactory("queryHBaseExecutor"));

    public static void init() {
        logger.info("QueryHBaseHandlerFactory begin to init.....");
        try {
            if (null == connection) {
                connection = ConnectionFactory.createConnection(LogConfFactory.hbaseConf);
            }
        } catch (Exception e) {
            logger.error("QueryHBaseHandlerFactory init failed", e);
        }

        logger.info("QueryHBaseHandlerFactory init successfully!");
    }

    protected static Connection getConnection() {
        return connection;
    }

    public static HBaseQueryHandler getQueryHBaseHandler(String tableName, QueryCondition queryCondition) {
        return new HBaseQueryHandler(tableName, queryCondition);
    }

    public static HBaseQueryCursorHandler getQueryHBaseCursorHandler(String tableName) {
        return new HBaseQueryCursorHandler(tableName);
    }

    public CompletionService newCompletionService() {
        return new ExecutorCompletionService(commExecutor);
    }
}
