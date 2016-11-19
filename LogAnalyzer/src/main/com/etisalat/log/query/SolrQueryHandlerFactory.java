package com.etisalat.log.query;

import com.etisalat.log.config.LogConfFactory;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class SolrQueryHandlerFactory {
    protected static final Logger logger = LoggerFactory.getLogger(SolrQueryHandlerFactory.class);
    private ThreadPoolExecutor commExecutor = new ThreadPoolExecutor(LogConfFactory.solrPoolSize,
            LogConfFactory.solrPoolMaxSize, 5, TimeUnit.SECONDS,
            // terminate idle threads after 5 sec
            new ArrayBlockingQueue<Runnable>(LogConfFactory.solrQueueSize, false), // directly hand
            // off tasks
            new DefaultSolrThreadFactory("querySolrExecutor"));

    public static SolrQueryHandler getSolrQueryHandler(String qString, String reqUrl) {
        return new SolrQueryHandler(qString, reqUrl);
    }

    public CompletionService<SolrQueryRsp> newCompletionService() {
        return new ExecutorCompletionService<SolrQueryRsp>(commExecutor);
    }

    public void submitQuerySolrTask(SolrQueryTask querySolrTask) {
        logger.info("Submit query session {}, with shardId {}, rows {}, lazy fetch rows {}, start {}.",
                querySolrTask.getCacheKey(), querySolrTask.getShardId(), querySolrTask.getRows(), querySolrTask.getFetchRows(), querySolrTask.getStartRows());
        commExecutor.submit(querySolrTask);
    }
}
