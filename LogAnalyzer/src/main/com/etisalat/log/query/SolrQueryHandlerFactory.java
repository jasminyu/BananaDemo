package com.etisalat.log.query;

import com.etisalat.log.config.LogConfFactory;
import org.apache.solr.util.DefaultSolrThreadFactory;

import java.util.concurrent.*;

public class SolrQueryHandlerFactory {
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
        commExecutor.submit(querySolrTask);
    }
}
