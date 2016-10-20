package com.etisalat.log.cache;

import com.etisalat.log.config.LogConfFactory;
import com.etisalat.log.query.HBaseQueryHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class QueryAndCacheThread extends Thread {
    public static final int processNum = LogConfFactory.getInt("cache.thread.num", 40);
    public static final int maxRetryCnt = LogConfFactory.getInt("cache.retry.max.cnt", 5);
    protected static final Logger logger = LoggerFactory.getLogger(QueryAndCacheThread.class);
    private LRUCache<String, CacheQueryResTaskInfo> lruTaskCache;
    private LRUCache<String, String> lruCache;
    private HBaseQueryHandlerFactory queryHBaseHandlerFactory;

    public QueryAndCacheThread(HBaseQueryHandlerFactory queryHBaseHandlerFactory,
            LRUCache<String, CacheQueryResTaskInfo> lruTaskCache, LRUCache<String, String> lruCache) {
        this.lruTaskCache = lruTaskCache;
        this.lruCache = lruCache;
        this.queryHBaseHandlerFactory = queryHBaseHandlerFactory;
    }

    @Override
    public void run() {
        logger.info("QueryAndCacheThread start");
        ExecutorService exec = Executors.newFixedThreadPool(processNum);
        CacheQueryResTaskInfo cacheQueryResTaskInfo = null;
        CompletionService completionService = new ExecutorCompletionService(exec);
        Set<Future> solrQueryPending = new HashSet<Future>();
        while (true) {
            Set<Map.Entry<String, CacheQueryResTaskInfo>> entrySet = lruTaskCache.getOldestCache(processNum).entrySet();
            for (Map.Entry<String, CacheQueryResTaskInfo> entry : entrySet) {
                cacheQueryResTaskInfo = entry.getValue();
                if (cacheQueryResTaskInfo.isFinish()) {
                    continue;
                }

                solrQueryPending.add(completionService
                        .submit(new CacheProcessTask(queryHBaseHandlerFactory, cacheQueryResTaskInfo, lruCache), null));
            }

            while (solrQueryPending.size() > 0) {
                try {
                    Future future = completionService.take();
                    solrQueryPending.remove(future);
                } catch (InterruptedException e) {
                    logger.warn(e.getMessage());
                    continue;
                }
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
