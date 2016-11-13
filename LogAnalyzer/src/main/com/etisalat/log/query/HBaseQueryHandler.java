package com.etisalat.log.query;

import com.etisalat.log.parser.QueryCondition;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;

public class HBaseQueryHandler {
    private static Logger logger = LoggerFactory.getLogger(HBaseQueryHandler.class);
    private String tableName;
    private Table table;
    private QueryCondition queryCondition = null;

    private List<Get> gets;

    private CompletionService<HBaseQueryRsp> completionService;
    private Set<Future<HBaseQueryRsp>> pending;

    public HBaseQueryHandler(String tableName, QueryCondition queryCondition) {
        this.tableName = tableName;
        this.queryCondition = queryCondition;

        try {
            this.table = HBaseQueryHandlerFactory.connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            logger.error("Get Table and IOException arised.", e);
            return;
        }
    }

    public CompletionService<HBaseQueryRsp> getCompletionService() {
        return completionService;
    }

    public void setCompletionService(CompletionService<HBaseQueryRsp> completionService) {
        this.completionService = completionService;
    }

    public List<Get> getGets() {
        return gets;
    }

    public void setGets(List<Get> gets) {
        this.gets = gets;
    }

    public Set<Future<HBaseQueryRsp>> getPending() {
        return pending;
    }

    public void setPending(Set<Future<HBaseQueryRsp>> pending) {
        this.pending = pending;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void submit() {
        if (gets == null || gets.isEmpty()) {
            logger.debug("Gets is Empty and end.");
            return;
        }

        Callable<HBaseQueryRsp> task = new Callable<HBaseQueryRsp>() {
            @Override
            public HBaseQueryRsp call() throws Exception {
                logger.debug("HBase data fetch task start");

                long start = System.currentTimeMillis();

                try {
                    Result[] results = table.get(gets);
                    if (results == null) {
                        return new HBaseQueryRsp();
                    }

                    HBaseQueryRsp HBaseQueryRsp = new HBaseQueryRsp(tableName, queryCondition);
                    HBaseQueryRsp.setTimeCost(System.currentTimeMillis() - start);
                    HBaseQueryRsp.process(results);
                    logger.info("query hbase table {}, with batch size {}, cost {} ms", tableName, results.length,
                            HBaseQueryRsp.getTimeCost());
                    return HBaseQueryRsp;
                } catch (IOException e) {
                    logger.error("hbase data fetch task failed and IOException arised", e);
                    return new HBaseQueryRsp();
                } finally {
                    try {
                        table.close();
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                        logger.warn("Table close and IOException arised");
                    }
                }
            }
        };

        pending.add(completionService.submit(task));
    }
}
