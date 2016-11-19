package com.etisalat.log.query;

import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by y00226935 on 2016/11/14.
 */

public class CacheStatsThread extends Thread {
	private static final Logger logger = LoggerFactory.getLogger(CacheStatsThread.class);
	
	@Override
	public void run() {
		while (true) {
			try {
				Thread.sleep(5*60*1000l);
			} catch (InterruptedException e) {
				
			}
			NamedList namedlist = QueryBatch.RESULTS_FOR_SHARDS.getStatistics();
			logger.info("RESULTS_FOR_SHARDS: {}", namedlist.toString());
		}
	}
	
}