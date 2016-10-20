package com.etisalat.log.cache;

import com.etisalat.log.config.LogConfFactory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.util.ConcurrentLRUCache;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class LRUCache<K, V> implements Cache<K, V> {
    private ConcurrentLRUCache<K, V> cache;

    private List<ConcurrentLRUCache.Stats> statsList;
    private String description;

    public LRUCache() {
    }

    protected static float calcHitRatio(long lookups, long hits) {
        return (lookups == 0) ?
                0.0f :
                BigDecimal.valueOf((double) hits / (double) lookups).setScale(2, RoundingMode.HALF_EVEN).floatValue();
    }

    private static String generateDescription(int limit, int initialSize, int minLimit, int acceptableLimit,
            boolean newThread) {
        String description = "Concurrent LRU Cache(maxSize=" + limit + ", initialSize=" + initialSize +
                ", minSize=" + minLimit + ", acceptableSize=" + acceptableLimit + ", cleanupThread=" + newThread;
        description += ')';
        return description;
    }

    @Override
    public Object init() {
        description = generateDescription(LogConfFactory.cacheLimit, LogConfFactory.initialCacheSize,
                LogConfFactory.minCacheLimit, LogConfFactory.acceptableCacheLimit, LogConfFactory.cleanCache);
        cache = new ConcurrentLRUCache<>(LogConfFactory.cacheLimit, LogConfFactory.minCacheLimit,
                LogConfFactory.acceptableCacheLimit, LogConfFactory.initialCacheSize, LogConfFactory.cleanCache, false,
                null);
        cache.setAlive(false);

        if (statsList == null) {
            // must be the first time a cache of this type is being created
            // Use a CopyOnWriteArrayList since puts are very rare and iteration may be a frequent operation
            // because it is used in getStatistics()
            statsList = new CopyOnWriteArrayList<>();

            // the first entry will be for cumulative stats of caches that have been closed.
            statsList.add(new ConcurrentLRUCache.Stats());
        }
        statsList.add(cache.getStats());
        return statsList;
    }

    @Override
    public String name() {
        return this.getClass().getSimpleName();
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public V put(K key, V value) {
        return cache.put(key, value);
    }

    @Override
    public V get(K key) {
        return cache.get(key);
    }

    @Override
    public V remove(K key) {
        return cache.remove(key);
    }

    @Override
    public void clear() {
        // add the stats to the cumulative stats object (the first in the statsList)
        statsList.get(0).add(cache.getStats());
        statsList.remove(cache.getStats());
        cache.destroy();
    }

    @Override
    public void close() {
        cache.destroy();
    }

    public String getDescription() {
        return description;
    }

    public NamedList getStatistics() {
        NamedList<Serializable> lst = new SimpleOrderedMap<>();
        if (cache == null)
            return lst;
        ConcurrentLRUCache.Stats stats = cache.getStats();
        long lookups = stats.getCumulativeLookups();
        long hits = stats.getCumulativeHits();
        long inserts = stats.getCumulativePuts();
        long evictions = stats.getCumulativeEvictions();
        long size = stats.getCurrentSize();
        long clookups = 0;
        long chits = 0;
        long cinserts = 0;
        long cevictions = 0;

        // NOTE: It is safe to iterate on a CopyOnWriteArrayList
        for (ConcurrentLRUCache.Stats statistiscs : statsList) {
            clookups += statistiscs.getCumulativeLookups();
            chits += statistiscs.getCumulativeHits();
            cinserts += statistiscs.getCumulativePuts();
            cevictions += statistiscs.getCumulativeEvictions();
        }

        lst.add("lookups", lookups);
        lst.add("hits", hits);
        lst.add("hitratio", calcHitRatio(lookups, hits));
        lst.add("inserts", inserts);
        lst.add("evictions", evictions);
        lst.add("size", size);

        lst.add("cumulative_lookups", clookups);
        lst.add("cumulative_hits", chits);
        lst.add("cumulative_hitratio", calcHitRatio(clookups, chits));
        lst.add("cumulative_inserts", cinserts);
        lst.add("cumulative_evictions", cevictions);

        return lst;
    }

    protected Map<K, V> getLatestCache(int n) {
        return cache.getLatestAccessedItems(n);
    }

    protected Map<K, V> getOldestCache(int n) {
        return cache.getOldestAccessedItems(n);
    }
}
