package com.etisalat.log.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Cache<K, V> {
    public final static Logger log = LoggerFactory.getLogger(Cache.class);

    public Object init();

    public String name();

    public int size();

    public V put(K key, V value);

    public V get(K key);

    public void clear();

    public void close();

    public V remove(K key);
}
