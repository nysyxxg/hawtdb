package org.fusesource.hawtdb.internal.cache;

import org.fusesource.hawtdb.util.LFUCache;


public class LFUPageCache<Integer, Value> implements PageCache<Integer, Value> {
    
    private final LFUCache<Integer, Value> cache;

    public LFUPageCache(int maxCacheSize, float evictionFactor) {
        cache = new LFUCache<Integer, Value>(maxCacheSize, evictionFactor);
    }

    synchronized public void put(Integer k, Value v) {
        cache.put(k, v);
    }

    synchronized public Value get(Integer k) {
        return cache.get(k);
    }

    synchronized public Value remove(Integer k) {
        return cache.remove(k);
    }

    synchronized public void clear() {
        cache.clear();
    }

    synchronized public int size() {
        return cache.size();
    }
}
