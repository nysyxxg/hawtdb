package org.fusesource.hawtdb.api;

import org.fusesource.hawtdb.internal.page.Paged;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Special purpose index factory providing APIs to create and open several indexes at a given {@link Paged} object:
 * each index is identified by a unique name, which must be used to refer to the index itself when opening or creating it.
 * This index factory is very useful when having to manage several correlated indexes, whose updates must be atomically executed
 * in the same transaction.
 *
 * 提供API的特殊用途索引工厂，用于在给定的对象上创建和打开多个索引：
 * *每个索引都由一个唯一的名称标识，在打开或创建索引时，必须使用该名称来引用索引本身。
 * *当必须管理多个相关索引时，这个索引工厂非常有用，这些索引的更新必须以原子方式执行在同一笔交易中。
 * 
 * @author Sergio Bossa
 */
public class MultiIndexFactory {

    private final static BTreeIndexFactory<String, Integer> INTERNAL_INDEX_FACTORY = new BTreeIndexFactory<String, Integer>();
    //
    private final Paged paged;
    private final SortedIndex<String, Integer> multiIndex;

    public MultiIndexFactory(Paged paged) {
        this.paged = paged;
        this.multiIndex = INTERNAL_INDEX_FACTORY.openOrCreate(paged);
    }

    /**
     * Create a new named index.
     * 
     * @param <Key> The index key type.
     * @param <Value> The index value type.
     * @param indexName The unique index name.
     * @param indexFactory The index factory used to actually create the index.
     * @return The newly created index.
     */
    public <Key, Value> Index<Key, Value> create(String indexName, IndexFactory<Key, Value> indexFactory) {
        if (!multiIndex.containsKey(indexName)) {
            Index<Key, Value> result = indexFactory.create(paged);
            multiIndex.put(indexName, result.getIndexLocation());
            return result;
        } else {
            throw new IllegalStateException("Index " + indexName + " already exists!");
        }
    }

    /**
     * Open named index.
     * 
     * @param <Key> The index key type.
     * @param <Value> The index value type.
     * @param indexName The unique index name.
     * @param indexFactory The index factory used to actually open the index.
     * @return The opened index.
     */
    public <Key, Value> Index<Key, Value> open(String indexName, IndexFactory<Key, Value> indexFactory) {
        Integer location = multiIndex.get(indexName);
        if (location != null) {
            Index<Key, Value> result = indexFactory.open(paged, location);
            return result;
        } else {
            throw new IllegalStateException("Index " + indexName + " doesn't exist!");
        }
    }

    /**
     * Open or create the named index.
     * 
     * @param <Key> The index key type.
     * @param <Value> The index value type.
     * @param indexName The unique index name.
     * @param indexFactory The index factory used to actually open or create the index.
     * @return The index.
     */
    public <Key, Value> Index<Key, Value> openOrCreate(String indexName, IndexFactory<Key, Value> indexFactory) {
        Integer location = multiIndex.get(indexName);
        Index<Key, Value> result = null;
        if (location != null) {
            result = indexFactory.open(paged, location);
        } else {
            result = indexFactory.create(paged);
            multiIndex.put(indexName, result.getIndexLocation());
        }
        return result;
    }

    /**
     * List the names of all indexes stored in the paged object.
     * 
     * @return The indexes names.
     */
    public Set<String> indexes() {
        Set<String> result = new HashSet<String>();
        for (Map.Entry<String, Integer> entry : multiIndex) {
            result.add(entry.getKey());
        }
        return result;
    }

}
