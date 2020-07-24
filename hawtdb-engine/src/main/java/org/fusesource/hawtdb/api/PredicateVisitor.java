package org.fusesource.hawtdb.api;

import java.util.Comparator;
import java.util.List;

public class PredicateVisitor<Key, Value> implements IndexVisitor<Key, Value> {
    
    public static final int UNLIMITED = -1;
    
    private final Predicate<Key> predicate;
    private int limit;
    
    public PredicateVisitor(Predicate<Key> predicate) {
        this(predicate, UNLIMITED);
    }
    
    public PredicateVisitor(Predicate<Key> predicate, int limit) {
        this.predicate = predicate;
        this.limit = limit;
    }
    
    @Override
    final public void visit(List<Key> keys, List<Value> values, Comparator comparator) {
        for (int i = 0; i < keys.size() && !isSatiated(); i++) {
            Key key = keys.get(i);
            if (predicate.isInterestedInKey(key, comparator)) {
                if (limit > 0)
                    limit--;
                matched(key, values.get(i));
            }
        }
    }
    @Override
    public boolean isInterestedInKeysBetween(Key first, Key second, Comparator comparator) {
        return predicate.isInterestedInKeysBetween(first, second, comparator);
    }
    // 判断是否满足条件
    @Override
    public boolean isSatiated() {
        return limit == 0;
    }
    
    /**
     * Subclasses should override.  This method will be called for each key,value pair that matches the predicate.
     * 子类应重写。将为与谓词匹配的每个键、值对调用此方法。
     * @param key
     * @param value
     */
    protected void matched(Key key, Value value) {
    
    }
    
}