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
    
    public boolean isInterestedInKeysBetween(Key first, Key second, Comparator comparator) {
        return predicate.isInterestedInKeysBetween(first, second, comparator);
    }
    
    public boolean isSatiated() {
        return limit == 0;
    }
    
    /**
     * Subclasses should override.  This method will be called for each key,
     * value pair that matches the predicate.
     *
     * @param key
     * @param value
     */
    protected void matched(Key key, Value value) {
    }
    
}