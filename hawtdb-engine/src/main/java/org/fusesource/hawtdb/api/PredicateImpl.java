/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.hawtdb.api;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Implements commonly used PredicateImpl like AND, OR, <, > etc. etc.
 * 实现常用谓词，如AND、OR、<、>等
 */
final public class PredicateImpl {
    
    /**
     * Implements a logical OR predicate over a list of predicate expressions.
     * 在谓词表达式列表上实现逻辑【或谓词】。
     */
    static class OrPredicate<Key> implements Predicate<Key> {
        private final List<Predicate<Key>> conditions;
        
        public OrPredicate(List<Predicate<Key>> conditions) {
            this.conditions = conditions;
        }
        
        final public boolean isInterestedInKeysBetween(Key first, Key second, Comparator comparator) {
            for (Predicate<Key> condition : conditions) {
                if (condition.isInterestedInKeysBetween(first, second, comparator)) {
                    return true;
                }
            }
            return false;
        }
        
        final public boolean isInterestedInKey(Key key, Comparator comparator) {
            for (Predicate<Key> condition : conditions) {
                if (condition.isInterestedInKey(key, comparator)) {
                    return true;
                }
            }
            return false;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (Predicate<Key> condition : conditions) {
                if (!first) {
                    sb.append(" OR ");
                }
                first = false;
                sb.append("(");
                sb.append(condition);
                sb.append(")");
            }
            return sb.toString();
        }
    }
    
    /**
     * Implements a logical AND predicate over a list of predicate expressions.
     * 在谓词表达式列表上实现逻辑【AND谓词】。
     */
    static class AndPredicate<Key> implements Predicate<Key> {
        private final List<Predicate<Key>> conditions;
        
        public AndPredicate(List<Predicate<Key>> conditions) {
            this.conditions = conditions;
        }
        
        final public boolean isInterestedInKeysBetween(Key first, Key second, Comparator comparator) {
            for (Predicate<Key> condition : conditions) {
                if (!condition.isInterestedInKeysBetween(first, second, comparator)) {
                    return false;
                }
            }
            return true;
        }
        
        final public boolean isInterestedInKey(Key key, Comparator comparator) {
            for (Predicate<Key> condition : conditions) {
                if (!condition.isInterestedInKey(key, comparator)) {
                    return false;
                }
            }
            return true;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (Predicate<Key> condition : conditions) {
                if (!first) {
                    sb.append(" AND ");
                }
                first = false;
                sb.append("(");
                sb.append(condition);
                sb.append(")");
            }
            return sb.toString();
        }
    }
    
    abstract static class ComparingPredicate<Key> implements Predicate<Key> {
        final public int compare(Key key, Key value, Comparator comparator) {
            if (comparator == null) {
                return ((Comparable) key).compareTo(value);
            } else {
                return comparator.compare(key, value);
            }
        }
        
    }
    
    /**
     * Implements a BETWEEN predicate between two key values.
     * It matches inclusive on  the first value and exclusive on the last value.
     * The predicate expression is  equivalent to: <code>(first <= x) AND (x < last)</code>
     * 在两个键值之间实现BETWEEN谓词。
     * 它在第一个值上匹配inclusive，在最后一个值上匹配exclusive。
     * 谓词表达式等价于：<code>（first <= x）和（ x<last ）</code>
     *
     * @param <Key> the class being compared
     */
    @SuppressWarnings({"unchecked"})
    static class BetweenPredicate<Key> extends ComparingPredicate<Key> {
        private final Key first;
        private final Key last;
        
        public BetweenPredicate(Key first, Key last) {
            this.first = first;
            this.last = last;
        }
        
        final public boolean isInterestedInKeysBetween(Key left, Key right, Comparator comparator) {
            return (right == null || compare(right, first, comparator) >= 0)
                    && (left == null || compare(left, last, comparator) < 0);
        }
        
        final public boolean isInterestedInKey(Key key, Comparator comparator) {
            return compare(key, first, comparator) >= 0 && compare(key, last, comparator) < 0;
        }
        
        @Override
        public String toString() {
            return first + " <= key < " + last;
        }
    }
    
    /**
     * Implements a greater than predicate.
     * The predicate expression is equivalent to: <code>x > value</code>
     * 实现大于谓词。
     * 谓词表达式等价于：<code> x>value </code>
     *
     * @param <Key> the class being compared
     */
    @SuppressWarnings({"unchecked"})
    static class GTPredicate<Key> extends ComparingPredicate<Key> {
        final private Key value;
        
        public GTPredicate(Key value) {
            this.value = value;
        }
        
        final public boolean isInterestedInKeysBetween(Key first, Key second, Comparator comparator) {
            return second == null || isInterestedInKey(second, comparator);
        }
        
        final public boolean isInterestedInKey(Key key, Comparator comparator) {
            return compare(key, value, comparator) > 0;
        }
        
        @Override
        public String toString() {
            return "key > " + value;
        }
    }
    
    /**
     * Implements a greater than or equal to predicate.
     * The predicate expression is equivalent to: <code>x >= value</code>
     * 实现大于或等于谓词。
     * 谓词表达式等价于：<code> x>=value </code>
     *
     * @param <Key> the class being compared
     */
    @SuppressWarnings({"unchecked"})
    static class GTEPredicate<Key> extends ComparingPredicate<Key> {
        final private Key value;
        
        public GTEPredicate(Key value) {
            this.value = value;
        }
        
        final public boolean isInterestedInKeysBetween(Key first, Key second, Comparator comparator) {
            return second == null || isInterestedInKey(second, comparator);
        }
        
        final public boolean isInterestedInKey(Key key, Comparator comparator) {
            return compare(key, value, comparator) >= 0;
        }
        
        @Override
        public String toString() {
            return "key >= " + value;
        }
    }
    
    /**
     * Implements a less than predicate.
     * The predicate expression is equivalent to: <code>x < value</code>
     * 实现小于谓词。谓词表达式是相当于：<code> x<value </code>
     *
     * @param <Key> the class being compared
     */
    @SuppressWarnings({"unchecked"})
    static class LTPredicate<Key> extends ComparingPredicate<Key> {
        final private Key value;
        
        public LTPredicate(Key value) {
            this.value = value;
        }
        
        final public boolean isInterestedInKeysBetween(Key first, Key second, Comparator comparator) {
            return first == null || isInterestedInKey(first, comparator);
        }
        
        final public boolean isInterestedInKey(Key key, Comparator comparator) {
            return compare(key, value, comparator) < 0;
        }
        
        @Override
        public String toString() {
            return "key < " + value;
        }
    }
    
    /**
     * Implements a less than or equal to predicate.
     * The predicate expression is equivalent to: <code>x <= value</code>.
     * 实现小于或等于谓词。
     * 谓词表达式等价于：<code>x<=value</code>。
     * @param <Key> the class being compared
     */
    @SuppressWarnings({"unchecked"})
    static class LTEPredicate<Key> extends ComparingPredicate<Key> {
        final private Key value;
        
        public LTEPredicate(Key value) {
            this.value = value;
        }
        
        final public boolean isInterestedInKeysBetween(Key first, Key second, Comparator comparator) {
            return first == null || isInterestedInKey(first, comparator);
        }
        
        final public boolean isInterestedInKey(Key key, Comparator comparator) {
            return compare(key, value, comparator) <= 0;
        }
        
        @Override
        public String toString() {
            return "key <= " + value;
        }
    }
    
    
    /**
     * Implements a predicate that matches all entries.
     * 实现与所有条目匹配的谓词。
     * @param <Key> the class being compared
     */
    final static class AllPredicate<Key> implements Predicate<Key> {
        
        public boolean isInterestedInKeysBetween(Key first, Key second, Comparator comparator) {
            return true;
        }
        
        public boolean isInterestedInKey(Key key, Comparator comparator) {
            return true;
        }
        
        @Override
        public String toString() {
            return "all";
        }
    }
    
    /**
     * Implements a predicate that matches no entries.
     * 实现不匹配任何项的谓词。
     * @param <Key> the class being compared
     */
    final static class NonePredicate<Key> implements Predicate<Key> {
        
        public boolean isInterestedInKeysBetween(Key first, Key second, Comparator comparator) {
            return false;
        }
        
        public boolean isInterestedInKey(Key key, Comparator comparator) {
            return false;
        }
        
        @Override
        public String toString() {
            return "none";
        }
    }
    
    //
    // Helper static methods to help create predicate expressions.
    // 帮助创建谓词表达式的帮助器静态方法。
    //
    public static <Key> Predicate<Key> none() {
        return new NonePredicate<Key>();
    }
    
    public static <Key> Predicate<Key> all() {
        return new AllPredicate<Key>();
    }
    
    public static <Key> Predicate<Key> or(Predicate<Key>... conditions) {
        return new OrPredicate<Key>(Arrays.asList(conditions));
    }
    
    public static <Key> Predicate<Key> or(List<Predicate<Key>> conditions) {
        return new OrPredicate<Key>(conditions);
    }
    
    public static <Key> Predicate<Key> and(Predicate<Key>... conditions) {
        return new AndPredicate<Key>(Arrays.asList(conditions));
    }
    
    public static <Key> Predicate<Key> and(List<Predicate<Key>> conditions) {
        return new AndPredicate<Key>(conditions);
    }
    
    public static <Key> Predicate<Key> gt(Key key) {
        return new GTPredicate<Key>(key);
    }
    
    public static <Key> Predicate<Key> gte(Key key) {
        return new GTEPredicate<Key>(key);
    }
    
    public static <Key> Predicate<Key> lt(Key key) {
        return new LTPredicate<Key>(key);
    }
    
    public static <Key> Predicate<Key> lte(Key key) {
        return new LTEPredicate<Key>(key);
    }
    
    public static <Key> Predicate<Key> lte(Key first, Key last) {
        return new BetweenPredicate<Key>(first, last);
    }
    
    /**
     * Uses a predicates to select the keys that will be visited.
     * 使用谓词选择要访问的键。
     * @param <Key>
     * @param <Value>
     */
    public static <Key, Value> IndexVisitor<Key, Value> visitor(Predicate<Key> predicate) {
        return new PredicateVisitor<Key, Value>(predicate);
    }
    
}