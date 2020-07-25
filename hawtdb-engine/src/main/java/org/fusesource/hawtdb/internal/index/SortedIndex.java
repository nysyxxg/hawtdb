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
package org.fusesource.hawtdb.internal.index;

import org.fusesource.hawtdb.api.IndexVisitor;
import org.fusesource.hawtdb.api.Predicate;
import org.fusesource.hawtdb.internal.index.Index;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Provides Key/Value storage and retrieval.
 * 提供键/值存储和检索。
 */
public interface SortedIndex<Key, Value> extends Index<Key, Value>, Iterable<Map.Entry<Key, Value>> {
    
    public Iterator<Map.Entry<Key, Value>> iterator();
    
    public Iterator<Map.Entry<Key, Value>> iterator(Predicate<Key> predicate);
    
    public Iterator<Map.Entry<Key, Value>> iterator(Key initialKey);
    
    /**
     * Traverses the visitor over the stored entries in this index.
     *  The visitor can control which keys and values are visited.
     *  遍历访问者在该索引中存储的条目。
     * 访问者可以控制访问哪些键和值。
     * @param visitor
     */
    public void visit(IndexVisitor<Key, Value> visitor);
    
    /**
     *索引中的第一个键/值对，如果为空，则为null。
     * @return the first key/value pair in the index or null if empty.
     */
    public Map.Entry<Key, Value> getFirst();
    
    /**
     * 索引中的最后一个键/值对，如果为空，则为null。
     * @return the last key/value pair in the index or null if empty.
     */
    public Map.Entry<Key, Value> getLast();
    
}
