/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.hawtdb.internal.indexfactory;

import org.fusesource.hawtbuf.codec.Codec;
import org.fusesource.hawtdb.internal.index.BTreeIndex;
import org.fusesource.hawtbuf.codec.ObjectCodec;
import org.fusesource.hawtdb.internal.index.SortedIndex;
import org.fusesource.hawtdb.internal.page.Paged;
import org.fusesource.hawtdb.util.Prefixer;

import java.util.Comparator;

/**
 * This object is used to create variable magnitude b+tree indexes. 
 * 
 * A b+tree can be used for set or map-based indexing. Leaf
 * nodes are linked together for faster iteration of the values.
 * 
 * <br>
 * The variable magnitude attribute means that the b+tree attempts 
 * to store as many values and pointers on one page as is possible.
 * 
 * <br>
 * It will act as a simple-prefix b+tree if a prefixer is configured.
 * 
 * <br>
 * In a simple-prefix b+tree, instead of promoting actual keys to branch pages, when
 * leaves are split, a shortest-possible separator is generated at the pivot.
 * That separator is what is promoted to the parent branch (and continuing up
 * the list). As a result, actual keys and pointers can only be found at the
 * leaf level. This also affords the index the ability to ignore costly merging
 * and redistribution of pages when deletions occur. Deletions only affect leaf
 * pages in this implementation, and so it is entirely possible for a leaf page
 * to be completely empty after all of its keys have been removed.
 *  此对象用于创建可变大小的b+树索引。
 * *b+树可用于基于集合或地图的索引。叶子将节点链接在一起以加快值的迭代。
 * *可变大小属性意味着b+树尝试在一个页面上存储尽可能多的值和指针。
 * *如果配置了前缀器，它将充当一个简单的前缀b+树。
 * *在一个简单的前缀b+树中，当叶被拆分，在轴上生成尽可能短的分隔符。
 * *这个分隔符被提升到父分支（并继续向上）列表）。因此，实际的键和指针只能在叶水平。
 * 这也为指数提供了忽略代价高昂的合并的能力以及在删除发生时重新分配页面。删除只影响叶
 * *在这个实现中，页是完全可能的在它的所有钥匙都被取下后完全空着。
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class BTreeIndexFactory<Key, Value> implements IndexFactory<Key, Value> {

    private Codec<Key> keyCodec = new ObjectCodec<Key>();
    private Codec<Value> valueCodec = new ObjectCodec<Value>();
    private boolean deferredEncoding = true;
    private Prefixer<Key> prefixer;
    private Comparator comparator = null;

    /**
     * Creates a new BTree index on the Paged object.
     */
    public SortedIndex<Key, Value> create(Paged paged) {
        BTreeIndex<Key, Value> index = createInstance(paged, paged.alloc());
        index.create();
        return index;
    }

    @Override
    public String toString() {
        return "{ deferredEncoding: " + deferredEncoding + " }";
    }

    public SortedIndex<Key, Value> open(Paged paged, int indexNumber) {
        return createInstance(paged, indexNumber);
    }

    public SortedIndex<Key, Value> open(Paged paged) {
        return createInstance(paged, 0);
    }

    public SortedIndex<Key, Value> openOrCreate(Paged paged) {
        if (paged.allocator().isAllocated(0)) {
            return createInstance(paged, 0);
        } else {
            BTreeIndex<Key, Value> index = createInstance(paged, paged.alloc());
            index.create();
            return index;
        }
    }

    private BTreeIndex<Key, Value> createInstance(Paged paged, int page) {
        return new BTreeIndex<Key, Value>(paged, page, this);
    }

  
    public Codec<Key> getKeyCodec() {
        return keyCodec;
    }
 
    public void setKeyCodec(Codec<Key> codec) {
        this.keyCodec = codec;
    }

    
    public Codec<Value> getValueCodec() {
        return valueCodec;
    }

   
    public void setValueCodec(Codec<Value> codec) {
        this.valueCodec = codec;
    }
 
    public boolean isDeferredEncoding() {
        return deferredEncoding;
    }

    public void setDeferredEncoding(boolean enable) {
        this.deferredEncoding = enable;
    }

    public Prefixer<Key> getPrefixer() {
        return prefixer;
    }

    public void setPrefixer(Prefixer<Key> prefixer) {
        this.prefixer = prefixer;
    }

    public Comparator getComparator() {
        return comparator;
    }

    
    public void setComparator(Comparator comparator) {
        this.comparator = comparator;
    }

}