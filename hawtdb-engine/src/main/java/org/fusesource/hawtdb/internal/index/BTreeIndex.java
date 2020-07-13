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
package org.fusesource.hawtdb.internal.index;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.fusesource.hawtbuf.codec.Codec;
import org.fusesource.hawtdb.api.*;
import org.fusesource.hawtdb.internal.index.BTreeNode.Data;
import org.fusesource.hawtdb.internal.page.Extent;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayInputStream;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;


/**
 * A variable magnitude b+tree indexes with support for optional
 * simple-prefix optimization.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class BTreeIndex<Key, Value> implements SortedIndex<Key, Value> {

    private final BTreeNode.DataPagedAccessor<Key, Value> DATA_ENCODER_DECODER = new BTreeNode.DataPagedAccessor<Key, Value>(this);

    private final Paged paged;
    private final int page;
    private final Codec<Key> keyCodec;
    private final Codec<Value> valueCodec;
    private final Prefixer<Key> prefixer;
    private final boolean deferredEncoding;
    private final Comparator comparator;

    public BTreeIndex(Paged paged, int page, BTreeIndexFactory<Key, Value> factory) {
        this.paged = paged;
        this.page = page;
        this.keyCodec = factory.getKeyCodec();
        this.valueCodec = factory.getValueCodec();

        // Deferred encoding can only done if the keys and value sizes can be computed.
        this.deferredEncoding = factory.isDeferredEncoding() &&
                ( keyCodec.isEstimatedSizeSupported() || keyCodec.getFixedSize()>=0 ) &&
                ( valueCodec.isEstimatedSizeSupported() || valueCodec.getFixedSize()>=0 );

        this.prefixer = factory.getPrefixer();
        this.comparator = factory.getComparator();
    }
    
    @Override
    public String toString() {
        return "{ page: "+page+", deferredEncoding: "+deferredEncoding+" }";
    }
    
    public void create() {
        // Store the root page..
        BTreeNode<Key, Value> root = new BTreeNode<Key, Value>(null, page);
        storeNode(root); 
    }

    public boolean containsKey(Key key) {
        return root().contains(this, key);
    }

    public Value get(Key key) {
        return root().get(this, key);
    }

    public Value put(Key key, Value value) {
        return root().put(this, key, value);
    }

    public Value putIfAbsent(Key key, Value value) {
        return root().putIfAbsent(this, key, value);
    }

    public Value remove(Key key) {
        return root().remove(this, key);
    }
    
    public int size() {
        return root().size(this);
    }

    public boolean isEmpty() {
        return root().isEmpty(this);
    }

    public void clear() {
        root().clear(this);
    }

    public int getMinLeafDepth() {
        return root().getMinLeafDepth(this, 0);
    }

    public int getMaxLeafDepth() {
        return root().getMaxLeafDepth(this, 0);
    }

    public void printStructure(PrintWriter out) {
        root().printStructure(this, out, "", "");
    }

    public void printStructure(OutputStream out) {
        PrintWriter pw = new PrintWriter(out, false);
        root().printStructure(this, pw, "", "");
        pw.flush();
    }

    public Iterator<Map.Entry<Key, Value>> iterator() {
        return root().iterator(this);
    }

    public Iterator<Map.Entry<Key, Value>> iterator(Predicate<Key> predicate) {
        return root().iterator(this, predicate);
    }

    public Iterator<Map.Entry<Key, Value>> iterator(final Key initialKey) {
        return root().iterator(this, initialKey);
    }

    public void visit(IndexVisitor<Key, Value> visitor) {
        root().visit(this, visitor);
    }

    public Map.Entry<Key, Value> getFirst() {
        return root().getFirst(this);
    }

    public Map.Entry<Key, Value> getLast() {
        return root().getLast(this);
    }

    // /////////////////////////////////////////////////////////////////
    // Internal implementation methods
    // /////////////////////////////////////////////////////////////////
    private BTreeNode<Key, Value> root() {
        return loadNode(null, page);
    }

    // /////////////////////////////////////////////////////////////////
    // Internal methods made accessible to BTreeNode
    // /////////////////////////////////////////////////////////////////
    BTreeNode<Key, Value> createNode(BTreeNode<Key, Value> parent, Data<Key, Value> data) {
        return new BTreeNode<Key, Value>(parent, paged.allocator().alloc(1), data);
    }
    
    BTreeNode<Key, Value> createNode(BTreeNode<Key, Value> parent) {
        return new BTreeNode<Key, Value>(parent, paged.allocator().alloc(1));
    }
    
    @SuppressWarnings("serial")
    static class PageOverflowIOException extends IndexException {
    }
    
    /**
     * 
     * @param node
     * @return false if page overflow occurred
     */
    boolean storeNode(BTreeNode<Key, Value> node) {
        if (deferredEncoding) {
            int size = BTreeNode.estimatedSize(this, node.data);
            size += 9; // The extent header.
            
            if( !node.allowPageOverflow() && size>paged.getPageSize()) {
                return false;
            }

            paged.put(DATA_ENCODER_DECODER, node.getPage(), node.data);
            node.storedInExtent=true;
        } else {
            
            if( node.storedInExtent ) {
                DATA_ENCODER_DECODER.pagesLinked(paged, node.page);
            }
            
            if (node.isLeaf()) {
                List<Integer> pages = DATA_ENCODER_DECODER.store(paged, node.page, node.data);
                if( !node.allowPageOverflow() && pages.size()>1 ) {
                    DATA_ENCODER_DECODER.pagesLinked(paged, node.page);
                    node.storedInExtent=false;
                    return false;
                }
                node.storedInExtent=true;
            } else {
                DataByteArrayOutputStream os = new DataByteArrayOutputStream(paged.getPageSize()) {
                    protected void resize(int newcount) {
                        throw new PageOverflowIOException();
                    };
                };
                try {
                    BTreeNode.write(os, this, node.data);
                    paged.write(node.page, os.toBuffer());
                    node.storedInExtent=false;
                } catch (IOException e) {
                    throw new IndexException("Could not write btree node");
                } catch (PageOverflowIOException e) {
                    return false;
                }
            }
        }
        return true;
    }
    


    BTreeNode<Key, Value> loadNode(BTreeNode<Key, Value> parent, int page) {
        BTreeNode<Key, Value> node = new BTreeNode<Key, Value>(parent, page);
        if( deferredEncoding ) {
            node.data = paged.get(DATA_ENCODER_DECODER, page);
            node.storedInExtent=true;
        } else {
            Buffer buffer = new Buffer(paged.getPageSize());
            paged.read(page, buffer);
            if ( buffer.startsWith(Extent.DEFAULT_MAGIC) ) {
                // Page data was stored in an extent..
                node.data = DATA_ENCODER_DECODER.load(paged, page);
                node.storedInExtent=true;
            } else {
                // It was just in a plain page..
                DataByteArrayInputStream is = new DataByteArrayInputStream(buffer);
                try {
                    node.data = BTreeNode.read(is, this);
                    node.storedInExtent=false;
                } catch (IOException e) {
                    throw new IndexException("Could not read btree node");
                }
            }
        }
        return node;
    }
    
    void free( BTreeNode<Key, Value> node ) {
        if( deferredEncoding ) {
            paged.clear(DATA_ENCODER_DECODER, node.page);
        } else {
            if ( node.storedInExtent ) {
                DATA_ENCODER_DECODER.pagesLinked(paged, node.page);
            }
        }
        paged.free(node.page);
    }

    // /////////////////////////////////////////////////////////////////
    // Property Accessors
    // /////////////////////////////////////////////////////////////////

    public Paged getPaged() {
        return paged;
    }

    public int getIndexLocation() {
        return page;
    }

    public Codec<Key> getKeyMarshaller() {
        return keyCodec;
    }

    public Codec<Value> getValueMarshaller() {
        return valueCodec;
    }

    public Prefixer<Key> getPrefixer() {
        return prefixer;
    }

    public Comparator getComparator() {
        return comparator;
    }

    public void destroy() {
        clear();
        paged.free(page);
    }


}
