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
package org.fusesource.hawtdb.internal.page;

import java.nio.ByteBuffer;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtdb.exception.IOPagingException;
import org.fusesource.hawtdb.internal.page.accessor.PagedAccessor;
import org.fusesource.hawtdb.internal.page.allocator.Allocator;

/**
 * 分页内存 接口
 * Implemented by objects which provides block io access to pages on file.
 * 由提供对文件页的块io访问的对象实现。
 */
public interface Paged {
    
    /**
     * 提供对分配/取消分配页的访问对象。
     *
     * @return An object which provides access to allocate/deallocate pages.
     */
    Allocator allocator();
    
    /**
     * does the same as allocator().alloc(1)
     *
     * @return a newly allocated page location.  新分配的页面位置。
     */
    int alloc();
    
    /**
     * does the same as allocator().free(page, 1)
     * 执行与allocator（）相同的操作
     */
    void free(int page);
    
    
    /**
     * Provides direct access to the memory associated with a page.
     * Specifying the correct mode argument is especially critical and
     * the Paged resources is being accessed in a transaction context
     * so that the transaction can  maintain snapshot isolation.
     *  提供对与页关联的内存的直接访问。
     * 指定正确的模式参数尤其重要
     * 正在事务上下文中访问分页资源
     * 这样事务可以保持快照隔离。
     * @param mode   how will the buffer be used.
     * @param pageId the starting page of the buffer
     * @param count  the number of pages to include in the buffer.
     * @return
     * @throws IOPagingException
     */
    public ByteBuffer slice(SliceType mode, int pageId, int count) throws IOPagingException;
    
    public void unslice(ByteBuffer buffer);
    
    /**
     * Copies the contents of a page into the buffer space.
     * The buffer offset will be updated to reflect the amount of data copied into the buffer.
     *
     * @param pageId
     * @param buffer
     */
    public void read(int pageId, Buffer buffer);
    
    /**
     * Copies the buffer into the page. The buffer offset will be updated to reflect the amount of data copied to the page.
     *  将缓冲区复制到页面中。缓冲区偏移量将被更新以反映复制到页的数据量。
     * @param pageId
     * @param buffer
     */
    public void write(int pageId, Buffer buffer);
    
    /**
     * 在一页上读或写的最大字节数。
     * @return the maximum number of bytes that be read or written to a page.
     */
    int getPageSize();
    
    /**
     * @return the number of pages that would be required to store the specified
     * number of bytes
     */
    int pages(int length);
    
    /**
     *
     */
    void flush();
    
    
    /**
     * Gets an object previously put at the given page. The returned object SHOULD NEVER be mutated.
     * 获取以前放在给定页上的对象。返回的对象永远不应改变。
     * @param page
     * @return
     */
    <T> T get(PagedAccessor<T> pagedAccessor, int page);
    
    /**
     * Put an object at a given page.  The supplied object SHOULD NEVER be mutated once it has been stored.
     * 在给定的页面上放置一个对象。所提供的对象一旦被存储，就不应被更改。
     * @param page
     * @param value
     */
    <T> void put(PagedAccessor<T> pagedAccessor, int page, T value);
    
    /**
     * 释放与存储在给定页上的值关联的任何页（如果有）。
     * Frees any pages associated with the value stored at the given page if any.
     * Does not free the page supplied. 无法释放提供的页面。
     *
     * @param page
     * @return
     */
    <T> void clear(PagedAccessor<T> pagedAccessor, int page);
    
}
