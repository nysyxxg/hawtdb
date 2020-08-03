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

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtdb.exception.IOPagingException;
import org.fusesource.hawtdb.internal.page.accessor.PagedAccessor;
import org.fusesource.hawtdb.internal.page.allocator.Allocator;
import org.fusesource.hawtdb.internal.page.extent.ExtentInputStream;
import org.fusesource.hawtdb.internal.page.extent.ExtentOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PageFileTest {
    
    private PageFileFactory pff;
    private PageFile pageFile;
    
    protected PageFileFactory createConcurrentPageFileFactory() {
        PageFileFactory rc = new PageFileFactory();
        rc.setFile(new File("target/test-data/" + getClass().getName() + ".db"));
        return rc;
    }
    
    @Before
    public void setUp() throws Exception {
        pff = createConcurrentPageFileFactory();
        pff.getFile().delete();
        pff.open();
        pageFile = pff.getPageFile();
    }
    
    @After
    public void tearDown() throws Exception {
        pff.close();
    }
    
    protected void reload() throws IOException {
        pff.close();
        pff.open();
        pageFile = pff.getPageFile();
    }
    
    protected int store(Paged paged, String value) throws IOException {
        int pageId = paged.allocator().alloc(1);
        store(paged, pageId, value);
        return pageId;
    }
    
    public Paged getRawPageFile() {
        return (DBPageFile) pageFile;
    }
    
    protected void store(Paged tx, int pageId, String value) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream os = new DataOutputStream(baos);
            os.writeUTF(value);
            os.close();
            tx.write(pageId, new Buffer(baos.toByteArray()));
        } catch (IOException e) {
            throw new IOPagingException(e);
        }
    }
    
    protected String load(Paged paged, int pageId) {
        try {
            Buffer buffer = new Buffer(pff.getPageSize());
            paged.read(pageId, buffer);
            ByteArrayInputStream bais = new ByteArrayInputStream(buffer.data, buffer.offset, buffer.length);
            DataInputStream is = new DataInputStream(bais);
            return is.readUTF();
        } catch (IOException e) {
            throw new IOPagingException(e);
        }
    }
    
    private final class StringPagedAccessor implements PagedAccessor<String> {
        public String load(Paged paged, int pageId) {
            return PageFileTest.this.load(paged, pageId);
        }
        
        public List<Integer> store(Paged paged, int pageId, String value) {
            PageFileTest.this.store(paged, pageId, value);
            return Collections.emptyList();
        }
        
        public List<Integer> pagesLinked(Paged paged, int pageId) {
            return Collections.emptyList();
        }
    }
    
    @Test
    public void cacheAPI() throws IOException, ClassNotFoundException {
        
        StringPagedAccessor ENCODER = new StringPagedAccessor();
        Allocator allocator = pageFile.allocator();
        int pageNum = allocator.alloc(1);
        pageFile.put(ENCODER, pageNum, "Hello");
        pageNum = allocator.alloc(1);
        pageFile.put(ENCODER, pageNum, "World");
        
        reload();
        
        assertEquals("Hello", pageFile.get(ENCODER, 0));
        assertEquals("World", pageFile.get(ENCODER, 1));
        
    }
    
    @Test
    public void cacheAPIConflictingUpdateFails() throws IOException, ClassNotFoundException {
        
        StringPagedAccessor ENCODER = new StringPagedAccessor();
        Allocator allocator = pageFile.allocator();
        int pageNum = allocator.alloc(1);
        pageFile.put(ENCODER, pageNum, "Hello");
        
        pageNum = allocator.alloc(1);
        pageFile.put(ENCODER, pageNum, "World");
        
        // Tx1 still does not see tx2's change...
        assertEquals("World", pageFile.get(ENCODER, pageNum));
        
        pageFile.put(ENCODER, 0, "Change 1");// 在事务1中修改数据
        
        // 在事务2中看不到事务1中修改的数据
        assertEquals("Change 1", pageFile.get(ENCODER, 0));  // We don't see tx1's change...
        
        pageFile.put(ENCODER, 0, "Change 2");
        assertEquals("Change 2", pageFile.get(ENCODER, 0)); // We can see our own change..
        
        
    }
    
    @Test
    public void conflictingUpdateFails() throws IOException, ClassNotFoundException {
        
        int pageId = store(pageFile, "Hello");
        assertEquals(0, pageId);
        
        pageId = store(pageFile, "World");
        assertEquals(1, pageId);
        
        store(pageFile, 0, "Change 1");
        
        // Now commit a change to page 0
        String data = load(pageFile, 0);
        assertEquals("Change 1", data); // We don't see tx1's change...
        
        store(pageFile, 0, "Change 2");
        data = load(pageFile, 0);
        assertEquals("Change 2", data); // We can see our own change..
        
        assertEquals("Change 2", load(pageFile, 0));
        
    }
    
    @Test
    public void pagesNotDirectlyUpdated() throws IOException, ClassNotFoundException {
        
        assertEquals(0, store(pageFile, "Hello"));
        assertEquals(1, store(pageFile, "World"));
        
        // It should be on the page file already..
        Paged paged = getRawPageFile();
        String data = load(paged, 0);
        assertEquals("Hello", data);
        assertEquals("World", load(getRawPageFile(), 1));
        
        // Apply the updates.
        pageFile.flush();
        
        // Should still be there..
        assertEquals("Hello", load(getRawPageFile(), 0));
        assertEquals("World", load(getRawPageFile(), 1));
        
        // Update the existing pages..
        store(pageFile, 0, "Good");
        store(pageFile, 1, "Bye");
        
        // A subsequent transaction can read the update.
        assertEquals("Good", load(pageFile, 0));
        assertEquals("Bye", load(pageFile, 1));
        
        // But the pages are should not be updated until the transaction gets
        // applied.
        String data1 = load(getRawPageFile(), 0);
        assertEquals("Good", data1);
        String data2 = load(getRawPageFile(), 1);
        assertEquals("Bye", data2);
        
        // Apply them
        pageFile.flush();
        
        // We should see them now.
        assertEquals("Good", load(getRawPageFile(), 0));
        assertEquals("Bye", load(getRawPageFile(), 1));
    }
    
    @Test
    public void crudOperations() throws IOException, ClassNotFoundException {
        int COUNT = 10;
        
        ArrayList<Integer> allocations = new ArrayList<Integer>();
        HashSet<String> expected = new HashSet<String>();// 存放期望的值
        
        for (int i = 0; i < COUNT; i++) {
            int page = pageFile.allocator().alloc(1);
            // Since the file is empty.. allocations should occur sequentially
            assertEquals(i, page);
            allocations.add(page);
            String value = "page:" + i;
            store(pageFile, page, value);// 存储数据
            expected.add(value);
        }
        
        // Reload it.. .
        reload();
        
        // Iterate it to make sure they are still there..
        HashSet<String> actual = new HashSet<String>();// 存储实际的值
        for (Integer page : allocations) {
            actual.add((String) load(pageFile, page));
        }
        // 比较两个集合是否相等
        assertEquals(expected, actual);
        
        // Remove the odd records..
        for (int i = 0; i < COUNT; i++) {
            if (i % 2 == 0) {
                break;
            }
            String t = "page:" + i;
            expected.remove(t);
        }
        for (Integer page : new ArrayList<Integer>(allocations)) {
            String t = (String) load(pageFile, page);// 加载查询数据
            if (!expected.contains(t)) {
                pageFile.free(page);//删除数据
                allocations.remove(page);
            }
        }
        
        // Reload it...
        reload();
        
        // Iterate it to make sure the even records are still there..
        actual.clear();
        for (Integer page : allocations) {
            String t = (String) load(pageFile, page);
            actual.add(t);
        }
        assertEquals(expected, actual);
        
        // Update the records...
        HashSet<String> t = expected;
        expected = new HashSet<String>();
        for (String s : t) {
            expected.add(s + ":updated");
        }
        for (Integer page : allocations) {
            String value = (String) load(pageFile, page);
            store(pageFile, page, value + ":updated");
        }
        
        // Reload it...
        reload();
        
        // Iterate it to make sure the updated records are still there..
        actual.clear();
        for (Integer page : allocations) {
            String value = (String) load(pageFile, page);
            actual.add(value);
        }
        assertEquals(expected, actual);
        
    }
    
    @Test
    public void testExtentStreams() throws IOException {
        ExtentOutputStream eos = new ExtentOutputStream(pageFile);
        DataOutputStream os = new DataOutputStream(eos);
        for (int i = 0; i < 10000; i++) {
            os.writeUTF("Test string:" + i);
        }
        os.close();
        int page = eos.getPage();
        
        // Reload the page file.
        reload();
        
        ExtentInputStream eis = new ExtentInputStream(pageFile, page);
        DataInputStream is = new DataInputStream(eis);
        for (int i = 0; i < 10000; i++) {
            assertEquals("Test string:" + i, is.readUTF());
        }
        assertEquals(-1, is.read());
        is.close();
    }
    
    @Test
    public void testAddRollback() throws IOException, ClassNotFoundException {
        
        for (int i = 5; i < 10; i++) {
            String t = "DATA:" + i;
            int page1 = store(pageFile, t);
            
            String data = (String) load(pageFile, page1);
            System.out.println(page1 + "--->data=" + data);
            
            int page2 = store(pageFile, t);
            String data2 = (String) load(pageFile, page2);
            System.out.println(page2 + "--->data2=" + data2);
        }
        System.out.println("----------------------------");
        for (int i = 0; i < 10; i++) {
            int page = pageFile.allocator().alloc(1);
            String data = (String) load(pageFile, page);
            System.out.println(page + "---->" + data);
        }
    }
}
