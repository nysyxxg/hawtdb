/**
 * Copyright (C) 2009, Progress Software Corporation and/or its
 * subsidiaries or affiliates.  All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.hawtdb.tests;

import org.fusesource.hawtbuf.codec.type.StringCodec;
import org.fusesource.hawtdb.internal.index.SortedIndex;
import org.fusesource.hawtdb.internal.indexfactory.BTreeIndexFactory;
import org.fusesource.hawtdb.internal.page.transaction.Transaction;
import org.fusesource.hawtdb.internal.page.transaction.TxPageFile;
import org.fusesource.hawtdb.internal.page.transaction.TxPageFileFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class GrowIssueTest {
    
    
    private final int size = 1024;
    private TxPageFileFactory pageFileFactory;
    private TxPageFile file;
    
    @Before
    public void setUp() throws Exception {
        File f = new File("target/data/hawtdb2.dat");
        f.delete();
    
        pageFileFactory = new TxPageFileFactory();
        pageFileFactory.setFile(f);
    
        pageFileFactory.setMappingSegementSize(16 * 1024);
        // set 1mb as max file
        pageFileFactory.setMaxFileSize(1024 * 1024);
    
        pageFileFactory.open();
        file = pageFileFactory.getTxPageFile();
    }
    
    @After
    public void tearDown() throws Exception {
        pageFileFactory.close();
    }
    
    @Test
    public void testGrowIssue() throws Exception {
        
        // a 1kb string for testing
        StringBuilder data = new StringBuilder(size);
        for (int i = 0; i < 1024; i++) {
            data.append("X");
        }
        
        BTreeIndexFactory<String, String> indexFactory = new BTreeIndexFactory<String, String>();
        indexFactory.setKeyCodec(StringCodec.INSTANCE);
        indexFactory.setValueCodec(StringCodec.INSTANCE);
//
        Transaction tx = file.tx();
        SortedIndex<String, String> index = indexFactory.create(tx);
        tx.commit();
        tx.flush();
        
        // we update using the same key, which means we should be able to do this within the file size limit
        // 我们使用相同的键进行更新，这意味着我们应该能够在文件大小限制内完成此操作
        long time1 = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            tx = file.tx();
            index = indexFactory.open(tx);
            index.put("foo", i + "-" + data);
            tx.commit();
            tx.flush();
        }
        long time2 = System.currentTimeMillis();
        System.out.println("花费时间：" + (time2 - time1));
    
       // testGetData();
    }
    
    public void testGetData() throws Exception {
        long time2 = System.currentTimeMillis();
        BTreeIndexFactory<String, String> indexFactory = new BTreeIndexFactory<String, String>();
        indexFactory.setKeyCodec(StringCodec.INSTANCE);
        indexFactory.setValueCodec(StringCodec.INSTANCE);
        
        Transaction tx2 = file.tx();
        SortedIndex<String, String> index2 = indexFactory.open(tx2);
        System.out.println(index2.get("foo"));
        long time3 = System.currentTimeMillis();
        System.out.println("花费时间：" + (time3 - time2));
        tx2.commit();
        
    }
}
