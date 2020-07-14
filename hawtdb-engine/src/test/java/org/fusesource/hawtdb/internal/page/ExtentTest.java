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

import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class ExtentTest {
    
    private PageFileFactory pageFileFactory;
    private PageFile pageFile;
    
    protected PageFileFactory createPageFileFactory() {
        PageFileFactory rc = new PageFileFactory();
        rc.setMappingSegementSize(rc.getPageSize() * 3);
        rc.setFile(new File("target/test-data/" + getClass().getName() + ".db"));
        return rc;
    }
    
    @Before
    public void setUp() throws Exception {
        pageFileFactory = createPageFileFactory();
        pageFileFactory.getFile().delete();
        pageFileFactory.open();
        pageFile = pageFileFactory.getPageFile();
    }
    
    @After
    public void tearDown() throws Exception {
        pageFileFactory.close();
    }
    
    protected void reload() throws IOException {
        pageFileFactory.close();
        pageFileFactory.open();
        pageFile = pageFileFactory.getPageFile();
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
        System.out.println("page: " + page);
        assertEquals(0, page);
        
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
}
