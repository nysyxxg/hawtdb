package org.fusesource.hawtdb.api;

import java.io.File;
import java.io.IOException;

import org.fusesource.hawtdb.internal.index.Index;
import org.fusesource.hawtdb.internal.indexfactory.IndexFactory;
import org.fusesource.hawtdb.internal.page.PageFile;
import org.fusesource.hawtdb.internal.page.PageFileFactory;
import org.junit.Test;

import static org.junit.Assert.*;

public abstract class AbstractApiTest {
    
    @Test
    public void testManuallyCreateAndOpenIndex() throws IOException {
        File tmpFile = File.createTempFile("hawtdb", "test");
        
        IndexFactory<String, String> indexFactory = getIndexFactory();// 得到索引工厂
        
        // create test  :
        PageFileFactory pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();
        PageFile page = pageFactory.getPageFile();
        Index<String, String> index = indexFactory.create(page);// 创建索引
        index.put("1", "1");
        pageFactory.close();
        
        // open test  :
        pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();
        page = pageFactory.getPageFile();
        Index<String, String> index2 = indexFactory.open(page);// 打开已经存在的索引对象
        assertEquals("1", index2.get("1"));
        pageFactory.close();
        
        //删除临时测试文件
        //tmpFile.delete();
    }
    
    @Test
    public void testOpenOrCreateIndex() throws IOException {
        File tmpFile = File.createTempFile("hawtdb", "test");
        
        IndexFactory<String, String> indexFactory = getIndexFactory();
        
        // Create:
        PageFileFactory pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();
        PageFile page = pageFactory.getPageFile();
        Index<String, String> index = indexFactory.openOrCreate(page);
        assertFalse(index.containsKey("1"));
        index.put("1", "1");
        pageFactory.close();
        
        // Open:
        pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();
        
        page = pageFactory.getPageFile();
        Index<String, String> index2 = indexFactory.openOrCreate(page);
        assertEquals("1", index2.get("1"));
        pageFactory.close();
        // 删除文件
        // tmpFile.delete();
    }
    
    protected abstract IndexFactory<String, String> getIndexFactory();
}