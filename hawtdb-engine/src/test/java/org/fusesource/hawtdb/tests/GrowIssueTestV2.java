package org.fusesource.hawtdb.tests;
import org.fusesource.hawtbuf.codec.type.StringCodec;
import org.fusesource.hawtdb.api.*;
import org.fusesource.hawtdb.transaction.Transaction;
import org.fusesource.hawtdb.transaction.TxPageFile;
import org.fusesource.hawtdb.transaction.TxPageFileFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
public class GrowIssueTestV2 {
    
    
    private final int size = 1024;
    private TxPageFileFactory factory;
    private TxPageFile file;
    
    @Before
    public void setUp() throws Exception {
        File f = new File("target/data/hawtdb.dat");
        f.delete();
        
        factory = new TxPageFileFactory();
        factory.setFile(f);
        
        factory.setMappingSegementSize(16 * 1024);
        // set 1mb as max file
        factory.setMaxFileSize(1024 * 1024);
        
        factory.open();
        file = factory.getTxPageFile();
    }
    
    @After
    public void tearDown() throws Exception {
        factory.close();
    }
    
    @Test
    public void testGrowIssue() throws Exception {
        
        // a 1kb string for testing
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < 1024; i++) {
            sb.append("X");
        }
        
        BTreeIndexFactory<String, String> indexFactory = new BTreeIndexFactory<String, String>();
        indexFactory.setKeyCodec(StringCodec.INSTANCE);
        indexFactory.setValueCodec(StringCodec.INSTANCE);
        
        Transaction tx = file.tx();
        SortedIndex<String, String> index = indexFactory.create(tx);
        tx.commit();
        tx.flush();
        
        // we update using the same key, which means we should be able to do this within the file size limit
        for (int i = 0; i < size; i++) {
            tx = file.tx();
            index = indexFactory.open(tx);
            index.put("foo", i + "-" + sb);
            tx.commit();
            tx.flush();
        }
        
        tx = file.tx();
        index = indexFactory.open(tx);
        System.out.println(index.get("foo"));
        tx.commit();
        
    }
}
