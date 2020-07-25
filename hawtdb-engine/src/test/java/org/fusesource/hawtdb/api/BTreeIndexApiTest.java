package org.fusesource.hawtdb.api;

import org.fusesource.hawtdb.internal.indexfactory.BTreeIndexFactory;
import org.fusesource.hawtdb.internal.indexfactory.IndexFactory;


public class BTreeIndexApiTest extends AbstractApiTest {
    
    @Override
    protected IndexFactory<String, String> getIndexFactory() {
        return new BTreeIndexFactory<String, String>();
    }
    
}
