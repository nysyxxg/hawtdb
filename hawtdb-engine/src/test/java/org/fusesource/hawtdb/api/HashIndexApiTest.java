package org.fusesource.hawtdb.api;

import org.fusesource.hawtdb.internal.indexfactory.HashIndexFactory;
import org.fusesource.hawtdb.internal.indexfactory.IndexFactory;

/**
 * @author Sergio Bossa
 */
public class HashIndexApiTest extends AbstractApiTest {
    
    @Override
    protected IndexFactory<String, String> getIndexFactory() {
        return new HashIndexFactory<String, String>();
    }
    
}
