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
import java.util.Iterator;
import java.util.Map;

/**
 * Provides Key/Value storage and retrieval. 
 *  提供键/值存储和检索。
 */
public interface Index<Key,Value> {

    /**
     * Frees any extra storage that the index created.
     * 释放索引创建的任何额外存储。
     */
    void destroy();
   
    void clear();

    boolean containsKey(Key key);
    
    Value remove(Key key);

    
    Value put(Key key, Value entry);

    Value putIfAbsent(Key key, Value entry);

    Value get(Key key);
    
    int size();
    
    boolean isEmpty();
 
    int getIndexLocation();

}
