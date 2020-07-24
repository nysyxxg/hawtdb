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
package org.fusesource.hawtdb.util;

import java.util.Comparator;

public class Comparators {

    public static final Comparator<Long> LONG_COMPARATOR = new Comparator<Long>() {
        public int compare(Long o1, Long o2) {
            return o1.compareTo(o2);
        }
    };
    
    /**
     * 如果参数字符串等于此字符串，则返回 0 值；
     * 如果此字符串小于字符串参数，则返回一个小于 0 的值；
     * 如果此字符串大于字符串参数，则返回一个大于 0 的值。
     */
    static class MyComparator implements Comparator {
        @Override
        public int compare(Object o1, Object o2) {
            String param1 = (String)o1;
            String param2 = (String)o2;
            return  param1.compareTo(param2);
        }
    }
}
