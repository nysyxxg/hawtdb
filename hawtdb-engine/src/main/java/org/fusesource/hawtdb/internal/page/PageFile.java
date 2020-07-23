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
package org.fusesource.hawtdb.internal.page;

import org.fusesource.hawtdb.internal.page.Paged;

import java.nio.ByteBuffer;


/**
 * A page file provides paged access to a physical file.
 *  页面文件提供对物理文件的分页访问。
 */
public interface PageFile extends Paged {

    /**
     * Writes a byte buffer to a page location.
     *
     * @param pageId the location to write to
     * @param buffer the buffer containing the data to write.
     */
    public void write(int pageId, ByteBuffer buffer);

}