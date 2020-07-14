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
package org.fusesource.hawtdb.transaction;

/**
 * Snapshot objects are created for transactions so that they can access
 * a consistent point in time view of the page file.
 * The are two parts to a snapshot: the base and the head. The base and head
 * track the range of updates which were not yet performed against the page file
 * when the snapshot was opened.  This range is tracked to ensure the snapshot
 * view remains consistent.  Direct updates to data in that range is now allowed 
 * while the snapshot is open.
 * When a snapshot is opened and closed, reference counters on the all
 * Batch objects between the base and the head get adjusted.
 *
 * 为事务创建快照对象，以便它们可以访问页面文件的时间视图中的一致点。
 * *快照有两个部分:底座和头部。base和head跟踪快照打开时尚未对页面文件执行的更新范围。
 * 跟踪此范围以确保快照视图保持一致。现在允许在快照打开时直接更新该范围内的数据。
 * 当快照被打开和关闭时，引用计数器就会显示在all上调整底座和头部之间的批处理对象。
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final class Snapshot {

    private final HawtTxPageFile parent;
    private final SnapshotTracker tracker;
    private final Batch base;
    private final Batch latest;
    
    public  Snapshot(HawtTxPageFile hawtPageFile, SnapshotTracker tracker, Batch base, Batch latest) {
        parent = hawtPageFile;
        this.tracker = tracker;
        this.base = base;
        this.latest = latest;
    }
    
    public Snapshot open() {
        tracker.snapshots++;
        Batch cur = base;
        while( true ) {
            cur.snapshots++;
            if(cur == latest) {
                break;
            }
            cur = cur.getNext();
            
        }
        return this;
    }
    
    public void close() {
        synchronized(parent.TRANSACTION_MUTEX) {
            tracker.snapshots--;
            Batch cur = base;
            while( true ) {
                cur.snapshots--;
                if(cur == latest) {
                    break;
                }
                cur = cur.getNext();
            }

            if( tracker.snapshots==0 && tracker.parentCommit!=null ) {
                tracker.parentCommit.snapshotTracker = null;
            }
        }
    }

    public SnapshotTracker getTracker() {
        return tracker;
    }
}