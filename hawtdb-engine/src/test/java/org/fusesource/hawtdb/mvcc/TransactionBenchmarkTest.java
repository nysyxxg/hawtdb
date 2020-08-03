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
package org.fusesource.hawtdb.mvcc;

import java.util.Random;

import org.fusesource.hawtdb.mvcc.thread.Action;
import org.fusesource.hawtdb.internal.page.transaction.TxPageFile;
import org.fusesource.hawtdb.internal.page.transaction.TxPageFileFactory;
import org.fusesource.hawtdb.internal.page.transaction.Transaction;
import org.fusesource.hawtdb.mvcc.thread.Benchmarker.BenchmarkAction;
import org.fusesource.hawtdb.mvcc.TransactionBenchmarker.Callback;
import org.fusesource.hawtbuf.Buffer;
import org.junit.Test;

//事务 衡量尺度 测试
public class TransactionBenchmarkTest {
    
    private static byte[] THE_DATA = new byte[1024 * 3];
    private volatile boolean bl = false;
    static {
        for(int i=0;i<THE_DATA.length;i++){
            THE_DATA[i] = (byte) i;
        }
    }
    
    
    private static class RandomTxActor extends TransactionActor<RandomTxActor> {
        public Random random;
        
        public void setName(String name) {
            super.setName(name);
            this.random = new Random(name.hashCode());
        }
    }
    
    private TransactionBenchmarker<RandomTxActor> benchmark = new TransactionBenchmarker<RandomTxActor>() {
        protected RandomTxActor createActor(TxPageFile pageFile, Action<RandomTxActor> action, int i) {
            return new RandomTxActor();
        }
    };
    
    private static class MyBenchmarkAction extends BenchmarkAction<RandomTxActor> {
        public MyBenchmarkAction(String name) {
            super(name);
        }
        
        @Override
        protected void execute(RandomTxActor actor) throws Exception {
            Transaction tx = actor.tx();
            int page = tx.allocator().alloc(1);
            System.out.println(Thread.currentThread().getName() + " page=" + page);
            tx.write(page, new Buffer(THE_DATA));
            tx.commit();
        }
    }
    
    @Test
    public void myAppend() throws Exception {
        benchmark.benchmark(1, new MyBenchmarkAction("myAppend"));
    }
    
    @Test
    public void append() throws Exception {
        benchmark.benchmark(1, new BenchmarkAction<RandomTxActor>("append") {
            @Override
            protected void execute(RandomTxActor actor) throws InterruptedException {
                Transaction tx = actor.tx();
                int pageId = tx.allocator().alloc(1);
                System.out.println("pageId=" + pageId + "--->" +  tx.getPageSize());
//                Thread.sleep(1);
                tx.write(pageId, new Buffer(THE_DATA));
                tx.commit();
            }
        });
    }
    
    @Test
    public void aupdate() throws Exception {
        long t1 = System.currentTimeMillis();
        final int INITIAL_PAGE_COUNT = 1024 * 100;
        preallocate(INITIAL_PAGE_COUNT);// 先执行这个保存操作
        Thread.sleep(1 * 1000);
        
        try {
            benchmark.benchmark(1, new BenchmarkAction<RandomTxActor>("update") {
                @Override
                protected void execute(RandomTxActor actor) {
                    int pageId = actor.random.nextInt(INITIAL_PAGE_COUNT);
                    //  System.out.println(Thread.currentThread().getName() + " 随机更新操作page=" + page);
//                    try {
//                        Thread.sleep(1 * 1);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    //                Transaction tx = actor.tx();
                    //                tx.write(page, new Buffer(THE_DATA));
                    //                tx.commit();// 提交保存操作
                    
                    //开启事务，进行更新，当并发进行更新的时候，出现问题
                    actor.tx().write(pageId, new Buffer(THE_DATA));
                    actor.tx().commit();
                }
            });
        } catch (Exception e) {
            // e.printStackTrace();
        } finally {
        }
        long t2 = System.currentTimeMillis();
        System.out.println("插入和更新数据-整个任务花费时间：" + (t2 - t1));
    }
    
    
    @Test
    public void read() throws Exception {
        final int INITIAL_PAGE_COUNT = 1024 * 100;
        preallocate(INITIAL_PAGE_COUNT);// 先进行插入
        benchmark.benchmark(1, new BenchmarkAction<RandomTxActor>("read") {
            @Override
            protected void execute(RandomTxActor actor) {
                // 后进行读取
                int pageId = actor.random.nextInt(INITIAL_PAGE_COUNT);
                actor.tx().read(pageId, new Buffer(THE_DATA));
                actor.tx().commit();
            }
        });
    }
    
    
    private void preallocate(final int INITIAL_PAGE_COUNT) {
        
        benchmark.setSetup(new Callback() {
            public void run(TxPageFileFactory pff) throws Exception {
                long t1 = System.currentTimeMillis();
                Transaction tx = pff.getTxPageFile().tx();
                for (int i = 0; i < INITIAL_PAGE_COUNT; i++) {
                    int pageId = tx.allocator().alloc(1);
                    //System.out.println("插入操作：" + pageId);
                    tx.write(pageId, new Buffer(THE_DATA));
                }
                tx.commit();
                long t2 = System.currentTimeMillis();
                System.out.println("插入数据花费时间：" + (t2 - t1));
            }
        });
        
        benchmark.setTearDown(new Callback() {
            public void run(TxPageFileFactory pff) throws Exception {
                System.out.println("任务结束...进行关闭！！！");
                pff.close();
            }
        });
    }
}
