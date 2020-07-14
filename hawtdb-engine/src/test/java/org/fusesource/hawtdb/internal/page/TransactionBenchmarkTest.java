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

import java.util.Random;

import org.fusesource.hawtdb.transaction.TxPageFile;
import org.fusesource.hawtdb.transaction.TxPageFileFactory;
import org.fusesource.hawtdb.transaction.Transaction;
import org.fusesource.hawtdb.internal.Action;
import org.fusesource.hawtdb.internal.Benchmarker.BenchmarkAction;
import org.fusesource.hawtdb.internal.page.TransactionBenchmarker.Callback;
import org.fusesource.hawtbuf.Buffer;
import org.junit.Test;

//事务 衡量尺度 测试
public class TransactionBenchmarkTest {
    
    private static byte[] THE_DATA = new byte[1024 * 3];
    private volatile boolean bl = false;
    
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
            System.out.println("page=" + page);
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
        benchmark.benchmark(2, new BenchmarkAction<RandomTxActor>("append") {
            @Override
            protected void execute(RandomTxActor actor) {
                Transaction tx = actor.tx();
                int page = tx.allocator().alloc(1);
                System.out.println("page=" + page);
                tx.write(page, new Buffer(THE_DATA));
                tx.commit();
            }
        });
    }
    
    @Test
    public void aupdate() throws Exception {
        final int INITIAL_PAGE_COUNT = 1024 * 100;
        preallocate(INITIAL_PAGE_COUNT);// 下面执行完成之后，开始回调
        System.out.println(bl);
//        while(!bl){
//             Thread.sleep(3 * 1000);
//             System.out.println(bl);
//        }
        benchmark.benchmark(1, new BenchmarkAction<RandomTxActor>("update") {
            @Override
            protected void execute(RandomTxActor actor) {
                int page = actor.random.nextInt(INITIAL_PAGE_COUNT);
                System.out.println("page=" + page);
//                Transaction tx = actor.tx();
//                tx.write(page, new Buffer(THE_DATA));
//                tx.commit();// 提交保存操作
                
                //开启事务，进行更新
                actor.tx().write(page, new Buffer(THE_DATA));
                actor.tx().commit();
            }
        });
    }
    
    
    @Test
    public void read() throws Exception {
        final int INITIAL_PAGE_COUNT = 1024 * 100;
        preallocate(INITIAL_PAGE_COUNT);
        benchmark.benchmark(1, new BenchmarkAction<RandomTxActor>("read") {
            @Override
            protected void execute(RandomTxActor actor) {
                int page = actor.random.nextInt(INITIAL_PAGE_COUNT);
                actor.tx().read(page, new Buffer(THE_DATA));
                actor.tx().commit();
            }
        });
    }
    
    
    private void preallocate(final int INITIAL_PAGE_COUNT) {
        benchmark.setSetup(new Callback() {
            public void run(TxPageFileFactory pff) throws Exception {
                Transaction tx = pff.getTxPageFile().tx();
                for (int i = 0; i < INITIAL_PAGE_COUNT; i++) {
                    int page = tx.allocator().alloc(1);
                    System.out.println("回调函数：" + page);
                    tx.write(page, new Buffer(THE_DATA));
                }
                tx.commit();
            }
        });
    }
}
