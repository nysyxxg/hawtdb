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
package org.fusesource.hawtdb.internal;

import java.util.ArrayList;
import java.util.List;

import org.fusesource.hawtdb.metric.MetricCounter;
import org.fusesource.hawtdb.metric.Period;


public class Benchmarker {
    
    public static abstract class BenchmarkAction<A extends Actor> implements Action<A> {
        public final MetricCounter success = new MetricCounter();
        public final MetricCounter failed = new MetricCounter();
        protected final String name;
        
        public BenchmarkAction(String name) {
            this.name = name;
            success.setName(name + " success");
            failed.setName(name + " failed");
        }
        
        public void init(A actor) {
        }
        
        final public void run(final A actor) throws Exception {
            try {
                execute(actor);
                success.increment();
            } catch (Throwable e) {
                e.printStackTrace();
                failed.increment();
            }
        }
        
        abstract protected void execute(A actor) throws Exception;
        
        public String getName() {
            return name;
        }
        
    }
    
    int samples = 3;
    int period = 1000 * 5;
    String name;
    
    public void benchmark(ArrayList<? extends Actor> actors, ArrayList<? extends MetricCounter> metrics) throws Exception {
        for (Actor actor : actors) {
            actor.start();// 启动 actor 任务执行线程
        }
        try {
            displayRates(metrics);//统计 【在这个运行时间段】中运行效率
        } finally {
            for (Actor actor : actors) {
                actor.stop();// 停止任务线程
            }
            for (Actor actor : actors) {
                actor.waitForStop();
            }
        }
    }
    
    protected void displayRates(List<? extends MetricCounter> metrics) throws InterruptedException {
        System.out.println("Gathering rates for: " + getName());
        for (int i = 0; i < samples; i++) {// 统计效率的样本数量
            Period p = new Period();
            Thread.sleep(period);
            for (MetricCounter metric : metrics) {
                System.out.println("样本-[" + i + "]" + metric.getRateSummary(p));
                metric.reset();
            }
        }
    }
    
    public int getSamples() {
        return samples;
    }
    
    public void setSamples(int samples) {
        this.samples = samples;
    }
    
    public int getPeriod() {
        return period;
    }
    
    public void setPeriod(int period) {
        this.period = period;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
}
