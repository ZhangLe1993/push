package com.biubiu.step;

import com.biubiu.config.SourceConfig;
import com.biubiu.config.TargetConfig;
import com.biubiu.engine.JdbcColumnHandle;
import com.biubiu.engine.Utils;
import com.biubiu.factory.Consumer;
import com.biubiu.factory.Produce;
import com.facebook.presto.spi.Page;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author yule.zhang
 * @date 2020/2/22 20:15
 * @email zhangyule1993@sina.com
 * @description 执行全量推送或增量推送
 */
public class Transfer {

    public void exec(Boolean off, SourceConfig sourceConfig, TargetConfig targetConfig, List<JdbcColumnHandle> columnHandleList, BlockingQueue<Page> queue) throws Exception {
        ExecutorService pool= Executors.newCachedThreadPool();
        try{
            System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " Transfer table called");
            Produce produce = new Produce(sourceConfig, columnHandleList, queue);
            produce.setName("produce");
            Consumer consumer = new Consumer(off, targetConfig, columnHandleList, queue);
            consumer.setName("MysqlProducer");
            produce.start();
            while (produce.isAlive() && queue.isEmpty()) {
                Thread.sleep(100);
            }
            consumer.start();
            produce.join();
            consumer.join();
            System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " Transfer table end");
        } catch(Exception e) {
            throw e;
        } finally {
            pool.shutdownNow();
        }
    }

}
