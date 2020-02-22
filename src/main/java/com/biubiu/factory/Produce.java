package com.biubiu.factory;

import com.biubiu.engine.JdbcColumnHandle;
import com.biubiu.engine.Utils;
import com.biubiu.config.SourceConfig;
import com.biubiu.engine.JdbcRecordSet;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordPageSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * @author yule.zhang
 * @date 2020/2/22 20:29
 * @email zhangyule1993@sina.com
 * @description 数据生产者，从数据源拉去数据放入篮子里
 */
public class Produce extends Thread {
    private final static Logger logger = LoggerFactory.getLogger(Produce.class);
    private Connection connection;
    private BlockingQueue<Page> queue;
    private RecordPageSource pageSource;

    public Produce(SourceConfig sourceConfig, List<JdbcColumnHandle> columnHandles, BlockingQueue<Page> queue) throws Exception {
        this.queue = queue;
        connection = Utils.createConnection(sourceConfig.getUrl(), sourceConfig.getUsername(), null);
        pageSource = new RecordPageSource(new JdbcRecordSet(sourceConfig, columnHandles, connection));
    }

    @Override
    public void run() {
        int conNum = 0;
        try {
            Page page = pageSource.getNextPage();
            while (!pageSource.isFinished()) {
                if (page != null) {
                    queue.put(page);
                    conNum += page.getPositionCount();
                    System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " get count: " + conNum);
                }
                page = pageSource.getNextPage();
            }
            if (page != null) {
                queue.put(page);
                conNum += page.getPositionCount();
            }
        } catch (Throwable e) {
            Signal.producerError = true;
            logger.error("", e);
        } finally {
            if (pageSource != null) {
                pageSource.close();
            }
            Utils.close(connection, null, null);
            System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " final get count: " + conNum);
        }
    }
}
