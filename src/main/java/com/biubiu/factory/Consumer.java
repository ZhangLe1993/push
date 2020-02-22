package com.biubiu.factory;

import com.biubiu.config.TargetConfig;
import com.biubiu.engine.JdbcColumnHandle;
import com.biubiu.engine.JdbcPageSink;
import com.biubiu.engine.Utils;
import com.facebook.presto.spi.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * @author yule.zhang
 * @date 2020/2/22 20:59
 * @email zhangyule1993@sina.com
 * @description 消费者将数据推送到MYSQL
 */
public class Consumer extends Thread {
    private final static Logger logger = LoggerFactory.getLogger(Consumer.class);
    private BlockingQueue<Page> queue;
    private Connection connection;
    private JdbcPageSink jdbcPageSink;

    public Consumer(Boolean off, TargetConfig targetConfig, List<JdbcColumnHandle> columnHandles, BlockingQueue<Page> queue) throws SQLException {
        this.queue = queue;
        connection = Utils.createConnection(targetConfig.getUrl(), targetConfig.getUsername(), targetConfig.getPassword());
        jdbcPageSink = new JdbcPageSink(off, targetConfig, columnHandles, connection);
    }

    @Override
    public void run() {
        int proNum = 0;
        try {
            Page page = null;
            while (!Signal.producerError && !queue.isEmpty()) {
                page = queue.take();
                if (page != null) {
                    jdbcPageSink.appendPage(page);
                    proNum += page.getPositionCount();
                    System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " producer count: " + proNum);
                }
            }
        } catch (Throwable e) {
            logger.error("", e);
        } finally {
            if (jdbcPageSink != null) {
                jdbcPageSink.finish();
            }
            Utils.close(connection, null, null);
            System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " final producer count: " + proNum);
        }
    }

    public void shutdown() {
    }
}
