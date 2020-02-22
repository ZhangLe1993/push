package com.biubiu.engine;

import com.alibaba.fastjson.JSONObject;
import com.biubiu.config.SourceConfig;
import com.biubiu.config.TargetConfig;
import com.biubiu.model.SourceSchema;
import com.biubiu.step.Fetch;
import com.biubiu.step.Prepare;
import com.biubiu.step.Transfer;
import com.facebook.presto.spi.Page;
import com.google.common.base.Strings;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.biubiu.engine.Utils.getCurrentTime;

/**
 * @author yule.zhang
 * @date 2020/2/22 18:38
 * @email zhangyule1993@sina.com
 * @description 推送的入口函数
 */
public class Engine {

    public void run(SourceConfig sourceConfig, TargetConfig targetConfig, Boolean off, String queueSize) throws Exception {
        boolean var0 = ValidateUtils.validateColumns(JSONObject.parseObject(JSONObject.toJSON(sourceConfig).toString()), ValidateUtils.SOURCE_FIELD_COLUMNS);
        boolean var1 = ValidateUtils.validateColumns(JSONObject.parseObject(JSONObject.toJSON(targetConfig).toString()), ValidateUtils.TARGET_FIELD_COLUMNS);
        if (!var0 || !var1) {
            StringBuilder sb = new StringBuilder();
            sb.append("Missing arguments!\n");
            sb.append("Required arguments missing!\n");
            sb.append("Required arguments missing!\n");
            sb.append("Please invoce the script with the following arguments: \n");
            sb.append("\n");
            sb.append("\tsource:[url username catalog schema table] target:[host port username password database table] [queuesize]\n");
            sb.append("\n");
            System.out.println(sb);
            throw new RuntimeException(sb.toString());
        }
        Fetch fetch = new Fetch();
        // 封装目标库信息到实体类
        System.out.println(getCurrentTime() + " " + Thread.currentThread().getName() + " migrate table: FROM presto 【" + sourceConfig.getSchema() + "." + sourceConfig.getTable() + "】 TO mysql【" + targetConfig.getDatabase() + "." + targetConfig.getTable() + "】");
        SourceSchema sourceSchema = fetch.getColumns(sourceConfig);
        if (off) {
            // 全量抽取
            System.out.println(getCurrentTime() + " " + Thread.currentThread().getName() + " drop/truncate mysql table...");
            // 清除表
            Prepare prepare = new Prepare();
            prepare.truncateTable(targetConfig);
            System.out.println(getCurrentTime() + " " + Thread.currentThread().getName() + " create mysql table...");
            // 创建表
            prepare.createTableTableInMysql(sourceSchema.getColumnMetadataList(), targetConfig);
        }
        System.out.println(getCurrentTime() + " " + Thread.currentThread().getName() + " Done\n");
        System.out.println(getCurrentTime() + " " + Thread.currentThread().getName() + " Migrating actual data from presto/hive to mysql...");
        BlockingQueue<Page> queue = null;
        if (Strings.isNullOrEmpty(queueSize)) {
            queue = new LinkedBlockingQueue<>(10000);
        } else {
            queue = new LinkedBlockingQueue<>(Integer.parseInt(queueSize));
        }
        Transfer transfer = new Transfer();
        transfer.exec(off, sourceConfig, targetConfig, sourceSchema.getColumnHandleList(), queue);
        System.out.println(getCurrentTime() + " " + Thread.currentThread().getName() + " Done\n");
    }



}
