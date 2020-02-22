package com.biubiu.model;

import com.biubiu.engine.JdbcColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;

import java.util.List;

/**
 * @author yule.zhang
 * @date 2020/2/22 18:51
 * @email zhangyule1993@sina.com
 * @description 从数据源信息获取的信息
 */
public class SourceSchema {
    private SchemaTableName fullName;               // 表的全名
    private List<ColumnMetadata> columnMetadataList;// 列的血缘数据
    private List<JdbcColumnHandle> columnHandleList;// 列数据

    public SchemaTableName getFullName() {
        return fullName;
    }

    public void setFullName(SchemaTableName fullName) {
        this.fullName = fullName;
    }

    public List<ColumnMetadata> getColumnMetadataList() {
        return columnMetadataList;
    }

    public void setColumnMetadataList(List<ColumnMetadata> columnMetadataList) {
        this.columnMetadataList = columnMetadataList;
    }

    public List<JdbcColumnHandle> getColumnHandleList() {
        return columnHandleList;
    }

    public void setColumnHandleList(List<JdbcColumnHandle> columnHandleList) {
        this.columnHandleList = columnHandleList;
    }
}
